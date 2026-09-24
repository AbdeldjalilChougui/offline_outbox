import 'dart:async';
import 'dart:math';

import 'coalesce.dart';
import 'conflict.dart';
import 'entry.dart';
import 'events.dart';
import 'retry_policy.dart';
import 'send_result.dart';
import 'store.dart';
import 'uuid.dart';

/// Delivers one entry to the server. Send [OutboxEntry.idempotencyKey] with
/// the request so the server can deduplicate retries.
///
/// Throwing counts as a [SendResult.retryableFailure].
typedef Sender = Future<SendResult> Function(OutboxEntry entry);

/// Returns the current time. Inject one to control time in tests.
typedef Clock = DateTime Function();

/// Returns a new unique id, used for entry ids and idempotency keys.
typedef IdGenerator = String Function();

/// A durable queue of mutations that are delivered later, in order, with
/// retries.
///
/// Call [start] once, then [enqueue] mutations. They are delivered by
/// [flush], which runs automatically on enqueue (see [autoFlush]), when the
/// [connectivity] stream reports the device back online, on the optional
/// [flushInterval] timer and when a scheduled retry becomes due.
///
/// Delivery rules:
///
/// * Entries in the same ordering group are sent one at a time in FIFO
///   order. Groups (and ungrouped entries) are sent in parallel, up to
///   [maxConcurrency] at once.
/// * A group waits while its oldest entry is backing off or needs
///   attention; other groups keep going.
/// * Each entry is persisted as in flight before it is sent. Entries found
///   in flight by [start] were interrupted and are sent again with the same
///   idempotency key.
/// * Refused entries are never discarded automatically.
///
/// The outbox assumes it is the only writer of its store while running.
class Outbox {
  /// Creates an outbox. Nothing happens until [start] is called.
  ///
  /// * [retryPolicy] defaults to an [ExponentialBackoff] of 1 second doubling
  ///   up to 5 minutes, with 20% jitter.
  /// * [defaultConflictStrategy] applies to operations without an entry in
  ///   [conflictStrategies].
  /// * [coalescers] maps an incoming operation type to its [Coalescer].
  /// * [maxConflictResolutions] bounds how many conflicts one entry may
  ///   resolve automatically before it needs attention.
  /// * [connectivity] reports online (`true`) and offline (`false`); the
  ///   state before its first event is [initiallyOnline].
  /// * [clock], [random] and [idGenerator] make the outbox deterministic in
  ///   tests. By default ids are UUID v4 from [random], or from a secure
  ///   source when [random] is not given.
  Outbox({
    required OutboxStore store,
    required Sender sender,
    RetryPolicy? retryPolicy,
    this.defaultConflictStrategy = ConflictStrategy.manual,
    Map<String, ConflictStrategy> conflictStrategies = const {},
    Map<String, Coalescer> coalescers = const {},
    this.maxConcurrency = 4,
    this.maxConflictResolutions = 3,
    Stream<bool>? connectivity,
    bool initiallyOnline = true,
    this.flushInterval,
    this.autoFlush = true,
    this.scheduleRetries = true,
    this.keepCompleted = false,
    Clock? clock,
    Random? random,
    IdGenerator? idGenerator,
  })  : _store = store,
        _sender = sender,
        retryPolicy = retryPolicy ?? ExponentialBackoff(),
        conflictStrategies = Map.unmodifiable(conflictStrategies),
        coalescers = Map.unmodifiable(coalescers),
        _connectivity = connectivity,
        _online = initiallyOnline,
        _clock = clock ?? DateTime.now,
        _random = random ?? Random(),
        _newId = idGenerator ??
            (random == null ? generateUuidV4 : () => generateUuidV4(random)) {
    if (maxConcurrency < 1) {
      throw ArgumentError.value(
          maxConcurrency, 'maxConcurrency', 'must be >= 1');
    }
    if (maxConflictResolutions < 0) {
      throw ArgumentError.value(
          maxConflictResolutions, 'maxConflictResolutions', 'must be >= 0');
    }
  }

  final OutboxStore _store;
  final Sender _sender;
  final Stream<bool>? _connectivity;
  final Clock _clock;
  final Random _random;
  final IdGenerator _newId;

  /// Decides retry delays and when to stop retrying.
  final RetryPolicy retryPolicy;

  /// Conflict strategy for operations not listed in [conflictStrategies].
  final ConflictStrategy defaultConflictStrategy;

  /// Conflict strategy per operation type.
  final Map<String, ConflictStrategy> conflictStrategies;

  /// Coalescer per incoming operation type.
  final Map<String, Coalescer> coalescers;

  /// Maximum number of entries in flight at once.
  final int maxConcurrency;

  /// Conflicts one entry may resolve automatically before it needs
  /// attention. Guards against two writers overwriting each other forever.
  final int maxConflictResolutions;

  /// When set, [flush] also runs on this interval while online.
  final Duration? flushInterval;

  /// Whether [start], [enqueue], [retry], [retryAllNeedingAttention] and
  /// [discard] trigger a flush by themselves.
  final bool autoFlush;

  /// Whether the outbox sets a timer to flush when the next backed-off
  /// entry becomes due.
  final bool scheduleRetries;

  /// Whether succeeded and cancelled entries stay in the store (until
  /// [clearCompleted]) instead of being deleted right away.
  final bool keepCompleted;

  final Map<String, OutboxEntry> _entries = {};
  int _nextSequence = 0;
  bool _started = false;
  bool _disposed = false;
  bool _online;
  Future<FlushResult>? _flushing;
  bool _flushAgain = false;
  Future<void> _writeTail = Future.value();
  StreamSubscription<bool>? _connectivitySub;
  Timer? _periodicTimer;
  Timer? _retryTimer;

  final StreamController<OutboxEvent> _events =
      StreamController<OutboxEvent>.broadcast();
  final StreamController<OutboxCounts> _countsController =
      StreamController<OutboxCounts>.broadcast();
  OutboxCounts _counts = const OutboxCounts();

  // ---------------------------------------------------------------- queries

  /// Everything that happens to entries. Broadcast. A failure of an
  /// automatic flush (for example a failing store) arrives as an
  /// [OutboxFlushFailed] event rather than a stream error, so listeners
  /// without `onError` never crash the app.
  Stream<OutboxEvent> get events => _events.stream;

  /// Live entry counts. Each listener gets the current value first, then
  /// every change.
  Stream<OutboxCounts> get counts => Stream.multi((controller) {
        controller.add(_counts);
        final sub = _countsController.stream
            .listen(controller.add, onDone: controller.close);
        controller.onCancel = sub.cancel;
      });

  /// The current entry counts.
  OutboxCounts get currentCounts => _counts;

  /// Snapshot of every entry the outbox holds, in queue order.
  List<OutboxEntry> get entries => sortedBySequence(_entries.values);

  /// The entry with [id], or `null`.
  OutboxEntry? entry(String id) => _entries[id];

  /// Whether the outbox currently considers the device online.
  bool get isOnline => _online;

  // -------------------------------------------------------------- lifecycle

  /// Loads the queue, recovers entries interrupted in flight, and starts
  /// listening to the triggers. Must be called exactly once.
  Future<void> start() async {
    if (_started) throw StateError('Outbox.start was already called');
    if (_disposed) throw StateError('Outbox was disposed');
    _started = true;

    final recovered = <OutboxEntry>[];
    for (final stored in await _store.loadAll()) {
      var e = stored;
      if (e.state == OutboxState.inFlight) {
        e = e.copyWith(state: OutboxState.pending, clearNextAttemptAt: true);
        recovered.add(e);
      }
      _entries[e.id] = e;
      _nextSequence = max(_nextSequence, e.sequence + 1);
    }
    if (recovered.isNotEmpty) await _store.commit(upserts: recovered);
    _updateCounts();
    for (final e in recovered) {
      _emit(OutboxRecovered(e));
    }

    _connectivitySub = _connectivity?.listen(_onConnectivity);
    final interval = flushInterval;
    if (interval != null) {
      _periodicTimer = Timer.periodic(interval, (_) => _trigger());
    }
    if (autoFlush) {
      _trigger();
    } else {
      _armRetryTimer();
    }
  }

  /// Stops all triggers, waits for sends in progress to be recorded, and
  /// closes the streams. The outbox cannot be used afterwards.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    _periodicTimer?.cancel();
    _retryTimer?.cancel();
    await _connectivitySub?.cancel();
    final running = _flushing;
    if (running != null) await running.then((_) {}, onError: (Object _) {});
    await _writeTail;
    await _events.close();
    await _countsController.close();
  }

  // --------------------------------------------------------------- commands

  /// Queues a mutation and returns the entry that now represents it.
  ///
  /// If a [Coalescer] is registered for [operation] and the most recent
  /// entry for [resourceKey] in the same [group] has never been attempted,
  /// the two may be combined: the result is then the updated queued entry,
  /// or the incoming entry in the cancelled state when both cancel out.
  ///
  /// [payload] must be JSON-encodable. [idempotencyKey] defaults to a new id.
  Future<OutboxEntry> enqueue({
    required String operation,
    required String resourceKey,
    Map<String, Object?> payload = const {},
    String? group,
    String? idempotencyKey,
  }) async {
    _checkOpen();
    final now = _clock();
    final incoming = OutboxEntry(
      id: _newId(),
      idempotencyKey: idempotencyKey ?? _newId(),
      sequence: _nextSequence++,
      operation: operation,
      resourceKey: resourceKey,
      payload: payload,
      createdAt: now,
      group: group,
    );

    final coalescer = coalescers[operation];
    final queued = coalescer == null ? null : _latestFor(resourceKey, group);
    if (coalescer != null &&
        queued != null &&
        queued.state == OutboxState.pending &&
        queued.attempts == 0) {
      switch (coalescer(queued, incoming)) {
        case CoalesceMerge(:final payload, :final operation):
          final merged = queued.copyWith(
            payload: payload,
            operation: operation,
            updatedAt: now,
          );
          await _apply(upserts: [merged]);
          _emit(OutboxCoalesced(merged, absorbed: incoming));
          _afterChange();
          return merged;
        case CoalesceCancelBoth():
          final cancelled = queued.copyWith(state: OutboxState.cancelled);
          final dropped = incoming.copyWith(state: OutboxState.cancelled);
          await _finish(cancelled);
          _emit(OutboxCancelled(cancelled, reason: CancelReason.coalesced));
          _emit(OutboxCancelled(dropped, reason: CancelReason.coalesced));
          _afterChange();
          return dropped;
        case CoalesceKeepBoth():
          break;
      }
    }

    await _apply(upserts: [incoming]);
    _emit(OutboxEnqueued(incoming));
    _afterChange();
    return incoming;
  }

  /// Sends every entry that is due, respecting groups and [maxConcurrency].
  ///
  /// Does nothing while offline. If a flush is already running, returns it
  /// and makes it take another pass for anything that changed meanwhile.
  /// Completes with an error if the store fails.
  Future<FlushResult> flush() {
    _checkOpen();
    final running = _flushing;
    if (running != null) {
      _flushAgain = true;
      return running;
    }
    final f = _runFlush().whenComplete(() {
      _flushing = null;
      _armRetryTimer();
    });
    _flushing = f;
    return f;
  }

  /// Puts a pending or needs-attention entry back in the queue, due now.
  ///
  /// Pass [payload] to send a corrected payload, or [resolution] to record
  /// how a person resolved a conflict (for example
  /// [ConflictResolution.clientWins] for "keep mine"). Either one also
  /// rotates the idempotency key, since the previous request was refused.
  ///
  /// Returns `false` if the entry is unknown, in flight or finished. The
  /// attempt count is kept, so an entry that hit the retry policy's
  /// `maxAttempts` gets exactly one more try.
  Future<bool> retry(
    String id, {
    Map<String, Object?>? payload,
    ConflictResolution? resolution,
  }) async {
    _checkOpen();
    final e = _entries[id];
    if (e == null || !_retryable(e)) return false;
    final changed = payload != null || resolution != null;
    final next = e.copyWith(
      state: OutboxState.pending,
      clearNextAttemptAt: true,
      payload: payload,
      conflictResolution: resolution,
      idempotencyKey: changed ? _newId() : null,
      updatedAt: changed ? _clock() : null,
    );
    await _apply(upserts: [next]);
    _emit(OutboxRequeued(next));
    _afterChange();
    return true;
  }

  /// Requeues every needs-attention entry, as [retry] without arguments
  /// would. Returns how many were requeued.
  Future<int> retryAllNeedingAttention() async {
    _checkOpen();
    final next = [
      for (final e in entries)
        if (e.state == OutboxState.needsAttention)
          e.copyWith(state: OutboxState.pending, clearNextAttemptAt: true),
    ];
    if (next.isEmpty) return 0;
    await _apply(upserts: next);
    for (final e in next) {
      _emit(OutboxRequeued(e));
    }
    _afterChange();
    return next.length;
  }

  /// Cancels an entry that is pending or needs attention, releasing its
  /// group. Returns `false` if it is unknown, in flight or finished.
  Future<bool> discard(String id) async {
    _checkOpen();
    final e = _entries[id];
    if (e == null || !_retryable(e)) return false;
    final cancelled = e.copyWith(state: OutboxState.cancelled);
    await _finish(cancelled);
    _emit(OutboxCancelled(cancelled, reason: CancelReason.discarded));
    _afterChange();
    return true;
  }

  /// Deletes succeeded and cancelled entries kept by [keepCompleted].
  /// Returns how many were deleted.
  Future<int> clearCompleted() async {
    _checkOpen();
    final ids = [
      for (final e in _entries.values)
        if (e.isTerminal) e.id,
    ];
    if (ids.isNotEmpty) await _apply(removals: ids);
    return ids.length;
  }

  // -------------------------------------------------------------- internals

  bool _retryable(OutboxEntry e) =>
      e.state == OutboxState.pending || e.state == OutboxState.needsAttention;

  bool get _canSend => _online && !_disposed;

  void _checkOpen() {
    if (!_started) throw StateError('Call Outbox.start first');
    if (_disposed) throw StateError('Outbox was disposed');
  }

  void _emit(OutboxEvent event) {
    if (!_events.isClosed) _events.add(event);
  }

  void _afterChange() {
    if (_flushing != null) _flushAgain = true;
    if (autoFlush) _trigger();
  }

  /// Flushes in the background if possible, reporting errors on [events].
  void _trigger() {
    if (!_started || _disposed || !_online) return;
    flush().then((_) {}, onError: (Object e, StackTrace st) {
      _emit(OutboxFlushFailed(e, st));
    });
  }

  void _onConnectivity(bool online) {
    final wasOnline = _online;
    _online = online;
    if (!online) {
      _retryTimer?.cancel();
    } else if (!wasOnline) {
      _trigger();
    }
  }

  void _armRetryTimer() {
    _retryTimer?.cancel();
    _retryTimer = null;
    if (!scheduleRetries || _disposed || !_online) return;
    final now = _clock();
    DateTime? earliest;
    for (final head in _heads().values) {
      final at = head.nextAttemptAt;
      if (head.state != OutboxState.pending || at == null || !at.isAfter(now)) {
        continue;
      }
      if (earliest == null || at.isBefore(earliest)) earliest = at;
    }
    if (earliest == null) return;
    _retryTimer = Timer(earliest.difference(now), _trigger);
  }

  /// Updates memory at once, then persists. Writes are serialized in call
  /// order. If the write fails, memory is rolled back (unless the entry was
  /// changed again meanwhile) and the error is rethrown.
  Future<void> _apply({
    List<OutboxEntry> upserts = const [],
    List<String> removals = const [],
  }) {
    final previous = <String, OutboxEntry?>{};
    final written = <String, OutboxEntry?>{};
    for (final e in upserts) {
      previous.putIfAbsent(e.id, () => _entries[e.id]);
      _entries[e.id] = e;
      written[e.id] = e;
    }
    for (final id in removals) {
      previous.putIfAbsent(id, () => _entries[id]);
      _entries.remove(id);
      written[id] = null;
    }
    _updateCounts();

    return _persist(upserts, removals).catchError((Object e, StackTrace st) {
      for (final MapEntry(key: id, value: before) in previous.entries) {
        if (!identical(_entries[id], written[id])) continue;
        if (before == null) {
          _entries.remove(id);
        } else {
          _entries[id] = before;
        }
      }
      _updateCounts();
      Error.throwWithStackTrace(e, st);
    });
  }

  /// Writes to the store strictly one commit at a time, in call order, and
  /// resumes callers in that same order.
  Future<void> _persist(
    List<OutboxEntry> upserts,
    List<String> removals,
  ) async {
    final previous = _writeTail;
    final done = Completer<void>();
    _writeTail = done.future;
    try {
      await previous;
      await _store.commit(upserts: upserts, removals: removals);
    } finally {
      done.complete();
    }
  }

  /// Records a terminal entry: kept or deleted per [keepCompleted].
  Future<void> _finish(OutboxEntry e) =>
      keepCompleted ? _apply(upserts: [e]) : _apply(removals: [e.id]);

  void _updateCounts() {
    var pending = 0, inFlight = 0, attention = 0;
    for (final e in _entries.values) {
      switch (e.state) {
        case OutboxState.pending:
          pending++;
        case OutboxState.inFlight:
          inFlight++;
        case OutboxState.needsAttention:
          attention++;
        case OutboxState.succeeded || OutboxState.cancelled:
          break;
      }
    }
    final next = OutboxCounts(
      pending: pending,
      inFlight: inFlight,
      needsAttention: attention,
    );
    if (next == _counts) return;
    _counts = next;
    if (!_countsController.isClosed) _countsController.add(next);
  }

  OutboxEntry? _latestFor(String resourceKey, String? group) {
    OutboxEntry? latest;
    for (final e in _entries.values) {
      if (e.isTerminal || e.resourceKey != resourceKey || e.group != group) {
        continue;
      }
      if (latest == null || e.sequence > latest.sequence) latest = e;
    }
    return latest;
  }

  static String _laneOf(OutboxEntry e) {
    final group = e.group;
    return group == null ? 'e:${e.id}' : 'g:$group';
  }

  /// The oldest live entry of every lane, keyed by lane, in queue order.
  Map<String, OutboxEntry> _heads() {
    final heads = <String, OutboxEntry>{};
    for (final e in entries) {
      if (!e.isTerminal) heads.putIfAbsent(_laneOf(e), () => e);
    }
    return heads;
  }

  OutboxEntry? _headOf(String lane) {
    OutboxEntry? head;
    for (final e in _entries.values) {
      if (e.isTerminal || _laneOf(e) != lane) continue;
      if (head == null || e.sequence < head.sequence) head = e;
    }
    return head;
  }

  bool _isDue(OutboxEntry e, DateTime now) {
    if (e.state != OutboxState.pending) return false;
    final at = e.nextAttemptAt;
    return at == null || !at.isAfter(now);
  }

  Future<FlushResult> _runFlush() async {
    final tally = _Tally();
    while (_canSend) {
      _flushAgain = false;
      await _pass(tally);
      if (!_flushAgain) break;
    }
    return tally.result(offline: !_online);
  }

  Future<void> _pass(_Tally tally) async {
    final now = _clock();
    final lanes = [
      for (final MapEntry(key: lane, value: head) in _heads().entries)
        if (_isDue(head, now)) lane,
    ];
    var next = 0;
    Future<void> worker() async {
      while (next < lanes.length && _canSend) {
        await _drainLane(lanes[next++], tally);
      }
    }

    await Future.wait([
      for (var i = 0; i < min(maxConcurrency, lanes.length); i++) worker(),
    ]);
  }

  Future<void> _drainLane(String lane, _Tally tally) async {
    while (_canSend) {
      final head = _headOf(lane);
      if (head == null || !_isDue(head, _clock())) return;
      if (!await _send(head, tally)) return;
    }
  }

  /// Sends one entry; returns whether its lane may continue.
  Future<bool> _send(OutboxEntry e, _Tally tally) async {
    final sending = e.copyWith(
      state: OutboxState.inFlight,
      attempts: e.attempts + 1,
    );
    // Persisted before the request: if the process dies during the send,
    // the next start knows the entry's fate is unknown and resends it.
    await _apply(upserts: [sending]);
    tally.attempted++;
    _emit(OutboxSending(sending));

    SendResult result;
    try {
      result = await _sender(sending);
    } on Object catch (error) {
      result = SendResult.retryableFailure(error: '$error');
    }

    switch (result) {
      case SendSuccess():
        final done = sending.copyWith(
          state: OutboxState.succeeded,
          clearLastError: true,
          clearNextAttemptAt: true,
        );
        await _finish(done);
        tally.succeeded++;
        _emit(OutboxSucceeded(done));
        return true;
      case RetryableFailure(:final retryAfter, :final error):
        final message = error ?? 'retryable failure';
        final limit = retryPolicy.maxAttempts;
        if (limit != null && sending.attempts >= limit) {
          await _park(
            sending,
            'gave up after ${sending.attempts} attempts: $message',
            tally,
          );
          return false;
        }
        var delay =
            retryAfter ?? retryPolicy.delayFor(sending.attempts, _random);
        if (delay.isNegative) delay = Duration.zero;
        final next = sending.copyWith(
          state: OutboxState.pending,
          nextAttemptAt: _clock().add(delay),
          lastError: message,
        );
        await _apply(upserts: [next]);
        tally.retryScheduled++;
        _emit(OutboxRetryScheduled(next, error: message));
        return false;
      case PermanentFailure(:final reason):
        await _park(sending, reason, tally);
        return false;
      case SendConflict(:final serverState, :final serverModifiedAt):
        tally.conflicts++;
        return _resolveConflict(sending, serverState, serverModifiedAt, tally);
    }
  }

  Future<void> _park(OutboxEntry e, String reason, _Tally tally) async {
    final parked = e.copyWith(
      state: OutboxState.needsAttention,
      lastError: reason,
      clearNextAttemptAt: true,
    );
    await _apply(upserts: [parked]);
    tally.needsAttention++;
    _emit(OutboxNeedsAttention(parked, reason: reason));
  }

  Future<bool> _resolveConflict(
    OutboxEntry sending,
    Object? serverState,
    DateTime? serverModifiedAt,
    _Tally tally,
  ) async {
    final e = sending.copyWith(
      conflictCount: sending.conflictCount + 1,
      serverState: serverState,
      clearServerState: serverState == null,
    );

    Future<bool> manual(String reason) async {
      _emit(OutboxConflictResolved(
        e,
        outcome: ConflictOutcome.manual,
        serverState: serverState,
      ));
      await _park(e, reason, tally);
      return false;
    }

    if (e.conflictCount > maxConflictResolutions) {
      return manual('conflict: still conflicting after '
          '${e.conflictCount - 1} automatic resolutions');
    }

    final strategy = conflictStrategies[e.operation] ?? defaultConflictStrategy;
    ConflictOutcome outcome;
    Map<String, Object?>? merged;
    switch (strategy) {
      case ServerWinsStrategy():
        outcome = ConflictOutcome.serverWins;
      case ClientWinsStrategy():
        outcome = ConflictOutcome.clientWins;
      case LastWriteWinsStrategy():
        if (serverModifiedAt == null) {
          return manual('conflict: no server timestamp for last-write-wins');
        }
        outcome = e.updatedAt.isAfter(serverModifiedAt)
            ? ConflictOutcome.clientWins
            : ConflictOutcome.serverWins;
      case MergeStrategy(:final merger):
        try {
          merged = await merger(e, serverState);
        } on Object catch (error) {
          return manual('conflict: merge failed: $error');
        }
        if (merged == null) return manual('conflict: merge declined');
        outcome = ConflictOutcome.merged;
      case ManualStrategy():
        return manual('conflict');
    }

    if (outcome == ConflictOutcome.serverWins) {
      final cancelled = e.copyWith(
        state: OutboxState.cancelled,
        lastError: 'conflict: server version kept',
      );
      await _finish(cancelled);
      _emit(OutboxConflictResolved(
        cancelled,
        outcome: outcome,
        serverState: serverState,
      ));
      _emit(OutboxCancelled(cancelled, reason: CancelReason.serverWins));
      return true;
    }

    // Resend at once. The refused request was not applied, so a fresh key
    // cannot cause a duplicate, and it avoids a server replaying the cached
    // conflict response for the old key.
    final resend = e.copyWith(
      state: OutboxState.pending,
      clearNextAttemptAt: true,
      idempotencyKey: _newId(),
      payload: merged,
      updatedAt: merged == null ? null : _clock(),
      conflictResolution: outcome == ConflictOutcome.merged
          ? ConflictResolution.merged
          : ConflictResolution.clientWins,
    );
    await _apply(upserts: [resend]);
    _emit(OutboxConflictResolved(
      resend,
      outcome: outcome,
      serverState: serverState,
    ));
    return true;
  }
}

class _Tally {
  int attempted = 0;
  int succeeded = 0;
  int retryScheduled = 0;
  int needsAttention = 0;
  int conflicts = 0;

  FlushResult result({required bool offline}) => FlushResult(
        attempted: attempted,
        succeeded: succeeded,
        retryScheduled: retryScheduled,
        needsAttention: needsAttention,
        conflicts: conflicts,
        offline: offline,
      );
}
