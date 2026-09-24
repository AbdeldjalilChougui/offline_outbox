import 'entry.dart';

/// Why an entry was cancelled.
enum CancelReason {
  /// `Outbox.discard` was called.
  discarded,

  /// Coalescing made it unnecessary.
  coalesced,

  /// A conflict was resolved in the server's favour.
  serverWins,
}

/// How a conflict was handled.
enum ConflictOutcome {
  /// The local change was dropped.
  serverWins,

  /// The local change is being resent to overwrite the server.
  clientWins,

  /// A merged payload is being resent.
  merged,

  /// The entry was left for a person (needs attention).
  manual,
}

/// Something that happened in the outbox. Listen with `Outbox.events`.
sealed class OutboxEvent {
  const OutboxEvent();
}

/// A flush that the outbox started by itself (on enqueue, reconnect, timer)
/// failed, usually because the store could not be written. Flushes you call
/// yourself report errors through their returned future instead.
final class OutboxFlushFailed extends OutboxEvent {
  /// Creates the event.
  const OutboxFlushFailed(this.error, this.stackTrace);

  /// The error.
  final Object error;

  /// Where it was thrown.
  final StackTrace stackTrace;

  @override
  String toString() => 'OutboxFlushFailed($error)';
}

/// Something that happened to one entry.
sealed class OutboxEntryEvent extends OutboxEvent {
  const OutboxEntryEvent(this.entry);

  /// The entry after the change.
  final OutboxEntry entry;

  @override
  String toString() => '$runtimeType(${entry.id})';
}

/// A new entry was queued.
final class OutboxEnqueued extends OutboxEntryEvent {
  /// Creates the event.
  const OutboxEnqueued(super.entry);
}

/// An incoming change was merged into a queued [entry].
final class OutboxCoalesced extends OutboxEntryEvent {
  /// Creates the event.
  const OutboxCoalesced(super.entry, {required this.absorbed});

  /// The incoming entry that was merged in and not stored.
  final OutboxEntry absorbed;
}

/// An entry found in flight at startup was put back to pending.
final class OutboxRecovered extends OutboxEntryEvent {
  /// Creates the event.
  const OutboxRecovered(super.entry);
}

/// An entry was handed to the sender.
final class OutboxSending extends OutboxEntryEvent {
  /// Creates the event.
  const OutboxSending(super.entry);
}

/// An entry was delivered.
final class OutboxSucceeded extends OutboxEntryEvent {
  /// Creates the event.
  const OutboxSucceeded(super.entry);
}

/// A send failed transiently; see [OutboxEntry.nextAttemptAt].
final class OutboxRetryScheduled extends OutboxEntryEvent {
  /// Creates the event.
  const OutboxRetryScheduled(super.entry, {required this.error});

  /// The failure description.
  final String error;
}

/// An entry was refused and now needs a person or the app to decide.
final class OutboxNeedsAttention extends OutboxEntryEvent {
  /// Creates the event.
  const OutboxNeedsAttention(super.entry, {required this.reason});

  /// Why it needs attention.
  final String reason;
}

/// The server reported a conflict and the configured strategy ran.
final class OutboxConflictResolved extends OutboxEntryEvent {
  /// Creates the event.
  const OutboxConflictResolved(
    super.entry, {
    required this.outcome,
    required this.serverState,
  });

  /// What the strategy decided.
  final ConflictOutcome outcome;

  /// The server's version reported with the conflict.
  final Object? serverState;
}

/// An entry was cancelled and will never be sent.
final class OutboxCancelled extends OutboxEntryEvent {
  /// Creates the event.
  const OutboxCancelled(super.entry, {required this.reason});

  /// Why it was cancelled.
  final CancelReason reason;
}

/// An entry was put back in the queue by `Outbox.retry` or
/// `Outbox.retryAllNeedingAttention`.
final class OutboxRequeued extends OutboxEntryEvent {
  /// Creates the event.
  const OutboxRequeued(super.entry);
}

/// Number of live entries in each state, for UI badges.
class OutboxCounts {
  /// Creates a counts snapshot.
  const OutboxCounts({
    this.pending = 0,
    this.inFlight = 0,
    this.needsAttention = 0,
  });

  /// Entries waiting to be sent.
  final int pending;

  /// Entries currently being sent.
  final int inFlight;

  /// Entries that need a person or the app to decide.
  final int needsAttention;

  /// Entries not yet delivered: `pending + inFlight + needsAttention`.
  int get outstanding => pending + inFlight + needsAttention;

  @override
  bool operator ==(Object other) =>
      other is OutboxCounts &&
      other.pending == pending &&
      other.inFlight == inFlight &&
      other.needsAttention == needsAttention;

  @override
  int get hashCode => Object.hash(pending, inFlight, needsAttention);

  @override
  String toString() => 'OutboxCounts(pending: $pending, inFlight: $inFlight, '
      'needsAttention: $needsAttention)';
}

/// Summary of one `Outbox.flush` call.
class FlushResult {
  /// Creates a summary.
  const FlushResult({
    this.attempted = 0,
    this.succeeded = 0,
    this.retryScheduled = 0,
    this.needsAttention = 0,
    this.conflicts = 0,
    this.offline = false,
  });

  /// Sends started.
  final int attempted;

  /// Entries delivered.
  final int succeeded;

  /// Transient failures scheduled for a later retry.
  final int retryScheduled;

  /// Entries moved to needs-attention.
  final int needsAttention;

  /// Conflict responses received (whatever the strategy did with them).
  final int conflicts;

  /// Whether the outbox was offline when the flush ended.
  final bool offline;

  /// Failed attempts: `retryScheduled + needsAttention`.
  int get failed => retryScheduled + needsAttention;

  @override
  String toString() => 'FlushResult(attempted: $attempted, succeeded: '
      '$succeeded, retryScheduled: $retryScheduled, needsAttention: '
      '$needsAttention, conflicts: $conflicts, offline: $offline)';
}
