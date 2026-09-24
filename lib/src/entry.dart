/// Lifecycle state of an [OutboxEntry].
enum OutboxState {
  /// Waiting to be sent, possibly not before [OutboxEntry.nextAttemptAt].
  pending,

  /// Handed to the sender and not answered yet. An entry found in this state
  /// when the outbox starts was interrupted (for example the app was killed)
  /// and goes back to [pending]; the idempotency key makes resending safe.
  inFlight,

  /// Delivered and acknowledged by the server.
  succeeded,

  /// Refused (permanent failure, unresolved conflict or too many attempts).
  /// A person or the app must decide; the outbox never discards it by itself.
  needsAttention,

  /// Will never be sent: discarded, coalesced away, or superseded by the
  /// server's version in a conflict.
  cancelled;

  /// Whether the entry is finished and will not be sent again.
  bool get isTerminal => this == succeeded || this == cancelled;
}

/// How the outbox resolved a conflict before resending an entry.
///
/// Senders use it to decide how to resend, for example by dropping an
/// `If-Match` precondition when the client won.
enum ConflictResolution {
  /// The local change should overwrite the server's version.
  clientWins,

  /// The payload was merged with the server's version.
  merged,
}

T _read<T>(Map<String, Object?> json, String key) {
  final value = json[key];
  if (value is T) return value;
  throw FormatException('OutboxEntry: "$key" has an invalid value: $value');
}

DateTime? _date(Object? v) {
  if (v == null) return null;
  if (v is String) return DateTime.parse(v);
  throw FormatException('OutboxEntry: invalid date $v');
}

bool _deepEquals(Object? a, Object? b) {
  if (a is Map && b is Map) {
    if (a.length != b.length) return false;
    for (final key in a.keys) {
      if (!b.containsKey(key) || !_deepEquals(a[key], b[key])) return false;
    }
    return true;
  }
  if (a is List && b is List) {
    if (a.length != b.length) return false;
    for (var i = 0; i < a.length; i++) {
      if (!_deepEquals(a[i], b[i])) return false;
    }
    return true;
  }
  return a == b;
}

/// One queued mutation. Immutable; the outbox replaces entries with
/// [copyWith] as they move through [OutboxState].
class OutboxEntry {
  /// Creates an entry. Apps normally call `Outbox.enqueue` instead.
  OutboxEntry({
    required this.id,
    required this.idempotencyKey,
    required this.sequence,
    required this.operation,
    required this.resourceKey,
    required Map<String, Object?> payload,
    required this.createdAt,
    DateTime? updatedAt,
    this.attempts = 0,
    this.nextAttemptAt,
    this.lastError,
    this.state = OutboxState.pending,
    this.group,
    this.conflictCount = 0,
    this.conflictResolution,
    this.serverState,
  })  : payload = Map.unmodifiable(payload),
        updatedAt = updatedAt ?? createdAt;

  /// Restores an entry written by [toJson].
  ///
  /// Throws a [FormatException] when a field is missing or has the wrong type.
  factory OutboxEntry.fromJson(Map<String, Object?> json) {
    try {
      final resolution = json['conflictResolution'] as String?;
      return OutboxEntry(
        id: _read<String>(json, 'id'),
        idempotencyKey: _read<String>(json, 'idempotencyKey'),
        sequence: _read<int>(json, 'sequence'),
        operation: _read<String>(json, 'operation'),
        resourceKey: _read<String>(json, 'resourceKey'),
        payload: _read<Map<String, Object?>>(json, 'payload'),
        createdAt: _date(json['createdAt'])!,
        updatedAt: _date(json['updatedAt']),
        attempts: _read<int>(json, 'attempts'),
        nextAttemptAt: _date(json['nextAttemptAt']),
        lastError: json['lastError'] as String?,
        state: OutboxState.values.byName(_read<String>(json, 'state')),
        group: json['group'] as String?,
        conflictCount: (json['conflictCount'] as int?) ?? 0,
        conflictResolution: resolution == null
            ? null
            : ConflictResolution.values.byName(resolution),
        serverState: json['serverState'],
      );
    } on FormatException {
      rethrow;
    } on Object catch (e) {
      throw FormatException('OutboxEntry: malformed JSON ($e)');
    }
  }

  /// Local, unique identifier of the entry.
  final String id;

  /// Key the server uses to deduplicate retries of the same mutation. Sent
  /// with every attempt; replaced only when the payload changes after a
  /// conflict, because the old request is known not to have been applied.
  final String idempotencyKey;

  /// Monotonic position in the queue; defines FIFO order.
  final int sequence;

  /// Operation type, for example `create`, `update` or `delete`.
  final String operation;

  /// The resource the mutation targets, for example `note:42`. Coalescing
  /// only combines entries with the same resource key.
  final String resourceKey;

  /// JSON-encodable body of the mutation. Unmodifiable.
  final Map<String, Object?> payload;

  /// When the entry was enqueued.
  final DateTime createdAt;

  /// When the local change was last modified (enqueue, coalescing or a
  /// merge). Used by the last-write-wins conflict strategy.
  final DateTime updatedAt;

  /// Number of times the entry has been handed to the sender.
  final int attempts;

  /// Earliest time of the next attempt, or `null` when due immediately.
  final DateTime? nextAttemptAt;

  /// Description of the most recent failure.
  final String? lastError;

  /// Current lifecycle state.
  final OutboxState state;

  /// Ordering group. Entries in the same group are sent one at a time in
  /// FIFO order, and a failure blocks the rest of the group. `null` means the
  /// entry is independent of every other entry.
  final String? group;

  /// Number of conflicts the entry has run into.
  final int conflictCount;

  /// How the latest conflict was resolved, if it was.
  final ConflictResolution? conflictResolution;

  /// The server's version reported with the latest conflict (JSON value).
  final Object? serverState;

  /// Whether the entry is finished and will not be sent again.
  bool get isTerminal => state.isTerminal;

  /// Returns a copy with the given fields replaced. The `clear*` flags set
  /// the matching nullable field to `null`.
  OutboxEntry copyWith({
    String? idempotencyKey,
    String? operation,
    Map<String, Object?>? payload,
    DateTime? updatedAt,
    int? attempts,
    DateTime? nextAttemptAt,
    bool clearNextAttemptAt = false,
    String? lastError,
    bool clearLastError = false,
    OutboxState? state,
    int? conflictCount,
    ConflictResolution? conflictResolution,
    bool clearConflictResolution = false,
    Object? serverState,
    bool clearServerState = false,
  }) =>
      OutboxEntry(
        id: id,
        idempotencyKey: idempotencyKey ?? this.idempotencyKey,
        sequence: sequence,
        operation: operation ?? this.operation,
        resourceKey: resourceKey,
        payload: payload ?? this.payload,
        createdAt: createdAt,
        updatedAt: updatedAt ?? this.updatedAt,
        attempts: attempts ?? this.attempts,
        nextAttemptAt:
            clearNextAttemptAt ? null : nextAttemptAt ?? this.nextAttemptAt,
        lastError: clearLastError ? null : lastError ?? this.lastError,
        state: state ?? this.state,
        group: group,
        conflictCount: conflictCount ?? this.conflictCount,
        conflictResolution: clearConflictResolution
            ? null
            : conflictResolution ?? this.conflictResolution,
        serverState: clearServerState ? null : serverState ?? this.serverState,
      );

  /// JSON representation, readable by [OutboxEntry.fromJson]. Dates are
  /// ISO 8601 strings in UTC.
  Map<String, Object?> toJson() => {
        'id': id,
        'idempotencyKey': idempotencyKey,
        'sequence': sequence,
        'operation': operation,
        'resourceKey': resourceKey,
        'payload': payload,
        'createdAt': createdAt.toUtc().toIso8601String(),
        'updatedAt': updatedAt.toUtc().toIso8601String(),
        'attempts': attempts,
        'nextAttemptAt': nextAttemptAt?.toUtc().toIso8601String(),
        'lastError': lastError,
        'state': state.name,
        'group': group,
        'conflictCount': conflictCount,
        'conflictResolution': conflictResolution?.name,
        'serverState': serverState,
      };

  @override
  bool operator ==(Object other) =>
      other is OutboxEntry && _deepEquals(toJson(), other.toJson());

  @override
  int get hashCode => Object.hash(id, sequence, state, attempts);

  @override
  String toString() =>
      'OutboxEntry($id, $operation $resourceKey, ${state.name}, '
      'attempts: $attempts, group: $group)';
}
