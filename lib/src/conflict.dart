import 'dart:async';

import 'entry.dart';

/// Produces a merged payload from the local entry and the server's version.
/// Return `null` to give up and leave the entry for a person.
typedef ConflictMerger = FutureOr<Map<String, Object?>?> Function(
  OutboxEntry local,
  Object? serverState,
);

/// What the outbox does when the sender reports a conflict.
sealed class ConflictStrategy {
  const ConflictStrategy._();

  /// Drop the local change and keep the server's version. The entry is
  /// cancelled and an `OutboxConflictResolved` event carries the server
  /// state so the app can update its local copy.
  static const ConflictStrategy serverWins = ServerWinsStrategy._();

  /// Resend the local change marked [ConflictResolution.clientWins], with a
  /// fresh idempotency key and the server state attached, so the sender can
  /// overwrite (for example by dropping its `If-Match` precondition).
  static const ConflictStrategy clientWins = ClientWinsStrategy._();

  /// Client wins when [OutboxEntry.updatedAt] is strictly after the server's
  /// modification time, server wins otherwise. Without a server timestamp the
  /// entry needs attention, since there is no safe way to choose.
  static const ConflictStrategy lastWriteWins = LastWriteWinsStrategy._();

  /// Leave the entry in needs-attention with the server state attached, and
  /// block its group until a person retries or discards it.
  static const ConflictStrategy manual = ManualStrategy._();

  /// Resend the payload returned by [merger], marked
  /// [ConflictResolution.merged], with a fresh idempotency key. If [merger]
  /// returns `null` or throws, the entry needs attention.
  const factory ConflictStrategy.merge(ConflictMerger merger) = MergeStrategy;
}

/// See [ConflictStrategy.serverWins].
final class ServerWinsStrategy extends ConflictStrategy {
  const ServerWinsStrategy._() : super._();
}

/// See [ConflictStrategy.clientWins].
final class ClientWinsStrategy extends ConflictStrategy {
  const ClientWinsStrategy._() : super._();
}

/// See [ConflictStrategy.lastWriteWins].
final class LastWriteWinsStrategy extends ConflictStrategy {
  const LastWriteWinsStrategy._() : super._();
}

/// See [ConflictStrategy.manual].
final class ManualStrategy extends ConflictStrategy {
  const ManualStrategy._() : super._();
}

/// See [ConflictStrategy.merge].
final class MergeStrategy extends ConflictStrategy {
  /// Creates a merge strategy.
  const MergeStrategy(this.merger) : super._();

  /// The user-supplied merge function.
  final ConflictMerger merger;
}
