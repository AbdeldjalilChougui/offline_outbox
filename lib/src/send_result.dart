import 'entry.dart';

/// What a `Sender` reports back after trying to deliver an [OutboxEntry].
sealed class SendResult {
  const SendResult._();

  /// The server applied the mutation (or had already applied it: a
  /// deduplicated retry is also a success).
  const factory SendResult.success() = SendSuccess;

  /// A transient failure (timeout, no network, 5xx, 429). The entry is
  /// retried after [RetryableFailure.retryAfter] if given, otherwise after
  /// the outbox's retry policy delay.
  const factory SendResult.retryableFailure({
    Duration? retryAfter,
    String? error,
  }) = RetryableFailure;

  /// The server will never accept this mutation as it is (4xx other than a
  /// conflict). The entry moves to needs-attention and blocks its group.
  const factory SendResult.permanentFailure(String reason) = PermanentFailure;

  /// The server holds a different version of the resource. [serverState] is
  /// that version (any JSON value) and [serverModifiedAt] its modification
  /// time, used by the last-write-wins strategy.
  const factory SendResult.conflict(
    Object? serverState, {
    DateTime? serverModifiedAt,
  }) = SendConflict;
}

/// See [SendResult.success].
final class SendSuccess extends SendResult {
  /// Creates a success result.
  const SendSuccess() : super._();
}

/// See [SendResult.retryableFailure].
final class RetryableFailure extends SendResult {
  /// Creates a retryable failure.
  const RetryableFailure({this.retryAfter, this.error}) : super._();

  /// Server-requested delay (for example from a `Retry-After` header). When
  /// set, it replaces the retry policy delay.
  final Duration? retryAfter;

  /// Description of the failure, stored in [OutboxEntry.lastError].
  final String? error;
}

/// See [SendResult.permanentFailure].
final class PermanentFailure extends SendResult {
  /// Creates a permanent failure.
  const PermanentFailure(this.reason) : super._();

  /// Why the server refused, stored in [OutboxEntry.lastError].
  final String reason;
}

/// See [SendResult.conflict].
final class SendConflict extends SendResult {
  /// Creates a conflict result.
  const SendConflict(this.serverState, {this.serverModifiedAt}) : super._();

  /// The server's current version of the resource (JSON value).
  final Object? serverState;

  /// When the server's version was last modified, if known.
  final DateTime? serverModifiedAt;
}
