import 'entry.dart';

/// Decides how a newly enqueued entry combines with the most recent queued
/// entry for the same resource and group.
///
/// Only called when that queued entry is pending and has never been
/// attempted, so the server cannot have seen it. Registered per operation
/// type of the incoming entry, via `Outbox(coalescers: {...})`.
typedef Coalescer = Coalesce Function(OutboxEntry queued, OutboxEntry incoming);

/// The outcome of a [Coalescer].
sealed class Coalesce {
  const Coalesce._();

  /// Replace the queued entry's payload with [payload] (and its operation
  /// with [operation], when given). The queued entry keeps its position and
  /// idempotency key; the incoming entry is not stored.
  const factory Coalesce.merge(
    Map<String, Object?> payload, {
    String? operation,
  }) = CoalesceMerge;

  /// The two mutations cancel out (for example create then delete): the
  /// queued entry is cancelled and the incoming one is never stored.
  static const Coalesce cancelBoth = CoalesceCancelBoth._();

  /// Do not combine; enqueue the incoming entry normally.
  static const Coalesce keepBoth = CoalesceKeepBoth._();
}

/// See [Coalesce.merge].
final class CoalesceMerge extends Coalesce {
  /// Creates a merge outcome.
  const CoalesceMerge(this.payload, {this.operation}) : super._();

  /// The combined payload.
  final Map<String, Object?> payload;

  /// The operation of the combined entry; `null` keeps the queued one.
  final String? operation;
}

/// See [Coalesce.cancelBoth].
final class CoalesceCancelBoth extends Coalesce {
  const CoalesceCancelBoth._() : super._();
}

/// See [Coalesce.keepBoth].
final class CoalesceKeepBoth extends Coalesce {
  const CoalesceKeepBoth._() : super._();
}
