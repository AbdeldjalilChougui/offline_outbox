import 'dart:math';

/// Decides how long to wait before retrying an entry after a retryable
/// failure, and when to stop retrying automatically.
abstract interface class RetryPolicy {
  /// Delay before the next attempt, after [attempts] attempts so far
  /// (1 after the first failure). [random] is the outbox's injected source,
  /// used for jitter.
  Duration delayFor(int attempts, Random random);

  /// After this many attempts a retryable failure moves the entry to
  /// needs-attention instead of scheduling another try. `null` means the
  /// outbox keeps retrying forever.
  int? get maxAttempts;
}

Duration _jittered(Duration nominal, double jitter, Random random) {
  if (jitter == 0) return nominal;
  final factor = 1 - jitter * random.nextDouble();
  return Duration(microseconds: (nominal.inMicroseconds * factor).round());
}

/// Exponential backoff with a cap and subtractive jitter.
///
/// The nominal delay is `initialDelay * multiplier^(attempts - 1)`, capped at
/// [maxDelay]. Jitter then shortens it by a random fraction of up to
/// [jitter], so the delay never exceeds the cap and clients that failed
/// together do not retry together.
class ExponentialBackoff implements RetryPolicy {
  /// Creates an exponential backoff policy.
  ///
  /// Defaults: 1 second, doubling, capped at 5 minutes, 20% jitter,
  /// unlimited attempts.
  ///
  /// Throws an [ArgumentError] for a jitter outside `[0, 1]`, a multiplier
  /// below 1 or a non-positive [maxAttempts].
  ExponentialBackoff({
    this.initialDelay = const Duration(seconds: 1),
    this.multiplier = 2,
    this.maxDelay = const Duration(minutes: 5),
    this.jitter = 0.2,
    this.maxAttempts,
  }) {
    if (jitter < 0 || jitter > 1) {
      throw ArgumentError.value(jitter, 'jitter', 'must be in [0, 1]');
    }
    if (multiplier < 1) {
      throw ArgumentError.value(multiplier, 'multiplier', 'must be >= 1');
    }
    if (maxAttempts != null && maxAttempts! <= 0) {
      throw ArgumentError.value(maxAttempts, 'maxAttempts', 'must be > 0');
    }
  }

  /// Delay after the first failure.
  final Duration initialDelay;

  /// Growth factor between consecutive attempts; at least 1.
  final double multiplier;

  /// Upper bound for any delay.
  final Duration maxDelay;

  /// Fraction in `[0, 1]` by which a delay may be randomly shortened.
  final double jitter;

  @override
  final int? maxAttempts;

  @override
  Duration delayFor(int attempts, Random random) {
    final n = max(attempts, 1) - 1;
    final cap = maxDelay.inMicroseconds.toDouble();
    // Computed in doubles and clamped, so huge attempt counts cannot overflow.
    final raw = initialDelay.inMicroseconds * pow(multiplier, n).toDouble();
    final nominal = raw.isFinite ? min(raw, cap) : cap;
    return _jittered(Duration(microseconds: nominal.round()), jitter, random);
  }
}

/// Backoff that follows a fixed list of delays and then stays on the last one.
class ScheduledBackoff implements RetryPolicy {
  /// Creates a policy that waits `steps[attempts - 1]`, or the last step once
  /// the list is exhausted.
  ///
  /// Throws an [ArgumentError] when [steps] is empty, [jitter] is outside
  /// `[0, 1]` or [maxAttempts] is not positive.
  ScheduledBackoff(List<Duration> steps, {this.jitter = 0, this.maxAttempts})
      : steps = List.unmodifiable(steps) {
    if (maxAttempts != null && maxAttempts! <= 0) {
      throw ArgumentError.value(maxAttempts, 'maxAttempts', 'must be > 0');
    }
    if (steps.isEmpty) {
      throw ArgumentError.value(steps, 'steps', 'must not be empty');
    }
    if (jitter < 0 || jitter > 1) {
      throw ArgumentError.value(jitter, 'jitter', 'must be in [0, 1]');
    }
  }

  /// A patient schedule for work that may stay refused for a long time:
  /// 1 minute, 5 minutes, 15 minutes, 1 hour, 6 hours, then once a day,
  /// forever. A genuine hiccup recovers quickly, and a hopeless entry settles
  /// down to one request a day without ever being dropped.
  static final ScheduledBackoff oneMinuteToOneDay = ScheduledBackoff(const [
    Duration(minutes: 1),
    Duration(minutes: 5),
    Duration(minutes: 15),
    Duration(hours: 1),
    Duration(hours: 6),
    Duration(hours: 24),
  ]);

  /// The delays, in order.
  final List<Duration> steps;

  /// Fraction in `[0, 1]` by which a delay may be randomly shortened.
  final double jitter;

  @override
  final int? maxAttempts;

  @override
  Duration delayFor(int attempts, Random random) {
    final i = min(max(attempts, 1), steps.length) - 1;
    return _jittered(steps[i], jitter, random);
  }
}
