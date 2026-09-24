import 'dart:math';

import 'package:offline_outbox/offline_outbox.dart';
import 'package:test/test.dart';

/// A Random that always returns the same double.
class FixedRandom implements Random {
  FixedRandom(this.value);
  final double value;
  @override
  double nextDouble() => value;
  @override
  int nextInt(int max) => (value * max).floor();
  @override
  bool nextBool() => value >= 0.5;
}

void main() {
  group('ExponentialBackoff', () {
    final policy = ExponentialBackoff(
      initialDelay: Duration(seconds: 1),
      multiplier: 2,
      maxDelay: Duration(minutes: 5),
      jitter: 0,
    );

    test('doubles from the initial delay', () {
      final r = Random(1);
      expect(policy.delayFor(1, r), const Duration(seconds: 1));
      expect(policy.delayFor(2, r), const Duration(seconds: 2));
      expect(policy.delayFor(3, r), const Duration(seconds: 4));
      expect(policy.delayFor(9, r), const Duration(seconds: 256));
    });

    test('is capped at maxDelay, even for huge attempt counts', () {
      final r = Random(1);
      expect(policy.delayFor(10, r), const Duration(minutes: 5));
      expect(policy.delayFor(1000000, r), const Duration(minutes: 5));
    });

    test('treats attempts below 1 as the first attempt', () {
      expect(policy.delayFor(0, Random(1)), const Duration(seconds: 1));
    });

    test('jitter only ever shortens the delay, by at most the fraction', () {
      final jittered = ExponentialBackoff(
        initialDelay: Duration(seconds: 10),
        maxDelay: Duration(minutes: 5),
        jitter: 0.5,
      );
      expect(jittered.delayFor(1, FixedRandom(0)), const Duration(seconds: 10));
      expect(jittered.delayFor(1, FixedRandom(0.5)),
          const Duration(milliseconds: 7500));
      expect(jittered.delayFor(1, FixedRandom(0.999999)),
          greaterThan(const Duration(seconds: 5)));
      // The cap still holds with jitter.
      expect(jittered.delayFor(50, FixedRandom(0)), const Duration(minutes: 5));
    });

    test('rejects invalid arguments', () {
      expect(() => ExponentialBackoff(jitter: 1.5), throwsArgumentError);
      expect(() => ExponentialBackoff(multiplier: 0.5), throwsArgumentError);
      expect(() => ExponentialBackoff(maxAttempts: 0), throwsArgumentError);
    });
  });

  group('ScheduledBackoff', () {
    test('oneMinuteToOneDay preset walks 1m, 5m, 15m, 1h, 6h, 24h', () {
      final p = ScheduledBackoff.oneMinuteToOneDay;
      final r = FixedRandom(0);
      expect(
        [for (var a = 1; a <= 6; a++) p.delayFor(a, r)],
        const [
          Duration(minutes: 1),
          Duration(minutes: 5),
          Duration(minutes: 15),
          Duration(hours: 1),
          Duration(hours: 6),
          Duration(hours: 24),
        ],
      );
    });

    test('stays at the last step and never gives up by default', () {
      final p = ScheduledBackoff.oneMinuteToOneDay;
      expect(p.delayFor(7, FixedRandom(0)), const Duration(hours: 24));
      expect(p.delayFor(500, FixedRandom(0)), const Duration(hours: 24));
      expect(p.maxAttempts, isNull);
    });

    test('applies jitter', () {
      final p = ScheduledBackoff(const [Duration(minutes: 10)], jitter: 0.1);
      expect(p.delayFor(1, FixedRandom(1)), const Duration(minutes: 9));
    });

    test('needs at least one step', () {
      expect(() => ScheduledBackoff(const []), throwsArgumentError);
    });
  });
}
