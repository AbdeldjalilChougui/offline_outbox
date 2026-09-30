import 'dart:math';

import 'package:fake_async/fake_async.dart';
import 'package:sync_outbox/sync_outbox.dart';
import 'package:test/test.dart';

import 'harness.dart';

void main() {
  late Harness h;
  setUp(() => h = Harness());

  test('schedules the next attempt from the policy', () async {
    await h.start();
    final a = await h.add('a');
    h.respond = (_) => const SendResult.retryableFailure(error: '502');
    final t0 = h.now;
    await h.outbox.flush();

    final e = h.outbox.entry(a.id)!;
    expect(e.state, OutboxState.pending);
    expect(e.attempts, 1);
    expect(e.lastError, '502');
    expect(e.nextAttemptAt, t0.add(const Duration(seconds: 10)));
  });

  test('does not send before nextAttemptAt, sends at it', () async {
    await h.start();
    await h.add('a');
    h.respond = (_) => const SendResult.retryableFailure();
    await h.outbox.flush();

    h.advance(const Duration(seconds: 9, milliseconds: 999));
    await h.outbox.flush();
    expect(h.sent, hasLength(1));

    h.advance(const Duration(milliseconds: 1));
    await h.outbox.flush();
    expect(h.sent, hasLength(2));
  });

  test('delays grow exponentially and are capped', () async {
    await h.start();
    final a = await h.add('a');
    h.respond = (_) => const SendResult.retryableFailure();
    final delays = <Duration>[];
    for (var i = 0; i < 8; i++) {
      await h.outbox.flush();
      final next = h.outbox.entry(a.id)!.nextAttemptAt!;
      delays.add(next.difference(h.now));
      h.now = next;
    }
    expect(
        delays.map((d) => d.inSeconds), [10, 20, 40, 80, 160, 320, 600, 600]);
  });

  test('a server retryAfter overrides the policy', () async {
    await h.start();
    final a = await h.add('a');
    h.respond = (_) => const SendResult.retryableFailure(
          retryAfter: Duration(minutes: 7),
        );
    await h.outbox.flush();
    expect(h.outbox.entry(a.id)!.nextAttemptAt,
        h.now.add(const Duration(minutes: 7)));
  });

  test('jitter comes from the injected random source', () async {
    final policy = ExponentialBackoff(
      initialDelay: const Duration(seconds: 100),
      jitter: 0.5,
    );
    final o = h.build(retryPolicy: policy, random: FixedRandom(0.5));
    await o.start();
    final a = await h.add('a');
    h.respond = (_) => const SendResult.retryableFailure();
    await o.flush();
    expect(
        o.entry(a.id)!.nextAttemptAt, h.now.add(const Duration(seconds: 75)));
  });

  test('jitter is reproducible for a seeded Random', () async {
    Future<DateTime> run() async {
      final hh = Harness();
      final o = hh.build(
        retryPolicy: ExponentialBackoff(jitter: 1),
        random: Random(42),
      );
      await o.start();
      final e = await hh.add('a');
      hh.respond = (_) => const SendResult.retryableFailure();
      await o.flush();
      return o.entry(e.id)!.nextAttemptAt!;
    }

    expect(await run(), await run());
  });

  test('gives up after maxAttempts and asks for attention', () async {
    await h.start(
      retryPolicy: ExponentialBackoff(
        initialDelay: const Duration(seconds: 1),
        jitter: 0,
        maxAttempts: 3,
      ),
    );
    final a = await h.add('a');
    h.respond = (_) => const SendResult.retryableFailure(error: '500');
    for (var i = 0; i < 5; i++) {
      await h.outbox.flush();
      h.advance(const Duration(hours: 1));
    }
    expect(h.sent, hasLength(3));
    final e = h.outbox.entry(a.id)!;
    expect(e.state, OutboxState.needsAttention);
    expect(e.lastError, contains('500'));
    expect(e.lastError, contains('3 attempts'));
    expect((await h.drained).whereType<OutboxNeedsAttention>(), hasLength(1));
  });

  test('the one-minute-to-one-day preset drives the schedule', () async {
    await h.start(retryPolicy: ScheduledBackoff.oneMinuteToOneDay);
    final a = await h.add('a');
    h.respond = (_) => const SendResult.retryableFailure();
    final delays = <Duration>[];
    for (var i = 0; i < 8; i++) {
      await h.outbox.flush();
      final next = h.outbox.entry(a.id)!.nextAttemptAt!;
      delays.add(next.difference(h.now));
      h.now = next;
    }
    expect(delays, const [
      Duration(minutes: 1),
      Duration(minutes: 5),
      Duration(minutes: 15),
      Duration(hours: 1),
      Duration(hours: 6),
      Duration(hours: 24),
      Duration(hours: 24),
      Duration(hours: 24),
    ]);
    expect(h.outbox.entry(a.id)!.state, OutboxState.pending);
  });

  test('scheduleRetries wakes the outbox up when the retry is due', () {
    fakeAsync((async) {
      final start = DateTime.utc(2026);
      final o = h.build(
        scheduleRetries: true,
        clock: () => start.add(async.elapsed),
      );
      o.start();
      async.flushMicrotasks();
      h.add('a');
      async.flushMicrotasks();
      var calls = 0;
      h.respond = (_) => ++calls == 1
          ? const SendResult.retryableFailure()
          : const SendResult.success();
      o.flush();
      async.flushMicrotasks();
      expect(h.sent, hasLength(1));

      async.elapse(const Duration(seconds: 9));
      expect(h.sent, hasLength(1));
      async.elapse(const Duration(seconds: 1));
      expect(h.sent, hasLength(2));
      expect(o.entries, isEmpty);
      o.dispose();
      async.flushMicrotasks();
    });
  });
}
