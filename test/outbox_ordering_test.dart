import 'dart:async';

import 'package:offline_outbox/offline_outbox.dart';
import 'package:test/test.dart';

import 'harness.dart';

void main() {
  late Harness h;
  setUp(() => h = Harness());

  group('ordering', () {
    test('sends a group in FIFO order, one at a time', () async {
      await h.start(maxConcurrency: 8);
      for (final r in ['a', 'b', 'c', 'd']) {
        await h.add(r, group: 'g');
      }
      var inFlight = 0;
      var peak = 0;
      h.respond = (_) async {
        inFlight++;
        peak = inFlight > peak ? inFlight : peak;
        await Future<void>.delayed(Duration.zero);
        inFlight--;
        return const SendResult.success();
      };
      await h.outbox.flush();
      expect(h.sentResources, ['a', 'b', 'c', 'd']);
      expect(peak, 1);
    });

    test('keeps FIFO order within each of several interleaved groups',
        () async {
      await h.start(maxConcurrency: 2);
      await h.add('x1', group: 'x');
      await h.add('y1', group: 'y');
      await h.add('x2', group: 'x');
      await h.add('y2', group: 'y');
      await h.add('x3', group: 'x');
      await h.outbox.flush();

      final xs = h.sentResources.where((r) => r.startsWith('x')).toList();
      final ys = h.sentResources.where((r) => r.startsWith('y')).toList();
      expect(xs, ['x1', 'x2', 'x3']);
      expect(ys, ['y1', 'y2']);
    });

    test('starts lanes in order of their oldest entry', () async {
      await h.start(maxConcurrency: 1);
      await h.add('b1', group: 'b');
      await h.add('a1', group: 'a');
      await h.add('solo');
      await h.add('b2', group: 'b');
      await h.outbox.flush();
      expect(h.sentResources, ['b1', 'b2', 'a1', 'solo']);
    });

    test(
        'a single group gives strict global FIFO that stops at the first '
        'error', () async {
      await h.start();
      for (final r in ['a', 'b', 'c']) {
        await h.add(r, group: 'all');
      }
      h.respond = (e) => e.resourceKey == 'b'
          ? const SendResult.retryableFailure()
          : const SendResult.success();
      await h.outbox.flush();
      expect(h.sentResources, ['a', 'b']);
      expect(h.outbox.entries.map((e) => e.resourceKey), ['b', 'c']);
    });
  });

  group('group blocking', () {
    test('a permanent failure blocks only its own group', () async {
      await h.start();
      final g1 = await h.add('g1', group: 'g');
      await h.add('g2', group: 'g');
      await h.add('o1', group: 'other');
      await h.add('o2', group: 'other');
      await h.add('solo');
      h.respond = (e) => e.resourceKey == 'g1'
          ? const SendResult.permanentFailure('bad')
          : const SendResult.success();
      await h.outbox.flush();

      expect(h.sentResources, unorderedEquals(['g1', 'o1', 'o2', 'solo']));
      expect(h.outbox.entry(g1.id)!.state, OutboxState.needsAttention);
      expect(h.outbox.entries.map((e) => e.resourceKey), ['g1', 'g2']);

      // Still blocked on later flushes.
      h.advance(const Duration(days: 1));
      await h.outbox.flush();
      expect(h.sentResources.where((r) => r == 'g2'), isEmpty);

      // Discarding the blocker releases the group.
      await h.outbox.discard(g1.id);
      await h.outbox.flush();
      expect(h.sentResources.last, 'g2');
      expect(h.outbox.entries, isEmpty);
    });

    test('a retryable failure holds the group until the head is due', () async {
      await h.start();
      await h.add('g1', group: 'g');
      await h.add('g2', group: 'g');
      var failures = 1;
      h.respond = (e) => e.resourceKey == 'g1' && failures-- > 0
          ? const SendResult.retryableFailure()
          : const SendResult.success();
      await h.outbox.flush();
      expect(h.sentResources, ['g1']);

      // g2 is due, but must wait behind g1.
      h.advance(const Duration(seconds: 9));
      await h.outbox.flush();
      expect(h.sentResources, ['g1']);

      h.advance(const Duration(seconds: 1));
      await h.outbox.flush();
      expect(h.sentResources, ['g1', 'g1', 'g2']);
    });

    test('retrying the blocked head releases the rest of the group', () async {
      await h.start();
      final g1 = await h.add('g1', group: 'g');
      await h.add('g2', group: 'g');
      h.respond = (_) => const SendResult.permanentFailure('no');
      await h.outbox.flush();
      h.respond = (_) => const SendResult.success();
      await h.outbox.retry(g1.id);
      await h.outbox.flush();
      expect(h.sentResources, ['g1', 'g1', 'g2']);
    });

    test('ungrouped entries never block each other', () async {
      await h.start();
      await h.add('a');
      await h.add('b');
      await h.add('c');
      h.respond = (e) => e.resourceKey == 'a'
          ? const SendResult.permanentFailure('no')
          : const SendResult.success();
      await h.outbox.flush();
      expect(h.sentResources, ['a', 'b', 'c']);
      expect(h.outbox.entries.map((e) => e.resourceKey), ['a']);
    });
  });

  group('concurrency limit', () {
    Future<int> peakWith(int limit, int groups) async {
      await h.start(maxConcurrency: limit);
      for (var i = 0; i < groups; i++) {
        await h.add('r$i', group: 'g$i');
        await h.add('r$i-2', group: 'g$i');
      }
      final gates = <Completer<SendResult>>[];
      var inFlight = 0;
      var peak = 0;
      h.respond = (_) {
        inFlight++;
        if (inFlight > peak) peak = inFlight;
        final c = Completer<SendResult>();
        gates.add(c);
        return c.future.whenComplete(() => inFlight--);
      };
      final f = h.outbox.flush();
      // Release sends one at a time until everything is through.
      while (h.sent.length < groups * 2 || gates.any((g) => !g.isCompleted)) {
        await h.settle();
        expect(inFlight, lessThanOrEqualTo(limit));
        final open = gates.where((g) => !g.isCompleted).toList();
        if (open.isNotEmpty) open.first.complete(const SendResult.success());
      }
      await f;
      expect(h.outbox.entries, isEmpty);
      return peak;
    }

    test('never exceeds maxConcurrency', () async {
      expect(await peakWith(3, 10), 3);
    });

    test('uses all slots when there are fewer groups', () async {
      expect(await peakWith(8, 2), 2);
    });

    test('maxConcurrency 1 sends strictly one at a time', () async {
      expect(await peakWith(1, 4), 1);
    });

    test('rejects a non-positive limit', () {
      expect(() => h.build(maxConcurrency: 0), throwsArgumentError);
    });

    test('a slow group does not hold back the others', () async {
      await h.start(maxConcurrency: 2);
      await h.add('slow', group: 's');
      for (var i = 0; i < 5; i++) {
        await h.add('fast$i', group: 'f');
      }
      final slow = Completer<SendResult>();
      h.respond = (e) =>
          e.resourceKey == 'slow' ? slow.future : const SendResult.success();
      final f = h.outbox.flush();
      for (var i = 0; i < 10; i++) {
        await h.settle();
      }
      expect(h.sentResources.where((r) => r.startsWith('fast')), hasLength(5));
      slow.complete(const SendResult.success());
      await f;
    });
  });
}
