import 'dart:async';

import 'package:sync_outbox/sync_outbox.dart';
import 'package:test/test.dart';

import 'harness.dart';

void main() {
  late Harness h;
  setUp(() => h = Harness());

  group('enqueue', () {
    test('persists a pending entry with generated ids and clock time',
        () async {
      await h.start();
      final e = await h.add('note:1', payload: {'title': 'a'}, group: 'g');

      expect(e.state, OutboxState.pending);
      expect(e.id, 'id0');
      expect(e.idempotencyKey, 'id1');
      expect(e.operation, 'update');
      expect(e.resourceKey, 'note:1');
      expect(e.payload, {'title': 'a'});
      expect(e.group, 'g');
      expect(e.createdAt, h.now);
      expect(e.attempts, 0);
      expect((await h.store.loadAll()).single, e);
      expect(h.outbox.entries.single, e);
      expect(h.outbox.entry(e.id), e);
    });

    test('accepts a caller-supplied idempotency key', () async {
      await h.start();
      final e = await h.outbox.enqueue(
        operation: 'create',
        resourceKey: 'r',
        idempotencyKey: 'mine',
      );
      expect(e.idempotencyKey, 'mine');
    });

    test('assigns increasing sequence numbers', () async {
      await h.start();
      final a = await h.add('a');
      final b = await h.add('b');
      final c = await h.add('c');
      expect([a.sequence, b.sequence, c.sequence], [0, 1, 2]);
    });

    test('copies the payload so later caller mutations do not leak in',
        () async {
      await h.start();
      final payload = <String, Object?>{'v': 1};
      final e = await h.add('a', payload: payload);
      payload['v'] = 2;
      expect(h.outbox.entry(e.id)!.payload['v'], 1);
    });

    test('before start or after dispose throws StateError', () async {
      final o = h.build();
      expect(
          () => o.enqueue(operation: 'x', resourceKey: 'r'), throwsStateError);
      expect(o.flush, throwsStateError);
      await o.start();
      await o.dispose();
      expect(
          () => o.enqueue(operation: 'x', resourceKey: 'r'), throwsStateError);
    });

    test('start twice throws StateError', () async {
      await h.start();
      expect(h.outbox.start, throwsStateError);
    });

    test('uses UUID v4 ids by default', () async {
      final o = Outbox(
        store: InMemoryOutboxStore(),
        sender: (_) async => const SendResult.success(),
        autoFlush: false,
      );
      await o.start();
      final e = await o.enqueue(operation: 'x', resourceKey: 'r');
      expect(e.id, matches(RegExp(r'^[0-9a-f-]{36}$')));
      expect(e.idempotencyKey, isNot(e.id));
      await o.dispose();
    });
  });

  group('flush and the state machine', () {
    test('success removes the entry by default', () async {
      await h.start();
      await h.add('a');
      final r = await h.outbox.flush();

      expect(h.sentResources, ['a']);
      expect(h.outbox.entries, isEmpty);
      expect(await h.store.loadAll(), isEmpty);
      expect(r.attempted, 1);
      expect(r.succeeded, 1);
      expect(r.failed, 0);
    });

    test('keepCompleted retains succeeded entries until cleared', () async {
      await h.start(keepCompleted: true);
      final e = await h.add('a');
      await h.outbox.flush();

      expect(h.outbox.entry(e.id)!.state, OutboxState.succeeded);
      expect((await h.store.loadAll()).single.state, OutboxState.succeeded);
      expect(await h.outbox.clearCompleted(), 1);
      expect(h.outbox.entries, isEmpty);
      expect(await h.store.loadAll(), isEmpty);
    });

    test('marks the entry in flight and persists that before sending',
        () async {
      await h.start();
      await h.add('a');
      late List<OutboxEntry> storedDuringSend;
      h.respond = (e) async {
        storedDuringSend = await h.store.loadAll();
        return const SendResult.success();
      };
      await h.outbox.flush();

      expect(h.sent.single.state, OutboxState.inFlight);
      expect(h.sent.single.attempts, 1);
      expect(storedDuringSend.single.state, OutboxState.inFlight);
      expect(storedDuringSend.single.attempts, 1);
    });

    test('keeps the same idempotency key across retries', () async {
      await h.start();
      await h.add('a');
      h.respond = (_) => const SendResult.retryableFailure();
      await h.outbox.flush();
      h.advance(const Duration(hours: 1));
      await h.outbox.flush();
      h.respond = (_) => const SendResult.success();
      h.advance(const Duration(hours: 1));
      await h.outbox.flush();

      expect(h.sent, hasLength(3));
      expect(h.sent.map((e) => e.idempotencyKey).toSet(), {'id1'});
      expect(h.sent.map((e) => e.attempts), [1, 2, 3]);
    });

    test('permanent failure moves the entry to needsAttention, not deleted',
        () async {
      await h.start();
      final e = await h.add('a');
      h.respond = (_) => const SendResult.permanentFailure('422 invalid');
      final r = await h.outbox.flush();

      final stored = h.outbox.entry(e.id)!;
      expect(stored.state, OutboxState.needsAttention);
      expect(stored.lastError, '422 invalid');
      expect(
          (await h.store.loadAll()).single.state, OutboxState.needsAttention);
      expect(r.failed, 1);

      // It is never retried automatically.
      h.advance(const Duration(days: 30));
      await h.outbox.flush();
      expect(h.sent, hasLength(1));
    });

    test('a throwing sender counts as a retryable failure', () async {
      await h.start();
      final e = await h.add('a');
      h.respond = (_) => throw StateError('socket closed');
      await h.outbox.flush();

      final stored = h.outbox.entry(e.id)!;
      expect(stored.state, OutboxState.pending);
      expect(stored.lastError, contains('socket closed'));
      expect(stored.nextAttemptAt, h.now.add(const Duration(seconds: 10)));
    });

    test('success clears the last error', () async {
      await h.start(keepCompleted: true);
      final e = await h.add('a');
      h.respond = (_) => const SendResult.retryableFailure(error: '503');
      await h.outbox.flush();
      expect(h.outbox.entry(e.id)!.lastError, '503');
      h.respond = (_) => const SendResult.success();
      h.advance(const Duration(minutes: 1));
      await h.outbox.flush();
      expect(h.outbox.entry(e.id)!.lastError, isNull);
      expect(h.outbox.entry(e.id)!.state, OutboxState.succeeded);
    });

    test('a concurrent flush call joins the running one', () async {
      await h.start();
      await h.add('a');
      final gate = Completer<SendResult>();
      h.respond = (_) => gate.future;
      final f1 = h.outbox.flush();
      final f2 = h.outbox.flush();
      await h.settle();
      expect(h.sent, hasLength(1));
      gate.complete(const SendResult.success());
      final results = await Future.wait([f1, f2]);
      expect(h.sent, hasLength(1));
      expect(results[0].succeeded, 1);
    });

    test('entries enqueued during a flush are sent by that flush', () async {
      await h.start();
      await h.add('a');
      final gate = Completer<SendResult>();
      h.respond = (e) =>
          e.resourceKey == 'a' ? gate.future : const SendResult.success();
      final f = h.outbox.flush();
      await h.settle();
      await h.add('b');
      gate.complete(const SendResult.success());
      final r = await f;
      expect(h.sentResources, ['a', 'b']);
      expect(r.succeeded, 2);
    });
  });

  group('retry, discard, retryAllNeedingAttention', () {
    test('discard cancels a pending or blocked entry', () async {
      await h.start();
      final a = await h.add('a');
      final b = await h.add('b');
      h.respond = (_) => const SendResult.permanentFailure('no');
      await h.outbox.flush();

      expect(await h.outbox.discard(a.id), isTrue);
      expect(await h.outbox.discard(b.id), isTrue);
      expect(h.outbox.entries, isEmpty);
      expect(await h.store.loadAll(), isEmpty);
      final cancels = (await h.drained).whereType<OutboxCancelled>().toList();
      expect(cancels.map((e) => e.reason),
          [CancelReason.discarded, CancelReason.discarded]);
      expect(cancels.first.entry.state, OutboxState.cancelled);
    });

    test('discard keeps the cancelled entry when keepCompleted', () async {
      await h.start(keepCompleted: true);
      final a = await h.add('a');
      await h.outbox.discard(a.id);
      expect(h.outbox.entry(a.id)!.state, OutboxState.cancelled);
    });

    test('discard refuses in-flight, finished and unknown entries', () async {
      await h.start(keepCompleted: true);
      final a = await h.add('a');
      final gate = Completer<SendResult>();
      h.respond = (_) => gate.future;
      final f = h.outbox.flush();
      await h.settle();
      expect(await h.outbox.discard(a.id), isFalse);
      gate.complete(const SendResult.success());
      await f;
      expect(await h.outbox.discard(a.id), isFalse);
      expect(await h.outbox.discard('unknown'), isFalse);
    });

    test('retry requeues a needsAttention entry', () async {
      await h.start();
      final a = await h.add('a');
      h.respond = (_) => const SendResult.permanentFailure('no');
      await h.outbox.flush();
      h.respond = (_) => const SendResult.success();

      expect(await h.outbox.retry(a.id), isTrue);
      expect(h.outbox.entry(a.id)!.state, OutboxState.pending);
      expect((await h.drained).last, isA<OutboxRequeued>());
      await h.outbox.flush();
      expect(h.sent, hasLength(2));
      expect(h.outbox.entries, isEmpty);
    });

    test('retry makes a backed-off entry due now', () async {
      await h.start();
      final a = await h.add('a');
      h.respond = (_) => const SendResult.retryableFailure();
      await h.outbox.flush();
      expect(h.outbox.entry(a.id)!.nextAttemptAt, isNotNull);

      expect(await h.outbox.retry(a.id), isTrue);
      expect(h.outbox.entry(a.id)!.nextAttemptAt, isNull);
      await h.outbox.flush();
      expect(h.sent, hasLength(2));
    });

    test('retry with a new payload rotates the idempotency key', () async {
      await h.start();
      final a = await h.add('a', payload: {'v': 1});
      h.respond = (_) => const SendResult.permanentFailure('no');
      await h.outbox.flush();
      h.respond = (_) => const SendResult.success();

      await h.outbox.retry(a.id, payload: {'v': 2});
      await h.outbox.flush();
      expect(h.sent.last.payload, {'v': 2});
      expect(h.sent.last.idempotencyKey, isNot(a.idempotencyKey));
    });

    test('retry without changes keeps the idempotency key', () async {
      await h.start();
      final a = await h.add('a');
      h.respond = (_) => const SendResult.permanentFailure('no');
      await h.outbox.flush();
      await h.outbox.retry(a.id);
      expect(h.outbox.entry(a.id)!.idempotencyKey, a.idempotencyKey);
    });

    test('retry refuses unknown, in-flight and finished entries', () async {
      await h.start(keepCompleted: true);
      final a = await h.add('a');
      expect(await h.outbox.retry('nope'), isFalse);
      await h.outbox.flush();
      expect(await h.outbox.retry(a.id), isFalse);
    });

    test('retryAllNeedingAttention requeues every blocked entry', () async {
      await h.start();
      await h.add('a');
      await h.add('b');
      await h.add('c');
      h.respond = (e) => e.resourceKey == 'b'
          ? const SendResult.success()
          : const SendResult.permanentFailure('no');
      await h.outbox.flush();
      expect(h.outbox.currentCounts.needsAttention, 2);

      h.respond = (_) => const SendResult.success();
      expect(await h.outbox.retryAllNeedingAttention(), 2);
      expect(h.outbox.currentCounts.needsAttention, 0);
      expect(h.outbox.currentCounts.pending, 2);
      await h.outbox.flush();
      expect(h.outbox.entries, isEmpty);
    });
  });

  group('events and counts', () {
    test('counts stream replays the current value and tracks changes',
        () async {
      await h.start();
      final seen = <OutboxCounts>[];
      final sub = h.outbox.counts.listen(seen.add);
      await h.settle();
      expect(seen, [const OutboxCounts()]);

      await h.add('a');
      await h.add('b');
      h.respond = (e) => e.resourceKey == 'a'
          ? const SendResult.permanentFailure('x')
          : const SendResult.success();
      await h.outbox.flush();
      await h.settle();
      await sub.cancel();

      expect(seen.first, const OutboxCounts());
      expect(seen, contains(const OutboxCounts(pending: 1)));
      expect(seen, contains(const OutboxCounts(pending: 2)));
      expect(seen.last, const OutboxCounts(needsAttention: 1));
      expect(seen.last.outstanding, 1);
      // Only changes are emitted.
      for (var i = 1; i < seen.length; i++) {
        expect(seen[i], isNot(seen[i - 1]));
      }
    });

    test('a second counts listener also gets the current value', () async {
      await h.start();
      await h.add('a');
      final first = await h.outbox.counts.first;
      final second = await h.outbox.counts.first;
      expect(first, const OutboxCounts(pending: 1));
      expect(second, first);
    });

    test('event sequence for a retried then successful entry', () async {
      await h.start();
      await h.add('a');
      h.respond = (_) => const SendResult.retryableFailure(error: '503');
      await h.outbox.flush();
      h.respond = (_) => const SendResult.success();
      h.advance(const Duration(minutes: 1));
      await h.outbox.flush();
      await h.settle();

      expect((await h.drained).map((e) => e.runtimeType), [
        OutboxEnqueued,
        OutboxSending,
        OutboxRetryScheduled,
        OutboxSending,
        OutboxSucceeded,
      ]);
      final retry = (await h.drained).whereType<OutboxRetryScheduled>().single;
      expect(retry.error, '503');
      expect(retry.entry.nextAttemptAt, isNotNull);
    });
  });
}
