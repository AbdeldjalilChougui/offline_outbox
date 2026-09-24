import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:offline_outbox/offline_outbox.dart';
import 'package:test/test.dart';

import 'harness.dart';

void main() {
  late Harness h;
  setUp(() => h = Harness());

  test('flush does nothing while offline', () async {
    final o = h.build(initiallyOnline: false);
    await o.start();
    await h.add('a');
    final r = await o.flush();
    expect(h.sent, isEmpty);
    expect(r.attempted, 0);
    expect(r.offline, isTrue);
    expect(o.isOnline, isFalse);
  });

  test('regaining connectivity flushes the queue', () async {
    final net = StreamController<bool>();
    final o = h.build(connectivity: net.stream, initiallyOnline: false);
    await o.start();
    await h.add('a');
    await h.add('b');
    await h.settle();
    expect(h.sent, isEmpty);

    net.add(true);
    await h.settle();
    await h.settle();
    expect(h.sentResources, ['a', 'b']);
    expect(o.isOnline, isTrue);
    await o.dispose();
    await net.close();
  });

  test('going offline stops new sends mid-flush', () async {
    final net = StreamController<bool>();
    final o = h.build(connectivity: net.stream);
    await o.start();
    await h.add('a', group: 'g');
    await h.add('b', group: 'g');
    final gate = Completer<SendResult>();
    h.respond =
        (e) => e.resourceKey == 'a' ? gate.future : const SendResult.success();
    final f = o.flush();
    await h.settle();
    net.add(false);
    await h.settle();
    gate.complete(const SendResult.success());
    await f;
    expect(h.sentResources, ['a']);
    expect(o.entries.single.resourceKey, 'b');
    expect(o.entries.single.state, OutboxState.pending);
    await o.dispose();
    await net.close();
  });

  test('start flushes right away when online', () async {
    final o = h.build(autoFlush: true);
    await h.store.commit(upserts: [
      OutboxEntry(
        id: 'x',
        idempotencyKey: 'k',
        sequence: 0,
        operation: 'op',
        resourceKey: 'r',
        payload: const {},
        createdAt: h.now,
      ),
    ]);
    await o.start();
    await h.settle();
    expect(h.sent.single.id, 'x');
  });

  test('autoFlush sends right after enqueue', () async {
    final o = h.build(autoFlush: true);
    await o.start();
    await h.add('a');
    await h.settle();
    expect(h.sentResources, ['a']);
  });

  test('autoFlush sends right after a retry', () async {
    final o = h.build(autoFlush: true);
    await o.start();
    h.respond = (_) => const SendResult.permanentFailure('no');
    final a = await h.add('a');
    await h.settle();
    h.respond = (_) => const SendResult.success();
    await o.retry(a.id);
    await h.settle();
    expect(h.sent, hasLength(2));
    expect(o.entries, isEmpty);
  });

  test('without autoFlush nothing is sent until flush is called', () async {
    final o = h.build();
    await o.start();
    await h.add('a');
    await h.settle();
    expect(h.sent, isEmpty);
  });

  test('periodic timer flushes on its interval', () {
    fakeAsync((async) {
      final o = h.build(flushInterval: const Duration(seconds: 30));
      o.start();
      async.flushMicrotasks();
      h.add('a');
      async.flushMicrotasks();
      expect(h.sent, isEmpty);

      async.elapse(const Duration(seconds: 29));
      expect(h.sent, isEmpty);
      async.elapse(const Duration(seconds: 1));
      expect(h.sent, hasLength(1));

      h.add('b');
      async.elapse(const Duration(seconds: 30));
      expect(h.sentResources, ['a', 'b']);
      o.dispose();
      async.flushMicrotasks();
      expect(async.periodicTimerCount, 0);
      expect(async.nonPeriodicTimerCount, 0);
    });
  });

  test('periodic timer skips while offline', () {
    fakeAsync((async) {
      final o = h.build(
        flushInterval: const Duration(seconds: 30),
        initiallyOnline: false,
      );
      o.start();
      async.flushMicrotasks();
      h.add('a');
      async.elapse(const Duration(minutes: 5));
      expect(h.sent, isEmpty);
      o.dispose();
      async.flushMicrotasks();
    });
  });

  test('dispose stops all triggers', () async {
    final net = StreamController<bool>.broadcast();
    final o = h.build(connectivity: net.stream, initiallyOnline: false);
    await o.start();
    await h.add('a');
    await o.dispose();
    net.add(true);
    await h.settle();
    expect(h.sent, isEmpty);
    expect(net.hasListener, isFalse);
    await net.close();
  });

  test('dispose waits for sends in progress', () async {
    final o = h.build();
    await o.start();
    await h.add('a');
    final gate = Completer<SendResult>();
    h.respond = (_) => gate.future;
    unawaited(o.flush());
    await h.settle();
    var disposed = false;
    final d = o.dispose().then((_) => disposed = true);
    await h.settle();
    expect(disposed, isFalse);
    gate.complete(const SendResult.success());
    await d;
    expect((await h.store.loadAll()), isEmpty);
  });

  test('errors in automatic flushes arrive as OutboxFlushFailed events',
      () async {
    final store = _InFlightWriteFails();
    final hh = Harness(store: store);
    final o = hh.build(autoFlush: true);
    await o.start();
    await hh.add('a'); // Enqueue is written; the auto flush's write fails.
    await hh.settle();
    final failed = (await hh.drained).whereType<OutboxFlushFailed>().single;
    expect(failed.error, isA<StateError>());
    expect(hh.sent, isEmpty);
    expect(o.entries.single.state, OutboxState.pending);
    await o.dispose();
  });
}

/// Fails any commit that marks an entry as in flight.
class _InFlightWriteFails extends InMemoryOutboxStore {
  @override
  Future<void> commit({
    Iterable<OutboxEntry> upserts = const [],
    Iterable<String> removals = const [],
  }) {
    if (upserts.any((e) => e.state == OutboxState.inFlight)) {
      return Future.error(StateError('disk full'));
    }
    return super.commit(upserts: upserts, removals: removals);
  }
}
