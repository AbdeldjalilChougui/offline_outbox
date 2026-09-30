import 'dart:async';

import 'package:sync_outbox/sync_outbox.dart';
import 'package:test/test.dart';

import 'harness.dart';

OutboxEntry stored(String id, int seq, OutboxState state,
        {int attempts = 0, String? group}) =>
    OutboxEntry(
      id: id,
      idempotencyKey: 'key-$id',
      sequence: seq,
      operation: 'update',
      resourceKey: 'r-$id',
      payload: const {},
      createdAt: DateTime.utc(2025),
      attempts: attempts,
      state: state,
      group: group,
    );

/// A store whose commits can be made to fail.
class FlakyStore extends InMemoryOutboxStore {
  bool fail = false;
  @override
  Future<void> commit({
    Iterable<OutboxEntry> upserts = const [],
    Iterable<String> removals = const [],
  }) {
    if (fail) return Future.error(StateError('disk full'));
    return super.commit(upserts: upserts, removals: removals);
  }
}

void main() {
  test('entries in flight at startup go back to pending', () async {
    final store = InMemoryOutboxStore([
      stored('a', 0, OutboxState.inFlight, attempts: 2),
      stored('b', 1, OutboxState.pending),
    ]);
    final h = Harness(store: store);
    await h.start();
    await h.settle();

    final a = h.outbox.entry('a')!;
    expect(a.state, OutboxState.pending);
    expect(a.attempts, 2);
    expect(a.nextAttemptAt, isNull);
    expect((await store.loadAll()).first.state, OutboxState.pending);
    expect((await h.drained).whereType<OutboxRecovered>().single.entry.id, 'a');

    await h.outbox.flush();
    expect(h.sent.first.id, 'a');
    expect(h.sent.first.idempotencyKey, 'key-a'); // Safe to resend.
    expect(h.sent.first.attempts, 3);
  });

  test('a kill during a send is recovered by the next run', () async {
    final store = InMemoryOutboxStore();
    final first = Harness(store: store);
    await first.start();
    final a = await first.add('a', group: 'g');
    await first.add('b', group: 'g');
    first.respond = (_) => Completer<SendResult>().future; // Never answers.
    unawaited(first.outbox.flush());
    await first.settle();
    expect((await store.loadAll()).first.state, OutboxState.inFlight);

    // "Restart": a new outbox over the same storage.
    final second = Harness(store: store);
    await second.start();
    await second.outbox.flush();
    expect(second.sent.map((e) => e.id), [a.id, isNot(a.id)]);
    expect(second.sent.first.idempotencyKey, a.idempotencyKey);
    expect(second.outbox.entries, isEmpty);
  });

  test('other states survive a restart unchanged', () async {
    final entries = [
      stored('p', 0, OutboxState.pending, attempts: 1),
      stored('n', 1, OutboxState.needsAttention, attempts: 4),
      stored('s', 2, OutboxState.succeeded, attempts: 1),
      stored('c', 3, OutboxState.cancelled),
    ];
    final h = Harness(store: InMemoryOutboxStore(entries));
    await h.start(keepCompleted: true);
    expect(h.outbox.entries, entries);
    expect(h.outbox.currentCounts,
        const OutboxCounts(pending: 1, needsAttention: 1));
  });

  test('sequence numbers continue after the stored maximum', () async {
    final h = Harness(
      store: InMemoryOutboxStore([stored('x', 41, OutboxState.pending)]),
    );
    await h.start();
    expect((await h.add('y')).sequence, 42);
  });

  test('does not send when the in-flight mark cannot be persisted', () async {
    final store = FlakyStore();
    final h = Harness(store: store);
    await h.start();
    final a = await h.add('a');
    store.fail = true;
    await expectLater(h.outbox.flush(), throwsStateError);
    expect(h.sent, isEmpty);
    expect(h.outbox.entry(a.id)!.state, OutboxState.pending);
    expect(h.outbox.entry(a.id)!.attempts, 0);

    store.fail = false;
    await h.outbox.flush();
    expect(h.sent, hasLength(1));
  });

  test('a failed enqueue write leaves nothing behind', () async {
    final store = FlakyStore();
    final h = Harness(store: store);
    await h.start();
    store.fail = true;
    await expectLater(h.add('a'), throwsStateError);
    expect(h.outbox.entries, isEmpty);
    expect(h.outbox.currentCounts, const OutboxCounts());
  });
}
