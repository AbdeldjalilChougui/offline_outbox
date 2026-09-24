import 'package:offline_outbox/offline_outbox.dart';
import 'package:test/test.dart';

OutboxEntry entry(String id, {int seq = 0, OutboxState? state}) => OutboxEntry(
      id: id,
      idempotencyKey: 'k-$id',
      sequence: seq,
      operation: 'update',
      resourceKey: 'r-$id',
      payload: {'v': id},
      createdAt: DateTime.utc(2026),
      state: state ?? OutboxState.pending,
    );

/// Behaviour every [OutboxStore] implementation must have.
void storeContract(Future<OutboxStore> Function() create) {
  test('starts empty', () async {
    final store = await create();
    expect(await store.loadAll(), isEmpty);
  });

  test('upserts and removes in one commit', () async {
    final store = await create();
    await store.commit(upserts: [entry('a', seq: 1), entry('b', seq: 2)]);
    expect((await store.loadAll()).map((e) => e.id), ['a', 'b']);

    await store.commit(
      upserts: [entry('a', seq: 1, state: OutboxState.inFlight)],
      removals: ['b'],
    );
    final all = await store.loadAll();
    expect(all, hasLength(1));
    expect(all.single.state, OutboxState.inFlight);
  });

  test('returns entries ordered by sequence', () async {
    final store = await create();
    await store.commit(upserts: [
      entry('c', seq: 3),
      entry('a', seq: 1),
      entry('b', seq: 2),
    ]);
    expect((await store.loadAll()).map((e) => e.id), ['a', 'b', 'c']);
  });

  test('removing an unknown id is a no-op', () async {
    final store = await create();
    await store.commit(removals: ['nope']);
    expect(await store.loadAll(), isEmpty);
  });

  test('keeps the full entry intact', () async {
    final store = await create();
    final e = entry('a').copyWith(
      attempts: 3,
      lastError: 'x',
      nextAttemptAt: DateTime.utc(2026, 2),
      serverState: {'v': 1},
      conflictResolution: ConflictResolution.clientWins,
    );
    await store.commit(upserts: [e]);
    expect((await store.loadAll()).single, e);
  });
}
