import 'dart:async';

import 'package:sync_outbox/sync_outbox.dart';
import 'package:test/test.dart';

import 'harness.dart';

/// update after create/update: merge the payloads, keep the first operation.
Coalesce mergeUpdates(OutboxEntry queued, OutboxEntry incoming) =>
    Coalesce.merge({...queued.payload, ...incoming.payload});

/// delete after create: the server never needs to hear about it.
/// delete after update: the update is pointless, send only the delete.
Coalesce foldDelete(OutboxEntry queued, OutboxEntry incoming) =>
    switch (queued.operation) {
      'create' => Coalesce.cancelBoth,
      'update' => Coalesce.merge(const {}, operation: 'delete'),
      _ => Coalesce.keepBoth,
    };

const coalescers = {'update': mergeUpdates, 'delete': foldDelete};

void main() {
  late Harness h;
  setUp(() => h = Harness());

  test('several updates to one resource merge into one entry', () async {
    await h.start(coalescers: coalescers);
    final first = await h.add('note:1', payload: {'title': 'a', 'body': 'x'});
    h.advance(const Duration(seconds: 5));
    final merged = await h.add('note:1', payload: {'title': 'b'});
    await h.add('note:1', payload: {'pinned': true});

    expect(merged.id, first.id);
    expect(h.outbox.entries, hasLength(1));
    final e = h.outbox.entries.single;
    expect(e.payload, {'title': 'b', 'body': 'x', 'pinned': true});
    expect(e.sequence, first.sequence);
    expect(e.createdAt, first.createdAt);
    expect(e.updatedAt, h.now);
    expect(e.idempotencyKey, first.idempotencyKey);
    expect((await h.store.loadAll()).single, e);

    await h.outbox.flush();
    expect(h.sent, hasLength(1));
    expect((await h.drained).whereType<OutboxCoalesced>(), hasLength(2));
  });

  test('update after create stays a create with the merged payload', () async {
    await h.start(coalescers: coalescers);
    await h.add('note:1', op: 'create', payload: {'title': 'a'});
    await h.add('note:1', payload: {'title': 'b'});
    expect(h.outbox.entries.single.operation, 'create');
    expect(h.outbox.entries.single.payload, {'title': 'b'});
  });

  test('create followed by delete cancels both', () async {
    await h.start(coalescers: coalescers);
    final create = await h.add('note:1', op: 'create');
    final delete = await h.add('note:1', op: 'delete');

    expect(delete.state, OutboxState.cancelled);
    expect(h.outbox.entries, isEmpty);
    expect(await h.store.loadAll(), isEmpty);
    await h.outbox.flush();
    expect(h.sent, isEmpty);

    final cancelled = (await h.drained).whereType<OutboxCancelled>().toList();
    expect(cancelled.map((e) => e.entry.id), [create.id, delete.id]);
    expect(cancelled.every((e) => e.reason == CancelReason.coalesced), isTrue);
  });

  test('a coalescer can change the operation', () async {
    await h.start(coalescers: coalescers);
    await h.add('note:1', payload: {'title': 'a'});
    await h.add('note:1', op: 'delete');
    expect(h.outbox.entries.single.operation, 'delete');
    expect(h.outbox.entries.single.payload, isEmpty);
  });

  test('keepBoth enqueues normally', () async {
    await h.start(coalescers: {'update': (_, __) => Coalesce.keepBoth});
    await h.add('note:1');
    await h.add('note:1');
    expect(h.outbox.entries, hasLength(2));
  });

  test('operations without a coalescer are never combined', () async {
    await h.start(coalescers: coalescers);
    await h.add('note:1', op: 'create');
    await h.add('note:1', op: 'create');
    expect(h.outbox.entries, hasLength(2));
  });

  test('only combines entries for the same resource and group', () async {
    await h.start(coalescers: coalescers);
    await h.add('note:1', group: 'a');
    await h.add('note:2', group: 'a');
    await h.add('note:1', group: 'b');
    await h.add('note:1');
    expect(h.outbox.entries, hasLength(4));
  });

  test('never merges into an entry that was already attempted', () async {
    await h.start(coalescers: coalescers);
    await h.add('note:1', payload: {'v': 1});
    h.respond = (_) => const SendResult.retryableFailure();
    await h.outbox.flush();

    // The server may already have applied v1; merging v2 into it under the
    // same idempotency key could be deduplicated away.
    await h.add('note:1', payload: {'v': 2});
    expect(h.outbox.entries, hasLength(2));
  });

  test('never merges into an in-flight entry', () async {
    await h.start(coalescers: coalescers);
    await h.add('note:1', payload: {'v': 1});
    final gate = Completer<SendResult>();
    h.respond = (_) => gate.future;
    final f = h.outbox.flush();
    await h.settle();
    await h.add('note:1', payload: {'v': 2});
    expect(h.outbox.entries, hasLength(2));
    gate.complete(const SendResult.success());
    await f;
  });

  test('never merges into a needsAttention entry', () async {
    await h.start(coalescers: coalescers);
    await h.add('note:1', payload: {'v': 1});
    h.respond = (_) => const SendResult.permanentFailure('no');
    await h.outbox.flush();
    await h.add('note:1', payload: {'v': 2});
    expect(h.outbox.entries, hasLength(2));
  });

  test('only looks at the most recent entry for the resource', () async {
    await h.start(coalescers: coalescers);
    await h.add('note:1', op: 'create'); // not attempted, but older
    await h.add('note:1', op: 'create'); // most recent, not coalescible
    await h.add('note:1', op: 'delete'); // cancels only the most recent
    expect(h.outbox.entries.map((e) => e.operation), ['create']);
  });
}
