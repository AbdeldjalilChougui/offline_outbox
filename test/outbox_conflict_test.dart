import 'package:offline_outbox/offline_outbox.dart';
import 'package:test/test.dart';

import 'harness.dart';

void main() {
  late Harness h;
  setUp(() => h = Harness());

  SendResult conflictThenOk(OutboxEntry e) => e.conflictResolution == null
      ? SendResult.conflict(const {'title': 'server'},
          serverModifiedAt: DateTime.utc(2026, 1, 1, 11))
      : const SendResult.success();

  test('serverWins cancels the local change and reports the server state',
      () async {
    await h.start(defaultConflictStrategy: ConflictStrategy.serverWins);
    final a = await h.add('a', group: 'g');
    await h.add('b', group: 'g');
    h.respond = (e) => e.resourceKey == 'a'
        ? const SendResult.conflict({'v': 'server'})
        : const SendResult.success();
    await h.outbox.flush();

    expect(h.sentResources, ['a', 'b']); // The group moves on.
    expect(h.outbox.entries, isEmpty);
    final resolved =
        (await h.drained).whereType<OutboxConflictResolved>().single;
    expect(resolved.outcome, ConflictOutcome.serverWins);
    expect(resolved.serverState, {'v': 'server'});
    expect(resolved.entry.id, a.id);
    final cancelled = (await h.drained).whereType<OutboxCancelled>().single;
    expect(cancelled.reason, CancelReason.serverWins);
  });

  test('clientWins resends at once with a fresh key and the server state',
      () async {
    await h.start(defaultConflictStrategy: ConflictStrategy.clientWins);
    final a = await h.add('a', payload: {'title': 'mine'});
    h.respond = conflictThenOk;
    final r = await h.outbox.flush();

    expect(h.sent, hasLength(2));
    final resend = h.sent.last;
    expect(resend.conflictResolution, ConflictResolution.clientWins);
    expect(resend.serverState, {'title': 'server'});
    expect(resend.payload, {'title': 'mine'});
    expect(resend.idempotencyKey, isNot(a.idempotencyKey));
    expect(resend.conflictCount, 1);
    expect(h.outbox.entries, isEmpty);
    expect(r.succeeded, 1);
    expect((await h.drained).whereType<OutboxConflictResolved>().single.outcome,
        ConflictOutcome.clientWins);
  });

  group('lastWriteWins', () {
    test('local change newer than the server: client wins', () async {
      await h.start(defaultConflictStrategy: ConflictStrategy.lastWriteWins);
      await h.add('a'); // updatedAt 12:00, server 11:00
      h.respond = conflictThenOk;
      await h.outbox.flush();
      expect(h.sent, hasLength(2));
      expect(h.sent.last.conflictResolution, ConflictResolution.clientWins);
    });

    test('server change newer (or equal): server wins', () async {
      await h.start(defaultConflictStrategy: ConflictStrategy.lastWriteWins);
      await h.add('a');
      await h.add('b');
      h.respond = (e) => SendResult.conflict(
            null,
            serverModifiedAt: e.resourceKey == 'a'
                ? h.now.add(const Duration(minutes: 1))
                : h.now,
          );
      await h.outbox.flush();
      expect(h.sent, hasLength(2));
      expect(h.outbox.entries, isEmpty);
      expect(
        (await h.drained)
            .whereType<OutboxConflictResolved>()
            .map((e) => e.outcome),
        [ConflictOutcome.serverWins, ConflictOutcome.serverWins],
      );
    });

    test('uses the time of the latest coalesced change', () async {
      await h.start(
        defaultConflictStrategy: ConflictStrategy.lastWriteWins,
        coalescers: {
          'update': (q, i) => Coalesce.merge({...q.payload, ...i.payload}),
        },
      );
      await h.add('a', payload: {'v': 1}); // 12:00
      h.advance(const Duration(hours: 2));
      await h.add('a', payload: {'v': 2}); // 14:00
      h.respond = (e) => e.conflictResolution == null
          ? SendResult.conflict(null,
              serverModifiedAt: DateTime.utc(2026, 1, 1, 13))
          : const SendResult.success();
      await h.outbox.flush();
      expect(h.sent.last.conflictResolution, ConflictResolution.clientWins);
    });

    test('without a server timestamp it asks for attention', () async {
      await h.start(defaultConflictStrategy: ConflictStrategy.lastWriteWins);
      final a = await h.add('a');
      h.respond = (_) => const SendResult.conflict({'v': 1});
      await h.outbox.flush();
      expect(h.outbox.entry(a.id)!.state, OutboxState.needsAttention);
      expect(h.outbox.entry(a.id)!.serverState, {'v': 1});
    });
  });

  group('merge', () {
    test('resends the merged payload with a fresh key', () async {
      Object? seenServer;
      await h.start(
        defaultConflictStrategy: ConflictStrategy.merge((local, server) {
          seenServer = server;
          return {...(server! as Map<String, Object?>), ...local.payload};
        }),
      );
      final a = await h.add('a', payload: {'body': 'mine'});
      h.respond = conflictThenOk;
      await h.outbox.flush();

      expect(seenServer, {'title': 'server'});
      expect(h.sent.last.payload, {'title': 'server', 'body': 'mine'});
      expect(h.sent.last.conflictResolution, ConflictResolution.merged);
      expect(h.sent.last.idempotencyKey, isNot(a.idempotencyKey));
      expect(h.sent.last.updatedAt, h.now);
      expect(
          (await h.drained).whereType<OutboxConflictResolved>().single.outcome,
          ConflictOutcome.merged);
    });

    test('an async merge callback works', () async {
      await h.start(
        defaultConflictStrategy: ConflictStrategy.merge(
          (local, server) async => {'merged': true},
        ),
      );
      await h.add('a');
      h.respond = conflictThenOk;
      await h.outbox.flush();
      expect(h.sent.last.payload, {'merged': true});
    });

    test('returning null asks for attention', () async {
      await h.start(
        defaultConflictStrategy: ConflictStrategy.merge((_, __) => null),
      );
      final a = await h.add('a');
      h.respond = conflictThenOk;
      await h.outbox.flush();
      expect(h.sent, hasLength(1));
      expect(h.outbox.entry(a.id)!.state, OutboxState.needsAttention);
    });

    test('a throwing callback asks for attention', () async {
      await h.start(
        defaultConflictStrategy:
            ConflictStrategy.merge((_, __) => throw StateError('cannot')),
      );
      final a = await h.add('a');
      h.respond = conflictThenOk;
      await h.outbox.flush();
      final e = h.outbox.entry(a.id)!;
      expect(e.state, OutboxState.needsAttention);
      expect(e.lastError, contains('cannot'));
    });
  });

  test('manual parks the entry for a person and blocks its group', () async {
    await h.start(); // manual is the default
    final a = await h.add('a', group: 'g');
    await h.add('b', group: 'g');
    h.respond = (_) => const SendResult.conflict({'v': 'server'});
    await h.outbox.flush();

    expect(h.sentResources, ['a']);
    final e = h.outbox.entry(a.id)!;
    expect(e.state, OutboxState.needsAttention);
    expect(e.serverState, {'v': 'server'});
    expect(e.conflictCount, 1);
    expect((await h.drained).whereType<OutboxConflictResolved>().single.outcome,
        ConflictOutcome.manual);
    expect((await h.drained).whereType<OutboxNeedsAttention>(), hasLength(1));

    // The person keeps their version.
    h.respond = (_) => const SendResult.success();
    await h.outbox.retry(a.id, resolution: ConflictResolution.clientWins);
    await h.outbox.flush();
    expect(h.sent[1].conflictResolution, ConflictResolution.clientWins);
    expect(h.sent[1].idempotencyKey, isNot(a.idempotencyKey));
    expect(h.sentResources, ['a', 'a', 'b']);
  });

  test('per-operation strategies override the default', () async {
    await h.start(
      defaultConflictStrategy: ConflictStrategy.manual,
      conflictStrategies: {'like': ConflictStrategy.serverWins},
    );
    final like = await h.add('a', op: 'like');
    final edit = await h.add('b', op: 'edit');
    h.respond = (_) => const SendResult.conflict(null);
    await h.outbox.flush();
    expect(h.outbox.entry(like.id), isNull);
    expect(h.outbox.entry(edit.id)!.state, OutboxState.needsAttention);
  });

  test('stops resolving after maxConflictResolutions conflicts', () async {
    await h.start(
      defaultConflictStrategy: ConflictStrategy.clientWins,
      maxConflictResolutions: 2,
    );
    final a = await h.add('a');
    h.respond = (_) => const SendResult.conflict({'v': 1});
    await h.outbox.flush();
    expect(h.sent, hasLength(3));
    final e = h.outbox.entry(a.id)!;
    expect(e.state, OutboxState.needsAttention);
    expect(e.conflictCount, 3);
    expect(e.lastError, contains('conflict'));
  });
}
