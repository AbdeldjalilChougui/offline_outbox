import 'dart:convert';

import 'package:offline_outbox/offline_outbox.dart';
import 'package:test/test.dart';

void main() {
  final created = DateTime.utc(2026, 1, 2, 3, 4, 5);

  OutboxEntry sample() => OutboxEntry(
        id: 'e1',
        idempotencyKey: 'k1',
        sequence: 3,
        operation: 'update',
        resourceKey: 'note:1',
        payload: const {
          'title': 'x',
          'tags': ['a', 'b'],
          'n': 1.5,
        },
        createdAt: created,
        updatedAt: created.add(const Duration(minutes: 1)),
        attempts: 2,
        nextAttemptAt: created.add(const Duration(hours: 1)),
        lastError: 'boom',
        state: OutboxState.needsAttention,
        group: 'notes',
        conflictCount: 1,
        conflictResolution: ConflictResolution.merged,
        serverState: const {'title': 'server'},
      );

  test('round-trips through JSON text', () {
    final e = sample();
    final back = OutboxEntry.fromJson(
      jsonDecode(jsonEncode(e.toJson())) as Map<String, Object?>,
    );
    expect(back, e);
    expect(back.toJson(), e.toJson());
  });

  test('defaults for a fresh entry', () {
    final e = OutboxEntry(
      id: 'e',
      idempotencyKey: 'k',
      sequence: 0,
      operation: 'create',
      resourceKey: 'r',
      payload: const {},
      createdAt: created,
    );
    expect(e.state, OutboxState.pending);
    expect(e.attempts, 0);
    expect(e.updatedAt, created);
    expect(e.group, isNull);
    expect(e.nextAttemptAt, isNull);
    expect(e.isTerminal, isFalse);
    final back = OutboxEntry.fromJson(
      jsonDecode(jsonEncode(e.toJson())) as Map<String, Object?>,
    );
    expect(back, e);
  });

  test('copyWith can clear nullable fields', () {
    final e = sample().copyWith(
      clearNextAttemptAt: true,
      clearLastError: true,
      clearServerState: true,
      clearConflictResolution: true,
    );
    expect(e.nextAttemptAt, isNull);
    expect(e.lastError, isNull);
    expect(e.serverState, isNull);
    expect(e.conflictResolution, isNull);
    expect(e.id, 'e1');
  });

  test('terminal states', () {
    expect(OutboxState.succeeded.isTerminal, isTrue);
    expect(OutboxState.cancelled.isTerminal, isTrue);
    expect(OutboxState.pending.isTerminal, isFalse);
    expect(OutboxState.inFlight.isTerminal, isFalse);
    expect(OutboxState.needsAttention.isTerminal, isFalse);
  });

  test('payload is unmodifiable', () {
    final e = sample();
    expect(() => e.payload['x'] = 1, throwsUnsupportedError);
  });

  test('fromJson rejects malformed input', () {
    expect(() => OutboxEntry.fromJson(const {'id': 1}), throwsFormatException);
  });
}
