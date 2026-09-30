// A runnable tour of sync_outbox with a simulated flaky server.
//
// Run with: dart run example/main.dart
import 'dart:async';

import 'package:sync_outbox/sync_outbox.dart';

/// Pretends to be a REST API: the first attempt at each entry times out,
/// and editing note 2 conflicts once with a newer server version.
class FakeServer {
  final _seen = <String>{};
  var _conflicted = false;

  Future<SendResult> send(OutboxEntry e) async {
    // A real sender would POST/PATCH/DELETE here and pass
    // e.idempotencyKey in an Idempotency-Key header.
    if (_seen.add(e.id)) {
      return const SendResult.retryableFailure(error: 'timeout');
    }
    if (e.resourceKey == 'note:2' && !_conflicted) {
      _conflicted = true;
      return SendResult.conflict(
        {'title': 'Edited on the web', 'body': 'server body'},
        serverModifiedAt: DateTime.now().subtract(const Duration(hours: 1)),
      );
    }
    print('  server applied ${e.operation} ${e.resourceKey} ${e.payload} '
        '(key ${e.idempotencyKey.substring(0, 8)}, '
        'resolution ${e.conflictResolution?.name ?? '-'})');
    return const SendResult.success();
  }
}

Future<void> main() async {
  final server = FakeServer();
  final network = StreamController<bool>();

  final outbox = Outbox(
    store: InMemoryOutboxStore(), // JsonFileOutboxStore from io.dart persists.
    sender: server.send,
    connectivity: network.stream,
    initiallyOnline: false,
    retryPolicy: ExponentialBackoff(
      initialDelay: const Duration(milliseconds: 50),
      maxDelay: const Duration(seconds: 1),
    ),
    coalescers: {
      // Later edits of a queued, unsent note fold into it.
      'update': (queued, incoming) =>
          Coalesce.merge({...queued.payload, ...incoming.payload}),
      // Creating then deleting a note before it is sent is a no-op.
      'delete': (queued, incoming) => queued.operation == 'create'
          ? Coalesce.cancelBoth
          : Coalesce.keepBoth,
    },
    defaultConflictStrategy: ConflictStrategy.merge(
      (local, server) =>
          {...?(server as Map<String, Object?>?), ...local.payload},
    ),
  );

  outbox.counts
      .map((c) => c.outstanding)
      .distinct()
      .listen((n) => print('badge: $n unsent'));
  outbox.events.listen((e) {
    if (e is OutboxEntryEvent) {
      print('event: ${e.runtimeType} ${e.entry.operation} '
          '${e.entry.resourceKey}');
    }
  });
  await outbox.start();

  print('Offline: queueing work');
  await outbox.enqueue(
    operation: 'create',
    resourceKey: 'note:1',
    payload: {'title': 'Groceries'},
    group: 'notes',
  );
  await outbox.enqueue(
    operation: 'update',
    resourceKey: 'note:1',
    payload: {'body': 'milk, eggs'},
    group: 'notes',
  );
  await outbox.enqueue(
    operation: 'update',
    resourceKey: 'note:2',
    payload: {'body': 'my body'},
    group: 'notes',
  );
  await outbox.enqueue(
      operation: 'create', resourceKey: 'note:3', group: 'notes');
  await outbox.enqueue(
      operation: 'delete', resourceKey: 'note:3', group: 'notes');
  print('Queued: ${outbox.entries.map((e) => e.resourceKey).toList()}');

  print('Back online');
  network.add(true);

  // Wait until the queue is empty.
  await outbox.counts.firstWhere((c) => c.outstanding == 0);
  await outbox.dispose();
  await network.close();
  print('All delivered.');
}
