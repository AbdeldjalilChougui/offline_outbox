@TestOn('vm')
library;

import 'dart:async';
import 'dart:io';

import 'package:offline_outbox/io.dart';
import 'package:offline_outbox/offline_outbox.dart';
import 'package:test/test.dart';

import 'harness.dart';

void main() {
  test('an outbox on a JSON file survives a kill mid-send', () async {
    final dir = await Directory.systemTemp.createTemp('outbox_it_');
    addTearDown(() => dir.delete(recursive: true));
    final path = '${dir.path}/queue.json';

    final first = Harness(store: JsonFileOutboxStore(path));
    await first.start();
    final a = await first.add('a', payload: {'v': 1}, group: 'g');
    await first.add('b', group: 'g');
    first.respond = (_) => Completer<SendResult>().future;
    unawaited(first.outbox.flush());
    await first.settle();
    await first.settle();

    final second = Harness(store: JsonFileOutboxStore(path));
    await second.start();
    expect(second.outbox.entry(a.id)!.state, OutboxState.pending);
    await second.outbox.flush();
    expect(second.sentResources, ['a', 'b']);
    expect(second.sent.first.idempotencyKey, a.idempotencyKey);
    expect(await JsonFileOutboxStore(path).loadAll(), isEmpty);
  });
}
