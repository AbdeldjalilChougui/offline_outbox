@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';

import 'package:sync_outbox/io.dart';
import 'package:test/test.dart';

import 'store_contract.dart';

void main() {
  late Directory dir;
  setUp(() async => dir = await Directory.systemTemp.createTemp('outbox_'));
  tearDown(() async => dir.delete(recursive: true));

  String path() => '${dir.path}/outbox.json';

  group('JsonFileOutboxStore', () {
    storeContract(() async => JsonFileOutboxStore(path()));

    test('reloads what a previous instance wrote', () async {
      final first = JsonFileOutboxStore(path());
      await first.commit(upserts: [entry('a', seq: 1), entry('b', seq: 2)]);
      await first.commit(removals: ['a']);

      final second = JsonFileOutboxStore(path());
      expect((await second.loadAll()).map((e) => e.id), ['b']);
    });

    test('writes a versioned JSON document', () async {
      final store = JsonFileOutboxStore(path());
      await store.commit(upserts: [entry('a')]);
      final doc = jsonDecode(await File(path()).readAsString()) as Map;
      expect(doc['version'], 1);
      expect((doc['entries'] as List).single, containsPair('id', 'a'));
    });

    test('leaves no temporary file behind after a commit', () async {
      final store = JsonFileOutboxStore(path());
      await store.commit(upserts: [entry('a')]);
      expect(
          dir.listSync().map((f) => f.uri.pathSegments.last), ['outbox.json']);
    });

    test('a failed write leaves the previous file untouched', () async {
      final store = JsonFileOutboxStore(path());
      await store.commit(upserts: [entry('a', seq: 1)]);
      final before = await File(path()).readAsString();

      // Block the temporary file path so the next write cannot happen.
      await Directory('${path()}.tmp').create();
      await expectLater(
        store.commit(upserts: [entry('b', seq: 2)]),
        throwsA(isA<FileSystemException>()),
      );
      expect(await File(path()).readAsString(), before);

      // The store's view did not move ahead of the disk either.
      expect((await store.loadAll()).map((e) => e.id), ['a']);
      await Directory('${path()}.tmp').delete();
      expect((await JsonFileOutboxStore(path()).loadAll()).map((e) => e.id),
          ['a']);
    });

    test('ignores a torn temporary file left by a crash', () async {
      final store = JsonFileOutboxStore(path());
      await store.commit(upserts: [entry('a')]);
      await File('${path()}.tmp').writeAsString('{"version":1,"entr');

      final reopened = JsonFileOutboxStore(path());
      expect((await reopened.loadAll()).single.id, 'a');
      await reopened.commit(upserts: [entry('b', seq: 1)]);
      expect(File('${path()}.tmp').existsSync(), isFalse);
    });

    test('concurrent commits are serialized and none is lost', () async {
      final store = JsonFileOutboxStore(path());
      await Future.wait([
        for (var i = 0; i < 20; i++)
          store.commit(upserts: [entry('e$i', seq: i)]),
      ]);
      final reloaded = await JsonFileOutboxStore(path()).loadAll();
      expect(reloaded, hasLength(20));
    });

    test('refuses a corrupt file instead of silently dropping entries',
        () async {
      await File(path()).writeAsString('not json');
      await expectLater(
        JsonFileOutboxStore(path()).loadAll(),
        throwsFormatException,
      );
    });

    test('creates missing parent directories', () async {
      final nested = JsonFileOutboxStore('${dir.path}/a/b/outbox.json');
      await nested.commit(upserts: [entry('a')]);
      expect(File('${dir.path}/a/b/outbox.json').existsSync(), isTrue);
    });
  });
}
