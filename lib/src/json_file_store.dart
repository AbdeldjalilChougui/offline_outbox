import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'entry.dart';
import 'store.dart';

/// An [OutboxStore] that keeps the whole queue in one JSON file.
///
/// Every [commit] rewrites the file atomically: the new content is written
/// to `<path>.tmp`, flushed to disk, and then renamed over `<path>`. A crash
/// at any point leaves either the old or the new file, never a torn one.
/// A leftover `<path>.tmp` from an interrupted write is ignored.
///
/// Rewriting the whole file suits queues of up to a few thousand entries.
/// For larger queues implement [OutboxStore] on a database.
class JsonFileOutboxStore implements OutboxStore {
  /// Creates a store backed by the file at [path]. Parent directories are
  /// created on the first write.
  JsonFileOutboxStore(this.path);

  /// Location of the JSON file.
  final String path;

  Map<String, OutboxEntry>? _cache;
  Future<void> _tail = Future.value();

  String get _tmpPath => '$path.tmp';

  @override
  Future<List<OutboxEntry>> loadAll() =>
      _serialized(() async => sortedBySequence((await _load()).values));

  @override
  Future<void> commit({
    Iterable<OutboxEntry> upserts = const [],
    Iterable<String> removals = const [],
  }) {
    final ups = upserts.toList();
    final rms = removals.toList();
    return _serialized(() async {
      final next = Map.of(await _load());
      for (final e in ups) {
        next[e.id] = e;
      }
      rms.forEach(next.remove);
      await _write(next);
      _cache = next; // Only after the file is safely replaced.
    });
  }

  Future<T> _serialized<T>(Future<T> Function() action) {
    final result = _tail.then((_) => action());
    _tail = result.then((_) {}, onError: (Object _) {});
    return result;
  }

  Future<Map<String, OutboxEntry>> _load() async {
    final cached = _cache;
    if (cached != null) return cached;
    final file = File(path);
    final entries = <String, OutboxEntry>{};
    if (await file.exists()) {
      final Object? doc;
      try {
        doc = jsonDecode(await file.readAsString());
      } on FormatException catch (e) {
        throw FormatException('Corrupt outbox file $path: ${e.message}');
      }
      if (doc is! Map<String, Object?> || doc['entries'] is! List) {
        throw FormatException('Unexpected outbox file layout in $path');
      }
      for (final raw in doc['entries'] as List) {
        if (raw is! Map<String, Object?>) {
          throw FormatException('Unexpected outbox entry in $path');
        }
        final e = OutboxEntry.fromJson(raw);
        entries[e.id] = e;
      }
    }
    return _cache = entries;
  }

  Future<void> _write(Map<String, OutboxEntry> entries) async {
    final target = File(path);
    await target.parent.create(recursive: true);
    final json = jsonEncode({
      'version': 1,
      'entries': [for (final e in sortedBySequence(entries.values)) e.toJson()],
    });
    final tmp = File(_tmpPath);
    final raf = await tmp.open(mode: FileMode.writeOnly);
    try {
      await raf.writeString(json);
      await raf.flush();
    } finally {
      await raf.close();
    }
    await tmp.rename(path);
  }
}
