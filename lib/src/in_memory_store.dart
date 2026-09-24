import 'entry.dart';
import 'store.dart';

/// An [OutboxStore] that keeps entries in memory only.
///
/// Useful for tests and for queues that do not need to survive a restart.
class InMemoryOutboxStore implements OutboxStore {
  /// Creates a store, optionally seeded with [initial] entries (handy for
  /// simulating what a previous run left behind).
  InMemoryOutboxStore([Iterable<OutboxEntry> initial = const []]) {
    for (final e in initial) {
      _entries[e.id] = e;
    }
  }

  final Map<String, OutboxEntry> _entries = {};

  @override
  Future<List<OutboxEntry>> loadAll() async =>
      sortedBySequence(_entries.values);

  @override
  Future<void> commit({
    Iterable<OutboxEntry> upserts = const [],
    Iterable<String> removals = const [],
  }) async {
    for (final e in upserts) {
      _entries[e.id] = e;
    }
    removals.forEach(_entries.remove);
  }
}
