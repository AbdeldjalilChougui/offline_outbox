import 'entry.dart';

/// Persistence for outbox entries.
///
/// The outbox keeps its working set in memory and writes every change
/// through this interface before acting on it, so an implementation only
/// needs to load everything and apply batches of changes. Implementations
/// must:
///
/// * make each [commit] atomic: after a crash either all of its upserts and
///   removals are visible or none are;
/// * return entries from [loadAll] ordered by [OutboxEntry.sequence];
/// * store every field of [OutboxEntry.toJson] (the simplest way is to store
///   the encoded JSON, keyed by [OutboxEntry.id]).
///
/// The outbox never calls [commit] concurrently with itself.
abstract interface class OutboxStore {
  /// Every stored entry, ordered by [OutboxEntry.sequence].
  Future<List<OutboxEntry>> loadAll();

  /// Atomically inserts or replaces [upserts] (matched by id) and deletes the
  /// entries whose ids are in [removals]. Unknown ids in [removals] are
  /// ignored.
  Future<void> commit({
    Iterable<OutboxEntry> upserts = const [],
    Iterable<String> removals = const [],
  });
}

/// Sorts entries by [OutboxEntry.sequence]; shared by the bundled stores.
List<OutboxEntry> sortedBySequence(Iterable<OutboxEntry> entries) =>
    entries.toList()..sort((a, b) => a.sequence.compareTo(b.sequence));
