# offline_outbox

A reliable, storage-agnostic outbox for offline-first Dart and Flutter apps. Your app
queues mutations while offline, and the outbox delivers them later, safely and in order.

[![pub package](https://img.shields.io/pub/v/offline_outbox.svg)](https://pub.dev/packages/offline_outbox)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Features

- **Durable queue.** Every entry carries a client idempotency key (a UUID v4 generated
  without extra packages, or your own generator). An entry is written to storage as
  *in flight* before it is sent. If the app is killed mid-request, the next `start()`
  puts the entry back to *pending* and resends it with the same key, and the server
  deduplicates.
- **Typed send results.** Your `Sender` returns one of `success`,
  `retryableFailure(retryAfter?)`, `permanentFailure(reason)` or `conflict(serverState)`.
  If it throws, the outbox treats that as a retryable failure.
- **Ordering groups.** Entries in the same group go out one at a time, in FIFO order.
  Different groups run in parallel, up to a concurrency limit. A failing entry blocks only
  its own group: later entries in that group wait, and other groups keep going.
- **Backoff.** Exponential backoff with jitter and a cap, or a fixed schedule. The
  `ScheduledBackoff.oneMinuteToOneDay` preset (1 m, 5 m, 15 m, 1 h, 6 h, then daily) never
  gives up. A server `retryAfter` takes precedence over the policy.
- **Nothing is dropped silently.** Rejected entries go to *needs attention* and stay there
  until you call `retry(id)`, `retryAllNeedingAttention()` or `discard(id)`.
- **Coalescing,** configured per operation type and optional. For example, several edits
  to an unsent note merge into one entry, and a create followed by a delete cancels both.
  You supply the merge function.
- **Conflict strategies:** `serverWins`, `clientWins`, `lastWriteWins`, `merge(callback)`
  and `manual`. You set a default and can override it per operation type.
- **Streams for the UI.** `Stream<OutboxEvent>` reports everything that happens, and
  `Stream<OutboxCounts>` drives badges such as "3 unsent" or "1 needs attention".
- **Triggers.** The outbox takes a `Stream<bool>` of connectivity, so it does not depend on
  `connectivity_plus`. It can also flush on a periodic timer, when a retry comes due, and
  right after enqueue.
- **Deterministic tests.** The clock, random source and id generator are all injectable,
  and it works with `fake_async`.
- **Storage-agnostic.** The package includes `InMemoryOutboxStore` and an atomic
  `JsonFileOutboxStore`. Any database fits behind a two-method interface. The main library
  is pure Dart with no dependencies and works on the web.

## Install

```sh
dart pub add offline_outbox
```

## Usage

```dart
import 'package:offline_outbox/io.dart'; // JsonFileOutboxStore (not on the web)
import 'package:offline_outbox/offline_outbox.dart';

/// Maps your HTTP client's answers onto SendResult.
Future<SendResult> send(OutboxEntry e) async {
  // Send e.idempotencyKey (for example as an Idempotency-Key header) so the
  // server can recognise a resend of a request it already applied.
  final status = await fakeHttp(e.operation, e.resourceKey, e.payload,
      idempotencyKey: e.idempotencyKey);
  return switch (status) {
    >= 200 && < 300 => const SendResult.success(),
    409 => const SendResult.conflict({'title': 'server copy'}),
    429 || >= 500 => const SendResult.retryableFailure(error: 'server busy'),
    _ => SendResult.permanentFailure('HTTP $status'),
  };
}

Future<void> main() async {
  final outbox = Outbox(
    store: JsonFileOutboxStore('/path/to/app_dir/outbox.json'),
    sender: send,
    // connectivity: Connectivity().onConnectivityChanged
    //     .map((r) => r.any((c) => c != ConnectivityResult.none)),
    flushInterval: const Duration(minutes: 1),
    retryPolicy: ScheduledBackoff.oneMinuteToOneDay,
    defaultConflictStrategy: ConflictStrategy.lastWriteWins,
  );
  await outbox.start(); // Recovers interrupted sends, then flushes.

  await outbox.enqueue(
    operation: 'update',
    resourceKey: 'note:42',
    payload: {'title': 'Groceries'},
    group: 'notes', // FIFO with the other note mutations
  );

  outbox.counts.listen((c) => print('${c.outstanding} unsent, '
      '${c.needsAttention} need attention'));

  await outbox.dispose();
}

Future<int> fakeHttp(String op, String key, Map<String, Object?> body,
        {required String idempotencyKey}) async =>
    200;
```

A complete runnable tour, with a simulated flaky server, is in
[`example/main.dart`](example/main.dart).

### Entry lifecycle

```
enqueue ──► pending ──► inFlight ──► succeeded (deleted unless keepCompleted)
              ▲  ▲          │
              │  └──────────┤ retryableFailure: back to pending with nextAttemptAt
              │             │ conflict (clientWins / merge): pending, fresh key, resent now
              │             │ conflict (serverWins) ──► cancelled
              │             ▼
   retry() ───┴──── needsAttention  ◄── permanentFailure, manual conflict,
                        │                maxAttempts reached
            discard() ──┴──► cancelled
```

On `start()`, any entry still marked `inFlight` goes back to `pending`.

### Ordering groups and concurrency

- Entries with the same `group` are sent strictly one after another, oldest first. While
  the oldest entry of a group is backing off or needs attention, the rest of the group
  waits.
- An entry with no group (`group: null`) is independent of every other entry.
- To get a single global FIFO that stops at the first error, put every entry in one group.
- `maxConcurrency` (default 4) caps how many entries are in flight across all groups.
  Groups start in the order of their oldest entry.

### Retry policies

```dart
final policies = <RetryPolicy>[
  // 1 s, 2 s, 4 s … capped at 5 min, randomly shortened by up to 20 %.
  ExponentialBackoff(),
  ExponentialBackoff(
    initialDelay: const Duration(seconds: 2),
    multiplier: 3,
    maxDelay: const Duration(hours: 1),
    jitter: 0.5,
    maxAttempts: 10, // then needs attention
  ),
  ScheduledBackoff.oneMinuteToOneDay,
];
```

Jitter only ever shortens a delay, so the cap always holds. With `maxAttempts` set, the
entry moves to *needs attention* after that many attempts. A later `retry(id)` allows one
more attempt.

### Coalescing

Coalescers are registered by the operation type of the incoming entry. A coalescer is
consulted only when the most recent entry for the same `resourceKey` and `group` is
pending and has never been attempted. This guarantees the server has not seen the
queued entry.

```dart
final coalescers = <String, Coalescer>{
  // Edits fold into the queued create/update; its position and key are kept.
  'update': (queued, incoming) =>
      Coalesce.merge({...queued.payload, ...incoming.payload}),
  // Create then delete: neither needs to reach the server.
  'delete': (queued, incoming) => queued.operation == 'create'
      ? Coalesce.cancelBoth
      : Coalesce.merge(const {}, operation: 'delete'),
};
```

### Conflicts

When the sender returns `SendResult.conflict(serverState, serverModifiedAt: ...)`:

| Strategy | Outcome |
| --- | --- |
| `serverWins` | The entry is cancelled. `OutboxConflictResolved` carries `serverState` so you can update your local copy. The group moves on. |
| `clientWins` | The entry is resent at once, with `conflictResolution == clientWins` and `serverState` attached. Your sender can then force the overwrite, for example by dropping `If-Match`. |
| `lastWriteWins` | The client wins if the entry's `updatedAt` is strictly after `serverModifiedAt`, otherwise the server wins. Without a server timestamp, the entry needs attention. |
| `merge(callback)` | The entry is resent with the payload the callback returns. If the callback returns `null` or throws, the entry needs attention. |
| `manual` (default) | The entry goes to needs attention with `serverState` attached. Resolve it with `retry(id, payload: ..., resolution: ...)` or `discard(id)`. |

A resend after a conflict uses a **fresh idempotency key**. The refused request was never
applied, so a new key cannot cause a duplicate, and it prevents a server from replaying
its cached conflict response. `maxConflictResolutions` (default 3) stops two writers from
overwriting each other forever.

### Triggers

| Trigger | Configuration |
| --- | --- |
| Connectivity regained | `connectivity: Stream<bool>`, `initiallyOnline` |
| Periodic | `flushInterval` |
| Backed-off entry becomes due | `scheduleRetries` (default `true`) |
| `start`, `enqueue`, `retry`, `discard` | `autoFlush` (default `true`) |
| Manual | `await outbox.flush()` returns a `FlushResult` |

While the outbox is offline, `flush()` sends nothing. When connectivity drops during a
flush, requests already on the wire are allowed to finish, but no new ones start.
Automatic flushes report failures as `OutboxFlushFailed` events, never as stream errors.

### Testing your code with the outbox

```dart
import 'dart:math';

import 'package:fake_async/fake_async.dart';
import 'package:offline_outbox/offline_outbox.dart';

void example() {
  fakeAsync((async) {
    final start = DateTime.utc(2026);
    var ids = 0;
    final outbox = Outbox(
      store: InMemoryOutboxStore(),
      sender: (e) async => const SendResult.success(),
      clock: () => start.add(async.elapsed),
      random: Random(1),
      idGenerator: () => 'id${ids++}',
    );
    outbox.start();
    async.flushMicrotasks();
    // ... enqueue, async.elapse(...), expect(...)
  });
}
```

## Storage backends

`OutboxStore` has two methods:

```dart
abstract interface class OutboxStore {
  Future<List<OutboxEntry>> loadAll(); // ordered by sequence
  Future<void> commit({
    Iterable<OutboxEntry> upserts = const [],
    Iterable<String> removals = const [],
  }); // atomic
}
```

The outbox keeps its working set in memory and writes every change through `commit`
before acting on it. `commit` is never called concurrently. The simplest schema is one row
per entry: the `id` as primary key, the `sequence` for ordering, and
`jsonEncode(entry.toJson())` as the body.

- **`InMemoryOutboxStore`** is for tests and for queues that do not need to survive a
  restart.
- **`JsonFileOutboxStore`** comes from `package:offline_outbox/io.dart`. On each commit it
  writes `<path>.tmp`, flushes it, and renames it over the file, so a crash leaves either
  the old file or the new one, never a torn file. It suits queues of up to a few thousand
  entries.

### drift

This example was compile-checked and run against drift 2.28 with an in-memory SQLite
database.

```dart
import 'dart:convert';

import 'package:drift/drift.dart';
import 'package:offline_outbox/offline_outbox.dart';

part 'drift_outbox_store.g.dart';

/// One row per entry; the entry itself is stored as JSON.
class OutboxRows extends Table {
  TextColumn get id => text()();
  IntColumn get sequence => integer()();
  TextColumn get body => text()();

  @override
  Set<Column> get primaryKey => {id};
}

@DriftDatabase(tables: [OutboxRows])
class AppDatabase extends _$AppDatabase {
  AppDatabase(super.e);

  @override
  int get schemaVersion => 1;
}

class DriftOutboxStore implements OutboxStore {
  DriftOutboxStore(this.db);

  final AppDatabase db;

  @override
  Future<List<OutboxEntry>> loadAll() async {
    final rows = await (db.select(db.outboxRows)
          ..orderBy([(t) => OrderingTerm.asc(t.sequence)]))
        .get();
    return [
      for (final row in rows)
        OutboxEntry.fromJson(jsonDecode(row.body) as Map<String, Object?>),
    ];
  }

  @override
  Future<void> commit({
    Iterable<OutboxEntry> upserts = const [],
    Iterable<String> removals = const [],
  }) {
    final ids = removals.toList();
    // A drift batch runs in one transaction: all or nothing.
    return db.batch((b) {
      for (final e in upserts) {
        b.insert(
          db.outboxRows,
          OutboxRowsCompanion.insert(
            id: e.id,
            sequence: e.sequence,
            body: jsonEncode(e.toJson()),
          ),
          mode: InsertMode.insertOrReplace,
        );
      }
      if (ids.isNotEmpty) {
        b.deleteWhere(db.outboxRows, (t) => t.id.isIn(ids));
      }
    });
  }
}
```

### sqflite

Use the same table (`id TEXT PRIMARY KEY, sequence INTEGER, body TEXT`). In `loadAll`,
run `db.query('outbox', orderBy: 'sequence')`. In `commit`, run a `db.transaction`
containing `txn.insert(..., conflictAlgorithm: ConflictAlgorithm.replace)` for each upsert
and a `txn.delete('outbox', where: 'id = ?', whereArgs: [id])` for each removal.

### hive_ce

Use a `Box<String>` keyed by entry id that holds `jsonEncode(entry.toJson())`. `commit`
calls `box.putAll({...})` for the upserts and `box.deleteAll(ids)` for the removals, and
`loadAll` decodes `box.values` and sorts them by `sequence`. Hive has no transaction that
spans those two calls, so a crash between them can apply one without the other. If that
matters, write removals as upserts of the entry in its terminal state (`succeeded` or
`cancelled` entries are never sent again) and delete them later.

## Limitations

- **Delivery is at-least-once.** Exactly-once needs the server to deduplicate on
  `idempotencyKey`. Without that, an entry interrupted in flight can be applied twice.
- One `Outbox` must own its store: do not run two outboxes on the same storage, for
  example from two isolates at once.
- The working set lives in memory and the scans are linear. That is fine for thousands of
  entries, not for millions.
- Coalescing combines an entry only with the latest queued entry for the same resource
  in the same group. Merging moves the incoming change to that entry's position in the
  queue.
- `payload` and `serverState` must be JSON-encodable for the bundled stores.
- `JsonFileOutboxStore` needs `dart:io`, so it is not available on the web. On the web,
  implement `OutboxStore` on IndexedDB or a similar store.
- The outbox does not detect connectivity or run background tasks. Feed it a
  connectivity stream, and call `flush()` from a background worker such as
  `workmanager` if you need that.

## License

MIT. See [LICENSE](LICENSE).

Made by [Abdeldjalil Chougui](https://abdeldjalilchougui.biz/).
