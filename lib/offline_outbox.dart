/// A reliable, storage-agnostic outbox for offline-first apps.
///
/// Queue mutations with `Outbox.enqueue`; the outbox delivers them through
/// your `Sender` with ordering groups, retries, backoff, coalescing and
/// conflict strategies. This library is web-compatible; the file-based store
/// lives in `package:offline_outbox/io.dart`.
library;

export 'src/coalesce.dart';
export 'src/conflict.dart';
export 'src/entry.dart';
export 'src/events.dart';
export 'src/in_memory_store.dart';
export 'src/outbox.dart';
export 'src/retry_policy.dart';
export 'src/send_result.dart';
export 'src/store.dart' show OutboxStore;
export 'src/uuid.dart';
