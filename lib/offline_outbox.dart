/// A reliable, storage-agnostic outbox for offline-first apps.
///
/// Queue mutations with `Outbox.enqueue`; the outbox delivers them through
/// your `Sender` with ordering groups, retries, backoff, coalescing and
/// conflict strategies. This library is web-compatible; the file-based store
/// lives in `package:offline_outbox/io.dart`.
library;

export 'src/entry.dart';
export 'src/retry_policy.dart';
export 'src/uuid.dart';
