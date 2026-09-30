import 'dart:async';
import 'dart:math';

import 'package:sync_outbox/sync_outbox.dart';

/// Builds outboxes against a manual clock and a scripted sender.
class Harness {
  Harness({OutboxStore? store}) : store = store ?? InMemoryOutboxStore();

  DateTime now = DateTime.utc(2026, 1, 1, 12);
  final OutboxStore store;

  /// Every entry handed to the sender, in call order.
  final List<OutboxEntry> sent = [];

  /// Decides the result of each send. Defaults to success.
  FutureOr<SendResult> Function(OutboxEntry e) respond =
      (_) => const SendResult.success();

  final List<OutboxEvent> events = [];
  int _ids = 0;
  late Outbox outbox;

  void advance(Duration d) => now = now.add(d);

  Future<SendResult> sender(OutboxEntry e) async {
    sent.add(e);
    return respond(e);
  }

  Outbox build({
    RetryPolicy? retryPolicy,
    ConflictStrategy defaultConflictStrategy = ConflictStrategy.manual,
    Map<String, ConflictStrategy> conflictStrategies = const {},
    Map<String, Coalescer> coalescers = const {},
    int maxConcurrency = 4,
    int maxConflictResolutions = 3,
    Stream<bool>? connectivity,
    bool initiallyOnline = true,
    Duration? flushInterval,
    bool autoFlush = false,
    bool scheduleRetries = false,
    bool keepCompleted = false,
    Random? random,
    DateTime Function()? clock,
  }) {
    outbox = Outbox(
      store: store,
      sender: sender,
      retryPolicy: retryPolicy ??
          ExponentialBackoff(
            initialDelay: const Duration(seconds: 10),
            maxDelay: const Duration(minutes: 10),
            jitter: 0,
          ),
      defaultConflictStrategy: defaultConflictStrategy,
      conflictStrategies: conflictStrategies,
      coalescers: coalescers,
      maxConcurrency: maxConcurrency,
      maxConflictResolutions: maxConflictResolutions,
      connectivity: connectivity,
      initiallyOnline: initiallyOnline,
      flushInterval: flushInterval,
      autoFlush: autoFlush,
      scheduleRetries: scheduleRetries,
      keepCompleted: keepCompleted,
      clock: clock ?? () => now,
      random: random ?? Random(1),
      idGenerator: () => 'id${_ids++}',
    );
    outbox.events.listen(events.add);
    return outbox;
  }

  Future<Outbox> start({
    Map<String, Coalescer> coalescers = const {},
    ConflictStrategy defaultConflictStrategy = ConflictStrategy.manual,
    Map<String, ConflictStrategy> conflictStrategies = const {},
    RetryPolicy? retryPolicy,
    int maxConcurrency = 4,
    int maxConflictResolutions = 3,
    bool keepCompleted = false,
  }) async {
    final o = build(
      coalescers: coalescers,
      defaultConflictStrategy: defaultConflictStrategy,
      conflictStrategies: conflictStrategies,
      retryPolicy: retryPolicy,
      maxConcurrency: maxConcurrency,
      maxConflictResolutions: maxConflictResolutions,
      keepCompleted: keepCompleted,
    );
    await o.start();
    return o;
  }

  Future<OutboxEntry> add(
    String resource, {
    String op = 'update',
    Map<String, Object?> payload = const {},
    String? group,
  }) =>
      outbox.enqueue(
        operation: op,
        resourceKey: resource,
        payload: payload,
        group: group,
      );

  List<String> get sentResources => [for (final e in sent) e.resourceKey];

  /// The events seen so far, after letting pending deliveries run.
  Future<List<OutboxEvent>> get drained async {
    await settle();
    return events;
  }

  /// Lets queued microtasks and zero-delay events run.
  Future<void> settle() => Future<void>.delayed(Duration.zero);
}

/// A Random that always returns the same double.
class FixedRandom implements Random {
  FixedRandom(this.value);
  final double value;
  @override
  double nextDouble() => value;
  @override
  int nextInt(int max) => (value * max).floor();
  @override
  bool nextBool() => value >= 0.5;
}
