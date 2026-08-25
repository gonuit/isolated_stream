part of 'isolated_stream_base.dart';

/// Defines different strategies for processing stream elements in isolates.
///
/// Each strategy provides different behavior for handling concurrency, ordering,
/// and backpressure when processing stream elements. Choose the strategy that
/// best fits your use case:
///
/// - **Sequential**: Process one element at a time in order (default, safest)
/// - **Concurrent**: Process multiple elements simultaneously while preserving output order
/// - **Restartable**: Cancel previous operations when new events arrive and at maximum concurrency
/// - **Droppable**: Drop new events when at maximum concurrency
///
/// ## Example Usage
///
/// ```dart
/// // Sequential processing (default)
/// stream.isolatedMap(handler);
///
/// // Concurrent processing with 3 parallel operations
/// stream.isolatedMap(handler,
///   strategy: IsolatedProcessingStrategy.concurrent(concurrency: 3));
///
/// // Restartable running on multiple isolates
/// stream.isolatedMap(handler,
///   strategy: IsolatedProcessingStrategy.restartable(
///     concurrency: 2,
///     isolates: 4
///   ));
/// ```
abstract interface class IsolatedProcessingStrategy {
  /// Creates a sequential processing strategy that processes one element at a time.
  ///
  /// This is the default and safest strategy. Elements are processed one by one
  /// in the order they arrive, ensuring no race conditions and minimal memory usage.
  ///
  /// **Parameters:**
  /// - [idleTimeout]: If set, the underlying isolate is released after this
  ///   duration elapses with no in-flight work, and transparently re-spawned
  ///   when the next event arrives.
  /// - [onIdleShutdown]: Optional callback invoked after each idle shutdown.
  ///   Not called on explicit disposal.
  const factory IsolatedProcessingStrategy.sequential({
    Duration? idleTimeout,
    void Function()? onIdleShutdown,
  }) = SequentialProcessingStrategy;

  /// Creates a restartable processing strategy that cancels previous operations
  /// when new events arrive.
  ///
  /// This strategy is useful for debouncing scenarios where only the most recent
  /// operation result is relevant. When a new event arrives and the concurrency
  /// limit is exceeded, older pending operations are cancelled.
  /// It is not possible to cancel an isolate computation once started,
  /// so the results of cancelled operations are ignored.
  ///
  /// **Parameters:**
  /// - [concurrency]: Maximum number of operations to process simultaneously (default: 1).
  /// - [isolates]: Number of isolates in the pool for load distribution (default: 1)
  /// - [idleTimeout]: If set, the isolate pool is released after this duration
  ///   elapses with no in-flight work, and transparently re-spawned on the
  ///   next event.
  /// - [onIdleShutdown]: Optional callback invoked after each idle shutdown.
  ///
  /// When [concurrency] is set to 2 and 4 events arrive, only the 2 most recent events will be processed.
  /// When [concurrency] is set to 2, and 2 events are already being processed, the new event will
  /// replace/cancel the oldest pending event/computation.
  factory IsolatedProcessingStrategy.restartable({
    int concurrency,
    int isolates,
    Duration? idleTimeout,
    void Function()? onIdleShutdown,
  }) = RestartableProcessingStrategy;

  /// Creates a droppable processing strategy that drops new events when at
  /// maximum concurrency.
  ///
  /// When the concurrency limit is reached, new incoming events are dropped
  /// rather than queued. This prevents memory buildup during high-load scenarios
  /// but may result in lost events.
  ///
  /// **Parameters:**
  /// - [concurrency]: Maximum number of operations to process simultaneously (default: 1)
  /// - [isolates]: Number of isolates in the pool for load distribution (default: 1)
  /// - [idleTimeout]: If set, the isolate pool is released after this duration
  ///   elapses with no in-flight work, and transparently re-spawned on the
  ///   next event.
  /// - [onIdleShutdown]: Optional callback invoked after each idle shutdown.
  ///
  /// When [concurrency] is set to 2 and 4 events arrive, only the 2 first events will be processed,
  /// the other 2 events will be dropped.
  /// When [concurrency] is set to 2, and 2 events are already being processed, the new event will
  /// be dropped.
  ///
  /// All events will be returned in the order they arrived - applies also for errors.
  factory IsolatedProcessingStrategy.droppable({
    int concurrency,
    int isolates,
    Duration? idleTimeout,
    void Function()? onIdleShutdown,
  }) = DroppableProcessingStrategy;

  /// Creates a concurrent processing strategy that processes multiple elements
  /// simultaneously while preserving output order.
  ///
  /// Elements are processed in parallel up to the concurrency limit, but results
  /// are emitted in the same order as the input stream. This provides the best
  /// of both worlds: parallel processing performance with predictable ordering.
  ///
  /// **Parameters:**
  /// - [concurrency]: Maximum number of operations to process simultaneously (default: 1)
  /// - [isolates]: Number of isolates in the pool for load distribution (default: 1)
  /// - [idleTimeout]: If set, the isolate pool is released after this duration
  ///   elapses with no in-flight work, and transparently re-spawned on the
  ///   next event.
  /// - [onIdleShutdown]: Optional callback invoked after each idle shutdown.
  ///
  /// When [concurrency] is set to 3 and 6 events arrive, up to 3 events will be processed
  /// simultaneously. Other events will be queued until an active operation completes.
  ///
  /// All events will be returned in the order they arrived - applies also for errors.
  factory IsolatedProcessingStrategy.concurrent({
    int concurrency,
    int isolates,
    Duration? idleTimeout,
    void Function()? onIdleShutdown,
  }) = ConcurrentProcessingStrategy;

  /// Processes elements from [source] using [handler], emitting results and
  /// errors into [controller]. Called internally by
  /// [IsolatedStreamExtension.isolatedMap].
  void process<S, R>(
    StreamController<R> controller,
    IsolatedHandler<S, R> handler,
    Stream<S> source,
  );
}

/// Tracks the processing state of a single event.
class _EventResult<S, R> {
  final S event;

  /// May be null even for successful processing when the handler returns
  /// null; [hasResult] distinguishes "no result yet" from "result is null".
  final R? result;

  final Object? exception;
  final StackTrace? stackTrace;

  /// Whether processing has completed (successfully or with an error).
  bool hasResult;

  _EventResult(this.event)
      : hasResult = false,
        result = null,
        exception = null,
        stackTrace = null;

  _EventResult._internal({
    required this.event,
    this.result,
    this.exception,
    this.stackTrace,
    this.hasResult = false,
  });

  _EventResult<S, R> withResult({R? result}) {
    return _EventResult<S, R>._internal(
      event: event,
      result: result,
      exception: null,
      stackTrace: null,
      hasResult: true,
    );
  }

  _EventResult<S, R> withException(
      {required Object exception, StackTrace? stackTrace}) {
    return _EventResult<S, R>._internal(
      event: event,
      exception: exception,
      stackTrace: stackTrace,
      result: null,
      hasResult: true,
    );
  }

  @override
  String toString() {
    return 'EventResult(Event: $event, Result: $result, HasResult: $hasResult)';
  }
}

/// Sequential processing strategy that processes one element at a time in order.
///
/// This is the default and most predictable strategy:
/// - Output order matches input order.
/// - Minimal memory usage (a single element in flight).
/// - Backpressure is automatic — the source is paused during processing.
///
/// ```dart
/// final processed = stream.isolatedMap(
///   MyHandler(),
///   strategy: IsolatedProcessingStrategy.sequential(), // Default
/// );
/// ```
class SequentialProcessingStrategy implements IsolatedProcessingStrategy {
  /// If set, the underlying isolate is released after this duration elapses
  /// with no in-flight work, and transparently re-spawned on the next event.
  final Duration? idleTimeout;

  /// Optional callback invoked after each idle shutdown. Not called on
  /// explicit disposal.
  final void Function()? onIdleShutdown;

  /// Creates a new sequential processing strategy.
  ///
  /// This strategy uses a single isolate and processes elements one at a time.
  const SequentialProcessingStrategy({
    this.idleTimeout,
    this.onIdleShutdown,
  });

  @override
  void process<T, E>(
    StreamController<E> controller,
    IsolatedHandler<T, E> handler,
    Stream<T> source,
  ) {
    final pool = _IsolatePool<T, E>(
      handler,
      1,
      idleTimeout: idleTimeout,
      onIdleShutdown: onIdleShutdown,
    );
    final policy = _SequentialPolicy<T, E>(pool.run);
    final subscription = source.listen(
      null,
      onError: controller.addError,
      onDone: controller.close,
    );

    final addError = controller.addError;
    final resume = subscription.resume;

    subscription.onData((event) {
      subscription.pause();
      policy
          .submit(event)
          .then(controller.add, onError: addError)
          .whenComplete(resume);
    });

    controller.onCancel = () {
      subscription.cancel().whenComplete(pool.dispose);
    };

    if (!source.isBroadcast) {
      controller
        ..onPause = subscription.pause
        ..onResume = resume;
    }
  }
}

/// Droppable processing strategy that drops new events when at maximum concurrency.
///
/// A load-shedding strategy: up to [concurrency] events are processed in
/// parallel, new events arriving at capacity are dropped, and results are
/// emitted in arrival order. Memory stays bounded at the cost of losing
/// events — suitable for sensor data, metrics sampling, or endpoints with
/// load-shedding requirements.
///
/// ```dart
/// final processed = sensorStream.isolatedMap(
///   HeavyProcessingHandler(),
///   strategy: IsolatedProcessingStrategy.droppable(
///     concurrency: 5,
///     isolates: 2,
///   ),
/// );
/// ```
///
/// ⚠️ **Important**: This strategy will lose events when the system is
/// overloaded. Only use when event loss is acceptable for your use case.
class DroppableProcessingStrategy implements IsolatedProcessingStrategy {
  /// Number of isolates in the pool for load distribution. Default: 1.
  final int isolates;

  /// Maximum number of operations processed simultaneously; new events
  /// arriving at this limit are dropped. Default: 1.
  final int concurrency;

  /// If set, the underlying isolate pool is released after this duration
  /// elapses with no in-flight work, and transparently re-spawned on the
  /// next event.
  final Duration? idleTimeout;

  /// Optional callback invoked after each idle shutdown. Not called on
  /// explicit disposal.
  final void Function()? onIdleShutdown;

  /// Creates a new droppable processing strategy.
  ///
  /// Throws [ArgumentError] if [isolates] or [concurrency] is less than 1.
  DroppableProcessingStrategy({
    this.isolates = 1,
    this.concurrency = 1,
    this.idleTimeout,
    this.onIdleShutdown,
  }) {
    if (isolates < 1) {
      throw ArgumentError.value(isolates, 'isolates', 'Must be at least 1');
    }
    if (concurrency < 1) {
      throw ArgumentError.value(
          concurrency, 'concurrency', 'Must be at least 1');
    }
  }

  @override
  void process<S, R>(
    StreamController<R> controller,
    IsolatedHandler<S, R> handler,
    Stream<S> source,
  ) {
    final pool = _IsolatePool<S, R?>(
      handler,
      isolates,
      idleTimeout: idleTimeout,
      onIdleShutdown: onIdleShutdown,
    );

    final activeEvents = <int, _EventResult<S, R>>{};
    int currentEventId = 0;

    bool sourceDone = false;

    void sendQueuedResults() {
      // toList: entries are removed during iteration.
      for (final entry in activeEvents.entries.toList()) {
        // Emit in arrival order; stop at the first pending entry.
        if (!entry.value.hasResult) break;

        if (entry.value.exception case final exception?) {
          controller.addError(exception, entry.value.stackTrace);
        } else if (entry.value.result case final R result) {
          controller.add(result);
        }

        activeEvents.remove(entry.key);
      }
    }

    void processEvent(S event) {
      if (activeEvents.length >= concurrency) {
        return; // Drop the new event.
      }

      final eventId = currentEventId++;
      activeEvents[eventId] = _EventResult<S, R>(event);

      pool.run(event).then((result) {
        if (controller.isClosed || !activeEvents.containsKey(eventId)) return;

        final currentEvent = activeEvents[eventId]!;
        activeEvents[eventId] = currentEvent.withResult(result: result);

        final oldestEventId = activeEvents.keys.first;
        if (eventId != oldestEventId) {
          return;
        } else {
          sendQueuedResults();
        }
      }).catchError((exception, stackTrace) {
        if (controller.isClosed || !activeEvents.containsKey(eventId)) return;

        final currentEvent = activeEvents[eventId]!;
        activeEvents[eventId] = currentEvent.withException(
          exception: exception,
          stackTrace: stackTrace,
        );

        final oldestEventId = activeEvents.keys.first;
        if (eventId != oldestEventId) {
          return;
        } else {
          sendQueuedResults();
        }
      }).whenComplete(() {
        if (sourceDone && activeEvents.isEmpty) {
          controller.close().whenComplete(() => pool.dispose());
        }
      });
    }

    final subscription = source.listen(
      processEvent,
      onError: controller.addError,
      onDone: () {
        sourceDone = true;
        // Nothing in flight — no whenComplete will fire to close later.
        if (activeEvents.isEmpty) {
          controller.close().whenComplete(() => pool.dispose());
        }
      },
    );

    controller.onCancel = () {
      subscription.cancel().whenComplete(() => pool.dispose());
    };

    if (!source.isBroadcast) {
      controller
        ..onPause = subscription.pause
        ..onResume = subscription.resume;
    }
  }
}

/// Restartable processing strategy that cancels previous operations when new events arrive.
///
/// A debouncing strategy: when the [concurrency] limit is exceeded, the
/// oldest tracked event is dropped and its eventual result ignored — only
/// the most recent operations matter. A computation already running in an
/// isolate cannot be interrupted; its result is discarded instead. Suitable
/// for search-as-you-type, live previews, and other cases where stale
/// results are irrelevant.
///
/// ```dart
/// // User types "ap", then "app", then "apple" — only the search for
/// // "apple" produces results.
/// final suggestions = searchQuery.stream.isolatedMap(
///   SearchHandler(),
///   strategy: IsolatedProcessingStrategy.restartable(concurrency: 1),
/// );
/// ```
class RestartableProcessingStrategy implements IsolatedProcessingStrategy {
  /// Number of isolates in the pool for load distribution. Default: 1.
  final int isolates;

  /// Maximum number of operations processed simultaneously; beyond this
  /// limit the oldest events are superseded and their results ignored.
  /// Default: 1.
  final int concurrency;

  /// If set, the underlying isolate pool is released after this duration
  /// elapses with no in-flight work, and transparently re-spawned on the
  /// next event.
  final Duration? idleTimeout;

  /// Optional callback invoked after each idle shutdown. Not called on
  /// explicit disposal.
  final void Function()? onIdleShutdown;

  /// Creates a new restartable processing strategy.
  ///
  /// Throws [ArgumentError] if [isolates] or [concurrency] is less than 1.
  RestartableProcessingStrategy({
    this.isolates = 1,
    this.concurrency = 1,
    this.idleTimeout,
    this.onIdleShutdown,
  }) {
    if (isolates < 1) {
      throw ArgumentError.value(isolates, 'isolates', 'Must be at least 1');
    }
    if (concurrency < 1) {
      throw ArgumentError.value(
          concurrency, 'concurrency', 'Must be at least 1');
    }
  }

  @override
  void process<S, R>(
    StreamController<R> controller,
    IsolatedHandler<S, R> handler,
    Stream<S> source,
  ) {
    final pool = _IsolatePool<S, R?>(
      handler,
      isolates,
      idleTimeout: idleTimeout,
      onIdleShutdown: onIdleShutdown,
    );

    final activeEvents = <int, _EventResult<S, R>>{};
    int currentEventId = 0;

    bool sourceDone = false;

    void sendQueuedResults() {
      // toList: entries are removed during iteration.
      for (final entry in activeEvents.entries.toList()) {
        // Emit in arrival order; stop at the first pending entry.
        if (!entry.value.hasResult) break;

        if (entry.value.exception case final exception?) {
          controller.addError(exception, entry.value.stackTrace);
        } else if (entry.value.result case final R result) {
          controller.add(result);
        }

        activeEvents.remove(entry.key);
      }
    }

    void processEvent(S event) {
      final eventId = currentEventId++;
      activeEvents[eventId] = _EventResult<S, R>(event);

      if (activeEvents.length > concurrency) {
        final oldestEventId = activeEvents.keys.first;
        activeEvents.remove(oldestEventId);
      }

      pool.run(event).then((result) {
        if (controller.isClosed || !activeEvents.containsKey(eventId)) return;

        final currentEvent = activeEvents[eventId]!;
        activeEvents[eventId] = currentEvent.withResult(result: result);

        final oldestEventId = activeEvents.keys.first;
        if (eventId != oldestEventId) {
          return;
        } else {
          sendQueuedResults();
        }
      }).catchError((exception, stackTrace) {
        if (controller.isClosed || !activeEvents.containsKey(eventId)) return;

        final currentEvent = activeEvents[eventId]!;
        activeEvents[eventId] = currentEvent.withException(
          exception: exception,
          stackTrace: stackTrace,
        );

        final oldestEventId = activeEvents.keys.first;
        if (eventId != oldestEventId) {
          return;
        } else {
          sendQueuedResults();
        }
      }).whenComplete(() {
        if (sourceDone && activeEvents.isEmpty) {
          controller.close().whenComplete(() => pool.dispose());
        }
      });
    }

    final subscription = source.listen(
      processEvent,
      onError: controller.addError,
      onDone: () {
        sourceDone = true;
        // Nothing in flight — no whenComplete will fire to close later.
        if (activeEvents.isEmpty) {
          controller.close().whenComplete(() => pool.dispose());
        }
      },
    );

    controller.onCancel = () {
      subscription.cancel().whenComplete(() => pool.dispose());
    };

    if (!source.isBroadcast) {
      controller
        ..onPause = subscription.pause
        ..onResume = subscription.resume;
    }
  }
}

/// Concurrent processing strategy that processes multiple elements simultaneously while preserving order.
///
/// Up to [concurrency] events are processed in parallel; results are
/// buffered as needed and emitted in input order. The source stream is
/// paused at the concurrency limit and resumed when a slot frees up, so no
/// events are lost. For CPU-bound work, set [concurrency] near the number
/// of CPU cores.
///
/// ```dart
/// final processedImages = imageStream.isolatedMap(
///   ImageProcessingHandler(),
///   strategy: IsolatedProcessingStrategy.concurrent(
///     concurrency: 3,
///     isolates: 2,
///   ),
/// );
/// ```
class ConcurrentProcessingStrategy implements IsolatedProcessingStrategy {
  /// Maximum number of operations processed simultaneously. Default: 1.
  final int concurrency;

  /// Number of isolates in the pool for load distribution. Default: 1.
  final int isolates;

  /// If set, the underlying isolate pool is released after this duration
  /// elapses with no in-flight work, and transparently re-spawned on the
  /// next event.
  final Duration? idleTimeout;

  /// Optional callback invoked after each idle shutdown. Not called on
  /// explicit disposal.
  final void Function()? onIdleShutdown;

  /// Creates a new concurrent processing strategy.
  ///
  /// Throws [ArgumentError] if [concurrency] or [isolates] is less than 1.
  ConcurrentProcessingStrategy({
    this.concurrency = 1,
    this.isolates = 1,
    this.idleTimeout,
    this.onIdleShutdown,
  }) {
    if (concurrency < 1) {
      throw ArgumentError.value(
          concurrency, 'concurrency', 'Must be at least 1');
    }
    if (isolates < 1) {
      throw ArgumentError.value(isolates, 'isolates', 'Must be at least 1');
    }
  }

  @override
  void process<T, E>(
    StreamController<E> controller,
    IsolatedHandler<T, E> handler,
    Stream<T> source,
  ) {
    final pool = _IsolatePool<T, E>(
      handler,
      isolates,
      idleTimeout: idleTimeout,
      onIdleShutdown: onIdleShutdown,
    );
    final subscription = source.listen(
      null,
      onError: controller.addError,
      onDone: () {},
    );

    final Map<int, Future<E>> pending = {};
    final Map<int, _EventResult<T, E>> completed = {};
    int nextInputIndex = 0;
    int nextOutputIndex = 0;
    bool inputDone = false;

    final policy = _ConcurrentPolicy<T, E>(
      pool.run,
      concurrency: concurrency,
      onSpaceAvailable: () {
        if (!inputDone) {
          subscription.resume();
        }
      },
    );

    void tryEmitNext() {
      while (completed.containsKey(nextOutputIndex)) {
        final result = completed.remove(nextOutputIndex)!;
        if (result.exception case final exception?) {
          controller.addError(exception, result.stackTrace);
        } else if (result.result case final E value) {
          controller.add(value);
        }
        nextOutputIndex++;
      }

      if (inputDone && pending.isEmpty && completed.isEmpty) {
        pool.dispose();
        controller.close();
      }
    }

    void processEvent(T event) {
      final index = nextInputIndex++;

      final computation = policy.submit(event);
      pending[index] = computation;

      computation.then((result) {
        completed[index] = _EventResult<T, E>(event).withResult(result: result);
        pending.remove(index);
        tryEmitNext();
      }).catchError((exception, stackTrace) {
        completed[index] = _EventResult<T, E>(event)
            .withException(exception: exception, stackTrace: stackTrace);
        pending.remove(index);
        tryEmitNext();
      });

      if (policy.isFull) {
        subscription.pause();
      }
    }

    subscription.onData(processEvent);

    subscription.onDone(() {
      inputDone = true;
      if (pending.isEmpty) {
        pool.dispose();
        controller.close();
      }
    });

    controller.onCancel = () {
      subscription.cancel().whenComplete(() => pool.dispose());
    };

    if (!source.isBroadcast) {
      controller
        ..onPause = subscription.pause
        ..onResume = subscription.resume;
    }
  }
}
