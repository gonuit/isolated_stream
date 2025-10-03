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
  const factory IsolatedProcessingStrategy.sequential() =
      SequentialProcessingStrategy;

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
  ///
  /// When [concurrency] is set to 2 and 4 events arrive, only the 2 most recent events will be processed.
  /// When [concurrency] is set to 2, and 2 events are already being processed, the new event will
  /// replace/cancel the oldest pending event/computation.
  factory IsolatedProcessingStrategy.restartable(
      {int concurrency, int isolates}) = RestartableProcessingStrategy;

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
  ///
  /// When [concurrency] is set to 2 and 4 events arrive, only the 2 first events will be processed,
  /// the other 2 events will be dropped.
  /// When [concurrency] is set to 2, and 2 events are already being processed, the new event will
  /// be dropped.
  ///
  /// All events will be returned in the order they arrived - applies also for errors.
  factory IsolatedProcessingStrategy.droppable(
      {int concurrency, int isolates}) = DroppableProcessingStrategy;

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
  ///
  /// When [concurrency] is set to 3 and 6 events arrive, up to 3 events will be processed
  /// simultaneously. Other events will be queued until an active operation completes.
  ///
  /// All events will be returned in the order they arrived - applies also for errors.
  factory IsolatedProcessingStrategy.concurrent(
      {int concurrency, int isolates}) = ConcurrentProcessingStrategy;

  /// Processes elements from the source stream using the specified handler.
  ///
  /// This method is called internally by the [isolatedMap].
  /// It manages the lifecycle of isolates, handles backpressure, and coordinates
  /// between the source stream and output controller.
  ///
  /// **Parameters:**
  /// - [controller]: The stream controller for emitting processed results
  /// - [handler]: The isolated handler that defines the processing logic
  /// - [source]: The input stream of elements to process
  ///
  /// **Type Parameters:**
  /// - [S]: Type of input elements from the source stream
  /// - [R]: Type of output elements emitted to the controller
  void process<S, R>(
    StreamController<R> controller,
    IsolatedHandler<S, R> handler,
    Stream<S> source,
  );
}

/// Internal class for tracking the processing state of an event.
class _EventResult<S, R> {
  /// The original event that was processed.
  final S event;

  /// The result of processing the event, if successful.
  ///
  /// This can be null even for successful processing if the handler
  /// returns null. Use [hasResult] to distinguish between "no result yet"
  /// and "result is null".
  final R? result;

  /// The exception that occurred during processing, if any.
  final Object? exception;

  /// The stack trace associated with the exception, if any.
  final StackTrace? stackTrace;

  /// Flag indicating whether processing has completed (successfully or with error).
  ///
  /// This is necessary because [result] can be null even for successful
  /// processing, so we need a separate flag to track completion state.
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
      result: result ?? this.result,
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
/// This is the default and most predictable processing strategy. It ensures:
/// - Elements are processed one at a time
/// - Output order matches input order
/// - Minimal memory usage
/// - No race conditions
/// - Backpressure is naturally handled
///
/// ## Characteristics
///
/// - **Concurrency**: 1 (single element processing)
/// - **Ordering**: Preserved (FIFO - First In, First Out)
/// - **Memory usage**: Minimal (only current element in processing)
/// - **Backpressure**: Automatic (source stream is paused during processing)
/// - **Error handling**: Errors are propagated immediately
///
/// ## Performance
///
/// While this strategy doesn't provide parallel processing benefits, it offers:
/// - Predictable resource usage
/// - Simple debugging and reasoning
/// - Reliable error propagation
/// - Consistent performance characteristics
///
/// ## Use Cases
///
/// Choose sequential processing when:
/// - Processing order is critical for correctness
/// - Memory constraints are tight
/// - Operations have dependencies on previous results
/// - Debugging and testing simplicity is important
/// - The processing workload doesn't justify parallelism overhead
///
/// ## Example
///
/// ```dart
/// // Process items one at a time
/// final stream = Stream.fromIterable([1, 2, 3, 4, 5]);
/// final processed = stream.isolatedMap(
///   MyHandler(),
///   strategy: IsolatedProcessingStrategy.sequential(), // Default
/// );
/// // Output: processed in order 1, 2, 3, 4, 5
/// ```
class SequentialProcessingStrategy implements IsolatedProcessingStrategy {
  /// Creates a new sequential processing strategy.
  ///
  /// This strategy uses a single isolate and processes elements one at a time.
  const SequentialProcessingStrategy();

  @override
  void process<T, E>(
    StreamController<E> controller,
    IsolatedHandler<T, E> handler,
    Stream<T> source,
  ) {
    final isolateWrapper = _IsolateWrapper<T, E>(handler);
    final subscription = source.listen(
      null,
      onError: controller.addError,
      onDone: controller.close,
    );

    FutureOr<void> add(E value) {
      controller.add(value);
    }

    final addError = controller.addError;
    final resume = subscription.resume;

    subscription.onData((event) {
      FutureOr<E> newValue;
      try {
        newValue = isolateWrapper.compute(event);
      } catch (e, st) {
        controller.addError(e, st);
        return;
      }
      if (newValue is Future<E>) {
        subscription.pause();
        newValue.then(add, onError: addError).whenComplete(resume);
      } else {
        controller.add(newValue);
      }
    });

    controller.onCancel = () {
      subscription.cancel().whenComplete(isolateWrapper.kill);
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
/// This strategy implements a load shedding mechanism where new incoming events
/// are dropped (ignored) when the maximum concurrency limit is reached. This
/// prevents memory buildup during high-load scenarios but results in lost events.
///
/// ## Characteristics
///
/// - **Concurrency**: Configurable (1 to N parallel operations)
/// - **Ordering**: Preserved for processed events
/// - **Memory usage**: Bounded by concurrency limit
/// - **Backpressure**: Load shedding (events are dropped)
/// - **Event loss**: Yes (events dropped when at capacity)
///
/// ## Behavior
///
/// 1. Events are processed up to the concurrency limit
/// 2. When limit is reached, new events are immediately dropped
/// 3. As operations complete, new events can be accepted again
/// 4. Results are emitted in the order they were started (not completed)
///
/// ## Performance Characteristics
///
/// - **Memory**: O(concurrency) - bounded memory usage
/// - **Throughput**: Limited by concurrency * processing_rate
/// - **Latency**: Immediate for dropped events, normal for processed events
/// - **Resource usage**: Bounded by isolate pool size
///
/// ## Use Cases
///
/// Choose droppable processing when:
/// - System is under high load and some data loss is acceptable
/// - Preventing memory exhaustion is critical
/// - Real-time processing where old events become irrelevant
/// - Sensor data or metrics where sampling is acceptable
/// - Server endpoints with load shedding requirements
///
/// ## Example
///
/// ```dart
/// // Drop sensor readings when processing falls behind
/// final sensorStream = Stream.periodic(
///   Duration(milliseconds: 10),
///   (i) => SensorReading(timestamp: DateTime.now(), value: i)
/// );
///
/// final processed = sensorStream.isolatedMap(
///   HeavyProcessingHandler(),
///   strategy: IsolatedProcessingStrategy.droppable(
///     concurrency: 5,    // Process up to 5 simultaneously
///     isolates: 2,       // Use 2 isolates for load distribution
///   ),
/// );
/// // Some readings will be dropped if processing can't keep up
/// ```
///
/// ⚠️ **Important**: This strategy will lose events when the system is overloaded.
/// Only use when event loss is acceptable for your use case.
class DroppableProcessingStrategy implements IsolatedProcessingStrategy {
  /// Number of isolates in the pool for load distribution.
  ///
  /// More isolates can help distribute the workload and improve performance,
  /// but also increase memory usage. The optimal number depends on:
  /// - Available CPU cores
  /// - Memory constraints
  /// - Nature of the processing work
  ///
  /// Default: 1
  final int isolates;

  /// Maximum number of operations to process simultaneously.
  ///
  /// This controls how many events can be processed in parallel. When this
  /// limit is reached, new events are dropped. Higher values increase
  /// throughput but also memory usage.
  ///
  /// Default: 1
  final int concurrency;

  /// Creates a new droppable processing strategy.
  ///
  /// **Parameters:**
  /// - [isolates]: Number of isolates to use for processing (must be ≥ 1)
  /// - [concurrency]: Maximum parallel operations (must be ≥ 1)
  ///
  /// **Throws:**
  /// - [ArgumentError] if [isolates] or [concurrency] is less than 1
  ///
  /// ```dart
  /// // Create strategy that processes up to 3 events simultaneously
  /// // using 2 isolates for load distribution
  /// final strategy = DroppableProcessingStrategy(
  ///   concurrency: 3,
  ///   isolates: 2,
  /// );
  /// ```
  DroppableProcessingStrategy({
    this.isolates = 1,
    this.concurrency = 1,
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
    final pool = _IsolatePool<S, R?>(handler, isolates);

    final activeEvents = <int, _EventResult<S, R>>{};
    int currentEventId = 0;

    bool sourceDone = false;

    void sendQueuedResults() {
      // Use toList to avoid concurrent modification during iteration
      for (final entry in activeEvents.entries.toList()) {
        // This entry has no result yet - stop processing further (they are in order)
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
      // If we exceed concurrency, remove the oldest event
      if (activeEvents.length >= concurrency) {
        return; // Drop the new event
      }

      final eventId = currentEventId++;

      // Add new event to active events
      activeEvents[eventId] = _EventResult<S, R>(event);

      pool.acquire().then((isolateWrapper) async {
        if (controller.isClosed) return;

        final result = await isolateWrapper.compute(event);

        // If the controller is closed or the event is no longer active, ignore the result
        if (controller.isClosed || !activeEvents.containsKey(eventId)) return;

        final currentEvent = activeEvents[eventId]!;
        activeEvents[eventId] = currentEvent.withResult(result: result);

        final oldestEventId = activeEvents.keys.first;
        // If this event is not the oldest, it means that we are waiting for an old event to complete
        if (eventId != oldestEventId) {
          return;
        } else {
          sendQueuedResults();
        }
      }).catchError((exception, stackTrace) {
        // If the controller is closed or the event is no longer active, ignore the result
        if (controller.isClosed || !activeEvents.containsKey(eventId)) return;

        final currentEvent = activeEvents[eventId]!;
        activeEvents[eventId] = currentEvent.withException(
          exception: exception,
          stackTrace: stackTrace,
        );

        final oldestEventId = activeEvents.keys.first;
        // If this event is not the oldest, it means that we are waiting for an old event to complete
        if (eventId != oldestEventId) {
          return;
        } else {
          sendQueuedResults();
        }
      }).whenComplete(() {
        // If source is done and no more active events, close the controller
        if (sourceDone && activeEvents.isEmpty) {
          controller.close().whenComplete(() => pool.dispose());
        }
      });
    }

    final subscription = source.listen(
      processEvent,
      onError: controller.addError,
      onDone: () {
        // Mark source as done (no more events will be added)
        // Close later when all active events are processed
        sourceDone = true;
      },
    );

    controller.onCancel = () {
      // Last subscription canceled, no target to send results to - dispose
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
/// This strategy implements a debouncing mechanism where older pending operations
/// are effectively cancelled (their results ignored) when new events arrive and
/// the concurrency limit is exceeded. Only the most recent operations matter.
///
/// ## Characteristics
///
/// - **Concurrency**: Configurable (1 to N parallel operations)
/// - **Ordering**: Preserved for the most recent events
/// - **Memory usage**: Bounded by concurrency limit
/// - **Backpressure**: Event replacement (old events are superseded)
/// - **Event loss**: Yes (older events are superseded by newer ones)
///
/// ## Behavior
///
/// 1. Events are processed up to the concurrency limit
/// 2. When limit is exceeded, the oldest event is removed from tracking
/// 3. New events take priority over older pending operations
/// 4. Results from superseded operations are ignored
/// 5. Only results from current active operations are emitted
///
/// ## Performance Characteristics
///
/// - **Memory**: O(concurrency) - bounded memory usage
/// - **Throughput**: Optimized for most recent events
/// - **Latency**: Low for recent events, cancelled for old events
/// - **Resource usage**: Efficient - work on outdated events is abandoned
///
/// ## Use Cases
///
/// Choose restartable processing when:
/// - Only the most recent results are relevant
/// - Implementing debouncing behavior (e.g., search-as-you-type)
/// - Real-time scenarios where old data becomes stale
/// - Interactive applications with frequent user input
/// - Live previews or suggestions that should reflect latest input
///
/// ## Example
///
/// ```dart
/// // Search suggestions that restart when user types more
/// final searchQuery = StreamController<String>();
///
/// final suggestions = searchQuery.stream.isolatedMap(
///   SearchHandler(),
///   strategy: IsolatedProcessingStrategy.restartable(
///     concurrency: 1,    // Only one search at a time
///     isolates: 2,       // Use 2 isolates for availability
///   ),
/// );
///
/// // User types "ap", then "app", then "apple"
/// // Only the search for "apple" will produce results
/// // Previous searches for "ap" and "app" are cancelled
/// ```
///
/// ## Comparison with Droppable
///
/// | Aspect | Restartable | Droppable |
/// |--------|-------------|-----------|
/// | New events | Replace old events | Dropped when at capacity |
/// | Memory usage | Bounded | Bounded |
/// | Use case | Debouncing | Load shedding |
/// | Event priority | Newer events win | First-come-first-served |
class RestartableProcessingStrategy implements IsolatedProcessingStrategy {
  /// Number of isolates in the pool for load distribution.
  ///
  /// More isolates can help distribute the workload and provide better
  /// availability when operations are cancelled and restarted. The optimal
  /// number depends on your specific use case and system resources.
  ///
  /// Default: 1
  final int isolates;

  /// Maximum number of operations to process simultaneously.
  ///
  /// This controls how many events can be processed in parallel. When this
  /// limit is exceeded, the oldest events are removed from tracking and
  /// their results will be ignored in favor of newer events.
  ///
  /// Default: 1
  final int concurrency;

  /// Creates a new restartable processing strategy.
  ///
  /// **Parameters:**
  /// - [isolates]: Number of isolates to use for processing (must be ≥ 1)
  /// - [concurrency]: Maximum parallel operations (must be ≥ 1)
  ///
  /// **Throws:**
  /// - [ArgumentError] if [isolates] or [concurrency] is less than 1
  ///
  /// ```dart
  /// // Create strategy that keeps only the 2 most recent operations
  /// // using 3 isolates for better availability
  /// final strategy = RestartableProcessingStrategy(
  ///   concurrency: 2,
  ///   isolates: 3,
  /// );
  /// ```
  RestartableProcessingStrategy({
    this.isolates = 1,
    this.concurrency = 1,
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
    final pool = _IsolatePool<S, R?>(handler, isolates);

    final activeEvents = <int, _EventResult<S, R>>{};
    int currentEventId = 0;

    bool sourceDone = false;

    void sendQueuedResults() {
      // Use toList to avoid concurrent modification during iteration
      for (final entry in activeEvents.entries.toList()) {
        // This entry has no result yet - stop processing further (they are in order)
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

      // Add new event to active events
      activeEvents[eventId] = _EventResult<S, R>(event);

      // If we exceed concurrency, remove the oldest event
      if (activeEvents.length > concurrency) {
        final oldestEventId = activeEvents.keys.first;
        activeEvents.remove(oldestEventId);
      }

      pool.acquire().then((isolateWrapper) async {
        if (controller.isClosed) return;

        final result = await isolateWrapper.compute(event);

        // If the controller is closed or the event is no longer active, ignore the result
        if (controller.isClosed || !activeEvents.containsKey(eventId)) return;

        final currentEvent = activeEvents[eventId]!;
        activeEvents[eventId] = currentEvent.withResult(result: result);

        final oldestEventId = activeEvents.keys.first;
        // If this event is not the oldest, it means that we are waiting for an old event to complete
        if (eventId != oldestEventId) {
          return;
        } else {
          sendQueuedResults();
        }
      }).catchError((exception, stackTrace) {
        // If the controller is closed or the event is no longer active, ignore the result
        if (controller.isClosed || !activeEvents.containsKey(eventId)) return;

        final currentEvent = activeEvents[eventId]!;
        activeEvents[eventId] = currentEvent.withException(
          exception: exception,
          stackTrace: stackTrace,
        );

        final oldestEventId = activeEvents.keys.first;
        // If this event is not the oldest, it means that we are waiting for an old event to complete
        if (eventId != oldestEventId) {
          return;
        } else {
          sendQueuedResults();
        }
      }).whenComplete(() {
        // If source is done and no more active events, close the controller
        if (sourceDone && activeEvents.isEmpty) {
          controller.close().whenComplete(() => pool.dispose());
        }
      });
    }

    final subscription = source.listen(
      processEvent,
      onError: controller.addError,
      onDone: () {
        // Mark source as done (no more events will be added)
        // Close later when all active events are processed
        sourceDone = true;
      },
    );

    controller.onCancel = () {
      // Last subscription canceled, no target to send results to - dispose
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
/// This strategy provides the best of both worlds: parallel processing for performance
/// and ordered output for predictability. Elements are processed concurrently up to
/// the specified limit, but results are buffered and emitted in the same order as
/// the input stream.
///
/// ## Characteristics
///
/// - **Concurrency**: Configurable (1 to N parallel operations)
/// - **Ordering**: Strictly preserved (input order = output order)
/// - **Memory usage**: O(concurrency) for buffering out-of-order results
/// - **Backpressure**: Automatic (source paused when concurrency limit reached)
/// - **Event loss**: None (all events are processed)
///
/// ## Behavior
///
/// 1. Up to `concurrency` events are processed simultaneously
/// 2. Results may complete out of order due to varying processing times
/// 3. Completed results are buffered until they can be emitted in order
/// 4. Source stream is paused when concurrency limit is reached
/// 5. Source stream is resumed when active operations drop below limit
///
/// ## Performance Characteristics
///
/// - **Throughput**: Up to `concurrency` × processing_rate
/// - **Memory**: O(concurrency) for result buffering
/// - **Latency**: May be higher due to result ordering
/// - **CPU utilization**: Excellent for CPU-bound operations
///
/// ## Memory Considerations
///
/// This strategy buffers results that complete out of order, which means:
/// - Memory usage grows with concurrency level
/// - Large result objects should be used carefully
/// - Consider memory-efficient data structures for results
///
/// ## Use Cases
///
/// Choose concurrent processing when:
/// - Operations are CPU-intensive and can benefit from parallelism
/// - Operations are independent (no shared state dependencies)
/// - Output order must match input order
/// - System has multiple CPU cores available
/// - Processing time varies significantly between elements
/// - Memory for result buffering is available
///
/// ## Example
///
/// ```dart
/// // Process images in parallel while maintaining order
/// final imageStream = Stream.fromIterable([
///   'image1.jpg', 'image2.jpg', 'image3.jpg', 'image4.jpg'
/// ]);
///
/// final processedImages = imageStream.isolatedMap(
///   ImageProcessingHandler(),
///   strategy: IsolatedProcessingStrategy.concurrent(
///     concurrency: 3,    // Process up to 3 images simultaneously
///     isolates: 2,       // Use 2 isolates for load distribution
///   ),
/// );
///
/// // Images are processed in parallel but output in original order:
/// // processed_image1, processed_image2, processed_image3, processed_image4
/// ```
///
/// ## Performance Tips
///
/// - Set `concurrency` to match available CPU cores for CPU-bound work
/// - Use fewer isolates than concurrency to reduce overhead
/// - Monitor memory usage with large result sets
/// - Consider async operations in handlers for I/O-bound work
///
/// ## Comparison with Other Strategies
///
/// | Aspect | Concurrent | Sequential | Restartable | Droppable |
/// |--------|------------|------------|-------------|-----------|
/// | Throughput | High | Low | Variable | High |
/// | Memory usage | Medium | Low | Low | Low |
/// | Order preservation | Yes | Yes | Partial | Partial |
/// | Event loss | None | None | Some | Some |
class ConcurrentProcessingStrategy implements IsolatedProcessingStrategy {
  /// Maximum number of operations to process simultaneously.
  ///
  /// This controls the level of parallelism. Higher values increase throughput
  /// but also memory usage for result buffering. Optimal values typically
  /// match the number of CPU cores for CPU-bound operations.
  ///
  /// Default: 1
  final int concurrency;

  /// Number of isolates in the pool for load distribution.
  ///
  /// More isolates can help distribute workload and reduce contention, but
  /// also increase memory overhead. Generally, you want fewer isolates than
  /// concurrency level to avoid over-subscription.
  ///
  /// Default: 1
  final int isolates;

  /// Creates a new concurrent processing strategy.
  ///
  /// **Parameters:**
  /// - [concurrency]: Maximum parallel operations (must be ≥ 1)
  /// - [isolates]: Number of isolates to use for processing (must be ≥ 1)
  ///
  /// **Throws:**
  /// - [ArgumentError] if [concurrency] or [isolates] is less than 1
  ///
  /// ```dart
  /// // Create strategy that processes up to 4 elements simultaneously
  /// // using 2 isolates for load distribution
  /// final strategy = ConcurrentProcessingStrategy(
  ///   concurrency: 4,
  ///   isolates: 2,
  /// );
  /// ```
  ///
  /// **Recommendations:**
  /// - For CPU-bound work: `concurrency` ≈ number of CPU cores
  /// - For I/O-bound work: `concurrency` can be higher
  /// - Generally: `isolates` ≤ `concurrency` to avoid over-subscription
  ConcurrentProcessingStrategy({this.concurrency = 1, this.isolates = 1}) {
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
    final pool = _IsolatePool<T, E>(handler, isolates);
    final subscription = source.listen(
      null,
      onError: controller.addError,
      onDone: () {},
    );

    final Map<int, Future<E>> pending = {};
    final Map<int, _EventResult<T, E>> completed = {};
    int nextInputIndex = 0;
    int nextOutputIndex = 0;
    int activeTasks = 0;
    bool inputDone = false;

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
      activeTasks++;

      final computation = pool.acquire().then((isolateWrapper) {
        return isolateWrapper.compute(event);
      });

      pending[index] = computation;

      computation.then((result) {
        completed[index] = _EventResult<T, E>(event).withResult(result: result);
        pending.remove(index);
        activeTasks--;
        tryEmitNext();

        if (activeTasks < concurrency && !inputDone) {
          subscription.resume();
        }
      }).catchError((exception, stackTrace) {
        completed[index] = _EventResult<T, E>(event)
            .withException(exception: exception, stackTrace: stackTrace);
        pending.remove(index);
        activeTasks--;
        tryEmitNext();

        if (activeTasks < concurrency && !inputDone) {
          subscription.resume();
        }
      });

      if (activeTasks >= concurrency) {
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
