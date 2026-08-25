import 'dart:async';
import 'dart:collection';
import 'dart:isolate';

part 'messages.dart';
part 'isolate_entry.dart';
part 'isolate_wrapper.dart';
part 'isolate_handler.dart';
part 'isolate_pool.dart';
part 'work_policy.dart';
part 'processing_strategy.dart';
part 'isolated_worker.dart';

/// Extension that adds isolated processing capabilities to streams.
extension IsolatedStreamExtension<T> on Stream<T> {
  /// Transforms each element using the provided [handler] in a separate isolate.
  ///
  /// This method is useful for CPU-intensive transformations that would otherwise
  /// block the main isolate. By default, elements are processed sequentially.
  ///
  /// The [handler] defines the transformation logic and must be sendable
  /// to an isolate.
  ///
  /// Example:
  /// ```dart
  /// final stream = Stream.fromIterable([1, 2, 3, 4, 5]);
  /// final transformed = stream.isolatedMap(
  ///   MultiplyHandler(10),
  ///   strategy: IsolatedProcessingStrategy.concurrent(
  ///     concurrency: 3,
  ///     isolates: 2,
  ///   ),
  /// );
  /// await for (final value in transformed) {
  ///   print(value); // 10, 20, 30, 40, 50 (in order)
  /// }
  /// ```
  ///
  /// Examples:
  /// ```dart
  /// // Sequential processing (default)
  /// stream.isolatedMap(handler);
  ///
  /// // Concurrent processing with 3 concurrent tasks
  /// stream.isolatedMap(handler,
  ///   strategy: IsolatedProcessingStrategy.concurrent(concurrency: 3));
  ///
  /// // Droppable processing that drops events when busy
  /// stream.isolatedMap(handler,
  ///   strategy: IsolatedProcessingStrategy.droppable(concurrency: 2));
  /// ```
  Stream<E> isolatedMap<E>(
    IsolatedHandler<T, E> handler, {
    IsolatedProcessingStrategy strategy =
        const IsolatedProcessingStrategy.sequential(),
  }) {
    final StreamController<E> controller;
    if (isBroadcast) {
      controller = StreamController.broadcast(sync: true);
    } else {
      controller = StreamController(sync: true);
    }

    bool initialized = false;

    controller.onListen = () {
      if (!initialized) {
        strategy.process(controller, handler, this);
        initialized = true;
      }
    };

    return controller.stream;
  }
}
