import 'dart:async';
import 'dart:isolate';

import 'package:meta/meta.dart';

part 'messages.dart';
part 'isolate_entry.dart';
part 'isolate_wrapper.dart';
part 'isolate_handler.dart';
part 'isolate_pool.dart';
part 'processing_strategy.dart';

/// Extension that adds isolated processing capabilities to streams.
extension IsolatedStreamExtension<T> on Stream<T> {
  /// Transforms each element using the provided [handler] in a separate isolate.
  ///
  /// This method is useful for CPU-intensive transformations that would otherwise
  /// block the main isolate. By default, elements are processed sequentially to
  /// maintain order and prevent resource exhaustion.
  ///
  /// Set [concurrency] to allow multiple elements to be processed simultaneously
  /// while still preserving the output order. Higher concurrency can improve
  /// throughput but uses more resources.
  ///
  /// Set [isolates] to use multiple isolates for distributing work. This can
  /// improve performance for high-throughput scenarios by reusing isolates
  /// and distributing work across multiple isolate instances.
  ///
  /// The [mode] parameter controls how concurrent processing behaves:
  /// - [IsolatedConcurrency.concurrent]: Standard concurrent processing (default)
  /// - [IsolatedConcurrency.sequential]: Process elements one at a time in order
  /// - [IsolatedConcurrency.droppable]: Drop new events when at max concurrency
  /// - [IsolatedConcurrency.restartable]: Cancel previous operations when new events arrive
  ///
  /// The [handler] defines the transformation logic and must be serializable.
  ///
  /// Example:
  /// ```dart
  /// final stream = Stream.fromIterable([1, 2, 3, 4, 5]);
  /// final transformed = stream.isolatedMap(
  ///   MultiplyHandler(10),
  ///   concurrency: 3,
  ///   isolates: 2,
  /// );
  /// await for (final value in transformed) {
  ///   print(value); // 10, 20, 30, 40, 50 (in order)
  /// }
  /// ```
  ///
  /// Throws [StateError] if the isolate fails to initialize.
  ///
  /// Examples:
  /// ```dart
  /// // Sequential processing (default)
  /// stream.isolatedMap(handler);
  ///
  /// // Concurrent processing with 3 concurrent tasks
  /// stream.isolatedMap(handler,
  ///   strategy: IsolatedProcessingStrategy.concurrent(3, 1));
  ///
  /// // Droppable processing that drops events when busy
  /// stream.isolatedMap(handler,
  ///   strategy: IsolatedProcessingStrategy.droppable(2, 1));
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
