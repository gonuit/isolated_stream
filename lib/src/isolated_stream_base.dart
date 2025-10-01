import 'dart:async';
import 'dart:isolate';

part 'messages.dart';
part 'isolate_entry.dart';
part 'isolate_wrapper.dart';
part 'isolate_handler.dart';
part 'isolate_pool.dart';

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
  /// Throws [ArgumentError] if [concurrency] or [isolates] are less than or equal to 0.
  /// Throws [StateError] if the isolate fails to initialize.
  Stream<E> isolatedMap<E>(
    IsolatedHandler<T, E> handler, {
    int concurrency = 1,
    int isolates = 1,
  }) {
    // Validate arguments
    if (concurrency <= 0) {
      throw ArgumentError.value(
        concurrency, 
        'concurrency', 
        'Must be greater than 0',
      );
    }
    if (isolates <= 0) {
      throw ArgumentError.value(
        isolates, 
        'isolates', 
        'Must be greater than 0',
      );
    }
    
    final StreamController<E> controller;
    if (isBroadcast) {
      controller = StreamController.broadcast(sync: true);
    } else {
      controller = StreamController(sync: true);
    }

    controller.onListen = () {
      if (concurrency == 1 && isolates == 1) {
        _setupSequentialProcessing<T, E>(controller, handler, this);
      } else if (isolates == 1) {
        _setupConcurrentProcessing<T, E>(controller, handler, this, concurrency);
      } else {
        _setupPooledProcessing<T, E>(controller, handler, this, concurrency, isolates);
      }
    };
    return controller.stream;
  }
}

void _setupSequentialProcessing<T, E>(
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

void _setupConcurrentProcessing<T, E>(
  StreamController<E> controller,
  IsolatedHandler<T, E> handler,
  Stream<T> source,
  int concurrency,
) {
  final isolateWrapper = _IsolateWrapper<T, E>(handler);
  final subscription = source.listen(
    null,
    onError: controller.addError,
    onDone: () {},
  );

  final Map<int, Future<E>> pending = {};
  final Map<int, E> completed = {};
  int nextInputIndex = 0;
  int nextOutputIndex = 0;
  int activeTasks = 0;
  bool inputDone = false;

  void tryEmitNext() {
    while (completed.containsKey(nextOutputIndex)) {
      final result = completed.remove(nextOutputIndex);
      if (result != null) {
        controller.add(result);
      }
      nextOutputIndex++;
    }
    
    if (inputDone && pending.isEmpty && completed.isEmpty) {
      controller.close();
    }
  }

  void processEvent(T event) {
    final index = nextInputIndex++;
    activeTasks++;
    
    final Future<E> future = isolateWrapper.compute(event);
    
    pending[index] = future;
    
    future.then((result) {
      completed[index] = result;
      pending.remove(index);
      activeTasks--;
      tryEmitNext();
      
      if (activeTasks < concurrency && !inputDone) {
        subscription.resume();
      }
    }).catchError((error, stackTrace) {
      pending.remove(index);
      activeTasks--;
      controller.addError(error, stackTrace);
      
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
      controller.close();
    }
  });

  controller.onCancel = () {
    subscription.cancel().whenComplete(isolateWrapper.kill);
  };
  
  if (!source.isBroadcast) {
    controller
      ..onPause = subscription.pause
      ..onResume = subscription.resume;
  }
}

void _setupPooledProcessing<T, E>(
  StreamController<E> controller,
  IsolatedHandler<T, E> handler,
  Stream<T> source,
  int concurrency,
  int isolateCount,
) {
  final pool = _IsolatePool<T, E>(handler, isolateCount);
  final subscription = source.listen(
    null,
    onError: controller.addError,
    onDone: () {},
  );

  final Map<int, Future<E>> pending = {};
  final Map<int, E> completed = {};
  int nextInputIndex = 0;
  int nextOutputIndex = 0;
  int activeTasks = 0;
  bool inputDone = false;

  void tryEmitNext() {
    while (completed.containsKey(nextOutputIndex)) {
      final result = completed.remove(nextOutputIndex);
      if (result != null) {
        controller.add(result);
      }
      nextOutputIndex++;
    }
    
    if (inputDone && pending.isEmpty && completed.isEmpty) {
      pool.dispose().whenComplete(() => controller.close());
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
      completed[index] = result;
      pending.remove(index);
      activeTasks--;
      tryEmitNext();
      
      if (activeTasks < concurrency && !inputDone) {
        subscription.resume();
      }
    }).catchError((error, stackTrace) {
      pending.remove(index);
      activeTasks--;
      controller.addError(error, stackTrace);
      
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
      pool.dispose().whenComplete(() => controller.close());
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
