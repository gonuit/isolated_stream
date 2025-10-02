part of 'isolated_stream_base.dart';

/// Abstract interface for different isolated stream processing strategies.
@immutable
abstract interface class IsolatedProcessingStrategy {
  const factory IsolatedProcessingStrategy.sequential() =
      SequentialProcessingStrategy;

  factory IsolatedProcessingStrategy.restartable(
      {int concurrency, int isolates}) = RestartableProcessingStrategy;
  @experimental
  factory IsolatedProcessingStrategy.droppable(
      {int concurrency, int isolates}) = DroppableProcessingStrategy;
  @experimental
  factory IsolatedProcessingStrategy.concurrent(
      {int concurrency, int isolates}) = ConcurrentProcessingStrategy;

  void process<T, E>(
    StreamController<E> controller,
    IsolatedHandler<T, E> handler,
    Stream<T> source,
  );
}

/// Sequential processing strategy - processes one element at a time in order.
class SequentialProcessingStrategy implements IsolatedProcessingStrategy {
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

class DroppableProcessingStrategy implements IsolatedProcessingStrategy {
  /// Number of isolates to use.
  final int isolates;
  final int concurrency;

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

// /// Droppable processing strategy - drops new events when at max concurrency.
// class DroppableProcessingStrategy implements IsolatedProcessingStrategy {
//   final int concurrency;
//   final int isolates;

//   DroppableProcessingStrategy({
//     this.concurrency = 1,
//     this.isolates = 1,
//   }) {
//     if (concurrency < 1) {
//       throw ArgumentError.value(
//           concurrency, 'concurrency', 'Must be at least 1');
//     }
//     if (isolates < 1) {
//       throw ArgumentError.value(isolates, 'isolates', 'Must be at least 1');
//     }
//   }

//   @override
//   void process<T, E>(
//     StreamController<E> controller,
//     IsolatedHandler<T, E> handler,
//     Stream<T> source,
//   ) {
//     final pool = _IsolatePool<T, E>(handler, isolates);
//     final subscription = source.listen(
//       null,
//       onError: controller.addError,
//       onDone: () {},
//     );

//     int activeTasks = 0;
//     bool inputDone = false;

//     void checkAndClose() {
//       if (inputDone && activeTasks == 0) {
//         pool.dispose();
//         controller.close();
//       }
//     }

//     void processEvent(T event) {
//       // Drop new events if we're at max concurrency
//       if (activeTasks >= concurrency) {
//         return;
//       }

//       activeTasks++;

//       pool.acquire().then((isolateWrapper) {
//         return isolateWrapper.compute(event);
//       }).then((result) {
//         activeTasks--;
//         if (!controller.isClosed) {
//           controller.add(result);
//         }
//         checkAndClose();
//       }).catchError((error, stackTrace) {
//         activeTasks--;
//         if (!controller.isClosed) {
//           controller.addError(error, stackTrace);
//         }
//         checkAndClose();
//       });
//     }

//     subscription.onData(processEvent);
//     subscription.onDone(() {
//       inputDone = true;
//       checkAndClose();
//     });

//     controller.onCancel = () {
//       subscription.cancel().whenComplete(() => pool.dispose());
//     };

//     if (!source.isBroadcast) {
//       controller
//         ..onPause = subscription.pause
//         ..onResume = subscription.resume;
//     }
//   }
// }

class _EventResult<S, R> {
  final S event;
  final R? result;
  final Object? exception;
  final StackTrace? stackTrace;

  /// Additional flag to indicate if a result is present as result may be null
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

/// Restartable processing strategy - cancels/ignores previous operations results when new events arrive.
class RestartableProcessingStrategy implements IsolatedProcessingStrategy {
  /// Number of isolates to use.
  final int isolates;
  final int concurrency;

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

/// Concurrent processing strategy - processes multiple elements simultaneously while preserving order.
class ConcurrentProcessingStrategy implements IsolatedProcessingStrategy {
  final int concurrency;
  final int isolates;

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
