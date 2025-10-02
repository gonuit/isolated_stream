import 'dart:async';
import 'package:test/test.dart';
import 'package:isolated_stream/isolated_stream.dart';
import 'test_handlers.dart';

void main() {
  group('RestartableProcessingStrategy', () {
    final handler = ValueHandler();

    test('only emits result from latest event with concurrency 1', () async {
      final results = <int>[];
      final subscription = Stream.fromIterable([
        TestHandlerEvent(value: 1, delayMs: 50),
        TestHandlerEvent(value: 2, delayMs: 50),
        TestHandlerEvent(value: 3, delayMs: 50),
      ])
          .isolatedMap(
            handler,
            strategy: IsolatedProcessingStrategy.restartable(concurrency: 1),
          )
          .listen(results.add);

      await subscription.asFuture();

      // Only the last event should produce a result
      expect(results, equals([3]));
    });

    test('drops oldest events when exceeding concurrency limit', () async {
      final results = <int>[];
      final subscription = Stream.fromIterable([
        TestHandlerEvent(value: 1, delayMs: 200),
        TestHandlerEvent(value: 2, delayMs: 100),
        TestHandlerEvent(value: 3, delayMs: 50),
        TestHandlerEvent(value: 4, delayMs: 0),
      ])
          .isolatedMap(handler,
              strategy: IsolatedProcessingStrategy.restartable(concurrency: 2))
          .listen(results.add);

      await subscription.asFuture();

      expect(results.length, equals(2));
      expect(results, equals([3, 4]));
    });

    test('process all events when not overlapping', () async {
      final results = <int>[];
      final controller = StreamController<TestHandlerEvent>.broadcast();

      final subscription = controller.stream
          .isolatedMap(handler,
              strategy: IsolatedProcessingStrategy.restartable(concurrency: 1))
          .listen(results.add);

      controller.add(TestHandlerEvent(value: 1, delayMs: 0));
      await Future.delayed(Duration(milliseconds: 10));
      controller.add(TestHandlerEvent(value: 2, delayMs: 0));
      await Future.delayed(Duration(milliseconds: 10));
      controller
        ..add(TestHandlerEvent(value: 3, delayMs: 0))
        ..close();

      await subscription.asFuture();

      // Should get the result from the latest event
      expect(results, equals([1, 2, 3]));
      expect(results, hasLength(3));
    });

    test('handles rapid event stream correctly', () async {
      final results = <int>[];
      final subscription = Stream.fromIterable(
        List.generate(
          10,
          (i) => TestHandlerEvent(value: i + 1, delayMs: 30),
        ),
      )
          .isolatedMap(
            handler,
            strategy: IsolatedProcessingStrategy.restartable(concurrency: 3),
          )
          .listen(results.add);

      await subscription.asFuture();

      // Should get results from the last 3 events (concurrency=3)
      // Events 8, 9, 10 should be the active ones
      expect(results.length, equals(3));
      expect(results, containsAll([8, 9, 10]));
    });

    test('emits intermediate results when events are spaced apart', () async {
      final results = <int>[];
      final controller = StreamController<TestHandlerEvent>();
      final subscription = controller.stream
          .isolatedMap(handler,
              strategy: IsolatedProcessingStrategy.restartable(concurrency: 1))
          .listen(results.add);

      // Send events with enough delay for processing to complete
      controller.add(TestHandlerEvent(value: 1, delayMs: 30));
      await Future.delayed(Duration(milliseconds: 50));
      expect(results, equals([1]));

      controller.add(TestHandlerEvent(value: 2, delayMs: 30));
      await Future.delayed(Duration(milliseconds: 50));
      expect(results, equals([1, 2]));

      controller.add(TestHandlerEvent(value: 3, delayMs: 30));
      controller.close();
      await subscription.asFuture();

      expect(results, equals([1, 2, 3]));
    });

    test('handles errors from active events', () async {
      final results = <int>[];
      final errors = <dynamic>[];
      final controller = StreamController<TestHandlerEvent>();

      final subscription = controller.stream
          .isolatedMap(
        handler,
        strategy: IsolatedProcessingStrategy.restartable(concurrency: 2),
      )
          .listen(
        cancelOnError: false,
        (value) {
          results.add(value);
        },
        onError: (error, st) {
          errors.add(error);
        },
      );

      // Send events quickly
      // Should be dropped
      controller.add(TestHandlerEvent(value: 1, delayMs: 30));
      // Should be dropped when event 4 arrives
      controller.add(TestHandlerEvent(value: 2, delayMs: 30));
      // Should be kept and error (concurrency=2 keeps last 2)
      controller.add(
        TestHandlerEvent(value: 3, delayMs: 30, exception: 'Error on value: 3'),
      );
      // Should be kept and produce result
      controller.add(TestHandlerEvent(value: 4, delayMs: 30));

      controller.close();

      final completer = Completer<void>();
      subscription.onDone(() {
        completer.complete();
      });
      await completer.future;

      expect(results, equals([4]));
      expect(errors, hasLength(1));
      expect(errors.first.toString(), contains('Error on value: 3'));
    });

    test('validates concurrency parameter', () {
      expect(
        () => IsolatedProcessingStrategy.restartable(concurrency: 0),
        throwsA(isA<ArgumentError>()),
      );

      expect(
        () => IsolatedProcessingStrategy.restartable(concurrency: -1),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('validates isolates parameter', () {
      expect(
        () => IsolatedProcessingStrategy.restartable(isolates: 0),
        throwsA(isA<ArgumentError>()),
      );

      expect(
        () => IsolatedProcessingStrategy.restartable(isolates: -1),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('higher concurrency keeps more recent events', () async {
      final results = <int>[];
      final subscription = Stream.fromIterable([
        TestHandlerEvent(value: 1, delayMs: 50),
        TestHandlerEvent(value: 2, delayMs: 50),
        TestHandlerEvent(value: 3, delayMs: 50),
        TestHandlerEvent(value: 4, delayMs: 50),
        TestHandlerEvent(value: 5, delayMs: 50),
        TestHandlerEvent(value: 6, delayMs: 50),
      ])
          .isolatedMap(handler,
              strategy: IsolatedProcessingStrategy.restartable(concurrency: 4))
          .listen(results.add);

      await subscription.asFuture();

      // Should emit results from last 4 events (concurrency=4): 3,4,5,6
      expect(results.length, equals(4));
      expect(results, containsAll([3, 4, 5, 6]));
    });

    test('stream cancellation cleans up resources', () async {
      final results = <int>[];
      final subscription = Stream.fromIterable([
        TestHandlerEvent(value: 1, delayMs: 30),
        TestHandlerEvent(value: 2, delayMs: 30),
        TestHandlerEvent(value: 3, delayMs: 30),
      ])
          .isolatedMap(handler,
              strategy: IsolatedProcessingStrategy.restartable(
                  concurrency: 2, isolates: 3))
          .listen(results.add);

      // Cancel before completion
      // Should handle cancellation gracefully without hanging
      await subscription.cancel();

      // No results due to early cancellation
      expect(results, isEmpty);
    });
  });
}
