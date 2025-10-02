import 'dart:async';
import 'package:test/test.dart';
import 'package:isolated_stream/isolated_stream.dart';
import 'test_handlers.dart';

void main() {
  group('SequentialProcessingStrategy', () {
    final handler = ValueHandler();

    test('emit all events with concurrency 1 - process one at a time',
        () async {
      final results = <int>[];
      final s = Stopwatch()..start();
      final subscription = Stream.fromIterable([
        TestHandlerEvent(value: 1, delayMs: 50),
        TestHandlerEvent(value: 2, delayMs: 50),
        TestHandlerEvent(value: 3, delayMs: 50),
      ])
          .isolatedMap(
            handler,
            strategy: IsolatedProcessingStrategy.sequential(),
          )
          .listen(results.add);

      await subscription.asFuture();
      s.stop();
      expect(s.elapsedMilliseconds, greaterThanOrEqualTo(150));

      // Preserve order
      expect(results, equals([1, 2, 3]));
    });

    test(
        'emit all sequential events with sequential strategy with varying delays',
        () async {
      final results = <int>[];
      final s = Stopwatch()..start();
      final subscription = Stream.fromIterable([
        TestHandlerEvent(value: 1, delayMs: 50),
        TestHandlerEvent(value: 2, delayMs: 45),
        TestHandlerEvent(value: 3, delayMs: 40),
      ])
          .isolatedMap(
            handler,
            strategy: IsolatedProcessingStrategy.sequential(),
          )
          .listen(results.add);

      await subscription.asFuture();
      s.stop();
      expect(s.elapsedMilliseconds, greaterThanOrEqualTo(50 + 45 + 40));

      // Preserve order
      expect(results, equals([1, 2, 3]));
    });

    test('process all events when not overlapping', () async {
      final results = <int>[];
      final controller = StreamController<TestHandlerEvent>();

      final subscription = controller.stream
          .isolatedMap(handler,
              strategy: IsolatedProcessingStrategy.sequential())
          .listen(results.add);

      controller.add(TestHandlerEvent(value: 1, delayMs: 0));
      await Future.delayed(Duration(milliseconds: 10));
      controller.add(TestHandlerEvent(value: 2, delayMs: 0));
      await Future.delayed(Duration(milliseconds: 10));
      controller
        ..add(TestHandlerEvent(value: 3, delayMs: 0))
        ..close();

      await subscription.asFuture();

      // Should get all the results
      expect(results, equals([1, 2, 3]));
      expect(results, hasLength(3));
    });

    test('handles rapid event stream correctly', () async {
      final results = <int>[];
      final subscription = Stream.fromIterable(
        List.generate(
          10,
          (i) => TestHandlerEvent(value: i + 1, delayMs: 20 - i * 2),
        ),
      )
          .isolatedMap(
            handler,
            strategy: IsolatedProcessingStrategy.sequential(),
          )
          .listen(results.add);

      await subscription.asFuture();

      // Emit all events and preserve order
      expect(results, equals([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
      expect(results.length, equals(10));
    });

    test('emits intermediate results when events are spaced apart', () async {
      final results = <int>[];
      final controller = StreamController<TestHandlerEvent>();
      final subscription = controller.stream
          .isolatedMap(handler,
              strategy: IsolatedProcessingStrategy.sequential())
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
        strategy: IsolatedProcessingStrategy.sequential(),
      )
          .listen(
        cancelOnError: false,
        (value) {
          results.add(value);
        },
        onError: (error, st) {
          print('error: $error');
          errors.add(error);
        },
      );

      // Should be kept
      controller.add(TestHandlerEvent(value: 1, delayMs: 30));
      // Should be kept and error (concurrency=2 keeps last 2)
      controller.add(TestHandlerEvent(
        value: 2,
        delayMs: 30,
        exception: 'Error on value: 2',
      ));
      // Should be dropped
      controller.add(TestHandlerEvent(value: 3, delayMs: 30));
      // Should be dropped (error are also dropped)
      controller.add(TestHandlerEvent(
        value: 4,
        delayMs: 30,
        exception: 'Error on value: 4',
      ));

      controller.close();

      final completer = Completer<void>();
      subscription.onDone(() {
        completer.complete();
      });
      await completer.future;

      expect(results, equals([1, 3]));
      expect(errors.first.toString(), contains('Error on value: 2'));
      expect(errors.last.toString(), contains('Error on value: 4'));
      expect(errors, hasLength(2));
    });

    test('stream cancellation cleans up resources', () async {
      final results = <int>[];
      final subscription = Stream.fromIterable([
        TestHandlerEvent(value: 1, delayMs: 30),
        TestHandlerEvent(value: 2, delayMs: 30),
        TestHandlerEvent(value: 3, delayMs: 30),
      ])
          .isolatedMap(
            handler,
            strategy: IsolatedProcessingStrategy.sequential(),
          )
          .listen(results.add);

      // Cancel before completion
      // Should handle cancellation gracefully without hanging
      await subscription.cancel();

      // No results due to early cancellation
      expect(results, isEmpty);
    });
  });
}
