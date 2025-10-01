import 'dart:async';
import 'package:test/test.dart';
import 'package:isolated_stream/isolated_stream.dart';
import 'test_handlers.dart';

void main() {
  group('IsolatedStream', () {
    group('basic transformation', () {
      test('transforms elements using synchronous handler', () async {
        // Arrange
        final stream = Stream.fromIterable([1, 2, 3, 4, 5]);
        final handler = MultiplyHandler(10);
        
        // Act
        final results = await stream.isolatedMap(handler).toList();
        
        // Assert
        expect(results, equals([10, 20, 30, 40, 50]));
      });

      test('transforms elements using asynchronous handler', () async {
        final stream = Stream.fromIterable([1, 2, 3]);
        final handler = AsyncMultiplyHandler(5);
        
        final results = await stream.isolatedMap(handler).toList();
        
        expect(results, equals([5, 10, 15]));
      });

      test('preserves element order despite variable processing delays', () async {
        final stream = Stream.fromIterable([1, 2, 3, 4, 5]);
        final handler = VariableDelayHandler();
        
        final results = await stream.isolatedMap(handler).toList();
        
        expect(results, equals([2, 4, 6, 8, 10]));
      });

      test('handles empty streams correctly', () async {
        final stream = Stream<int>.empty();
        final handler = MultiplyHandler(10);
        
        final results = await stream.isolatedMap(handler).toList();
        
        expect(results, isEmpty);
      });

      test('handles single element streams', () async {
        final stream = Stream.value(42);
        final handler = MultiplyHandler(2);
        
        final results = await stream.isolatedMap(handler).toList();
        
        expect(results, equals([84]));
      });
    });

    group('Concurrent Processing', () {
      test('concurrent processing preserves order', () async {
        final numbers = [1, 2, 3, 4, 5];
        
        final results = await Stream.fromIterable(numbers)
            .isolatedMap(AsyncMultiplyHandler(10), concurrency: 3)
            .toList();
        
        expect(results, equals([10, 20, 30, 40, 50]));
      });

      test('concurrency limits are respected', () async {
        final numbers = [1, 2, 3, 4];
        final handler = AsyncMultiplyHandler(2, delay: const Duration(milliseconds: 50));
        
        final timer = Stopwatch()..start();
        final results = await Stream.fromIterable(numbers)
            .isolatedMap(handler, concurrency: 2)
            .toList();
        timer.stop();
        
        expect(results, equals([2, 4, 6, 8]));
        // Should be faster than sequential but not immediate
        expect(timer.elapsedMilliseconds, greaterThan(90));
        expect(timer.elapsedMilliseconds, lessThan(250));
      });

      test('should handle variable processing times with concurrency', () async {
        final stream = Stream.fromIterable([1, 2, 3, 4, 5, 6]);
        final transformed = stream.isolatedMap(VariableDelayHandler(), concurrency: 3);
        
        final results = await transformed.toList();
        expect(results, equals([2, 4, 6, 8, 10, 12]));
      });

      test('should maintain order even when later elements complete first', () async {
        // Create a handler where later elements finish faster
        final handler = AsyncMultiplyHandler(1);
        final stream = Stream.fromIterable([5, 1, 4, 2, 3]); // 5 takes longest, 1 takes shortest
        
        final transformed = stream.isolatedMap(handler, concurrency: 5);
        final results = await transformed.toList();
        
        expect(results, equals([5, 1, 4, 2, 3])); // Order preserved
      });
    });

    group('Isolate Pool Tests', () {
      test('should use single isolate when isolates parameter is 1', () async {
        final stream = Stream.fromIterable([1, 2, 3, 4, 5]);
        final transformed = stream.isolatedMap(
          MultiplyHandler(2), 
          concurrency: 3,
          isolates: 1, // Single isolate
        );
        
        final results = await transformed.toList();
        expect(results, equals([2, 4, 6, 8, 10]));
      });

      test('should use isolate pool when isolates parameter > 1', () async {
        final stream = Stream.fromIterable([1, 2, 3, 4, 5, 6, 7, 8]);
        final transformed = stream.isolatedMap(
          AsyncMultiplyHandler(3, delay: const Duration(milliseconds: 50)), 
          concurrency: 4,
          isolates: 2, // Use 2 isolates
        );
        
        final results = await transformed.toList();
        expect(results, equals([3, 6, 9, 12, 15, 18, 21, 24]));
      });

      test('should maintain order with multiple isolates', () async {
        final stream = Stream.fromIterable([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        final transformed = stream.isolatedMap(
          VariableDelayHandler(), // Variable processing times
          concurrency: 5,
          isolates: 3, // Use 3 isolates
        );
        
        final results = await transformed.toList();
        expect(results, equals([2, 4, 6, 8, 10, 12, 14, 16, 18, 20]));
      });
    });

    group('error propagation', () {
      test('propagates synchronous handler errors', () async {
        final stream = Stream.fromIterable([1, 2, 3]);
        final errorHandler = ErrorHandler('Handler failed');
        
        final future = stream.isolatedMap(errorHandler).toList();
        
        await expectLater(
          future,
          throwsA(isA<Exception>().having(
            (e) => e.toString(), 
            'message', 
            contains('Handler failed'),
          )),
        );
      });

      test('propagates asynchronous handler errors', () async {
        final stream = Stream.fromIterable([1, 2, 3]);
        final asyncErrorHandler = AsyncErrorHandler('Async operation failed');
        
        final future = stream.isolatedMap(asyncErrorHandler).toList();
        
        await expectLater(
          future,
          throwsA(isA<Exception>().having(
            (e) => e.toString(), 
            'message', 
            contains('Async operation failed'),
          )),
        );
      });

      test('propagates errors during concurrent processing', () async {
        final stream = Stream.fromIterable([1, 2, 3]);
        final errorHandler = ErrorHandler('Concurrent processing error');
        
        final future = stream.isolatedMap(errorHandler, concurrency: 2).toList();
        
        await expectLater(
          future,
          throwsA(isA<Exception>().having(
            (e) => e.toString(), 
            'message', 
            contains('Concurrent processing error'),
          )),
        );
      });
    });

    group('Type Safety and Generic Support', () {
      test('should handle different input/output types', () async {
        final stream = Stream.fromIterable(['1', '2', '3', '4']);
        final transformed = stream.isolatedMap(StringToIntHandler());
        
        final results = await transformed.toList();
        expect(results, equals([1, 2, 3, 4]));
        expect(results, everyElement(isA<int>()));
      });

      test('should handle complex data types', () async {
        final input = <Map<String, dynamic>>[
          {'name': 'Alice', 'age': 30},
          {'name': 'Bob', 'age': 25},
        ];
        final stream = Stream<Map<String, dynamic>>.fromIterable(input);
        final transformed = stream.isolatedMap(ComplexDataHandler());
        
        final results = await transformed.toList();
        expect(results, hasLength(2));
        expect(results[0]['original'], equals(input[0]));
        expect(results[0]['keys_count'], equals(2));
        expect(results[1]['original'], equals(input[1]));
      });

      test('should handle boolean return types', () async {
        final stream = Stream.fromIterable([2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
        final transformed = stream.isolatedMap(PrimeCheckHandler());
        
        final results = await transformed.toList();
        expect(results, equals([true, true, false, true, false, true, false, false, false, true]));
        expect(results, everyElement(isA<bool>()));
      });
    });

    group('Stream Behavior', () {
      test('should work with broadcast streams', () async {
        final controller = StreamController<int>.broadcast();
        final transformed = controller.stream.isolatedMap(MultiplyHandler(3));
        
        // Add listener first
        final future = transformed.take(3).toList();
        
        // Then add data
        controller.add(1);
        controller.add(2);
        controller.add(3);
        await controller.close();
        
        final results = await future;
        expect(results, equals([3, 6, 9]));
      });

      test('should handle stream cancellation', () async {
        final controller = StreamController<int>();
        final transformed = controller.stream.isolatedMap(AsyncMultiplyHandler(2));
        
        late StreamSubscription subscription;
        final results = <int>[];
        
        subscription = transformed.listen((value) {
          results.add(value);
          if (results.length == 2) {
            subscription.cancel();
          }
        });
        
        controller.add(1);
        controller.add(2);
        controller.add(3); // This should not be processed
        
        // Wait a bit for processing
        await Future.delayed(const Duration(milliseconds: 100));
        
        expect(results, equals([2, 4]));
        await controller.close();
      });
    });

    group('Handler Features', () {
      test('should use debug names for handlers', () {
        final handler = MultiplyHandler(5);
        expect(handler.debugName, equals('Multiply by 5'));
      });

      test('should handle handlers without debug names', () {
        final handler = MultiplyHandler(3);
        expect(handler.debugName, isNotNull);
      });
    });

    group('parameter validation', () {
      late Stream<int> stream;
      late MultiplyHandler handler;
      
      setUp(() {
        stream = Stream.fromIterable([1, 2, 3]);
        handler = MultiplyHandler(2);
      });
      
      test('throws ArgumentError when concurrency is zero', () {
        expect(
          () => stream.isolatedMap(handler, concurrency: 0),
          throwsArgumentError,
        );
      });

      test('throws ArgumentError when concurrency is negative', () {
        expect(
          () => stream.isolatedMap(handler, concurrency: -1),
          throwsArgumentError,
        );
      });

      test('throws ArgumentError when isolates count is zero', () {
        expect(
          () => stream.isolatedMap(handler, isolates: 0),
          throwsArgumentError,
        );
      });

      test('throws ArgumentError when isolates count is negative', () {
        expect(
          () => stream.isolatedMap(handler, isolates: -1),
          throwsArgumentError,
        );
      });

      test('rejects invalid combinations of parameters', () {
        expect(
          () => stream.isolatedMap(handler, concurrency: 0, isolates: 0),
          throwsArgumentError,
        );
      });

      test('accepts valid positive parameter values', () async {
        final transformed = stream.isolatedMap(
          handler, 
          concurrency: 1, 
          isolates: 1,
        );
        
        final results = await transformed.toList();
        expect(results, equals([2, 4, 6]));
      });
    });
  });
}