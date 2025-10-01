import 'dart:async';
import 'package:test/test.dart';
import 'package:isolated_stream/isolated_stream.dart';
import 'test_handlers.dart';

void main() {
  group('concurrent processing', () {
    test('concurrent processing reduces total time', () async {
      final numbers = [1, 2, 3, 4, 5, 6];
      final handler = AsyncMultiplyHandler(2, delay: const Duration(milliseconds: 30));
      
      // Test sequential processing time
      final sequential = Stopwatch()..start();
      final sequentialResults = await Stream.fromIterable(numbers)
          .isolatedMap(handler, concurrency: 1)
          .toList();
      sequential.stop();
      
      // Test concurrent processing time  
      final concurrent = Stopwatch()..start();
      final concurrentResults = await Stream.fromIterable(numbers)
          .isolatedMap(handler, concurrency: 3)
          .toList();
      concurrent.stop();
      
      expect(sequentialResults, equals([2, 4, 6, 8, 10, 12]));
      expect(concurrentResults, equals([2, 4, 6, 8, 10, 12]));
      
      // Concurrent should be faster, but allow for system variations
      expect(concurrent.elapsedMilliseconds, lessThan(sequential.elapsedMilliseconds));
    });

    test('order preserved despite variable processing times', () async {
      final handler = VariableDelayHandler();
      final numbers = [5, 1, 3, 2, 4]; // Mixed order input
      
      final results = await Stream.fromIterable(numbers)
          .isolatedMap(handler, concurrency: 5)
          .toList();
      
      // Output maintains input order regardless of completion time
      expect(results, equals([10, 2, 6, 4, 8]));
    });

    test('backpressure prevents overwhelming downstream', () async {
      final controller = StreamController<int>();
      final handler = AsyncMultiplyHandler(1, delay: const Duration(milliseconds: 50));
      
      final transformed = controller.stream.isolatedMap(handler, concurrency: 2);
      final future = transformed.take(4).toList();
      
      // Rapidly add more items than concurrency allows
      for (int i = 1; i <= 6; i++) {
        controller.add(i);
      }
      
      final results = await future;
      expect(results, equals([1, 2, 3, 4]));
      
      await controller.close();
    });

    test('high concurrency handles many items efficiently', () async {
      final items = List.generate(30, (i) => i + 1);
      
      final results = await Stream.fromIterable(items)
          .isolatedMap(AsyncMultiplyHandler(2, delay: const Duration(milliseconds: 5)), 
                      concurrency: 15)
          .toList();
      
      expect(results, hasLength(30));
      expect(results.first, equals(2));
      expect(results.last, equals(60));
    });

    test('errors during concurrent processing are propagated', () async {
      final handler = AsyncErrorHandler('Processing failed');
      
      final future = Stream.fromIterable([1, 2, 3])
          .isolatedMap(handler, concurrency: 2)
          .toList();
      
      await expectLater(
        future,
        throwsA(isA<Exception>().having(
          (e) => e.toString(), 
          'message', 
          contains('Processing failed'),
        )),
      );
    });

    test('stream completion during processing works correctly', () async {
      final controller = StreamController<int>();
      final handler = AsyncMultiplyHandler(2, delay: const Duration(milliseconds: 40));
      
      final future = controller.stream
          .isolatedMap(handler, concurrency: 2)
          .toList();
      
      // Add data then close immediately 
      controller.add(1);
      controller.add(2);
      controller.add(3);
      await controller.close();
      
      final results = await future;
      expect(results, equals([2, 4, 6]));
    });
  });

  group('resource management', () {
    test('supports multiple simultaneous operations', () async {
      final sourceData = [1, 2, 3, 4, 5];
      
      final doubleOperation = Stream.fromIterable(sourceData)
          .isolatedMap(MultiplyHandler(2))
          .toList();
      
      final tripleOperation = Stream.fromIterable(sourceData)
          .isolatedMap(MultiplyHandler(3))
          .toList();
      
      final results = await Future.wait([doubleOperation, tripleOperation]);
      
      expect(results[0], equals([2, 4, 6, 8, 10]));
      expect(results[1], equals([3, 6, 9, 12, 15]));
    });

    test('subscription cancellation stops processing correctly', () async {
      final controller = StreamController<int>();
      final handler = AsyncMultiplyHandler(2, delay: const Duration(milliseconds: 50));
      
      final results = <int>[];
      late StreamSubscription subscription;
      
      subscription = controller.stream
          .isolatedMap(handler, concurrency: 2)
          .listen((value) {
            results.add(value);
            if (results.length == 2) {
              subscription.cancel();
            }
          });
      
      // Add more data than we want to process
      for (int i = 1; i <= 6; i++) {
        controller.add(i);
      }
      
      await Future.delayed(const Duration(milliseconds: 150));
      
      expect(results, hasLength(2));
      expect(results, equals([2, 4]));
      
      await controller.close();
    });
  });

  group('Performance Comparison', () {
    test('concurrency improves processing time for async work', () async {
      final numbers = List.generate(8, (i) => i + 1);
      final handler = AsyncMultiplyHandler(2, delay: const Duration(milliseconds: 25));
      
      // Measure sequential processing
      final sequentialTimer = Stopwatch()..start();
      final sequentialResults = await Stream.fromIterable(numbers)
          .isolatedMap(handler, concurrency: 1)
          .toList();
      sequentialTimer.stop();
      
      // Measure concurrent processing
      final concurrentTimer = Stopwatch()..start();
      final concurrentResults = await Stream.fromIterable(numbers)
          .isolatedMap(handler, concurrency: 4)
          .toList();
      concurrentTimer.stop();
      
      expect(sequentialResults, equals(concurrentResults));
      expect(concurrentTimer.elapsedMilliseconds, 
             lessThan(sequentialTimer.elapsedMilliseconds));
    });
  });
}