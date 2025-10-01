import 'dart:async';
import 'package:test/test.dart';
import 'package:isolated_stream/isolated_stream.dart';

/// Handler that selectively throws errors based on input
class SelectiveErrorHandler extends IsolatedHandler<int, int> {
  final Set<int> errorValues;
  
  SelectiveErrorHandler(this.errorValues) : super(debugName: 'SelectiveError');
  
  @override
  int compute(int value) {
    if (errorValues.contains(value)) {
      throw Exception('Error for value $value');
    }
    return value * 2;
  }
}

/// Handler that has initialization logic
class InitializingHandler extends IsolatedHandler<int, String> {
  static bool _initialized = false;
  
  InitializingHandler() : super(debugName: 'Initializing');
  
  @override
  String compute(int value) {
    if (!_initialized) {
      // Simulate initialization
      _initialized = true;
    }
    return 'processed_$value';
  }
}

/// Handler for testing serialization edge cases
class SerializationTestHandler extends IsolatedHandler<List<int>, Map<String, Object>> {
  SerializationTestHandler() : super(debugName: 'SerializationTest');
  
  @override
  Map<String, Object> compute(List<int> value) {
    return {
      'sum': value.reduce((a, b) => a + b),
      'length': value.length,
      'first': value.first,
      'last': value.last,
    };
  }
}

void main() {
  group('Edge Cases and Unusual Scenarios', () {
    test('large streams process efficiently', () async {
      const itemCount = 500;
      final largeStream = Stream.fromIterable(List.generate(itemCount, (i) => i + 1));
      
      final results = await largeStream
          .isolatedMap(SelectiveErrorHandler({}), concurrency: 8)
          .toList();
      
      expect(results, hasLength(itemCount));
      expect(results.first, equals(2));
      expect(results.last, equals(itemCount * 2));
    });

    test('extreme values are handled correctly', () async {
      final extremeValues = [0, -1, -100, 999999, -2147483648, 2147483647];
      
      final results = await Stream.fromIterable(extremeValues)
          .isolatedMap(SelectiveErrorHandler({}))
          .toList();
      
      expect(results, equals([0, -2, -200, 1999998, -4294967296, 4294967294]));
    });

    test('should handle complex serializable data structures', () async {
      final complexData = [
        [1, 2, 3],
        [4, 5, 6, 7],
        [8, 9],
        [10, 11, 12, 13, 14],
      ];
      
      final stream = Stream<List<int>>.fromIterable(complexData);
      final transformed = stream.isolatedMap(SerializationTestHandler());
      
      final results = await transformed.toList();
      expect(results, hasLength(4));
      expect(results[0]['sum'], equals(6));
      expect(results[1]['length'], equals(4));
      expect(results[2]['first'], equals(8));
      expect(results[3]['last'], equals(14));
    });

    test('immediate stream closure is handled gracefully', () async {
      final controller = StreamController<int>();
      
      final future = controller.stream
          .isolatedMap(SelectiveErrorHandler({}), concurrency: 3)
          .toList();
      
      // Add data and close rapidly
      controller.add(1);
      controller.add(2);
      controller.add(3);
      await controller.close();
      
      final results = await future;
      expect(results, equals([2, 4, 6]));
    });

    test('should handle stream that produces elements faster than processing', () async {
      final controller = StreamController<int>();
      final transformed = controller.stream.isolatedMap(
        // Slow handler using our test helper
        _InlineHandler<int, int>((value) async {
          await Future.delayed(const Duration(milliseconds: 100));
          return value * 2;
        }),
        concurrency: 2,
      );
      
      // Start collecting results
      final future = transformed.take(10).toList();
      
      // Add elements very quickly
      for (int i = 1; i <= 10; i++) {
        controller.add(i);
        // Small delay to simulate high-frequency events
        await Future.delayed(const Duration(milliseconds: 5));
      }
      
      final results = await future;
      expect(results, equals([2, 4, 6, 8, 10, 12, 14, 16, 18, 20]));
      
      await controller.close();
    });

    test('should handle concurrent limit changes during processing', () async {
      // This is more of a conceptual test since our API doesn't support dynamic changes
      final stream = Stream.fromIterable(List.generate(20, (i) => i + 1));
      
      // Test with different concurrency levels
      final low = await stream.isolatedMap(SelectiveErrorHandler({}), concurrency: 1).toList();
      final medium = await stream.isolatedMap(SelectiveErrorHandler({}), concurrency: 5).toList();
      final high = await stream.isolatedMap(SelectiveErrorHandler({}), concurrency: 15).toList();
      
      // All should produce the same results
      expect(low, equals(medium));
      expect(medium, equals(high));
    });

    test('should handle memory pressure gracefully', () async {
      // Test with a large number of concurrent operations
      final stream = Stream.fromIterable(List.generate(100, (i) => i + 1));
      final transformed = stream.isolatedMap(
        SelectiveErrorHandler({}), 
        concurrency: 50, // High concurrency
      );
      
      final results = await transformed.toList();
      expect(results, hasLength(100));
      expect(results, equals(List.generate(100, (i) => (i + 1) * 2)));
    });

    test('should handle handler that returns different types', () async {
      final stream = Stream.fromIterable([1, 2, 3, 4, 5]);
      final transformed = stream.isolatedMap(InitializingHandler());
      
      final results = await transformed.toList();
      expect(results, equals(['processed_1', 'processed_2', 'processed_3', 'processed_4', 'processed_5']));
      expect(results, everyElement(isA<String>()));
    });

    test('ridiculously high concurrency works but is pointless', () async {
      final numbers = [1, 2, 3, 4, 5];
      
      // More concurrent workers than actual work items
      final results = await Stream.fromIterable(numbers)
          .isolatedMap(SelectiveErrorHandler({}), concurrency: 500)
          .toList();
      
      expect(results, equals([2, 4, 6, 8, 10]));
    });

    test('should handle zero-length delays in async handlers', () async {
      final handler = _InlineHandler<int, int>((value) async {
        await Future.delayed(Duration.zero);
        return value * 3;
      });
      
      final stream = Stream.fromIterable([1, 2, 3]);
      final transformed = stream.isolatedMap(handler, concurrency: 3);
      
      final results = await transformed.toList();
      expect(results, equals([3, 6, 9]));
    });
  });

  group('Stress Testing', () {
    test('repeated operations don\'t leak resources', () async {
      // Simulate a user repeatedly calling isolatedMap
      for (int i = 0; i < 10; i++) {
        final data = [1, 2, 3];
        final results = await Stream.fromIterable(data)
            .isolatedMap(SelectiveErrorHandler({}))
            .toList();
        
        expect(results, equals([2, 4, 6]));
      }
    });

    test('many simultaneous operations don\'t interfere', () async {
      final operations = <Future<List<int>>>[];
      
      // Launch a bunch of operations at once
      for (int i = 0; i < 15; i++) {
        final data = [1, 2, 3, 4, 5];
        final operation = Stream.fromIterable(data)
            .isolatedMap(SelectiveErrorHandler({}), concurrency: 2)
            .toList();
        operations.add(operation);
      }
      
      final allResults = await Future.wait(operations);
      
      // Every operation should produce the same result
      final expected = [2, 4, 6, 8, 10];
      for (final result in allResults) {
        expect(result, equals(expected));
      }
    });
  });

  group('Real-world Edge Cases', () {
    test('burst of data followed by silence', () async {
      final controller = StreamController<int>();
      
      final future = controller.stream
          .isolatedMap(SelectiveErrorHandler({}), concurrency: 3)
          .take(8)
          .toList();
      
      // Rapid burst
      for (int i = 1; i <= 5; i++) {
        controller.add(i);
      }
      
      // Pause then add more
      await Future.delayed(const Duration(milliseconds: 50));
      controller.add(6);
      controller.add(7);
      controller.add(8);
      
      final results = await future;
      expect(results, hasLength(8));
      expect(results, equals([2, 4, 6, 8, 10, 12, 14, 16]));
      
      await controller.close();
    });

    test('processing items that vary wildly in size', () async {
      final data = [
        [1], // Small
        List.generate(100, (i) => i), // Medium  
        [42], // Small again
        List.generate(1000, (i) => i * 2), // Large
      ];
      
      final results = await Stream.fromIterable(data)
          .isolatedMap(SerializationTestHandler(), concurrency: 2)
          .toList();
      
      expect(results, hasLength(4));
      expect(results[0]['length'], equals(1));
      expect(results[1]['length'], equals(100));
      expect(results[2]['length'], equals(1));
      expect(results[3]['length'], equals(1000));
    });

    test('stream pausing and resuming mid-processing', () async {
      final controller = StreamController<int>();
      final results = <int>[];
      
      late StreamSubscription subscription;
      subscription = controller.stream
          .isolatedMap(SelectiveErrorHandler({}), concurrency: 2)
          .listen((value) {
            results.add(value);
            // Pause after getting some results
            if (results.length == 3) {
              subscription.pause();
              // Resume after a delay
              Timer(const Duration(milliseconds: 100), subscription.resume);
            }
          });
      
      // Add data continuously
      for (int i = 1; i <= 6; i++) {
        controller.add(i);
        await Future.delayed(const Duration(milliseconds: 10));
      }
      
      // Wait for all processing to complete
      await Future.delayed(const Duration(milliseconds: 200));
      
      expect(results, hasLength(6));
      expect(results, equals([2, 4, 6, 8, 10, 12]));
      
      await subscription.cancel();
      await controller.close();
    });
  });
}

/// Helper class to create handlers inline for testing

class _InlineHandler<T, E> extends IsolatedHandler<T, E> {
  final FutureOr<E> Function(T) _compute;
  
  _InlineHandler(this._compute) : super(debugName: 'InlineHandler');
  
  @override
  FutureOr<E> compute(T value) => _compute(value);
}