import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;
import 'package:test/test.dart';
import 'package:isolated_stream/isolated_stream.dart';

/// Real-world handler that processes JSON data
class JsonProcessorHandler extends IsolatedHandler<String, Map<String, dynamic>> {
  JsonProcessorHandler() : super(debugName: 'JsonProcessor');
  
  @override
  Map<String, dynamic> compute(String jsonString) {
    final data = json.decode(jsonString) as Map<String, dynamic>;
    
    // Add some processing
    data['processed_at'] = DateTime.now().millisecondsSinceEpoch;
    data['length'] = jsonString.length;
    
    return data;
  }
}

/// Handler that simulates image processing
class ImageProcessorHandler extends IsolatedHandler<List<int>, Map<String, Object>> {
  ImageProcessorHandler() : super(debugName: 'ImageProcessor');
  
  @override
  Map<String, Object> compute(List<int> imageData) {
    // Simulate image processing operations
    final sum = imageData.fold<int>(0, (prev, element) => prev + element);
    final average = sum / imageData.length;
    final variance = imageData.fold<double>(0, (prev, element) => prev + math.pow(element - average, 2)) / imageData.length;
    
    return {
      'size': imageData.length,
      'checksum': sum,
      'average_brightness': average,
      'variance': variance,
      'processed': true,
    };
  }
}

/// Handler for mathematical computations
class MathComputationHandler extends IsolatedHandler<Map<String, double>, double> {
  MathComputationHandler() : super(debugName: 'MathComputation');
  
  @override
  double compute(Map<String, double> params) {
    final a = params['a'] ?? 0;
    final b = params['b'] ?? 0;
    final operation = params['operation']?.toInt() ?? 0;
    
    switch (operation) {
      case 0: return a + b;
      case 1: return a - b;
      case 2: return a * b;
      case 3: return b != 0 ? a / b : 0;
      case 4: return math.pow(a, b).toDouble();
      case 5: return math.sqrt(a.abs());
      default: return 0;
    }
  }
}

/// Handler that simulates network request processing
class NetworkRequestHandler extends IsolatedHandler<Map<String, String>, Map<String, Object>> {
  NetworkRequestHandler() : super(debugName: 'NetworkRequest');
  
  @override
  Future<Map<String, Object>> compute(Map<String, String> request) async {
    // Simulate network delay
    final delay = math.Random().nextInt(100) + 50;
    await Future.delayed(Duration(milliseconds: delay));
    
    // Simulate response processing
    return {
      'url': request['url'] ?? '',
      'method': request['method'] ?? 'GET',
      'status': 200,
      'response_time': delay,
      'body_length': (request['body'] ?? '').length,
      'timestamp': DateTime.now().millisecondsSinceEpoch,
    };
  }
}

void main() {
  group('integration scenarios', () {
    test('JSON processing pipeline works correctly', () async {
      final apiResponses = [
        '{"user_id": 1, "name": "Alice", "score": 95.5}',
        '{"user_id": 2, "name": "Bob", "score": 87.2}',
        '{"user_id": 3, "name": "Charlie", "score": 92.8}',
        '{"user_id": 4, "name": "Diana", "score": 89.1}',
        '{"user_id": 5, "name": "Eve", "score": 96.3}',
      ];
      
      final processed = await Stream.fromIterable(apiResponses)
          .isolatedMap(JsonProcessorHandler(), concurrency: 3)
          .toList();
      
      expect(processed, hasLength(5));
      
      // Check that processing metadata was added
      for (final result in processed) {
        expect(result['processed_at'], isA<int>());
        expect(result['length'], greaterThan(0));
        expect(result['user_id'], isNotNull);
      }
    });

    test('image analysis pipeline handles different sizes', () async {
      // Mock different resolution images
      final thumbnailData = List.generate(64, (i) => math.Random().nextInt(256));
      final mediumData = List.generate(256, (i) => math.Random().nextInt(256)); 
      final largeData = List.generate(1024, (i) => math.Random().nextInt(256));
      
      final images = [thumbnailData, mediumData, largeData];
      
      final analyzed = await Stream.fromIterable(images)
          .isolatedMap(ImageProcessorHandler(), concurrency: 2)
          .toList();
      
      expect(analyzed, hasLength(3));
      expect(analyzed[0]['size'], equals(64));
      expect(analyzed[1]['size'], equals(256));
      expect(analyzed[2]['size'], equals(1024));
      
      // All should have calculated statistics
      for (final analysis in analyzed) {
        expect(analysis['average_brightness'], isA<double>());
        expect(analysis['variance'], isA<double>());
        expect(analysis['processed'], isTrue);
      }
    });

    test('should perform mathematical computations concurrently', () async {
      final computations = [
        {'a': 10.0, 'b': 5.0, 'operation': 0.0}, // Addition
        {'a': 10.0, 'b': 5.0, 'operation': 1.0}, // Subtraction
        {'a': 10.0, 'b': 5.0, 'operation': 2.0}, // Multiplication
        {'a': 10.0, 'b': 5.0, 'operation': 3.0}, // Division
        {'a': 2.0, 'b': 3.0, 'operation': 4.0}, // Power
        {'a': 16.0, 'b': 0.0, 'operation': 5.0}, // Square root
      ];
      
      final stream = Stream.fromIterable(computations);
      final processed = stream.isolatedMap(MathComputationHandler(), concurrency: 4);
      
      final results = await processed.toList();
      
      expect(results, hasLength(6));
      expect(results[0], equals(15.0)); // 10 + 5
      expect(results[1], equals(5.0));  // 10 - 5
      expect(results[2], equals(50.0)); // 10 * 5
      expect(results[3], equals(2.0));  // 10 / 5
      expect(results[4], equals(8.0));  // 2^3
      expect(results[5], equals(4.0));  // sqrt(16)
    });

    test('should simulate network request processing with variable delays', () async {
      final requests = [
        {'url': 'https://api.example.com/users', 'method': 'GET'},
        {'url': 'https://api.example.com/posts', 'method': 'GET'},
        {'url': 'https://api.example.com/comments', 'method': 'GET'},
        {'url': 'https://api.example.com/data', 'method': 'POST', 'body': 'test data'},
      ];
      
      final stream = Stream.fromIterable(requests);
      final processed = stream.isolatedMap(NetworkRequestHandler(), concurrency: 3);
      
      final results = await processed.toList();
      
      expect(results, hasLength(4));
      
      for (int i = 0; i < results.length; i++) {
        expect(results[i]['url'], equals(requests[i]['url']));
        expect(results[i]['method'], equals(requests[i]['method']));
        expect(results[i]['status'], equals(200));
        expect(results[i]['response_time'], isA<int>());
        expect(results[i]['timestamp'], isA<int>());
      }
      
      // POST request should have body_length > 0
      expect(results[3]['body_length'], greaterThan(0));
    });

    test('should handle mixed workload efficiently', () async {
      final stopwatch = Stopwatch()..start();
      
      // Create a mixed workload with different processing times
      final workItems = List.generate(20, (i) {
        return {'a': i.toDouble(), 'b': (i + 1).toDouble(), 'operation': (i % 6).toDouble()};
      });
      
      final stream = Stream.fromIterable(workItems);
      final processed = stream.isolatedMap(MathComputationHandler(), concurrency: 5);
      
      final results = await processed.toList();
      stopwatch.stop();
      
      expect(results, hasLength(20));
      
      // Verify some specific calculations
      expect(results[0], equals(1.0));   // 0 + 1
      expect(results[1], equals(-1.0));  // 1 - 2
      expect(results[2], equals(6.0));   // 2 * 3
      
      // Should complete reasonably quickly with concurrency
      expect(stopwatch.elapsedMilliseconds, lessThan(1000));
    });

    test('should handle backpressure in real-world scenario', () async {
      final controller = StreamController<String>();
      
      // Use a slow JSON processor to test backpressure
      final transformed = controller.stream.isolatedMap(JsonProcessorHandler(), concurrency: 2);
      
      final future = transformed.take(5).toList();
      
      // Add data rapidly
      final jsonItems = [
        '{"message": "Item 1", "priority": "high"}',
        '{"message": "Item 2", "priority": "medium"}',
        '{"message": "Item 3", "priority": "low"}',
        '{"message": "Item 4", "priority": "high"}',
        '{"message": "Item 5", "priority": "medium"}',
        '{"message": "Item 6", "priority": "low"}', // Extra item
      ];
      
      for (final item in jsonItems) {
        controller.add(item);
        // Small delay to simulate high-frequency data
        await Future.delayed(const Duration(milliseconds: 5));
      }
      
      final results = await future;
      expect(results, hasLength(5));
      
      // Verify that all items were processed correctly
      for (int i = 0; i < 5; i++) {
        expect(results[i]['message'], equals('Item ${i + 1}'));
        expect(results[i]['processed_at'], isA<int>());
      }
      
      await controller.close();
    });

    test('should maintain performance with realistic data sizes', () async {
      // Test with larger data structures
      final largeObjects = List.generate(50, (i) {
        return List.generate(1000, (j) => (i * 1000 + j) % 256);
      });
      
      final stream = Stream.fromIterable(largeObjects);
      final processed = stream.isolatedMap(ImageProcessorHandler(), concurrency: 8);
      
      final stopwatch = Stopwatch()..start();
      final results = await processed.toList();
      stopwatch.stop();
      
      expect(results, hasLength(50));
      
      // Each result should have the expected structure
      for (final result in results) {
        expect(result['size'], equals(1000));
        expect(result['processed'], isTrue);
        expect(result['average_brightness'], isA<double>());
      }
      
      // Should complete in reasonable time even with large data
      expect(stopwatch.elapsedMilliseconds, lessThan(5000));
    });
  });

  group('Error Recovery in Real-World Scenarios', () {
    test('should handle malformed JSON gracefully', () async {
      final jsonData = [
        '{"valid": "json"}',
        'invalid json string',
        '{"another": "valid json"}',
      ];
      
      final stream = Stream.fromIterable(jsonData);
      final processed = stream.isolatedMap(JsonProcessorHandler());
      
      // Should throw on the invalid JSON
      expect(() => processed.toList(), throwsA(isA<FormatException>()));
    });

    test('should handle division by zero in math operations', () async {
      final computations = [
        {'a': 10.0, 'b': 0.0, 'operation': 3.0}, // Division by zero
      ];
      
      final stream = Stream.fromIterable(computations);
      final processed = stream.isolatedMap(MathComputationHandler());
      
      final results = await processed.toList();
      expect(results[0], equals(0.0)); // Handler returns 0 for division by zero
    });
  });

  group('Performance Characteristics', () {
    test('should show performance improvement with concurrency', () async {
      const itemCount = 20;
      
      // Create network requests that will have simulated delays
      final requests = List.generate(itemCount, (i) => {
        'url': 'https://api.example.com/item/$i',
        'method': 'GET',
      });
      
      final stream1 = Stream.fromIterable(requests);
      final stream2 = Stream.fromIterable(requests);
      
      // Sequential processing
      final sequentialTime = Stopwatch()..start();
      final sequentialResults = await stream1.isolatedMap(NetworkRequestHandler(), concurrency: 1).toList();
      sequentialTime.stop();
      
      // Concurrent processing
      final concurrentTime = Stopwatch()..start();
      final concurrentResults = await stream2.isolatedMap(NetworkRequestHandler(), concurrency: 5).toList();
      concurrentTime.stop();
      
      // Results should be the same
      expect(sequentialResults, hasLength(itemCount));
      expect(concurrentResults, hasLength(itemCount));
      
      // Concurrent should be significantly faster
      final improvement = sequentialTime.elapsedMilliseconds / concurrentTime.elapsedMilliseconds;
      expect(improvement, greaterThan(2.0)); // At least 2x improvement
    });
  });
}