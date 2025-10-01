// Copyright (c) 2024, the isolated_stream authors. Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'dart:async';
import 'package:test/test.dart';
import 'package:isolated_stream/isolated_stream.dart';

/// Handler that simulates CPU-intensive prime number checking
class PrimeValidatorHandler extends IsolatedHandler<int, bool> {
  PrimeValidatorHandler() : super(debugName: 'PrimeValidator');
  
  @override
  bool compute(int number) {
    if (number < 2) return false;
    if (number == 2 || number == 3) return true;
    if (number % 2 == 0 || number % 3 == 0) return false;
    
    // Simulate CPU work
    for (int i = 5; i * i <= number; i += 6) {
      if (number % i == 0 || number % (i + 2) == 0) {
        return false;
      }
    }
    return true;
  }
}

/// Handler that simulates text processing workload
class TextAnalysisHandler extends IsolatedHandler<String, Map<String, int>> {
  TextAnalysisHandler() : super(debugName: 'TextAnalyzer');
  
  @override
  Map<String, int> compute(String text) {
    final words = text.toLowerCase().split(RegExp(r'\W+'));
    final analysis = <String, int>{};
    
    for (final word in words) {
      if (word.isNotEmpty) {
        analysis[word] = (analysis[word] ?? 0) + 1;
      }
    }
    
    return {
      'word_count': words.where((w) => w.isNotEmpty).length,
      'unique_words': analysis.keys.length,
      'character_count': text.length,
    };
  }
}

/// Handler for simulating data transformation pipeline
class DataTransformHandler extends IsolatedHandler<Map<String, dynamic>, Map<String, dynamic>> {
  DataTransformHandler() : super(debugName: 'DataTransform');
  
  @override
  Map<String, dynamic> compute(Map<String, dynamic> data) {
    // Simulate data validation and transformation
    final transformed = Map<String, dynamic>.from(data);
    
    // Add computed fields
    if (transformed.containsKey('timestamp')) {
      transformed['age_seconds'] = DateTime.now().millisecondsSinceEpoch - 
          (transformed['timestamp'] as int);
    }
    
    if (transformed.containsKey('values') && transformed['values'] is List) {
      final values = (transformed['values'] as List<num>);
      transformed['sum'] = values.fold<num>(0, (a, b) => a + b);
      transformed['average'] = values.isEmpty ? 0 : transformed['sum'] / values.length;
    }
    
    transformed['processed'] = true;
    return transformed;
  }
}

void main() {
  group('performance characteristics', () {
    test('demonstrates performance improvement with CPU-intensive work', () async {
      // Test data that would be slow to process sequentially
      final testNumbers = [97, 101, 103, 107, 109, 113, 127, 131, 137, 139];
      final handler = PrimeValidatorHandler();
      
      // Sequential processing benchmark
      final sequentialTimer = Stopwatch()..start();
      final sequentialResults = await Stream.fromIterable(testNumbers)
          .isolatedMap(handler, concurrency: 1)
          .toList();
      sequentialTimer.stop();
      
      // Concurrent processing benchmark  
      final concurrentTimer = Stopwatch()..start();
      final concurrentResults = await Stream.fromIterable(testNumbers)
          .isolatedMap(handler, concurrency: 4)
          .toList();
      concurrentTimer.stop();
      
      // Verify results are identical
      expect(concurrentResults, equals(sequentialResults));
      expect(concurrentResults, everyElement(isTrue)); // All should be prime
      
      // Concurrent should show some improvement (allowing for test environment variability)
      expect(concurrentTimer.elapsedMilliseconds, 
             lessThanOrEqualTo(sequentialTimer.elapsedMilliseconds));
    });
    
    test('handles realistic text processing workload', () async {
      final documents = [
        'The quick brown fox jumps over the lazy dog',
        'To be or not to be, that is the question',
        'All that glitters is not gold',
        'A picture is worth a thousand words',
        'The early bird catches the worm',
      ];
      
      final handler = TextAnalysisHandler();
      
      final results = await Stream.fromIterable(documents)
          .isolatedMap(handler, concurrency: 3)
          .toList();
      
      expect(results, hasLength(5));
      
      // Verify realistic analysis results
      expect(results[0]['word_count'], equals(9)); // "The quick brown..."
      expect(results[1]['word_count'], equals(10)); // "To be or not..."
      expect(results[0]['character_count'], greaterThan(0));
      expect(results[0]['unique_words'], greaterThan(0));
    });
    
    test('processes data transformation pipeline', () async {
      final now = DateTime.now().millisecondsSinceEpoch;
      final testData = [
        {
          'id': 1,
          'timestamp': now - 1000,
          'values': [1, 2, 3, 4, 5],
          'status': 'pending'
        },
        {
          'id': 2, 
          'timestamp': now - 2000,
          'values': [10, 20, 30],
          'status': 'active'
        },
        {
          'id': 3,
          'timestamp': now - 500,
          'values': <num>[],
          'status': 'completed'
        },
      ];
      
      final handler = DataTransformHandler();
      
      final results = await Stream<Map<String, dynamic>>.fromIterable(testData)
          .isolatedMap(handler, concurrency: 2)
          .toList();
      
      expect(results, hasLength(3));
      
      // Verify transformations were applied
      for (final result in results) {
        expect(result['processed'], isTrue);
        expect(result['age_seconds'], isA<int>());
        expect(result['sum'], isA<num>());
        expect(result['average'], isA<num>());
      }
      
      // Verify specific calculations
      expect(results[0]['sum'], equals(15)); // 1+2+3+4+5
      expect(results[1]['average'], equals(20)); // (10+20+30)/3
      expect(results[2]['sum'], equals(0)); // empty list
    });
  });
  
  group('realistic usage patterns', () {
    test('handles bursty data with backpressure', () async {
      final controller = StreamController<int>();
      final handler = PrimeValidatorHandler();
      
      // Transform stream with limited concurrency
      final processingFuture = controller.stream
          .isolatedMap(handler, concurrency: 2)
          .take(5)
          .toList();
      
      // Simulate burst of data followed by pause
      final testData = [2, 3, 5, 7, 11, 13, 17]; // Prime numbers
      
      // Send first batch quickly
      for (int i = 0; i < 3; i++) {
        controller.add(testData[i]);
      }
      
      // Small delay then send remaining
      await Future.delayed(const Duration(milliseconds: 50));
      for (int i = 3; i < testData.length; i++) {
        controller.add(testData[i]);
      }
      
      final results = await processingFuture;
      expect(results, hasLength(5));
      expect(results, everyElement(isTrue)); // All primes
      
      await controller.close();
    });
    
    test('supports stream subscription management', () async {
      final controller = StreamController<String>();
      final handler = TextAnalysisHandler();
      
      final results = <Map<String, int>>[];
      late StreamSubscription subscription;
      
      subscription = controller.stream
          .isolatedMap(handler, concurrency: 2)
          .listen((analysis) {
            results.add(analysis);
            // Cancel after processing 2 items
            if (results.length == 2) {
              subscription.cancel();
            }
          });
      
      // Add more data than we want to process
      controller.add('First document to analyze');
      controller.add('Second document for processing');
      controller.add('Third document should not be processed');
      controller.add('Fourth document should also be ignored');
      
      // Wait for processing to complete
      await Future.delayed(const Duration(milliseconds: 100));
      
      expect(results, hasLength(2));
      expect(results[0]['word_count'], greaterThan(0));
      expect(results[1]['word_count'], greaterThan(0));
      
      await controller.close();
    });
  });
}