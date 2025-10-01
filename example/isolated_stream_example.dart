import 'dart:async';
import 'package:isolated_stream/isolated_stream.dart';

/// Example handler for CPU-intensive prime number checking
class PrimeCheckHandler extends IsolatedHandler<int, bool> {
  PrimeCheckHandler() : super(debugName: 'PrimeChecker');
  
  @override
  bool compute(int number) {
    if (number < 2) return false;
    if (number == 2) return true;
    if (number % 2 == 0) return false;
    
    for (int i = 3; i * i <= number; i += 2) {
      if (number % i == 0) return false;
    }
    return true;
  }
}

/// Example handler for asynchronous operations
class AsyncMultiplyHandler extends IsolatedHandler<int, int> {
  final int factor;
  
  AsyncMultiplyHandler(this.factor) : super(debugName: 'AsyncMultiply by $factor');
  
  @override
  Future<int> compute(int value) async {
    // Simulate some async work
    await Future.delayed(const Duration(milliseconds: 10));
    return value * factor;
  }
}

/// Example handler for complex data processing
class DataProcessorHandler extends IsolatedHandler<Map<String, dynamic>, Map<String, dynamic>> {
  DataProcessorHandler() : super(debugName: 'DataProcessor');
  
  @override
  Map<String, dynamic> compute(Map<String, dynamic> data) {
    // Process the data
    final processed = Map<String, dynamic>.from(data);
    processed['processed_at'] = DateTime.now().millisecondsSinceEpoch;
    processed['hash'] = data.toString().hashCode;
    
    return processed;
  }
}

void main() async {
  print('=== Isolated Stream Examples ===\n');
  
  // Example 1: Basic synchronous transformation
  print('1. Basic synchronous transformation:');
  await basicExample();
  
  // Example 2: CPU-intensive operations
  print('\n2. CPU-intensive prime checking:');
  await primeCheckingExample();
  
  // Example 3: Asynchronous operations
  print('\n3. Asynchronous operations:');
  await asyncExample();
  
  // Example 4: Concurrent processing
  print('\n4. Concurrent processing:');
  await concurrentExample();
  
  // Example 5: Isolate pooling
  print('\n5. Isolate pooling:');
  await isolatePoolExample();
  
  // Example 6: Complex data processing
  print('\n6. Complex data processing:');
  await complexDataExample();
  
  print('\n=== All examples completed ===');
}

Future<void> basicExample() async {
  final numbers = Stream.fromIterable([1, 2, 3, 4, 5]);
  final doubled = numbers.isolatedMap(AsyncMultiplyHandler(2));
  
  print('Input: [1, 2, 3, 4, 5]');
  print('Processing: multiply by 2 in isolate');
  
  final results = await doubled.toList();
  print('Output: $results');
}

Future<void> primeCheckingExample() async {
  final candidates = Stream.fromIterable([17, 18, 19, 20, 21, 22, 23]);
  final primeResults = candidates.isolatedMap(PrimeCheckHandler());
  
  print('Checking if these numbers are prime: [17, 18, 19, 20, 21, 22, 23]');
  
  final results = await primeResults.toList();
  for (int i = 0; i < results.length; i++) {
    final number = [17, 18, 19, 20, 21, 22, 23][i];
    final isPrime = results[i];
    print('  $number is ${isPrime ? 'prime' : 'not prime'}');
  }
}

Future<void> asyncExample() async {
  final numbers = Stream.fromIterable([1, 2, 3, 4]);
  final processed = numbers.isolatedMap(AsyncMultiplyHandler(3));
  
  print('Processing [1, 2, 3, 4] with async multiply by 3:');
  
  await for (final result in processed) {
    print('  Processed: $result');
  }
}

Future<void> concurrentExample() async {
  print('Comparing sequential vs concurrent processing:');
  
  // Sequential processing
  final stopwatch1 = Stopwatch()..start();
  final sequential = Stream.fromIterable([1, 2, 3, 4, 5, 6])
      .isolatedMap(AsyncMultiplyHandler(2), concurrency: 1);
  await sequential.toList();
  stopwatch1.stop();
  print('  Sequential processing took: ${stopwatch1.elapsedMilliseconds}ms');
  
  // Concurrent processing
  final stopwatch2 = Stopwatch()..start();
  final concurrent = Stream.fromIterable([1, 2, 3, 4, 5, 6])
      .isolatedMap(AsyncMultiplyHandler(2), concurrency: 3);
  await concurrent.toList();
  stopwatch2.stop();
  print('  Concurrent processing took: ${stopwatch2.elapsedMilliseconds}ms');
  
  final improvement = stopwatch1.elapsedMilliseconds / stopwatch2.elapsedMilliseconds;
  print('  Performance improvement: ${improvement.toStringAsFixed(1)}x faster');
}

Future<void> isolatePoolExample() async {
  print('Comparing single isolate vs isolate pool:');
  
  final workload = List.generate(12, (i) => i + 1);
  
  // Single isolate with high concurrency
  final stopwatch1 = Stopwatch()..start();
  final singleIsolate = Stream.fromIterable(workload)
      .isolatedMap(
        AsyncMultiplyHandler(2),
        concurrency: 6,
        isolates: 1, // Single isolate
      );
  await singleIsolate.toList();
  stopwatch1.stop();
  print('  Single isolate (6 concurrent): ${stopwatch1.elapsedMilliseconds}ms');
  
  // Multiple isolates with high concurrency
  final stopwatch2 = Stopwatch()..start();
  final multipleIsolates = Stream.fromIterable(workload)
      .isolatedMap(
        AsyncMultiplyHandler(2),
        concurrency: 6,
        isolates: 3, // 3 isolates in pool
      );
  await multipleIsolates.toList();
  stopwatch2.stop();
  print('  Isolate pool (6 concurrent, 3 isolates): ${stopwatch2.elapsedMilliseconds}ms');
  
  print('  Isolate pooling helps distribute work for better resource utilization');
}

Future<void> complexDataExample() async {
  final data = <Map<String, dynamic>>[
    {'name': 'Alice', 'age': 30, 'city': 'New York'},
    {'name': 'Bob', 'age': 25, 'city': 'London'},
    {'name': 'Charlie', 'age': 35, 'city': 'Tokyo'},
  ];
  
  final stream = Stream<Map<String, dynamic>>.fromIterable(data);
  final processed = stream.isolatedMap(DataProcessorHandler());
  
  print('Processing complex data structures:');
  
  final results = await processed.toList();
  for (final result in results) {
    print('  ${result['name']}: processed at ${result['processed_at']}, hash: ${result['hash']}');
  }
}