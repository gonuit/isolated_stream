import 'dart:async';
import 'dart:math' as math;
import 'package:isolated_stream/isolated_stream.dart';

/// Simple synchronous handler for testing
class MultiplyHandler extends IsolatedHandler<int, int> {
  final int factor;
  
  MultiplyHandler(this.factor) : super(debugName: 'Multiply by $factor');
  
  @override
  int compute(int value) => value * factor;
}

/// Simple asynchronous handler for testing
class AsyncMultiplyHandler extends IsolatedHandler<int, int> {
  final int factor;
  final Duration delay;
  
  AsyncMultiplyHandler(this.factor, {this.delay = const Duration(milliseconds: 10)}) 
    : super(debugName: 'AsyncMultiply by $factor');
  
  @override
  Future<int> compute(int value) async {
    await Future.delayed(delay);
    return value * factor;
  }
}

/// Handler that throws an error for testing error handling
class ErrorHandler extends IsolatedHandler<int, int> {
  final String errorMessage;
  
  ErrorHandler(this.errorMessage) : super(debugName: 'ErrorHandler');
  
  @override
  int compute(int value) {
    throw Exception(errorMessage);
  }
}

/// Handler that throws an error asynchronously
class AsyncErrorHandler extends IsolatedHandler<int, int> {
  final String errorMessage;
  
  AsyncErrorHandler(this.errorMessage) : super(debugName: 'AsyncErrorHandler');
  
  @override
  Future<int> compute(int value) async {
    await Future.delayed(const Duration(milliseconds: 10));
    throw Exception(errorMessage);
  }
}

/// CPU-intensive handler for performance testing
class PrimeCheckHandler extends IsolatedHandler<int, bool> {
  PrimeCheckHandler() : super(debugName: 'PrimeChecker');
  
  @override
  bool compute(int number) {
    if (number < 2) return false;
    if (number == 2) return true;
    if (number % 2 == 0) return false;
    
    for (int i = 3; i <= math.sqrt(number); i += 2) {
      if (number % i == 0) return false;
    }
    return true;
  }
}

/// Handler with variable processing time for concurrency testing
class VariableDelayHandler extends IsolatedHandler<int, int> {
  VariableDelayHandler() : super(debugName: 'VariableDelay');
  
  @override
  Future<int> compute(int value) async {
    // Simulate variable processing time based on value
    final delay = Duration(milliseconds: (value % 5) * 10 + 10);
    await Future.delayed(delay);
    return value * 2;
  }
}

/// Handler that converts types for testing generic support
class StringToIntHandler extends IsolatedHandler<String, int> {
  StringToIntHandler() : super(debugName: 'StringToInt');
  
  @override
  int compute(String value) {
    return int.parse(value);
  }
}

/// Handler for testing complex data types
class ComplexDataHandler extends IsolatedHandler<Map<String, dynamic>, Map<String, dynamic>> {
  ComplexDataHandler() : super(debugName: 'ComplexData');
  
  @override
  Map<String, dynamic> compute(Map<String, dynamic> value) {
    return {
      'original': value,
      'processed_at': DateTime.now().millisecondsSinceEpoch,
      'keys_count': value.keys.length,
    };
  }
}