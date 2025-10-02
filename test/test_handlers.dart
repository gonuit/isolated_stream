import 'dart:async';
import 'package:isolated_stream/isolated_stream.dart';

class TestHandlerEvent {
  final int value;
  final int delayMs;
  final String? exception;

  TestHandlerEvent({
    required this.value,
    this.delayMs = 0,
    this.exception,
  });
}

/// Handler with variable processing time for concurrency testing
class ValueHandler extends IsolatedHandler<TestHandlerEvent, int> {
  ValueHandler([String debugName = 'ValueHandler'])
      : super(debugName: debugName);

  @override
  FutureOr<int> compute(TestHandlerEvent event) {
    int handleResult() {
      if (event.exception != null) {
        throw Exception(event.exception);
      } else {
        return event.value;
      }
    }

    // Simulate variable processing time based on value
    final delay = Duration(milliseconds: event.delayMs);
    if (event.delayMs > 0) {
      return Future.delayed(delay).then((_) {
        return handleResult();
      });
    } else {
      return handleResult();
    }
  }
}

/// Handler that tracks concurrent executions by embedding info in results
class ConcurrencyTrackingHandler
    extends IsolatedHandler<int, ConcurrencyResult> {
  final int delayMs;
  static int _currentConcurrentCalls = 0;
  static int _maxConcurrentCalls = 0;

  ConcurrencyTrackingHandler(this.delayMs)
      : super(debugName: 'ConcurrencyTracking');

  @override
  Future<ConcurrencyResult> compute(int value) async {
    _currentConcurrentCalls++;
    final currentAtStart = _currentConcurrentCalls;
    if (_currentConcurrentCalls > _maxConcurrentCalls) {
      _maxConcurrentCalls = _currentConcurrentCalls;
    }

    try {
      await Future.delayed(Duration(milliseconds: delayMs));
      return ConcurrencyResult(
        value: value * 2,
        concurrentCallsAtStart: currentAtStart,
        maxConcurrentCalls: _maxConcurrentCalls,
      );
    } finally {
      _currentConcurrentCalls--;
    }
  }

  static void reset() {
    _currentConcurrentCalls = 0;
    _maxConcurrentCalls = 0;
  }
}

/// Result class that includes concurrency information
class ConcurrencyResult {
  final int value;
  final int concurrentCallsAtStart;
  final int maxConcurrentCalls;

  ConcurrencyResult({
    required this.value,
    required this.concurrentCallsAtStart,
    required this.maxConcurrentCalls,
  });

  @override
  String toString() =>
      'ConcurrencyResult(value: $value, concurrent: $concurrentCallsAtStart, max: $maxConcurrentCalls)';
}

class TestHandlerResult {
  final int value;
  final List<int> processedValues;

  TestHandlerResult({required this.value, required this.processedValues});
}

/// Handler that tracks which values were processed (for restart mode testing)
class RestartTrackingHandler
    extends IsolatedHandler<TestHandlerEvent, TestHandlerResult> {
  final List<int> processedValues = [];

  RestartTrackingHandler() : super(debugName: 'RestartTracking');

  @override
  Future<TestHandlerResult> compute(TestHandlerEvent value) async {
    await Future.delayed(Duration(milliseconds: value.delayMs));
    final result = value.value;
    processedValues.add(result);
    return TestHandlerResult(
      value: result,
      processedValues: List.unmodifiable(processedValues),
    );
  }
}
