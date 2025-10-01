// Copyright (c) 2024, the isolated_stream authors. Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'dart:async';
import 'package:test/test.dart';
import 'package:isolated_stream/isolated_stream.dart';

/// Creates a stream with the specified number of sequential integers.
Stream<int> createNumberStream(int count, {int start = 1}) {
  return Stream.fromIterable(List.generate(count, (i) => start + i));
}

/// Creates a delayed stream that emits items with specified intervals.
Stream<T> createDelayedStream<T>(Iterable<T> items, Duration delay) async* {
  for (final item in items) {
    await Future.delayed(delay);
    yield item;
  }
}

/// Helper for testing stream transformations with expected results.
Future<void> expectTransformation<T, E>(
  Stream<T> stream,
  IsolatedHandler<T, E> handler,
  List<E> expectedResults, {
  int concurrency = 1,
  int isolates = 1,
}) async {
  final results = await stream
      .isolatedMap(handler, concurrency: concurrency, isolates: isolates)
      .toList();
  
  expect(results, equals(expectedResults));
}

/// Helper for testing that operations complete within a time limit.
Future<void> expectCompletesWithin<T>(
  Future<T> operation,
  Duration timeLimit,
) async {
  final completer = Completer<T>();
  final timer = Timer(timeLimit, () {
    if (!completer.isCompleted) {
      completer.completeError(
        TimeoutException('Operation did not complete within $timeLimit'),
      );
    }
  });
  
  try {
    operation.then(completer.complete).catchError(completer.completeError);
    await completer.future;
  } finally {
    timer.cancel();
  }
}

/// Helper for creating controlled stream controllers in tests.
StreamController<T> createTestController<T>({bool broadcast = false}) {
  return broadcast 
      ? StreamController<T>.broadcast()
      : StreamController<T>();
}

class TimeoutException implements Exception {
  final String message;
  TimeoutException(this.message);
  
  @override
  String toString() => 'TimeoutException: $message';
}