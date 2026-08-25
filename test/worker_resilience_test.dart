import 'dart:async';
import 'dart:isolate';

import 'package:isolated_stream/isolated_stream.dart';
import 'package:test/test.dart';

class _UnsendableError implements Exception {
  _UnsendableError(this.message);
  final String message;
  final ReceivePort port = ReceivePort();

  @override
  String toString() => 'UnsendableError($message)';
}

class _UnsendableResult {
  final ReceivePort port = ReceivePort();
}

class _ThrowsUnsendableHandler extends IsolatedHandler<int, int> {
  const _ThrowsUnsendableHandler();

  @override
  int compute(int value) {
    if (value < 0) {
      throw _UnsendableError('value: $value');
    }
    return value * 2;
  }
}

class _UnsendableResultHandler extends IsolatedHandler<int, Object> {
  const _UnsendableResultHandler();

  @override
  Object compute(int value) {
    if (value < 0) {
      return _UnsendableResult();
    }
    return value;
  }
}

class _ExitOnNegativeHandler extends IsolatedHandler<int, int> {
  const _ExitOnNegativeHandler();

  @override
  int compute(int value) {
    if (value < 0) {
      Isolate.exit();
    }
    return value * 2;
  }
}

class _AsyncCrashHandler extends IsolatedHandler<int, int> {
  const _AsyncCrashHandler();

  @override
  Future<int> compute(int value) {
    if (value < 0) {
      // The uncaught async throw kills the isolate while this request is
      // still in flight.
      Timer.run(() => throw StateError('async boom: $value'));
      return Completer<int>().future;
    }
    return Future.value(value * 2);
  }
}

class _DynamicEchoHandler extends IsolatedHandler<Object?, int> {
  const _DynamicEchoHandler();

  @override
  int compute(Object? value) => value is int ? value : -1;
}

class _UnsendableHandler extends IsolatedHandler<int, int> {
  _UnsendableHandler();
  final ReceivePort port = ReceivePort();

  @override
  int compute(int value) => value;
}

void main() {
  group('unsendable handler errors', () {
    test(
        'unsendable thrown error is delivered as RemoteError instead of '
        'hanging', () async {
      final worker = IsolatedWorker<int, int>(const _ThrowsUnsendableHandler());
      try {
        await expectLater(
          worker.compute(-1),
          throwsA(isA<RemoteError>().having(
              (e) => e.toString(), 'toString', contains('UnsendableError'))),
        );
      } finally {
        worker.dispose();
      }
    });

    test(
        'worker isolate survives an unsendable error and serves the next '
        'call', () async {
      final worker = IsolatedWorker<int, int>(const _ThrowsUnsendableHandler());
      try {
        await expectLater(worker.compute(-1), throwsA(isA<RemoteError>()));
        expect(await worker.compute(3), 6);
      } finally {
        worker.dispose();
      }
    });

    test(
        'unsendable result surfaces the serialization error and worker '
        'stays usable', () async {
      final worker =
          IsolatedWorker<int, Object>(const _UnsendableResultHandler());
      try {
        await expectLater(
          worker.compute(-1),
          throwsA(predicate((Object? e) =>
              e.toString().contains('Illegal argument in isolate message'))),
        );
        expect(await worker.compute(5), 5);
      } finally {
        worker.dispose();
      }
    });

    test('unsendable handler fails compute() instead of hanging', () async {
      final handler = _UnsendableHandler();
      final worker = IsolatedWorker<int, int>(handler);
      try {
        await expectLater(worker.compute(1), throwsA(isA<ArgumentError>()));
      } finally {
        worker.dispose();
        handler.port.close();
      }
    });
  });

  group('isolate death', () {
    test(
        'in-flight compute fails with StateError when the isolate exits '
        'cleanly', () async {
      final worker = IsolatedWorker<int, int>(const _ExitOnNegativeHandler());
      try {
        await expectLater(
          worker.compute(-1),
          throwsA(isA<StateError>().having(
              (e) => e.message, 'message', contains('exited unexpectedly'))),
        );
      } finally {
        worker.dispose();
      }
    });

    test(
        'in-flight compute fails with RemoteError carrying the crash '
        'description', () async {
      final worker = IsolatedWorker<int, int>(const _AsyncCrashHandler());
      try {
        await expectLater(
          worker.compute(-1),
          throwsA(isA<RemoteError>()
              .having((e) => e.toString(), 'toString', contains('async boom'))),
        );
      } finally {
        worker.dispose();
      }
    });

    test('next compute() after isolate death runs on a fresh isolate',
        () async {
      final worker = IsolatedWorker<int, int>(const _ExitOnNegativeHandler());
      try {
        await expectLater(worker.compute(-1), throwsA(isA<StateError>()));
        expect(await worker.compute(21), 42);
      } finally {
        worker.dispose();
      }
    });

    test('pool with isolates: 2 heals the dead member', () async {
      final worker = IsolatedWorker<int, int>(
        const _ExitOnNegativeHandler(),
        isolates: 2,
      );
      try {
        await expectLater(worker.compute(-1), throwsA(isA<StateError>()));
        for (var i = 1; i <= 4; i++) {
          expect(await worker.compute(i), i * 2);
        }
      } finally {
        worker.dispose();
      }
    });

    test('idle shutdown after an isolate death does not throw', () async {
      final worker = IsolatedWorker<int, int>(
        const _ExitOnNegativeHandler(),
        idleTimeout: const Duration(milliseconds: 50),
      );
      try {
        await expectLater(worker.compute(-1), throwsA(isA<StateError>()));
        await Future.delayed(const Duration(milliseconds: 150));
        expect(await worker.compute(3), 6);
      } finally {
        worker.dispose();
      }
    });

    test('dispose() after isolate death is clean', () async {
      final worker = IsolatedWorker<int, int>(const _ExitOnNegativeHandler());
      await expectLater(worker.compute(-1), throwsA(isA<StateError>()));
      expect(worker.dispose, returnsNormally);
      await Future.delayed(const Duration(milliseconds: 50));
    });
  });

  group('unsendable input', () {
    test(
        'unsendable input value throws ArgumentError and does not poison '
        'the worker', () async {
      final worker = IsolatedWorker<Object?, int>(const _DynamicEchoHandler());
      final port = ReceivePort();
      try {
        await expectLater(
          worker.compute(port),
          throwsA(isA<ArgumentError>()),
        );
        expect(await worker.compute(5), 5);
      } finally {
        worker.dispose();
        port.close();
      }
    });

    test(
        'dispose() after an unsendable input causes no unhandled async '
        'error', () async {
      final worker = IsolatedWorker<Object?, int>(const _DynamicEchoHandler());
      final port = ReceivePort();
      try {
        await worker.compute(port);
      } catch (_) {}
      worker.dispose();
      await Future.delayed(const Duration(milliseconds: 100));
      port.close();
    });
  });

  group('stream strategies', () {
    test('sequential strategy delivers unsendable handler errors and closes',
        () async {
      final errors = <Object>[];
      final results = <int>[];
      await Stream.fromIterable([-1, -2, -3])
          .isolatedMap(const _ThrowsUnsendableHandler())
          .handleError(errors.add)
          .forEach(results.add);
      expect(results, isEmpty);
      expect(errors, hasLength(3));
      expect(errors, everyElement(isA<RemoteError>()));
    });
  });
}
