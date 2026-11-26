@TestOn('vm')
library;

import 'dart:async';
import 'dart:isolate';

import 'package:isolated_stream/isolated_stream.dart';
import 'package:test/test.dart';

Future<void> _waitFor(
  bool Function() condition, {
  Duration timeout = const Duration(seconds: 5),
}) async {
  final sw = Stopwatch()..start();
  while (!condition()) {
    if (sw.elapsed > timeout) fail('Timed out waiting for condition');
    await Future.delayed(const Duration(milliseconds: 10));
  }
}

class _LifecycleHandler extends IsolatedHandler<int, int> {
  const _LifecycleHandler(this.notify, {this.initDelayMs = 0});
  final SendPort notify;
  final int initDelayMs;

  @override
  Future<void> onInit() async {
    if (initDelayMs > 0) {
      await Future.delayed(Duration(milliseconds: initDelayMs));
    }
    notify.send('init');
  }

  @override
  void onDispose() {
    notify.send('dispose');
  }

  @override
  int compute(int value) {
    notify.send('compute:$value');
    return value * 2;
  }
}

class _FailingInitHandler extends IsolatedHandler<int, int> {
  _FailingInitHandler();
  bool shouldFail = true;

  @override
  void onInit() {
    if (shouldFail) {
      throw StateError('init boom');
    }
  }

  @override
  int compute(int value) => value * 2;
}

class _UnsendableInitError implements Exception {
  _UnsendableInitError();
  final ReceivePort port = ReceivePort();

  @override
  String toString() => 'UnsendableInitError';
}

class _UnsendableInitErrorHandler extends IsolatedHandler<int, int> {
  const _UnsendableInitErrorHandler();

  @override
  void onInit() {
    throw _UnsendableInitError();
  }

  @override
  int compute(int value) => value;
}

class _ThrowingDisposeHandler extends IsolatedHandler<int, int> {
  const _ThrowingDisposeHandler(this.notify);
  final SendPort notify;

  @override
  void onDispose() {
    notify.send('dispose-start');
    throw StateError('dispose boom');
  }

  @override
  int compute(int value) => value * 2;
}

class _ExitingHandler extends IsolatedHandler<int, int> {
  const _ExitingHandler(this.notify);
  final SendPort notify;

  @override
  void onDispose() {
    notify.send('dispose');
  }

  @override
  int compute(int value) {
    if (value < 0) {
      Isolate.exit();
    }
    return value * 2;
  }
}

class _BlockingComputeHandler extends IsolatedHandler<int, int> {
  const _BlockingComputeHandler(this.notify);
  final SendPort notify;

  @override
  void onDispose() {
    notify.send('dispose');
  }

  @override
  int compute(int value) {
    final sw = Stopwatch()..start();
    while (sw.elapsedMilliseconds < value) {}
    notify.send('compute-done');
    return value;
  }
}

void main() {
  late ReceivePort port;
  late List<String> events;

  setUp(() {
    port = ReceivePort();
    events = <String>[];
    port.listen((dynamic m) => events.add(m as String));
  });

  tearDown(() {
    port.close();
  });

  group('onInit', () {
    test('runs in the worker isolate before the first compute', () async {
      final worker = IsolatedWorker<int, int>(_LifecycleHandler(port.sendPort));
      try {
        expect(await worker.compute(1), 2);
        await _waitFor(() => events.length >= 2);
        expect(events.sublist(0, 2), ['init', 'compute:1']);
      } finally {
        worker.dispose();
      }
    });

    test('runs exactly once across many computes', () async {
      final worker = IsolatedWorker<int, int>(_LifecycleHandler(port.sendPort));
      try {
        for (var i = 0; i < 20; i++) {
          await worker.compute(i);
        }
        await _waitFor(
            () => events.where((e) => e.startsWith('compute')).length == 20);
        expect(events.where((e) => e == 'init'), hasLength(1));
      } finally {
        worker.dispose();
      }
    });

    test('async onInit is awaited before the first compute starts', () async {
      final worker = IsolatedWorker<int, int>(
        _LifecycleHandler(port.sendPort, initDelayMs: 100),
      );
      try {
        expect(await worker.compute(1), 2);
        await _waitFor(() => events.contains('compute:1'));
        expect(events.indexOf('init'), lessThan(events.indexOf('compute:1')));
      } finally {
        worker.dispose();
      }
    });

    test('runs once per isolate in a pool', () async {
      final worker = IsolatedWorker<int, int>(
        _LifecycleHandler(port.sendPort),
        isolates: 2,
      );
      try {
        await Future.wait(List.generate(20, (i) => worker.compute(i)));
        await _waitFor(() => events.where((e) => e == 'init').length == 2);
        expect(events.where((e) => e == 'init'), hasLength(2));
      } finally {
        worker.dispose();
      }
    });
  });

  group('onInit errors', () {
    test('onInit error fails compute() with the original error', () async {
      final worker = IsolatedWorker<int, int>(_FailingInitHandler());
      try {
        await expectLater(
          worker.compute(1),
          throwsA(isA<StateError>()
              .having((e) => e.message, 'message', contains('init boom'))),
        );
      } finally {
        worker.dispose();
      }
    });

    test('onInit error fails all pending compute() calls', () async {
      final worker = IsolatedWorker<int, int>(_FailingInitHandler());
      try {
        final expectations = [
          expectLater(worker.compute(1), throwsA(isA<StateError>())),
          expectLater(worker.compute(2), throwsA(isA<StateError>())),
          expectLater(worker.compute(3), throwsA(isA<StateError>())),
        ];
        await Future.wait(expectations);
      } finally {
        worker.dispose();
      }
    });

    test('unsendable onInit error is delivered as RemoteError', () async {
      final worker =
          IsolatedWorker<int, int>(const _UnsendableInitErrorHandler());
      try {
        await expectLater(
          worker.compute(1),
          throwsA(isA<RemoteError>().having((e) => e.toString(), 'toString',
              contains('UnsendableInitError'))),
        );
      } finally {
        worker.dispose();
      }
    });

    test('re-spawn after onInit failure retries onInit', () async {
      final handler = _FailingInitHandler();
      final worker = IsolatedWorker<int, int>(handler);
      try {
        await expectLater(worker.compute(1), throwsA(isA<StateError>()));
        await Future.delayed(const Duration(milliseconds: 100));
        handler.shouldFail = false;
        expect(await worker.compute(21), 42);
      } finally {
        worker.dispose();
      }
    });

    test('dispose() after onInit failure is clean', () async {
      final worker = IsolatedWorker<int, int>(_FailingInitHandler());
      await expectLater(worker.compute(1), throwsA(isA<StateError>()));
      expect(worker.dispose, returnsNormally);
      await Future.delayed(const Duration(milliseconds: 100));
    });
  });

  group('onDispose', () {
    test('runs on dispose()', () async {
      final worker = IsolatedWorker<int, int>(_LifecycleHandler(port.sendPort));
      await worker.compute(1);
      worker.dispose();
      await _waitFor(() => events.contains('dispose'));
    });

    test('runs on idle shutdown and onInit runs again on the next compute',
        () async {
      final worker = IsolatedWorker<int, int>(
        _LifecycleHandler(port.sendPort),
        idleTimeout: const Duration(milliseconds: 50),
      );
      try {
        expect(await worker.compute(1), 2);
        await _waitFor(() => events.contains('dispose'));
        expect(await worker.compute(2), 4);
        await _waitFor(() => events.where((e) => e == 'init').length == 2);
      } finally {
        worker.dispose();
      }
    });

    test('onDispose errors are swallowed', () async {
      final worker =
          IsolatedWorker<int, int>(_ThrowingDisposeHandler(port.sendPort));
      await worker.compute(1);
      expect(worker.dispose, returnsNormally);
      await _waitFor(() => events.contains('dispose-start'));
      await Future.delayed(const Duration(milliseconds: 100));
    });

    test('hard-kill after shutdownTimeout when sync compute blocks shutdown',
        () async {
      final worker = IsolatedWorker<int, int>(
        _BlockingComputeHandler(port.sendPort),
        shutdownTimeout: const Duration(milliseconds: 100),
      );
      final pending = worker.compute(600);
      final expectation = expectLater(
        pending,
        throwsA(isA<StateError>().having((e) => e.message, 'message',
            contains('IsolatedWorker has been disposed'))),
      );
      await Future.delayed(const Duration(milliseconds: 50));
      worker.dispose();
      await expectation;
      await Future.delayed(const Duration(milliseconds: 800));
      expect(events, isNot(contains('dispose')));
      expect(events, isNot(contains('compute-done')));
    });

    test('does not run when the isolate already died', () async {
      final worker = IsolatedWorker<int, int>(_ExitingHandler(port.sendPort));
      await expectLater(worker.compute(-1), throwsA(isA<StateError>()));
      worker.dispose();
      await Future.delayed(const Duration(milliseconds: 150));
      expect(events, isNot(contains('dispose')));
    });

    test('shutdownTimeout: Duration.zero kills immediately without onDispose',
        () async {
      final worker = IsolatedWorker<int, int>(
        _LifecycleHandler(port.sendPort),
        shutdownTimeout: Duration.zero,
      );
      await worker.compute(1);
      worker.dispose();
      await Future.delayed(const Duration(milliseconds: 200));
      expect(events, isNot(contains('dispose')));
    });
  });

  group('stream strategies', () {
    test(
        'sequential isolatedMap runs onInit before the first event and '
        'onDispose after completion', () async {
      final results = await Stream.fromIterable([1, 2, 3])
          .isolatedMap(
            _LifecycleHandler(port.sendPort),
            strategy: const IsolatedProcessingStrategy.sequential(
              shutdownTimeout: Duration(seconds: 1),
            ),
          )
          .toList();
      expect(results, [2, 4, 6]);
      expect(events.first, 'init');
      await _waitFor(() => events.contains('dispose'));
    });
  });
}
