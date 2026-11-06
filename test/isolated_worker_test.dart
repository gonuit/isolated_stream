@TestOn('vm')
library;

import 'dart:async';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:isolated_stream/isolated_stream.dart';
import 'package:test/test.dart';

import 'test_handlers.dart';

class _EchoIsolateIdHandler extends IsolatedHandler<int, int> {
  const _EchoIsolateIdHandler({super.debugName});

  @override
  int compute(int value) => identityHashCode(Isolate.current);
}

class _ThrowingHandler extends IsolatedHandler<int, int> {
  const _ThrowingHandler() : super(debugName: 'Throwing');

  @override
  int compute(int value) {
    throw StateError('boom: $value');
  }
}

class _SlowEchoIdHandler extends IsolatedHandler<int, int> {
  const _SlowEchoIdHandler({super.debugName});

  @override
  Future<int> compute(int value) async {
    await Future.delayed(Duration(milliseconds: value));
    return identityHashCode(Isolate.current);
  }
}

/// Sleeps [value] ms and returns the original value. Useful for reasoning
/// about ordering and concurrency-limit tests without relying on isolate
/// identity.
class _SleepReturnHandler extends IsolatedHandler<int, int> {
  const _SleepReturnHandler({super.debugName});

  @override
  Future<int> compute(int value) async {
    await Future.delayed(Duration(milliseconds: value));
    return value;
  }
}

/// Names its result's buffer to move: natively nothing is moved.
class _MovingBytesHandler extends IsolatedHandler<int, Uint8List> {
  const _MovingBytesHandler();

  @override
  Uint8List compute(int value) =>
      Uint8List.fromList(List.generate(value, (i) => i));

  @override
  List<ByteBuffer> transferOf(Uint8List result) => [result.buffer];
}

void main() {
  group('IsolatedWorker', () {
    test('a result whose buffers are named to move arrives whole', () async {
      final worker = IsolatedWorker<int, Uint8List>(
        const _MovingBytesHandler(),
      );
      try {
        expect(await worker.compute(4), [0, 1, 2, 3]);
        expect(await worker.compute(3), [0, 1, 2]);
      } finally {
        worker.dispose();
      }
    });

    test('single compute returns the expected value', () async {
      final worker = IsolatedWorker<TestHandlerEvent, int>(ValueHandler());
      try {
        final result = await worker.compute(TestHandlerEvent(value: 21));
        expect(result, 21);
      } finally {
        worker.dispose();
      }
    });

    test('multiple sequential compute calls return correct results', () async {
      final worker = IsolatedWorker<TestHandlerEvent, int>(ValueHandler());
      try {
        final a = await worker.compute(TestHandlerEvent(value: 1));
        final b = await worker.compute(TestHandlerEvent(value: 2));
        final c = await worker.compute(TestHandlerEvent(value: 3));
        expect([a, b, c], equals([1, 2, 3]));
      } finally {
        worker.dispose();
      }
    });

    test('100 concurrent compute calls all return correct results', () async {
      final worker = IsolatedWorker<TestHandlerEvent, int>(ValueHandler());
      try {
        final results = await Future.wait(
          List.generate(
            100,
            (i) => worker.compute(TestHandlerEvent(value: i, delayMs: 1)),
          ),
        );
        expect(results, equals(List.generate(100, (i) => i)));
      } finally {
        worker.dispose();
      }
    });

    test('compute after dispose throws StateError', () async {
      final worker = IsolatedWorker<TestHandlerEvent, int>(ValueHandler());
      // Ensure the isolate is actually spawned before dispose.
      await worker.compute(TestHandlerEvent(value: 1));
      worker.dispose();

      expect(
        () => worker.compute(TestHandlerEvent(value: 2)),
        throwsA(isA<StateError>()),
      );
    });

    test('dispose is idempotent', () async {
      final worker = IsolatedWorker<TestHandlerEvent, int>(ValueHandler());
      await worker.compute(TestHandlerEvent(value: 1));
      worker.dispose();
      expect(worker.dispose, returnsNormally);
    });

    test('handler errors propagate with stack trace', () async {
      final worker = IsolatedWorker<int, int>(const _ThrowingHandler());
      try {
        await expectLater(
          worker.compute(42),
          throwsA(
            isA<StateError>().having(
              (e) => e.message,
              'message',
              contains('boom: 42'),
            ),
          ),
        );
      } finally {
        worker.dispose();
      }
    });

    test('handler thrown Exception propagates', () async {
      final worker = IsolatedWorker<TestHandlerEvent, int>(ValueHandler());
      try {
        await expectLater(
          worker.compute(TestHandlerEvent(value: 0, exception: 'nope')),
          throwsA(isA<Exception>()),
        );
        // Worker is still usable after an error.
        expect(await worker.compute(TestHandlerEvent(value: 7)), 7);
      } finally {
        worker.dispose();
      }
    });

    test('with isolates: 4, work is distributed across the pool', () async {
      final worker = IsolatedWorker<int, int>(
        const _EchoIsolateIdHandler(debugName: 'PoolWorker'),
        isolates: 4,
      );
      try {
        // Round-robin is deterministic: 40 calls across 4 isolates must
        // hit every isolate at least once, so there are exactly 4 distinct
        // isolate ids.
        final ids = await Future.wait(
          List.generate(40, (i) => worker.compute(i)),
        );
        expect(ids.toSet().length, equals(4));
      } finally {
        worker.dispose();
      }
    });

    test('isolates: 0 throws ArgumentError', () {
      expect(
        () => IsolatedWorker<TestHandlerEvent, int>(
          ValueHandler(),
          isolates: 0,
        ),
        throwsA(isA<ArgumentError>()),
      );
    });

    group('lifecycle', () {
      test('in-flight compute() fails with StateError when dispose() runs',
          () async {
        final worker = IsolatedWorker<TestHandlerEvent, int>(ValueHandler());
        // Start a slow computation.
        final pending =
            worker.compute(TestHandlerEvent(value: 1, delayMs: 500));
        // Give the call time to reach the await inside the wrapper.
        await Future.delayed(const Duration(milliseconds: 50));
        worker.dispose();

        await expectLater(
          pending,
          throwsA(
            isA<StateError>().having(
              (e) => e.message,
              'message',
              'IsolatedWorker has been disposed',
            ),
          ),
        );
      });

      test('many in-flight compute() calls all fail on dispose()', () async {
        final worker = IsolatedWorker<TestHandlerEvent, int>(
          ValueHandler(),
          isolates: 3,
        );
        // Attach error listeners eagerly — otherwise the calls that reject
        // before `expectLater` runs on them would surface as unhandled
        // async errors.
        final expectations = [
          for (var i = 0; i < 10; i++)
            expectLater(
              worker.compute(TestHandlerEvent(value: i, delayMs: 500)),
              throwsA(isA<StateError>()),
            ),
        ];
        // Let the calls reach their await points.
        await Future.delayed(const Duration(milliseconds: 50));
        worker.dispose();
        await Future.wait(expectations);
      });

      test('dispose() immediately after construction does not hang or leak',
          () async {
        // Construct and dispose before any work — exercises kill() running
        // concurrently with _spawn().
        for (var i = 0; i < 5; i++) {
          final worker = IsolatedWorker<TestHandlerEvent, int>(
            ValueHandler(),
            isolates: 2,
          );
          worker.dispose();
        }
        // If any of the above hang or leak, this test would time out.
      });

      test(
          'compute() after dispose-during-first-spawn surfaces canonical error',
          () async {
        final worker = IsolatedWorker<TestHandlerEvent, int>(ValueHandler());
        // Kick off the first compute (triggers spawn).
        final first = worker.compute(TestHandlerEvent(value: 1));
        // Dispose immediately — likely hits during spawn.
        worker.dispose();

        await expectLater(
          first,
          throwsA(
            isA<StateError>().having(
              (e) => e.message,
              'message',
              'IsolatedWorker has been disposed',
            ),
          ),
        );
      });
    });

    group('concurrency', () {
      test('concurrency: 1 serializes calls in submission order', () async {
        final worker = IsolatedWorker<int, int>(
          const _SleepReturnHandler(debugName: 'Seq'),
          concurrency: 1,
        );
        try {
          // Submit calls with decreasing sleep. With unbounded concurrency
          // these would return in completion order [10, 20, 30, 40, 50].
          // With concurrency:1 they run one at a time in submission order,
          // so they complete in submission order: [50, 40, 30, 20, 10].
          final results = await Future.wait([
            worker.compute(50),
            worker.compute(40),
            worker.compute(30),
            worker.compute(20),
            worker.compute(10),
          ]);
          expect(results, equals([50, 40, 30, 20, 10]));
        } finally {
          worker.dispose();
        }
      });

      test('concurrency: 1 forces strictly sequential execution timing',
          () async {
        // 5 calls × 50ms each, single-threaded → total ~250ms.
        final worker = IsolatedWorker<int, int>(
          const _SleepReturnHandler(debugName: 'SeqTiming'),
          concurrency: 1,
        );
        try {
          final sw = Stopwatch()..start();
          await Future.wait(
            List.generate(5, (_) => worker.compute(50)),
          );
          sw.stop();
          expect(sw.elapsedMilliseconds, greaterThanOrEqualTo(250));
        } finally {
          worker.dispose();
        }
      });

      test('concurrency: N bounds in-flight calls to N', () async {
        final worker = IsolatedWorker<int, int>(
          const _SleepReturnHandler(debugName: 'Bounded'),
          concurrency: 3,
        );
        try {
          // 9 calls × 50ms each, bound 3 → ~150ms total (3 batches of 3).
          final sw = Stopwatch()..start();
          final results = await Future.wait(
            List.generate(9, (_) => worker.compute(50)),
          );
          sw.stop();
          expect(results, equals(List.filled(9, 50)));
          expect(sw.elapsedMilliseconds, greaterThanOrEqualTo(150));
          // Generous ceiling to absorb spawn/scheduler noise on CI.
          expect(sw.elapsedMilliseconds, lessThan(450));
        } finally {
          worker.dispose();
        }
      });

      test('concurrency: null is unbounded (all calls in parallel)', () async {
        final worker = IsolatedWorker<int, int>(
          const _SleepReturnHandler(debugName: 'Unbounded'),
          isolates: 4,
        );
        try {
          // 20 calls × 100ms each, no bound → runs in parallel across
          // isolates; well under 5 × 100ms.
          final sw = Stopwatch()..start();
          await Future.wait(
            List.generate(20, (_) => worker.compute(100)),
          );
          sw.stop();
          expect(sw.elapsedMilliseconds, lessThan(500));
        } finally {
          worker.dispose();
        }
      });

      test('concurrency: 0 throws ArgumentError', () {
        expect(
          () => IsolatedWorker<int, int>(
            const _SleepReturnHandler(),
            concurrency: 0,
          ),
          throwsA(isA<ArgumentError>()),
        );
      });

      test('concurrency works together with idleTimeout', () async {
        var shutdownCount = 0;
        final worker = IsolatedWorker<int, int>(
          const _EchoIsolateIdHandler(debugName: 'ConcIdle'),
          concurrency: 2,
          idleTimeout: const Duration(milliseconds: 50),
          onIdleShutdown: () => shutdownCount++,
        );
        try {
          final first = await worker.compute(0);
          await Future.delayed(const Duration(milliseconds: 150));
          expect(shutdownCount, 1);
          final second = await worker.compute(0);
          expect(first, isNot(equals(second)));
        } finally {
          worker.dispose();
        }
      });
    });

    group('idleTimeout', () {
      test('back-to-back calls within timeout reuse the same isolate', () async {
        final worker = IsolatedWorker<int, int>(
          const _EchoIsolateIdHandler(debugName: 'Reuse'),
          idleTimeout: const Duration(seconds: 5),
        );
        try {
          final first = await worker.compute(0);
          final second = await worker.compute(0);
          expect(first, second,
              reason: 'Same isolate should handle both calls within timeout');
        } finally {
          worker.dispose();
        }
      });

      test('next call after idle period runs on a new isolate', () async {
        var shutdownCount = 0;
        final worker = IsolatedWorker<int, int>(
          const _EchoIsolateIdHandler(debugName: 'Idle'),
          idleTimeout: const Duration(milliseconds: 50),
          onIdleShutdown: () => shutdownCount++,
        );
        try {
          final first = await worker.compute(0);
          // Wait longer than idleTimeout so the isolate is released.
          await Future.delayed(const Duration(milliseconds: 150));
          expect(shutdownCount, 1,
              reason: 'Idle shutdown callback should fire exactly once');
          final second = await worker.compute(0);
          expect(first, isNot(equals(second)),
              reason: 'A fresh isolate should serve the second call');
        } finally {
          worker.dispose();
        }
      });

      test('in-flight work prevents the idle timer from firing', () async {
        var shutdownCount = 0;
        final worker = IsolatedWorker<int, int>(
          const _SlowEchoIdHandler(debugName: 'Busy'),
          idleTimeout: const Duration(milliseconds: 30),
          onIdleShutdown: () => shutdownCount++,
        );
        try {
          // Five long-running calls in parallel, each ~100ms, idleTimeout is 30ms.
          // They must all complete on the same pool, and the timer must not fire
          // mid-flight.
          final ids = await Future.wait(
            List.generate(5, (_) => worker.compute(100)),
          );
          expect(ids.toSet().length, 1,
              reason: 'All calls should share one isolate with no shutdown');
          expect(shutdownCount, 0,
              reason: 'Timer must not fire while work is in flight');
        } finally {
          worker.dispose();
        }
      });

      test('dispose cancels the pending idle timer', () async {
        var shutdownCount = 0;
        final worker = IsolatedWorker<int, int>(
          const _EchoIsolateIdHandler(debugName: 'DisposeCancel'),
          idleTimeout: const Duration(milliseconds: 50),
          onIdleShutdown: () => shutdownCount++,
        );
        await worker.compute(0);
        // Dispose before the idle timer fires.
        worker.dispose();
        // Wait past the timeout to prove the timer never runs.
        await Future.delayed(const Duration(milliseconds: 150));
        expect(shutdownCount, 0,
            reason: 'Explicit dispose should not trigger onIdleShutdown');
      });

      test('onIdleShutdown fires only once per idle period', () async {
        var shutdownCount = 0;
        final worker = IsolatedWorker<int, int>(
          const _EchoIsolateIdHandler(debugName: 'Once'),
          isolates: 3,
          idleTimeout: const Duration(milliseconds: 50),
          onIdleShutdown: () => shutdownCount++,
        );
        try {
          // Drive the pool, then let it go idle.
          await Future.wait(List.generate(6, (i) => worker.compute(0)));
          await Future.delayed(const Duration(milliseconds: 150));
          expect(shutdownCount, 1,
              reason: 'One callback per idle shutdown, regardless of pool size');

          // Do another round and let it idle again → second callback.
          await Future.wait(List.generate(6, (i) => worker.compute(0)));
          await Future.delayed(const Duration(milliseconds: 150));
          expect(shutdownCount, 2);
        } finally {
          worker.dispose();
        }
      });

      test('Duration.zero is allowed and works', () async {
        final worker = IsolatedWorker<int, int>(
          const _EchoIsolateIdHandler(debugName: 'Zero'),
          idleTimeout: Duration.zero,
        );
        try {
          final a = await worker.compute(0);
          // Yield so the zero-delay timer has a chance to fire.
          await Future.delayed(const Duration(milliseconds: 10));
          final b = await worker.compute(0);
          // With Duration.zero, b should almost certainly run on a new isolate.
          expect(a, isNot(equals(b)));
        } finally {
          worker.dispose();
        }
      });
    });
  });
}
