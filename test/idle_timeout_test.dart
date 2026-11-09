@TestOn('vm')
library;

import 'dart:async';
import 'dart:isolate';

import 'package:isolated_stream/isolated_stream.dart';
import 'package:test/test.dart';

class _EchoIsolateIdHandler extends IsolatedHandler<int, int> {
  const _EchoIsolateIdHandler({super.debugName});

  @override
  int compute(int value) => identityHashCode(Isolate.current);
}

/// Drives a stream through a strategy with a short idle timeout: emit an
/// event, wait past the timeout, emit another. Returns `[firstId, secondId]`.
Future<List<int>> _runIdleShutdownScenario(
  IsolatedProcessingStrategy strategy,
) async {
  final controller = StreamController<int>();
  final results = <int>[];
  final done = Completer<void>();

  final sub = controller.stream.isolatedMap(
    const _EchoIsolateIdHandler(debugName: 'Strategy'),
    strategy: strategy,
  ).listen(
    results.add,
    onDone: () => done.complete(),
    onError: (Object e, StackTrace st) => done.completeError(e, st),
  );

  controller.add(0);
  // Wait for the first event to be processed and the idle timer to fire.
  await Future.delayed(const Duration(milliseconds: 150));
  controller.add(0);
  await Future.delayed(const Duration(milliseconds: 50));
  await controller.close();
  await done.future;
  await sub.cancel();

  return results;
}

void main() {
  group('idleTimeout across strategies', () {
    test('Sequential: shuts down isolate when idle, re-spawns on next event',
        () async {
      var shutdownCount = 0;
      final results = await _runIdleShutdownScenario(
        IsolatedProcessingStrategy.sequential(
          idleTimeout: const Duration(milliseconds: 50),
          onIdleShutdown: () => shutdownCount++,
        ),
      );
      expect(results.length, 2);
      expect(results[0], isNot(equals(results[1])),
          reason: 'Second event should run on a fresh isolate after idle shutdown');
      expect(shutdownCount, greaterThanOrEqualTo(1),
          reason: 'Idle shutdown callback should have fired at least once');
    });

    test('Concurrent: shuts down isolate pool when idle, re-spawns on next event',
        () async {
      var shutdownCount = 0;
      final results = await _runIdleShutdownScenario(
        IsolatedProcessingStrategy.concurrent(
          concurrency: 2,
          idleTimeout: const Duration(milliseconds: 50),
          onIdleShutdown: () => shutdownCount++,
        ),
      );
      expect(results.length, 2);
      expect(results[0], isNot(equals(results[1])));
      expect(shutdownCount, greaterThanOrEqualTo(1));
    });

    test('Droppable: shuts down isolate pool when idle, re-spawns on next event',
        () async {
      var shutdownCount = 0;
      final results = await _runIdleShutdownScenario(
        IsolatedProcessingStrategy.droppable(
          concurrency: 2,
          idleTimeout: const Duration(milliseconds: 50),
          onIdleShutdown: () => shutdownCount++,
        ),
      );
      expect(results.length, 2);
      expect(results[0], isNot(equals(results[1])));
      expect(shutdownCount, greaterThanOrEqualTo(1));
    });

    test('Restartable: shuts down isolate pool when idle, re-spawns on next event',
        () async {
      var shutdownCount = 0;
      final results = await _runIdleShutdownScenario(
        IsolatedProcessingStrategy.restartable(
          concurrency: 2,
          idleTimeout: const Duration(milliseconds: 50),
          onIdleShutdown: () => shutdownCount++,
        ),
      );
      expect(results.length, 2);
      expect(results[0], isNot(equals(results[1])));
      expect(shutdownCount, greaterThanOrEqualTo(1));
    });

    test('Sequential preserves ordering across an idle shutdown', () async {
      final results = <int>[];
      final done = Completer<void>();

      final controller = StreamController<int>();
      final sub = controller.stream.isolatedMap(
        const _EchoIsolateIdHandler(debugName: 'Ordered'),
        strategy: IsolatedProcessingStrategy.sequential(
          idleTimeout: const Duration(milliseconds: 50),
        ),
      ).listen(
        results.add,
        onDone: () => done.complete(),
      );

      // First batch.
      for (var i = 0; i < 3; i++) {
        controller.add(i);
      }
      await Future.delayed(const Duration(milliseconds: 50));
      // Let the pool go idle.
      await Future.delayed(const Duration(milliseconds: 150));
      // Second batch after idle shutdown.
      for (var i = 0; i < 3; i++) {
        controller.add(i);
      }
      await Future.delayed(const Duration(milliseconds: 50));
      await controller.close();
      await done.future;
      await sub.cancel();

      expect(results.length, 6,
          reason: 'All six events must be processed across the shutdown');
      // First three should share one isolate id, next three should share a
      // different one (since the pool was rebuilt).
      final firstBatch = results.take(3).toSet();
      final secondBatch = results.skip(3).toSet();
      expect(firstBatch.length, 1);
      expect(secondBatch.length, 1);
      expect(firstBatch, isNot(equals(secondBatch)),
          reason: 'Batches must run on different isolate generations');
    });

    test('null idleTimeout keeps the isolate alive indefinitely', () async {
      var shutdownCount = 0;
      final results = await _runIdleShutdownScenario(
        IsolatedProcessingStrategy.sequential(
          onIdleShutdown: () => shutdownCount++,
        ),
      );
      expect(results.length, 2);
      expect(results[0], equals(results[1]),
          reason: 'With no idle timeout, the same isolate handles both events');
      expect(shutdownCount, 0);
    });
  });
}
