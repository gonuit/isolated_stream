@TestOn('browser')
library;

import 'dart:typed_data';

import 'package:isolated_stream/isolated_stream.dart';
import 'package:test/test.dart';

import 'handlers.dart';

// The worker is compiled before the tests run (tool/test_web.sh).
void main() {
  setUp(() => served = 0);

  for (final compiled in ['worker.js', 'worker.wasm', 'worker']) {
    group('handler with a web worker ($compiled)', () {
      setUp(() => script = compiled);
      workerTests();
    });
  }
  group('handler without a web worker', () {
    test('computes on the page', () async {
      final worker = IsolatedWorker<int, int>(PageHandler(), isolates: 2);
      addTearDown(worker.dispose);

      expect(await Future.wait([worker.compute(1), worker.compute(2)]), [2, 3]);
      expect(served, 2);
    });

    test('works with isolatedMap', () async {
      final results = await Stream.fromIterable([1, 2, 3])
          .isolatedMap(PageHandler())
          .toList();
      expect(results, [2, 3, 4]);
    });
  });
}

void workerTests() {
  test('computes in the worker, not on the page', () async {
    final worker = IsolatedWorker<int, int>(ScaleHandler(3));
    addTearDown(worker.dispose);

    expect(await worker.compute(2), 6);
    expect(await Future.wait([for (var i = 0; i < 5; i++) worker.compute(i)]),
        [0, 3, 6, 9, 12]);
    expect(served, 0);
  });

  test('keeps one worker per isolate across calls', () async {
    final worker = IsolatedWorker<int, int>(ServedHandler(), isolates: 2);
    addTearDown(worker.dispose);

    final counts = [for (var i = 0; i < 6; i++) await worker.compute(0)];
    expect(counts, [1, 1, 2, 2, 3, 3]);
  });

  test('runs calls in parallel across workers', () async {
    final worker = IsolatedWorker<int, int>(ServedHandler(), isolates: 3);
    addTearDown(worker.dispose);

    final counts = await Future.wait([
      for (var i = 0; i < 3; i++) worker.compute(50),
    ]);
    expect(counts, [1, 1, 1]);
  });

  test('rethrows the handler error as RemoteWorkerError', () async {
    final worker = IsolatedWorker<int, int>(ScaleHandler(3));
    addTearDown(worker.dispose);

    await expectLater(
      worker.compute(-1),
      throwsA(isA<RemoteWorkerError>()
          .having((e) => e.message, 'message', contains('Negative'))
          .having((e) => e.stackTrace, 'stackTrace', isNotNull)),
    );
    expect(await worker.compute(1), 3);
  });

  test('passes typed data', () async {
    final worker = IsolatedWorker<Uint8List, Uint8List>(ReverseBytesHandler());
    addTearDown(worker.dispose);

    final result = await worker.compute(Uint8List.fromList([1, 2, 3, 250]));
    expect(result, isA<Uint8List>());
    expect(result, [250, 3, 2, 1]);
  });

  test('moves the buffers the handler names instead of copying them', () async {
    final worker = IsolatedWorker<int, Object?>(MovingHandler());
    addTearDown(worker.dispose);

    final result = await worker.compute(4) as List<Object?>;
    expect(result, List.filled(4, [0, 1, 2, 3]));
    // Moved: gone from the worker; the one not named: still there.
    final held = await worker.compute(-1) as List<Object?>;
    expect(held.take(3), [0, 0, 4]);
  });

  test('passes lists, maps and numbers', () async {
    final worker = IsolatedWorker<List<Object?>, Object?>(SummaryHandler());
    addTearDown(worker.dispose);

    final result = await worker.compute([1, 2, 5]) as Map<Object?, Object?>;
    expect(result['count'], isA<int>());
    expect(result, {
      'count': 3,
      'sum': 8,
      'half': [0.5, 1, 2.5],
    });
  });

  test('passes whole doubles as doubles', () async {
    final worker = IsolatedWorker<double, double>(HalfHandler());
    addTearDown(worker.dispose);

    expect(await worker.compute(4), 2.0);
    expect(await worker.compute(3), 1.5);
  });

  test('fails a call whose result cannot be sent', () async {
    final worker = IsolatedWorker<int, Object?>(UnsendableHandler());
    addTearDown(worker.dispose);

    await expectLater(
      worker.compute(1),
      throwsA(isA<RemoteWorkerError>()
          .having((e) => e.message, 'message', contains('could not be sent'))),
    );
  });

  test('fails a call whose value cannot be sent', () async {
    final worker = IsolatedWorker<Object?, Object?>(SummaryHandler());
    addTearDown(worker.dispose);

    await expectLater(worker.compute(() => 1), throwsA(anything));
    expect(await worker.compute([2]), containsPair('sum', 2));
  });

  test('works with isolatedMap', () async {
    final results = await Stream.fromIterable([1, 2, 3, 4])
        .isolatedMap(
          ScaleHandler(10),
          strategy: IsolatedProcessingStrategy.concurrent(
            concurrency: 2,
            isolates: 2,
          ),
        )
        .toList();
    expect(results, [10, 20, 30, 40]);
    expect(served, 0);
  });

  test('fails calls when its script does not load', () async {
    final worker = IsolatedWorker<int, int>(MissingWorkerHandler());
    addTearDown(worker.dispose);

    await expectLater(
      worker.compute(1),
      throwsA(isA<RemoteWorkerError>()
          .having((e) => e.message, 'message', contains('missing'))),
    );
    await expectLater(worker.compute(1), throwsA(isA<RemoteWorkerError>()));
  });

  test('fails calls when the worker cannot build the handler', () async {
    final worker = IsolatedWorker<int, int>(BrokenInitHandler());
    addTearDown(worker.dispose);

    await expectLater(
      worker.compute(1),
      throwsA(isA<RemoteWorkerError>()
          .having((e) => e.message, 'message', contains('Unknown handler'))),
    );
  });

  test('dispose fails the calls in flight', () async {
    final worker = IsolatedWorker<int, int>(ServedHandler());
    final call = worker.compute(200);
    await Future<void>.delayed(const Duration(milliseconds: 20));
    worker.dispose();
    await expectLater(call, throwsA(isA<StateError>()));
  });

  test('idle timeout stops the worker and starts a new one', () async {
    var shutdowns = 0;
    final worker = IsolatedWorker<int, int>(
      ServedHandler(),
      idleTimeout: const Duration(milliseconds: 20),
      onIdleShutdown: () => shutdowns++,
    );
    addTearDown(worker.dispose);

    expect(await worker.compute(0), 1);
    expect(await worker.compute(0), 2);
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(shutdowns, 1);
    expect(await worker.compute(0), 1);
  });
}
