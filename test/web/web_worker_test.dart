@TestOn('browser')
library;

import 'dart:js_interop';
import 'dart:js_interop_unsafe';
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
      lifecycleTests(LifecycleHandler.new, page: false);
    });
  }
  group('handler without a web worker', () {
    lifecycleTests(PageLifecycleHandler.new, page: true);

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

var _tags = 0;

void lifecycleTests(
  LifecycleHandler Function(String tag, {bool fail}) create, {
  required bool page,
}) {
  late String tag;
  late List<String> events;
  late Reports received;

  int count(String event) => events.where((e) => e == event).length;

  Future<void> until(bool Function() condition) async {
    final watch = Stopwatch()..start();
    while (!condition()) {
      if (watch.elapsed > const Duration(seconds: 5)) fail('Timed out');
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
  }

  setUp(() {
    tag = 'test${_tags++}';
    events = [];
    received = Reports('isolated_stream_test')
      ..onmessage = (JSObject event) {
        final [from, what] = (event['data'] as JSString).toDart.split(':');
        if (from == tag) events.add(what);
      }.toJS;
  });
  tearDown(() => received.close());

  test('awaits onInit before the first call', () async {
    final worker = IsolatedWorker<int, int>(create(tag));
    addTearDown(worker.dispose);

    expect(
        await Future.wait([worker.compute(1), worker.compute(2)]), [101, 102]);
    await until(() => events.isNotEmpty);
    expect(events, ['init']);
  });

  test('runs onInit once per worker, once on the page', () async {
    final worker = IsolatedWorker<int, int>(create(tag), isolates: 2);
    addTearDown(worker.dispose);

    for (var i = 0; i < 4; i++) {
      expect(await worker.compute(i), 100 + i);
    }
    await until(() => count('init') == (page ? 1 : 2));
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(count('init'), page ? 1 : 2);
  });

  test('runs onDispose on dispose', () async {
    final worker = IsolatedWorker<int, int>(create(tag), isolates: 2);
    expect(
        await Future.wait([worker.compute(1), worker.compute(2)]), [101, 102]);
    worker.dispose();

    await until(() => count('dispose') == (page ? 1 : 2));
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(count('dispose'), page ? 1 : 2);
  });

  test('skips onDispose with a zero shutdownTimeout', () async {
    final worker =
        IsolatedWorker<int, int>(create(tag), shutdownTimeout: Duration.zero);
    expect(await worker.compute(1), 101);
    worker.dispose();

    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(events, ['init']);
  });

  test('runs onDispose on idle shutdown and onInit again after', () async {
    final worker = IsolatedWorker<int, int>(
      create(tag),
      idleTimeout: const Duration(milliseconds: 20),
    );
    addTearDown(worker.dispose);

    expect(await worker.compute(1), 101);
    await until(() => count('dispose') == 1);
    expect(await worker.compute(2), 102);
    await until(() => count('init') == 2);
  });

  test('a failed onInit fails the calls waiting and is tried again', () async {
    final worker = IsolatedWorker<int, int>(create(tag, fail: true));
    addTearDown(worker.dispose);
    final failure = throwsA(page
        ? isA<StateError>()
        : isA<RemoteWorkerError>()
            .having((e) => e.message, 'message', contains('init failed'))
            .having((e) => e, 'error', isNot(isA<WorkerLoadError>())));

    await Future.wait([
      expectLater(worker.compute(1), failure),
      expectLater(worker.compute(2), failure),
    ]);
    await expectLater(worker.compute(3), failure);
    await until(() => count('init') == 2);
    worker.dispose();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(count('dispose'), 0);
  });
}

void workerTests() {
  test('loads from a nested route under a <base href>', () async {
    final page = _location.href;
    final base = _document.createElement('base')..setAttribute('href', page);
    _document.head.append(base);
    _history.replaceState(
      null,
      '',
      Uri.parse(page).resolve('deep/route/page').toString(),
    );
    addTearDown(() {
      _history.replaceState(null, '', page);
      base.remove();
    });

    final worker = IsolatedWorker<int, int>(ScaleHandler(3));
    addTearDown(worker.dispose);

    expect(await worker.compute(2), 6);
  });

  test('computes in the worker, not on the page', () async {
    final worker = IsolatedWorker<int, int>(ScaleHandler(3));
    addTearDown(worker.dispose);

    expect(await worker.compute(2), 6);
    expect(await Future.wait([for (var i = 0; i < 5; i++) worker.compute(i)]), [
      0,
      3,
      6,
      9,
      12,
    ]);
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
      throwsA(
        isA<RemoteWorkerError>()
            .having((e) => e.message, 'message', contains('Negative'))
            .having((e) => e.stackTrace, 'stackTrace', isNotNull),
      ),
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

  test('moves the buffers of an input the handler names', () async {
    final worker = IsolatedWorker<Object?, int>(TakingHandler());
    addTearDown(worker.dispose);
    // In JavaScript's memory: the buffer itself moves.
    final js = Uint8List.fromList([1, 2, 3]).toJS;
    expect(await worker.compute(js.toDart), 6);
    expect(js.getProperty<JSNumber>('byteLength'.toJS).toDartInt, 0);
    // Made in Dart: compiled to WebAssembly a copy moves and the bytes
    // stay; compiled to JavaScript they are JavaScript's and move.
    final dart = Uint8List.fromList([4, 5]);
    expect(await worker.compute(dart), 9);
    expect(
      dart.length,
      const bool.fromEnvironment('dart.tool.dart2wasm') ? 2 : 0,
    );
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
      throwsA(
        isA<RemoteWorkerError>().having(
          (e) => e.message,
          'message',
          contains('could not be sent'),
        ),
      ),
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
      throwsA(
        isA<WorkerLoadError>().having(
          (e) => e.message,
          'message',
          contains('missing'),
        ),
      ),
    );
    await expectLater(worker.compute(1), throwsA(isA<WorkerLoadError>()));
  });

  test(
    'fails calls of a worker failing after it loaded, not as a load error',
    () async {
      final worker = IsolatedWorker<int, int>(CrashingHandler());
      addTearDown(worker.dispose);

      await expectLater(
        worker.compute(1),
        throwsA(
          isA<RemoteWorkerError>().having(
            (e) => e,
            'error',
            isNot(isA<WorkerLoadError>()),
          ),
        ),
      );
    },
  );

  test('fails calls when the worker cannot build the handler', () async {
    final worker = IsolatedWorker<int, int>(BrokenInitHandler());
    addTearDown(worker.dispose);

    await expectLater(
      worker.compute(1),
      throwsA(
        isA<RemoteWorkerError>().having(
          (e) => e.message,
          'message',
          contains('Unknown handler'),
        ),
      ),
    );
  });

  test('stops a worker whose init cannot be sent', () async {
    final terminated = _countTerminatedWorkers();
    addTearDown(() => terminated.restore());
    final worker = IsolatedWorker<int, int>(UnsendableInitHandler());
    addTearDown(worker.dispose);

    await expectLater(worker.compute(1), throwsArgumentError);
    expect(terminated.count, 1);
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

extension type _Element._(JSObject _) implements JSObject {
  external void setAttribute(String name, String value);
  external void append(_Element child);
  external void remove();
}

extension type _Document._(JSObject _) implements JSObject {
  external _Element get head;
  external _Element createElement(String tag);
}

extension type _Location._(JSObject _) implements JSObject {
  external String get href;
}

extension type _History._(JSObject _) implements JSObject {
  external void replaceState(JSAny? state, String unused, String url);
}

extension type _TerminatedWorkers._(JSObject _) implements JSObject {
  external int get count;
  external void restore();
}

/// Counts the workers terminated from here on, until `restore`.
_TerminatedWorkers _countTerminatedWorkers() => _function('''
  const terminate = Worker.prototype.terminate;
  const counter = {
    count: 0,
    restore: () => Worker.prototype.terminate = terminate,
  };
  Worker.prototype.terminate = function () {
    counter.count++;
    return terminate.call(this);
  };
  return counter;
''').callAsFunction()! as _TerminatedWorkers;

@JS('Function')
external JSFunction _function(String body);

@JS('document')
external _Document get _document;

@JS('location')
external _Location get _location;

@JS('history')
external _History get _history;
