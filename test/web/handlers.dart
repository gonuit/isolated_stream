import 'dart:typed_data';

import 'package:isolated_stream/isolated_stream.dart';

/// Calls served by this thread, worker or page.
int served = 0;

/// The compiled test worker: `worker.dart.js` or `worker.wasm`.
String script = 'worker.dart.js';

class ScaleHandler extends IsolatedHandler<int, int> {
  ScaleHandler(this.factor);

  final int factor;

  @override
  int compute(int value) {
    served++;
    if (value < 0) throw ArgumentError.value(value, 'value', 'Negative');
    return value * factor;
  }

  @override
  IsolatedWebWorker get webWorker =>
      IsolatedWebWorker(script, init: ['scale', factor]);
}

/// Counts the calls its worker served, to tell workers apart.
class ServedHandler extends IsolatedHandler<int, int> {
  @override
  Future<int> compute(int delayMs) async {
    await Future<void>.delayed(Duration(milliseconds: delayMs));
    return ++served;
  }

  @override
  IsolatedWebWorker get webWorker =>
      IsolatedWebWorker(script, init: ['served']);
}

class ReverseBytesHandler extends IsolatedHandler<Uint8List, Uint8List> {
  @override
  Uint8List compute(Uint8List value) =>
      Uint8List.fromList(value.reversed.toList());

  @override
  IsolatedWebWorker get webWorker => IsolatedWebWorker(script, init: ['bytes']);
}

class SummaryHandler extends IsolatedHandler<List<Object?>, Object?> {
  @override
  Object? compute(List<Object?> value) => {
        'count': value.length,
        'sum': value.fold<num>(0, (sum, item) => sum + (item as num)),
        'half': [for (final item in value) (item as num) / 2],
      };

  @override
  IsolatedWebWorker get webWorker =>
      IsolatedWebWorker(script, init: ['summary']);
}

class HalfHandler extends IsolatedHandler<double, double> {
  @override
  double compute(double value) => value / 2;

  @override
  IsolatedWebWorker get webWorker => IsolatedWebWorker(script, init: ['half']);
}

/// Returns what cannot cross to the page.
class UnsendableHandler extends IsolatedHandler<int, Object?> {
  @override
  Object? compute(int value) => () => value;

  @override
  IsolatedWebWorker get webWorker =>
      IsolatedWebWorker(script, init: ['unsendable']);
}

/// Names a worker script that does not exist.
class MissingWorkerHandler extends IsolatedHandler<int, int> {
  @override
  int compute(int value) => value;

  @override
  IsolatedWebWorker get webWorker =>
      IsolatedWebWorker(script.replaceFirst('worker', 'missing'));
}

/// The worker cannot build it.
class BrokenInitHandler extends IsolatedHandler<int, int> {
  @override
  int compute(int value) => value;

  @override
  IsolatedWebWorker get webWorker =>
      IsolatedWebWorker(script, init: ['unknown']);
}

/// Names no worker: runs on the page.
class PageHandler extends IsolatedHandler<int, int> {
  @override
  int compute(int value) {
    served++;
    return value + 1;
  }
}

IsolatedHandler<dynamic, dynamic> handlerFor(Object? init) {
  final [name as String, ...rest] = init as List<Object?>;
  switch (name) {
    case 'scale':
      return ScaleHandler(rest.single as int);
    case 'served':
      return ServedHandler();
    case 'bytes':
      return ReverseBytesHandler();
    case 'summary':
      return SummaryHandler();
    case 'half':
      return HalfHandler();
    case 'unsendable':
      return UnsendableHandler();
  }
  throw ArgumentError.value(name, 'init', 'Unknown handler');
}
