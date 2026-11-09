import 'dart:js_interop';
import 'dart:js_interop_unsafe';
import 'dart:typed_data';

import 'package:isolated_stream/isolated_stream.dart';

/// Calls served by this thread, worker or page.
int served = 0;

/// The compiled test worker: `worker.js`, `worker.wasm`, or `worker` for
/// whichever the browser runs.
String script = 'worker.js';

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

/// Bytes in JavaScript's memory, where moving them shows: `[moved,
/// movedAgain, kept, movedFromDart]`, the first two one buffer, named
/// twice in [transferOf]; the last made in Dart's memory. A negative value
/// asks for the bytes the worker still holds of each of the last ones.
class MovingHandler extends IsolatedHandler<int, Object?> {
  /// The last bytes, as JavaScript has them: Dart's typed data on a moved
  /// buffer may throw when read.
  List<JSUint8Array> _last = const [];

  @override
  Object? compute(int value) {
    if (value < 0) {
      return [
        for (final bytes in _last)
          bytes.getProperty<JSNumber>('byteLength'.toJS).toDartInt,
      ];
    }
    Uint8List counted(Uint8List bytes) {
      for (var i = 0; i < bytes.length; i++) {
        bytes[i] = i;
      }
      return bytes;
    }

    final moved = counted(Uint8List(value).toJS.toDart);
    final kept = counted(Uint8List(value).toJS.toDart);
    final fromDart = counted(Uint8List(value));
    _last = [moved.toJS, moved.toJS, kept.toJS, fromDart.toJS];
    return [moved, moved.buffer.asUint8List(), kept, fromDart];
  }

  @override
  List<ByteBuffer> transferOf(Object? result) => switch (result) {
        [Uint8List moved, Uint8List again, _, Uint8List fromDart] => [
            moved.buffer,
            again.buffer,
            fromDart.buffer,
          ],
        _ => const [],
      };

  @override
  IsolatedWebWorker get webWorker =>
      IsolatedWebWorker(script, init: ['moving']);
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
    case 'moving':
      return MovingHandler();
    case 'unsendable':
      return UnsendableHandler();
  }
  throw ArgumentError.value(name, 'init', 'Unknown handler');
}
