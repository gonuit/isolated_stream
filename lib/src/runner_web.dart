import 'dart:async';
import 'dart:convert';
import 'dart:js_interop';
import 'dart:js_interop_unsafe';
import 'dart:typed_data';

import 'handler.dart';
import 'runner.dart';

/// A runner of [handler]: in its [IsolatedHandler.webWorker] if it names
/// one and the browser has workers, on the main thread otherwise.
HandlerRunner<T, E> spawnRunner<T, E>(IsolatedHandler<T, E> handler) {
  final worker = handler.webWorker;
  if (worker != null && globalContext.has('Worker')) {
    return _WorkerRunner<T, E>(_compiled(worker.script), worker.init);
  }
  return _InPlaceRunner<T, E>(handler);
}

/// Serves a handler in a Web Worker: the `main` of a worker script.
///
/// [create] builds the handler from the [IsolatedWebWorker.init] the
/// worker's creator sent, once, before the first call. Call it at the
/// start of `main`, before any `await`: calls are not received before.
///
/// Values crossing to and from a worker are copied by the browser
/// (structured clone), so they have to be `null`, `bool`, numbers,
/// `String`, typed data, or `List`s and `Map`s with `String` keys of
/// those. They arrive as `List<Object?>` and `Map<Object?, Object?>`,
/// and a number arrives as an `int` when it is whole, unless a `double`
/// is expected.
void runIsolatedHandler<T, E>(
  IsolatedHandler<T, E> Function(Object? init) create,
) {
  IsolatedHandler<T, E>? handler;
  Object? failure;
  StackTrace? failureStack;

  void onMessage(_MessageEvent event) {
    final [kind as int, id as int, payload, ...] =
        _fromJs(event.data) as List<Object?>;
    if (kind == _init) {
      try {
        handler = create(payload);
      } catch (error, stackTrace) {
        failure = error;
        failureStack = stackTrace;
      }
      return;
    }
    _answer(id, () {
      final ready = handler;
      if (ready == null) {
        Error.throwWithStackTrace(failure!, failureStack!);
      }
      return ready.compute(_input(ready, payload) as T);
    }, (result) => handler!.transferOf(result as E));
  }

  _self.onmessage = onMessage.toJS;
}

Future<void> _answer(
  int id,
  FutureOr<Object?> Function() run,
  List<ByteBuffer> Function(Object? result) transferOf,
) async {
  try {
    final result = await run();
    try {
      final transfer = <JSArrayBuffer>[];
      final message =
          _toJs(_ok, id, result, moved: transferOf(result), transfer: transfer);
      _self.postMessage(message, transfer.toJS);
      return;
    } catch (error, stackTrace) {
      _self.postMessage(_toJs(_error, id, 'Result could not be sent: $error',
          stackTrace: stackTrace.toString()));
    }
  } catch (error, stackTrace) {
    _self.postMessage(
        _toJs(_error, id, error.toString(), stackTrace: '$stackTrace'));
  }
}

final class _WorkerRunner<T, E> implements HandlerRunner<T, E> {
  _WorkerRunner(this._script, Object? init)
      : _bootstrap = _wasmBootstrap(_script) {
    try {
      _worker = switch (_bootstrap) {
        final bootstrap? =>
          _Worker(bootstrap.toJS, _WorkerOptions(type: 'module')),
        null => _Worker(_script.toJS),
      }
        ..onmessage = _onMessage.toJS
        ..onerror = _onError.toJS
        ..onmessageerror = _onMessageError.toJS
        ..postMessage(_toJs(_init, 0, init));
    } catch (_) {
      _stop();
      rethrow;
    }
  }

  final String _script;

  /// The object URL of a WebAssembly worker's bootstrap module.
  final String? _bootstrap;
  _Worker? _worker;
  final _calls = <int, Completer<E>>{};
  int _nextId = 0;
  bool _dead = false;
  bool _killed = false;

  @override
  bool get isDead => _dead;

  @override
  Future<E> compute(T value) {
    if (_killed) {
      return Future.error(StateError('Worker has been killed'));
    }
    if (_dead) {
      return Future.error(StateError('Worker exited unexpectedly'));
    }
    final id = _nextId++;
    final call = Completer<E>();
    try {
      _worker!.postMessage(_toJs(_call, id, value));
    } catch (error, stackTrace) {
      return Future.error(error, stackTrace);
    }
    _calls[id] = call;
    return call.future;
  }

  void _onMessage(_MessageEvent event) {
    final [kind as int, id as int, payload, remoteStack] =
        _fromJs(event.data) as List<Object?>;
    final call = _calls.remove(id);
    if (call == null) return;
    if (kind == _ok) {
      try {
        call.complete(_as<E>(payload));
      } catch (error, stackTrace) {
        call.completeError(error, stackTrace);
      }
    } else {
      final error = RemoteWorkerError('$payload', remoteStack as String?);
      call.completeError(error, error.stackTrace);
    }
  }

  void _onError(_ErrorEvent event) {
    // Handled here; the browser would report it as uncaught otherwise.
    event.preventDefault();
    if (_killed || _dead) return;
    _dead = true;
    _stop();
    final reason = event.message;
    _failAll(RemoteWorkerError(
      reason == null || reason.isEmpty
          ? 'Web Worker $_script failed to load'
          : 'Web Worker $_script failed: $reason',
    ));
  }

  /// A message from the worker could not be read: the worker is not
  /// usable, as when it fails.
  void _onMessageError(JSAny? _) {
    if (_killed || _dead) return;
    _dead = true;
    _stop();
    _failAll(RemoteWorkerError('A message from Web Worker $_script '
        'could not be read'));
  }

  void _stop() {
    _worker?.terminate();
    if (_bootstrap case final bootstrap?) _URL.revokeObjectURL(bootstrap);
  }

  void _failAll(Object error) {
    final calls = _calls.values.toList();
    _calls.clear();
    for (final call in calls) {
      call.completeError(error);
    }
  }

  @override
  void kill() {
    if (_killed) return;
    _killed = true;
    _stop();
    _failAll(
      StateError('Worker was killed while a computation was in progress'),
    );
  }
}

/// Runs the handler on the main thread, each call in its own event-loop
/// task so that frames can be drawn between calls.
final class _InPlaceRunner<T, E> implements HandlerRunner<T, E> {
  _InPlaceRunner(this._handler);

  final IsolatedHandler<T, E> _handler;
  final _calls = <Completer<E>>{};
  bool _killed = false;

  @override
  bool get isDead => false;

  @override
  Future<E> compute(T value) {
    if (_killed) {
      return Future.error(StateError('Runner has been killed'));
    }
    final call = Completer<E>();
    _calls.add(call);
    Future<E>(() => _handler.compute(value)).then(
      (result) {
        if (_calls.remove(call)) call.complete(result);
      },
      onError: (Object error, StackTrace stackTrace) {
        if (_calls.remove(call)) call.completeError(error, stackTrace);
      },
    );
    return call.future;
  }

  @override
  void kill() {
    if (_killed) return;
    _killed = true;
    final calls = _calls.toList();
    _calls.clear();
    for (final call in calls) {
      call.completeError(
        StateError('Runner was killed while a computation was in progress'),
      );
    }
  }
}

/// [script] with the extension of the compiled script to load: as it is
/// if it names one (`.js`, `.wasm`), else `.wasm` if the browser runs
/// WebAssembly built by `dart compile wasm` and `.js` if not.
String _compiled(String script) {
  final uri = Uri.parse(script);
  if (uri.path.endsWith('.js') || uri.path.endsWith('.wasm')) return script;
  return uri
      .replace(path: '${uri.path}${_runsWasm ? '.wasm' : '.js'}')
      .toString();
}

/// Whether the browser has the WebAssembly features `dart compile wasm`
/// needs (garbage collection and SIMD), checked as its `.support.js` does.
final bool _runsWasm = _wasm ||
    _validate([0, 97, 115, 109, 1, 0, 0, 0, 1, 5, 1, 95, 1, 120, 0]) &&
        _validate([
          0, 97, 115, 109, 1, 0, 0, 0, 1, 5, 1, 96, 0, 1, 123, 3, 2, 1, 0, //
          10, 10, 1, 8, 0, 65, 0, 253, 15, 253, 98, 11,
        ]);

bool _validate(List<int> module) =>
    _webAssemblyValidate(Uint8List.fromList(module).toJS);

@JS('WebAssembly.validate')
external bool _webAssemblyValidate(JSUint8Array bytes);

/// For a [script] compiled with `dart compile wasm`, the object URL of a
/// module worker starting it with the `.mjs` loader beside it; null for
/// one compiled to JavaScript. Messages coming while the module loads are
/// kept and handed to the program once its `main` has run.
String? _wasmBootstrap(String script) {
  final wasm = Uri.base.resolve(script);
  if (!wasm.path.endsWith('.wasm')) return null;
  final loader = wasm.replace(
    path: '${wasm.path.substring(0, wasm.path.length - '.wasm'.length)}.mjs',
  );
  final source = '''
const early = [];
self.onmessage = (event) => early.push(event);
const { compileStreaming } = await import(${jsonEncode('$loader')});
const app = await compileStreaming(fetch(${jsonEncode('$wasm')}));
const instance = await app.instantiate({});
instance.invokeMain();
for (const event of early) self.onmessage(event);
''';
  return _URL.createObjectURL(_Blob(
    [source.toJS].toJS,
    _BlobOptions(type: 'text/javascript'),
  ));
}

const _init = 0;
const _call = 1;
const _ok = 2;
const _error = 3;

/// Every message, either way: `[kind, id, payload, stackTrace]`. The
/// buffers of [moved] it holds go to [transfer], to move with it.
JSAny? _toJs(
  int kind,
  int id,
  Object? payload, {
  String? stackTrace,
  List<ByteBuffer> moved = const [],
  List<JSArrayBuffer>? transfer,
}) =>
    _jsified([kind, id, payload, stackTrace], moved, transfer ?? []);

/// [value] in JavaScript, if it can cross to or from a worker; throws
/// otherwise: compiled to JavaScript, other objects would be copied as
/// plain objects instead. The JavaScript buffers of [moved] it holds are
/// added to [transfer], each once.
JSAny? _jsified(
  Object? value,
  List<ByteBuffer> moved,
  List<JSArrayBuffer> transfer,
) {
  switch (value) {
    case null || bool() || num() || String():
      return value.jsify();
    case TypedData():
      // A typed array or a DataView, in JavaScript's memory.
      final typed = value.jsify()! as JSObject;
      if (moved.contains(value.buffer)) {
        _move(typed.getProperty<JSArrayBuffer>('buffer'.toJS), transfer);
      }
      return typed;
    case ByteBuffer():
      final buffer = value.jsify()! as JSArrayBuffer;
      if (moved.contains(value)) _move(buffer, transfer);
      return buffer;
    case List():
      return [for (final item in value) _jsified(item, moved, transfer)].toJS;
    case Map():
      final object = JSObject();
      for (final MapEntry(:key, value: item) in value.entries) {
        if (key is! String) {
          throw ArgumentError.value(
              key, 'key', 'Only String map keys can cross to a Web Worker');
        }
        object[key] = _jsified(item, moved, transfer);
      }
      return object;
    default:
      throw ArgumentError.value(value, 'value', 'Cannot cross to a Web Worker');
  }
}

/// Adds [buffer] to [transfer] unless it is there: a buffer listed twice
/// cannot be sent.
void _move(JSArrayBuffer buffer, List<JSArrayBuffer> transfer) {
  if (!transfer.any((listed) => listed.strictEquals(buffer).toDart)) {
    transfer.add(buffer);
  }
}

Object? _fromJs(JSAny? message) {
  final value = message.dartify();
  return _wasm ? _wholeNumbers(value) : value;
}

/// [value] as [R], a whole number as a `double` if [R] takes one and not
/// an `int` (compiled to WebAssembly the two differ).
R _as<R>(Object? value) => switch (value) {
      int() when value is! R && 0.0 is R => value.toDouble() as R,
      _ => value as R,
    };

/// [value] for [handler]: a whole number as a `double` if the handler
/// takes one and not an `int`.
Object? _input(IsolatedHandler<Object?, Object?> handler, Object? value) =>
    value is int &&
            handler is! IsolatedHandler<int, Object?> &&
            handler is IsolatedHandler<double, Object?>
        ? value.toDouble()
        : value;

/// Whether this is compiled to WebAssembly, where a JavaScript number
/// arrives as a `double`; compiled to JavaScript a whole one is an `int`.
const _wasm = bool.fromEnvironment('dart.tool.dart2wasm');

/// Numbers beyond it are not exact in JavaScript.
const _maxSafeInteger = 9007199254740991;

Object? _wholeNumbers(Object? value) => switch (value) {
      double()
          when value.abs() <= _maxSafeInteger &&
              value == value.truncateToDouble() =>
        value.toInt(),
      List() when value is! TypedData => [
          for (final item in value) _wholeNumbers(item)
        ],
      Map() => {
          for (final MapEntry(:key, value: item) in value.entries)
            _wholeNumbers(key): _wholeNumbers(item),
        },
      _ => value,
    };

@JS('Worker')
extension type _Worker._(JSObject _) implements JSObject {
  external factory _Worker(JSString url, [_WorkerOptions options]);
  external void postMessage(JSAny? message);
  external void terminate();
  external set onmessage(JSFunction? handler);
  external set onerror(JSFunction? handler);
  external set onmessageerror(JSFunction? handler);
}

extension type _WorkerOptions._(JSObject _) implements JSObject {
  external factory _WorkerOptions({String type});
}

@JS('Blob')
extension type _Blob._(JSObject _) implements JSObject {
  external factory _Blob(JSArray<JSString> parts, _BlobOptions options);
}

extension type _BlobOptions._(JSObject _) implements JSObject {
  external factory _BlobOptions({String type});
}

@JS('URL')
extension type _URL._(JSObject _) implements JSObject {
  external static String createObjectURL(_Blob blob);
  external static void revokeObjectURL(String url);
}

extension type _MessageEvent._(JSObject _) implements JSObject {
  external JSAny? get data;
}

extension type _ErrorEvent._(JSObject _) implements JSObject {
  external String? get message;
  external void preventDefault();
}

extension type _WorkerScope._(JSObject _) implements JSObject {
  external void postMessage(JSAny? message, [JSArray<JSArrayBuffer> transfer]);
  external set onmessage(JSFunction? handler);
}

@JS('self')
external _WorkerScope get _self;
