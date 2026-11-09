import 'dart:async';
import 'dart:typed_data';

/// A handler that defines how to transform data in an isolate.
///
/// This class should be extended to define custom transformation logic
/// that will run in a separate isolate for CPU-intensive operations.
///
/// Example:
/// ```dart
/// class MultiplyHandler extends IsolatedHandler<int, int> {
///   final int factor;
///   MultiplyHandler(this.factor) : super(debugName: 'Multiply by $factor');
///
///   @override
///   int compute(int value) => value * factor;
/// }
///
/// // Usage
/// stream.isolatedMap(MultiplyHandler(10));
/// ```
///
/// On the web there are no isolates. A handler runs on the main thread
/// there, unless it names a Web Worker script in [webWorker].
abstract class IsolatedHandler<T, E> {
  /// Creates an isolated handler with an optional debug name.
  ///
  /// The [debugName] is used for debugging and isolate identification.
  const IsolatedHandler({this.debugName});

  /// Optional name for debugging and isolate identification.
  final String? debugName;

  /// Transforms a value of type [T] to type [E] in an isolate.
  ///
  /// This method is called in the isolate for each stream element.
  /// It can return either a synchronous value of type [E] or a
  /// [Future<E>] for asynchronous operations.
  ///
  /// The input [value] must be serializable (primitive types, lists, maps, etc.)
  /// The return value must also be serializable.
  FutureOr<E> compute(T value);

  /// The buffers of [result] to move to the caller instead of copying
  /// them, none by default.
  ///
  /// On the web a Web Worker transfers them with its result (the
  /// `postMessage` transfer list): they cross at no cost, and are empty
  /// in the worker after, so it must not use them again. Compiled
  /// to WebAssembly, bytes made in Dart's memory are first copied to
  /// JavaScript's, in the worker, and that copy moves. On native platforms
  /// nothing moves: the isolate that computes a result copies it as it
  /// sends it, and the caller takes that copy as it is.
  ///
  /// ```dart
  /// @override
  /// List<ByteBuffer> transferOf(Uint8List result) => [result.buffer];
  /// ```
  List<ByteBuffer> transferOf(E result) => const [];

  /// The Web Worker this handler runs in on the web, or null to run it on
  /// the main thread there. Ignored on native platforms, where the handler
  /// itself is sent to an isolate.
  ///
  /// ```dart
  /// @override
  /// IsolatedWebWorker get webWorker =>
  ///     IsolatedWebWorker('workers/multiply.worker', init: factor);
  /// ```
  IsolatedWebWorker? get webWorker => null;
}

/// A compiled Web Worker script that runs an [IsolatedHandler] on the web.
///
/// The script is a Dart file whose `main` calls `runIsolatedHandler`,
/// compiled to JavaScript and WebAssembly by `dart run isolated_stream:build`:
///
/// ```dart
/// // web/workers/multiply.worker.dart
/// import 'package:isolated_stream/isolated_stream.dart';
///
/// void main() => runIsolatedHandler((init) => MultiplyHandler(init as int));
/// ```
///
/// ```sh
/// dart run isolated_stream:build
/// ```
///
/// Named without an extension (`workers/multiply.worker`), the worker is
/// the WebAssembly one where the browser can run it and the JavaScript one
/// elsewhere; named with `.js` or `.wasm`, it is that one.
final class IsolatedWebWorker {
  /// A worker loaded from [script], a URL relative to the page, whose
  /// handler is built from [init].
  const IsolatedWebWorker(this.script, {this.init});

  /// The URL of the compiled worker script, relative to the page.
  final String script;

  /// What the worker builds its handler from: sent once to every worker
  /// started, so it has to cross to a Web Worker (see [runIsolatedHandler]).
  final Object? init;
}

/// An error thrown by a handler in a Web Worker, or the worker failing,
/// with the error's text and stack trace.
final class RemoteWorkerError extends Error {
  RemoteWorkerError(this.message, [String? stackTrace])
      : _stackTrace = stackTrace == null || stackTrace.isEmpty
            ? null
            : StackTrace.fromString(stackTrace);

  /// The error's text, as the worker had it.
  final String message;

  final StackTrace? _stackTrace;

  @override
  StackTrace? get stackTrace => _stackTrace;

  @override
  String toString() => message;
}
