export 'runner_io.dart' if (dart.library.js_interop) 'runner_web.dart';

/// Runs one handler away from the caller: an isolate on native platforms,
/// a Web Worker or the main thread on the web (`spawnRunner`).
abstract interface class HandlerRunner<T, E> {
  /// The handler's result for [value].
  Future<E> compute(T value);

  /// Whether it stopped on its own (not by [kill]) and cannot be used.
  bool get isDead;

  /// Stops it. Calls in flight fail with a [StateError].
  void kill();
}
