part of 'isolated_stream_base.dart';

/// Runs an [IsolatedHandler] in one or more background isolates with a
/// request/response API. Each [compute] call returns a [Future] that
/// completes with the handler's result (or its error).
///
/// Unlike [IsolatedStreamExtension.isolatedMap], an [IsolatedWorker] is
/// long-lived: create it once, call [compute] as many times as you want,
/// then call [dispose] when finished.
///
/// Example:
/// ```dart
/// final worker = IsolatedWorker<int, int>(MultiplyHandler(10));
/// final a = await worker.compute(5); // 50
/// final b = await worker.compute(7); // 70
/// worker.dispose();
/// ```
///
/// Concurrency: by default there's no per-worker in-flight limit — calls
/// run in parallel and are distributed round-robin across [isolates]
/// worker isolates. Pass [concurrency] to bound how many calls may be in
/// flight at once:
///
/// - `concurrency: 1` processes calls one at a time (sequential / FIFO).
/// - `concurrency: N` caps in-flight calls at N; excess calls are queued.
/// - `concurrency: null` (default) is unbounded.
///
/// Idle offloading: pass [idleTimeout] to have the worker release its
/// isolates after that much time has elapsed with no in-flight work. The
/// next [compute] transparently re-spawns them. Use [onIdleShutdown] to
/// observe idle shutdowns (e.g. for logging or metrics).
///
/// ```dart
/// final worker = IsolatedWorker<int, int>(
///   MultiplyHandler(10),
///   concurrency: 4,
///   idleTimeout: const Duration(minutes: 1),
///   onIdleShutdown: () => print('isolate napped'),
/// );
/// ```
class IsolatedWorker<T, E> {
  /// Creates a worker backed by [isolates] background isolate(s).
  ///
  /// All worker isolates are spawned on the first [compute] call; they
  /// remain alive until [dispose] is called (or, if [idleTimeout] is set,
  /// until they go idle for that duration). With [isolates] > 1, calls are
  /// distributed round-robin across the pool.
  ///
  /// [concurrency] bounds the number of in-flight [compute] calls. `null`
  /// (the default) means unbounded. `1` serializes calls (FIFO). `N` caps
  /// in-flight calls at N and queues the rest.
  ///
  /// If [idleTimeout] is set, the pool releases its isolates after that
  /// duration elapses with no in-flight work; the next [compute] call
  /// transparently re-spawns them. [onIdleShutdown], if provided, is
  /// called after each idle shutdown (not on explicit [dispose]).
  ///
  /// Throws [ArgumentError] if [isolates] or [concurrency] is less than 1.
  IsolatedWorker(
    IsolatedHandler<T, E> handler, {
    int isolates = 1,
    int? concurrency,
    Duration? idleTimeout,
    void Function()? onIdleShutdown,
  }) {
    if (isolates < 1) {
      throw ArgumentError.value(isolates, 'isolates', 'Must be at least 1');
    }
    if (concurrency != null && concurrency < 1) {
      throw ArgumentError.value(
          concurrency, 'concurrency', 'Must be at least 1');
    }
    _pool = _IsolatePool<T, E>(
      handler,
      isolates,
      idleTimeout: idleTimeout,
      onIdleShutdown: onIdleShutdown,
    );
    if (concurrency == 1) {
      _policy = _SequentialPolicy<T, E>(_pool.run);
    } else if (concurrency != null) {
      _policy = _ConcurrentPolicy<T, E>(_pool.run, concurrency: concurrency);
    } else {
      _policy = null;
    }
  }

  late final _IsolatePool<T, E> _pool;
  late final _WorkPolicy<T, E>? _policy;
  bool _disposed = false;

  /// Sends [value] to a worker isolate and returns its result.
  ///
  /// Multiple concurrent calls are safe — each gets its own reply channel.
  /// If a concurrency bound was set, calls beyond the bound are queued
  /// until a slot frees up. Throws [StateError] if called after [dispose],
  /// or if [dispose] runs while the call is in flight.
  Future<E> compute(T value) async {
    if (_disposed) {
      throw StateError('IsolatedWorker has been disposed');
    }
    try {
      return await (_policy?.submit(value) ?? _pool.run(value));
    } catch (_) {
      // A dispose() racing this call surfaces the canonical error instead
      // of the internal pool/runner one.
      if (_disposed) {
        throw StateError('IsolatedWorker has been disposed');
      }
      rethrow;
    }
  }

  /// Kills all worker isolates and releases resources. Any in-flight
  /// [compute] calls will complete with a [StateError]. The instance must
  /// not be used after calling [dispose].
  void dispose() {
    if (_disposed) return;
    _disposed = true;
    _pool.dispose();
  }
}
