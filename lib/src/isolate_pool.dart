part of 'isolated_stream_base.dart';

class _IsolatePool<T, E> {
  _IsolatePool(
    this._handler,
    this._maxSize, {
    Duration? idleTimeout,
    void Function()? onIdleShutdown,
  })  : _idleTimeout = idleTimeout,
        _onIdleShutdown = onIdleShutdown;

  final List<HandlerRunner<T, E>> _pool = [];
  final IsolatedHandler<T, E> _handler;
  final int _maxSize;
  final Duration? _idleTimeout;
  final void Function()? _onIdleShutdown;
  int _currentIndex = 0;
  int _inFlight = 0;
  Timer? _idleTimer;
  bool _disposed = false;

  Future<void> _ensurePoolSize() async {
    if (_disposed) {
      throw StateError('Isolate pool disposed');
    }
    _pool.removeWhere((runner) => runner.isDead);
    while (_pool.length < _maxSize) {
      _pool.add(spawnRunner<T, E>(_handler));
    }
  }

  /// Acquire a runner using round-robin selection.
  ///
  /// Callers using [acquire] directly do NOT get idle-timeout tracking.
  /// If [idleTimeout] was set on the pool, use [run] instead.
  Future<HandlerRunner<T, E>> acquire() async {
    await _ensurePoolSize();

    if (_pool.isEmpty) {
      throw StateError('Isolate pool is empty');
    }

    final runner = _pool[_currentIndex % _pool.length];
    _currentIndex = (_currentIndex + 1) % _pool.length;

    return runner;
  }

  /// Runs [value] through an acquired runner, tracking in-flight work and
  /// (re)arming the idle timer. After an idle shutdown, the next call
  /// re-spawns isolates.
  Future<E> run(T value) async {
    if (_disposed) {
      throw StateError('Isolate pool disposed');
    }
    _inFlight++;
    _idleTimer?.cancel();
    _idleTimer = null;
    try {
      final runner = await acquire();
      return await runner.compute(value);
    } finally {
      _inFlight--;
      if (_inFlight == 0 && !_disposed && _idleTimeout != null) {
        _idleTimer = Timer(_idleTimeout, _onIdle);
      }
    }
  }

  void _onIdle() {
    if (_inFlight > 0 || _disposed) return;
    _shutdownIsolates();
    _idleTimer = null;
    _onIdleShutdown?.call();
  }

  void _shutdownIsolates() {
    for (final runner in _pool) {
      runner.kill();
    }
    _pool.clear();
  }

  void dispose() {
    if (_disposed) return;
    _disposed = true;
    _idleTimer?.cancel();
    _idleTimer = null;
    _shutdownIsolates();
  }
}
