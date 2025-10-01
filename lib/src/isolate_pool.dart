part of 'isolated_stream_base.dart';

class _IsolatePool<T, E> {
  final List<_IsolateWrapper<T, E>> _pool = [];
  final IsolatedHandler<T, E> _handler;
  final int _maxSize;
  int _currentIndex = 0;
  
  _IsolatePool(this._handler, this._maxSize);

  Future<void> _ensurePoolSize() async {
    while (_pool.length < _maxSize) {
      final wrapper = _IsolateWrapper<T, E>(_handler);
      _pool.add(wrapper);
    }
  }

  /// Acquire an isolate wrapper using round-robin selection
  Future<_IsolateWrapper<T, E>> acquire() async {
    await _ensurePoolSize();
    
    if (_pool.isEmpty) {
      throw StateError('Isolate pool is empty');
    }
    
    final wrapper = _pool[_currentIndex % _pool.length];
    _currentIndex = (_currentIndex + 1) % _pool.length;
    
    return wrapper;
  }



  Future<void> dispose() async {
    await Future.wait(_pool.map((wrapper) => wrapper.kill()));
    _pool.clear();
  }
}