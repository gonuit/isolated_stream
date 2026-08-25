part of 'isolated_stream_base.dart';

/// Owns a single worker isolate: spawning, request/reply routing, teardown.
class _IsolateWrapper<T, E> {
  _IsolateWrapper(this._handler) {
    // Avoids an unhandled async error when spawn fails or kill() runs
    // before the first compute() awaits _ready.
    _ready.future.ignore();
    _spawn();
  }

  final IsolatedHandler<T, E> _handler;

  bool _killed = false;
  bool _dead = false;
  Isolate? _isolate;
  SendPort? _requestPort;
  ReceivePort? _handshake;
  ReceivePort? _monitorPort;
  final Completer<void> _ready = Completer<void>();
  final Map<ReceivePort, Completer<E>> _openReplies = {};

  /// Whether the isolate terminated on its own (not via [kill]).
  bool get isDead => _dead;

  Future<void> _spawn() async {
    final handshake = ReceivePort();
    _handshake = handshake;
    final monitor = ReceivePort();
    _monitorPort = monitor;
    // onError delivers [errorDescription, stackDescription], onExit null;
    // with errorsAreFatal both mean the isolate is gone.
    monitor.listen((dynamic message) {
      if (message is List && message.length == 2) {
        _onIsolateDeath(RemoteError('${message[0]}', '${message[1] ?? ''}'));
      } else {
        _onIsolateDeath();
      }
    });

    try {
      final isolate = await Isolate.spawn<_IsolateInitMessage<T, E>>(
        _isolateEntry,
        _IsolateInitMessage<T, E>(handshake.sendPort, _handler),
        onExit: monitor.sendPort,
        onError: monitor.sendPort,
        errorsAreFatal: true,
        debugName: _handler.debugName,
      );

      if (_killed) {
        isolate.kill(priority: Isolate.immediate);
        _closeHandshake();
        _failReady();
        return;
      }

      _isolate = isolate;

      // kill() and _onIsolateDeath() close the handshake port, making
      // `first` throw so this method can unwind.
      final SendPort sendPort;
      try {
        sendPort = await handshake.first as SendPort;
      } catch (_) {
        _closeHandshake();
        _failReady();
        return;
      }

      _closeHandshake();

      if (_killed) {
        _failReady();
        return;
      }

      _requestPort = sendPort;
      if (!_ready.isCompleted) {
        _ready.complete();
      }
    } catch (e, st) {
      _closeMonitorPort();
      _closeHandshake();
      if (!_ready.isCompleted) {
        _ready.completeError(e, st);
      }
    }
  }

  void _closeMonitorPort() {
    _monitorPort?.close();
    _monitorPort = null;
  }

  void _onIsolateDeath([RemoteError? error]) {
    if (_killed || _dead) return;
    _dead = true;
    _isolate = null;
    _requestPort = null;
    _closeMonitorPort();
    _closeHandshake();
    _failReady();
    _failAllInFlight(
      error ??
          StateError(
            'Worker isolate exited unexpectedly '
            'while a computation was in progress',
          ),
      error?.stackTrace,
    );
  }

  void _failAllInFlight(Object error, [StackTrace? stackTrace]) {
    for (final entry in _openReplies.entries) {
      entry.key.close();
      if (!entry.value.isCompleted) {
        entry.value.completeError(error, stackTrace);
      }
    }
    _openReplies.clear();
  }

  void _closeHandshake() {
    _handshake?.close();
    _handshake = null;
  }

  /// Completes [_ready] normally instead of with an error: an errored
  /// completer with no listener would surface as an unhandled async error.
  /// [compute] re-checks [_killed]/[_dead] right after the await.
  void _failReady() {
    if (!_ready.isCompleted) {
      _ready.complete();
    }
  }

  Future<E> compute(T value) async {
    await _ready.future;

    // No awaits from here through send() — kill() cannot interleave.
    if (_killed) {
      throw StateError('Isolate has been killed');
    }
    if (_dead) {
      throw StateError('Worker isolate exited unexpectedly');
    }
    final reqPort = _requestPort;
    if (reqPort == null) {
      throw StateError('Isolate not ready');
    }

    final reply = ReceivePort();
    final completer = Completer<E>();
    _openReplies[reply] = completer;

    late final StreamSubscription<dynamic> sub;
    sub = reply.listen((dynamic message) {
      if (!completer.isCompleted) {
        if (message is _OkMessage<E>) {
          completer.complete(message.value);
        } else if (message is _ErrorMessage) {
          completer.completeError(message.error, message.stackTrace);
        } else {
          completer.completeError(
            StateError('Unexpected message type: ${message.runtimeType}'),
          );
        }
      }
      sub.cancel();
      reply.close();
      _openReplies.remove(reply);
    });

    try {
      reqPort.send(_Req<T>(value, reply.sendPort));
    } catch (_) {
      sub.cancel();
      reply.close();
      _openReplies.remove(reply);
      rethrow;
    }

    return completer.future;
  }

  void kill() {
    if (_killed) return;
    _killed = true;

    _failAllInFlight(
      StateError('Isolate was killed while a computation was in progress'),
    );

    _closeHandshake();
    _closeMonitorPort();

    _isolate?.kill(priority: Isolate.immediate);
    _isolate = null;
    _requestPort = null;

    _failReady();
  }
}
