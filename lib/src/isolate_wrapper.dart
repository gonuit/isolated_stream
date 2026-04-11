part of 'isolated_stream_base.dart';

class _IsolateWrapper<T, E> {
  _IsolateWrapper(this._handler) {
    // Pre-attach an error listener so that a spawn failure or kill-before-
    // first-compute doesn't surface as an unhandled async error. Real
    // awaiters of compute() attach their own listener and still see the
    // error when they await _ready.future.
    _ready.future.ignore();
    // Start spawning immediately. kill() coordinates via the _killed flag
    // and by closing the handshake port.
    _spawn();
  }

  final IsolatedHandler<T, E> _handler;

  bool _killed = false;
  Isolate? _isolate;
  SendPort? _requestPort;
  ReceivePort? _handshake;
  final Completer<void> _ready = Completer<void>();
  final Map<ReceivePort, Completer<E>> _openReplies = {};

  Future<void> _spawn() async {
    final handshake = ReceivePort();
    _handshake = handshake;

    try {
      final isolate = await Isolate.spawn<_IsolateInitMessage<T, E>>(
        _isolateEntry,
        _IsolateInitMessage<T, E>(handshake.sendPort, _handler),
        errorsAreFatal: true,
        debugName: _handler.debugName,
      );

      // If kill() ran before the isolate came back, tear it down now so
      // we never leak a live isolate nobody references.
      if (_killed) {
        isolate.kill(priority: Isolate.immediate);
        _closeHandshake();
        _failReady();
        return;
      }

      _isolate = isolate;

      // Wait for the worker's SendPort via the handshake port. If kill()
      // runs at any point while we're awaiting here, it will close the
      // handshake port, which makes this `first` complete with a
      // StateError('No element') so we can unwind.
      final SendPort sendPort;
      try {
        sendPort = await handshake.first as SendPort;
      } catch (_) {
        // handshake port was closed by kill() before the worker replied.
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
      // Isolate.spawn itself failed (rare). Propagate to any compute()
      // callers waiting on _ready.
      _closeHandshake();
      if (!_ready.isCompleted) {
        _ready.completeError(e, st);
      }
    }
  }

  void _closeHandshake() {
    _handshake?.close();
    _handshake = null;
  }

  void _failReady() {
    // Don't use completeError here: if nobody is awaiting yet (kill()
    // running before the first compute() reaches `await _ready.future`),
    // an errored completer becomes an unhandled async error. Instead,
    // complete normally — compute() checks _killed right after the await
    // and throws the canonical error synchronously.
    if (!_ready.isCompleted) {
      _ready.complete();
    }
  }

  Future<E> compute(T value) async {
    // Wait for spawn to finish; throws if spawn failed or was killed.
    await _ready.future;

    // After the await, the wrapper may have been killed. Everything from
    // here through reqPort.send(...) is synchronous (no awaits), so kill()
    // cannot interleave between the check and the registration in
    // _openReplies.
    if (_killed) {
      throw StateError('Isolate has been killed');
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

    // send [value, replyPort] to the isolate
    reqPort.send(_Req<T>(value, reply.sendPort));

    return completer.future;
  }

  void kill() {
    if (_killed) return;
    _killed = true;

    // Fail any in-flight compute() calls so their futures don't hang.
    for (final entry in _openReplies.entries) {
      entry.key.close();
      if (!entry.value.isCompleted) {
        entry.value.completeError(
          StateError(
            'Isolate was killed while a computation was in progress',
          ),
        );
      }
    }
    _openReplies.clear();

    // Unblock a pending _spawn() that may be awaiting handshake.first.
    _closeHandshake();

    _isolate?.kill(priority: Isolate.immediate);
    _isolate = null;
    _requestPort = null;

    // If spawn hadn't finished yet, fail _ready so awaiters unblock.
    _failReady();
  }
}
