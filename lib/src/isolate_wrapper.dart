part of 'isolated_stream_base.dart';

class _IsolateWrapper<T, E> {
  _IsolateWrapper(this._handler) {
    _spawn();
  }

  final IsolatedHandler<T, E> _handler;

  Isolate? _isolate;
  SendPort? _requestPort;
  final Completer<void> _ready = Completer<void>();
  final Set<ReceivePort> _openReplies = {};

  Future<void> _spawn() async {
    final handshake = ReceivePort();
    _isolate = await Isolate.spawn<_IsolateInitMessage<T, E>>(
      _isolateEntry,
      _IsolateInitMessage<T, E>(handshake.sendPort, _handler),
      errorsAreFatal: true,
      debugName: _handler.debugName,
    );

    _requestPort = await handshake.first as SendPort;
    handshake.close();
    _ready.complete();
  }

  Future<E> compute(T value) async {
    await _ready.future;
    final reqPort = _requestPort;
    if (reqPort == null) {
      throw StateError('Isolate not ready');
    }

    final reply = ReceivePort();
    _openReplies.add(reply);

    final completer = Completer<E>();

    StreamSubscription? sub;
    sub = reply.listen((dynamic message) async {
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

      await sub?.cancel();
      reply.close();
      _openReplies.remove(reply);
    });

    // send [value, replyPort] to the isolate
    reqPort.send(_Req<T>(value, reply.sendPort));

    final result = await completer.future;
    return result;
  }

  void kill() {
    // Close outstanding replies
    for (final r in _openReplies) {
      r.close();
    }
    _openReplies.clear();

    _isolate?.kill(priority: Isolate.immediate);
    _isolate = null;
    _requestPort = null;
  }
}
