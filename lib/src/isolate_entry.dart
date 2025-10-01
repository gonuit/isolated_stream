part of 'isolated_stream_base.dart';

void _isolateEntry<S, R>(_IsolateInitMessage<S, R> init) {
  final requests = ReceivePort();
  init.handshake.send(requests.sendPort);

  requests.listen((dynamic msg) async {
    if (msg is! _Req<S>) return;
    final req = msg;

    try {
      final result = init.handler.compute(req.value);
      if (result is Future<R>) {
        final v = await result;
        req.replyTo.send(_OkMessage<R>(v));
      } else {
        req.replyTo.send(_OkMessage<R>(result));
      }
    } catch (e, st) {
      req.replyTo.send(_ErrorMessage(e, st));
    }
  });
}
