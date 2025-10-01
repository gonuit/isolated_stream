part of 'isolated_stream_base.dart';

class _IsolateInitMessage<S, R> {
  _IsolateInitMessage(this.handshake, this.handler);
  final SendPort handshake;
  final IsolatedHandler<S, R> handler;
}

class _Req<T> {
  _Req(this.value, this.replyTo);
  final T value;
  final SendPort replyTo;
}

class _OkMessage<T> {
  _OkMessage(this.value);
  final T value;
}

class _ErrorMessage {
  _ErrorMessage(this.error, this.stackTrace);
  final Object error;
  final StackTrace stackTrace;
}
