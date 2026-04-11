part of 'isolated_stream_base.dart';

/// Policy that governs *how* individual work submissions are scheduled
/// against a single worker function. The policy owns concurrency/ordering
/// decisions; the worker function owns the actual computation.
///
/// Used by both the stream strategies in [processing_strategy.dart] and
/// by [IsolatedWorker] so that the semantics of "sequential" and "bounded
/// concurrent" are defined in exactly one place.
abstract class _WorkPolicy<T, E> {
  _WorkPolicy(this._worker);

  final Future<E> Function(T) _worker;

  /// Submit [value] for processing. Returns a [Future] that completes with
  /// the worker's result, or completes with an error that the worker threw.
  Future<E> submit(T value);
}

/// Processes submissions one at a time in FIFO order.
///
/// Semantically equivalent to `_ConcurrentPolicy(concurrency: 1)` but
/// slightly cheaper: no queue object, no counter — just a chained future.
class _SequentialPolicy<T, E> extends _WorkPolicy<T, E> {
  _SequentialPolicy(super.worker);

  /// Resolves when the most-recently-submitted call has completed (success
  /// or error). New submissions chain on this, guaranteeing FIFO execution.
  Future<void> _tail = Future<void>.value();

  @override
  Future<E> submit(T value) {
    final completer = Completer<E>();
    final previous = _tail;
    _tail = _runAfter(previous, value, completer);
    return completer.future;
  }

  Future<void> _runAfter(
    Future<void> previous,
    T value,
    Completer<E> completer,
  ) async {
    await previous;
    try {
      completer.complete(await _worker(value));
    } catch (e, st) {
      completer.completeError(e, st);
    }
  }
}

/// Bounds in-flight work to [concurrency] submissions at a time. Extra
/// submissions queue in FIFO order. Does **not** preserve result order —
/// callers that need input-order output (e.g. [ConcurrentProcessingStrategy])
/// layer that on top.
class _ConcurrentPolicy<T, E> extends _WorkPolicy<T, E> {
  _ConcurrentPolicy(
    super.worker, {
    required this.concurrency,
    this.onSpaceAvailable,
  }) : assert(concurrency >= 1, 'concurrency must be >= 1');

  final int concurrency;

  /// Invoked when an in-flight slot frees up *and* the queue is empty —
  /// i.e. when [isFull] transitions from true to false. Stream callers
  /// use this to `resume()` a paused source subscription.
  void Function()? onSpaceAvailable;

  int _active = 0;
  final ListQueue<_PendingSubmit<T, E>> _queue = ListQueue();

  /// True when the policy has no free slot and any [submit] call would
  /// be enqueued rather than started immediately. Stream callers check
  /// this after each submission to decide whether to pause their source.
  bool get isFull => _active >= concurrency;

  @override
  Future<E> submit(T value) {
    final completer = Completer<E>();
    if (_active < concurrency) {
      _startWork(value, completer);
    } else {
      _queue.add(_PendingSubmit(value, completer));
    }
    return completer.future;
  }

  void _startWork(T value, Completer<E> completer) {
    _active++;
    _worker(value).then(
      (result) {
        if (!completer.isCompleted) completer.complete(result);
      },
      onError: (Object e, StackTrace st) {
        if (!completer.isCompleted) completer.completeError(e, st);
      },
    ).whenComplete(_slotFreed);
  }

  void _slotFreed() {
    _active--;
    if (_queue.isNotEmpty) {
      final next = _queue.removeFirst();
      _startWork(next.value, next.completer);
    } else if (!isFull) {
      onSpaceAvailable?.call();
    }
  }
}

class _PendingSubmit<T, E> {
  _PendingSubmit(this.value, this.completer);
  final T value;
  final Completer<E> completer;
}
