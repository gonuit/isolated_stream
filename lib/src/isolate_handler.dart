part of 'isolated_stream_base.dart';

/// A handler that defines how to transform data in an isolate.
///
/// This class should be extended to define custom transformation logic
/// that will run in a separate isolate for CPU-intensive operations.
///
/// Example:
/// ```dart
/// class MultiplyHandler extends IsolatedHandler<int, int> {
///   final int factor;
///   MultiplyHandler(this.factor) : super(debugName: 'Multiply by $factor');
///
///   @override
///   int compute(int value) => value * factor;
/// }
///
/// // Usage
/// stream.isolatedMap(MultiplyHandler(10));
/// ```
abstract class IsolatedHandler<T, E> {
  /// Creates an isolated handler with an optional debug name.
  ///
  /// The [debugName] is used for debugging and isolate identification.
  const IsolatedHandler({this.debugName});

  /// Optional name for debugging and isolate identification.
  final String? debugName;

  /// Transforms a value of type [T] to type [E] in an isolate.
  ///
  /// This method is called in the isolate for each stream element.
  /// It can return either a synchronous value of type [E] or a
  /// [Future<E>] for asynchronous operations.
  ///
  /// The input [value] must be serializable (primitive types, lists, maps, etc.)
  /// The return value must also be serializable.
  FutureOr<E> compute(T value);
}
