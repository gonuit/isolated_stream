# isolated_stream

Make isolates as simple as streams. Transform data in separate isolates with the same ease as regular stream operations - no complex setup.


## Features 🎯

- 🔄 **Non-blocking processing** - Transform streams in separate isolates
- ⚡ **Configurable concurrency** - Process multiple elements while preserving order  
- 🎱 **Isolate pooling** - Distribute work across multiple isolates
- 🎮 **Simple API** - Easy-to-use extension method on streams

## When to use 🤔

This package is perfect for:
- CPU-intensive computations on stream data (image processing, mathematical calculations, parsing)
- Preventing UI freezes in Flutter apps during heavy processing  
- Maintaining responsiveness while processing large datasets

## Quick start 🚀

1. **Install this package.**
   ```yaml
   dependencies:
     isolated_stream: ^2.0.0
   ```

2. **Import it in your Dart file.**
   ```dart
   import 'package:isolated_stream/isolated_stream.dart';
   ```

3. **Create a handler and transform your stream.**
   ```dart
   // Simple handler that doubles numbers
   class DoubleHandler extends IsolatedHandler<int, int> {
     @override
     int compute(int value) => value * 2; // Runs in separate isolate!
   }
   
   final stream = Stream.fromIterable([1, 2, 3, 4, 5]);
   final transformed = stream.isolatedMap(DoubleHandler());
   // Results: [2, 4, 6, 8, 10] - all calculated in separate isolate
   ```

4. **Enjoy non-blocking stream processing!** 😎

Less time waiting for frames, more time enjoying fluent animations! 🚀

# Features

## 🔄 Basic transformation

Transform stream elements in separate isolates without blocking your main thread:

```dart
// Define your computation logic
class MultiplyHandler extends IsolatedHandler<int, int> {
  final int factor;
  
  MultiplyHandler(this.factor) : super(debugName: 'Multiply by $factor');
  
  @override
  int compute(int value) => value * factor; // Runs in isolate!
}

// Use it like any other stream transformation
final stream = Stream.fromIterable([1, 2, 3, 4, 5]);
final transformed = stream.isolatedMap(MultiplyHandler(10)); // Sequential by default

await for (final value in transformed) {
  print(value); // Output: 10, 20, 30, 40, 50 (processed one at a time)
}
```

## ⚡ Processing strategies

Choose the right strategy for your use case:

```dart
final stream = Stream.fromIterable([1, 2, 3, 4, 5, 6, 7, 8]);

// Sequential: process one at a time (default)
stream.isolatedMap(handler); // or .sequential()

// Concurrent: process multiple simultaneously, preserving order
stream.isolatedMap(handler, 
  strategy: IsolatedProcessingStrategy.concurrent(concurrency: 3));

// Droppable: drop new events when at max concurrency
stream.isolatedMap(handler,
  strategy: IsolatedProcessingStrategy.droppable(concurrency: 2));

// Restartable: cancel previous work when new events arrive
stream.isolatedMap(handler,
  strategy: IsolatedProcessingStrategy.restartable());
```

## 🎱 Isolate pooling

Distribute work across multiple isolates for maximum performance:

```dart
final bigStream = Stream.fromIterable(List.generate(1000, (i) => i));

// Use multiple isolates for high-throughput processing
final processed = bigStream.isolatedMap(
  HeavyComputationHandler(),
  strategy: IsolatedProcessingStrategy.concurrent(
    concurrency: 8,  // 8 total concurrent operations
    isolates: 4,     // Work distributed among 4 isolates 💪
  ),
);

// Scales beautifully with available CPU cores!
print('Processed ${await processed.length} items lightning fast! ⚡');
```

## 🌐 Async operations

Async operations work seamlessly:

```dart
class HeavyApiDataParser extends IsolatedHandler<String, Map<String, dynamic>> {
  @override
  Future<Map<String, dynamic>> compute(String endpoint) async {
    final response = await http.get(Uri.parse(endpoint));
    return json.decode(response.body);
  }
}

// Fetch and parse data from multiple APIs concurrently
final apis = Stream.fromIterable([
  'https://example.com/data-1',
  'https://example.com/data-2', 
  'https://example.com/data-3'
]);

final responses = apis.isolatedMap(
  HeavyApiDataParser(),
  strategy: IsolatedProcessingStrategy.concurrent(concurrency: 3),
);
```

## ⚠️ Important notes

### 📦 Serialization requirements
Data must be sendable through Dart's `SendPort` mechanism for inter-isolate communication. Since isolates created with `Isolate.spawn` share the same code, most objects are sendable:

**✅ Always sendable:**
- Primitives: `null`, `true`, `false`, `int`, `double`, `String`
- Collections: `List`, `Map`, `LinkedHashMap`, `Set`, `LinkedHashSet` (with sendable contents)
- Typed data: `TransferableTypedData` and related types
- Special types: `Capability`, `SendPort`, `Type` instances
- Custom objects: Any class instance (with exceptions below)

**❌ Cannot be sent:**
- Objects with native resources: `Socket`, `File`, `HttpClient`, etc.
- Isolate infrastructure: `ReceivePort`, `DynamicLibrary`
- Finalizers: `Finalizable`, `Finalizer`, `NativeFinalizer`
- VM internals: `UserTag`, `MirrorReference`
- Classes marked with `@pragma('vm:isolate-unsendable')`

**⚠️ Closures limitation:**
Functions and closures may capture more state than needed due to VM implementation, potentially causing larger object graphs than expected ([https://github.com/dart-lang/sdk/issues/36983](https://github.com/dart-lang/sdk/issues/36983)).

### ℹ️ Performance tips
- **Use for CPU-heavy work**: There's communication overhead, so only use for intensive computations

## 📚 Overview (API reference)

### Creating handlers
```dart
class MyHandler extends IsolatedHandler<InputType, OutputType> {
  @override
  OutputType compute(InputType input) {
    // Your transformation logic here
    return transformedOutput;
  }
}
```

### Using isolatedMap
```dart
stream.isolatedMap(
  handler,           // Your IsolatedHandler instance
  strategy: IsolatedProcessingStrategy.concurrent(
    concurrency: 4,  // How many operations to run in parallel
    isolates: 2,     // How many isolates to distribute work across
  ),
)
```

### Strategies
- **`sequential()`**: Process one at a time *(default)*
- **`concurrent(concurrency, isolates)`**: Process multiple simultaneously
- **`droppable(concurrency, isolates)`**: Drop events when busy
- **`restartable(concurrency, isolates)`**: Cancel previous work for new events

**Returns**: `Stream<E>` with transformed elements in original order

## 📄 License

MIT License - see the [LICENSE](LICENSE) file for details.

