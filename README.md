# isolated_stream

Make isolates as simple as streams — *or as simple as a function call*. Offload CPU-heavy work to background isolates with either a stream API or a long-lived request/response worker.


## Features 🎯

- 🔄 **Non-blocking processing** - Transform streams in separate isolates
- 📞 **Request/response API** - `IsolatedWorker` for awaiting individual calls on long-lived isolates
- ⚡ **Configurable concurrency** - Cap in-flight work with a single parameter
- 🎱 **Isolate pooling** - Distribute work across multiple isolates
- 💤 **Idle offloading** - Automatically release isolates after inactivity and transparently re-spawn on demand
- 🎮 **Simple API** - Easy-to-use extension method on streams

## When to use 🤔

This package is perfect for:
- CPU-intensive computations on stream data (image processing, mathematical calculations, parsing)
- Background workers that handle repeated ad-hoc requests (parsers, encoders, solvers)
- Preventing UI freezes in Flutter apps during heavy processing
- Maintaining responsiveness while processing large datasets

## Quick start 🚀

1. **Install this package.**
   ```yaml
   dependencies:
     isolated_stream: ^2.1.0-dev.1
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

## 📞 Request/response with `IsolatedWorker`

When you don't have a stream of data — just individual values you want to process on demand — use `IsolatedWorker`. It keeps isolates alive across calls and returns a `Future<E>` for each `compute(value)`.

```dart
class ParseJsonHandler extends IsolatedHandler<String, Map<String, dynamic>> {
  @override
  Map<String, dynamic> compute(String raw) => json.decode(raw);
}

final worker = IsolatedWorker<String, Map<String, dynamic>>(
  ParseJsonHandler(),
  isolates: 2,        // optional: spread calls across 2 isolates
  concurrency: 4,     // optional: cap in-flight calls to 4 (queue the rest)
);

// Call it like a function, as many times as you like.
final a = await worker.compute('{"hello": "world"}');
final b = await worker.compute('[1, 2, 3]');

// Many calls in parallel — bounded by `concurrency`.
final results = await Future.wait([
  for (final raw in payloads) worker.compute(raw),
]);

worker.dispose(); // kill isolates when done
```

### Concurrency modes

- `concurrency: null` *(default)* — unbounded; calls run in parallel, distributed round-robin across the isolate pool.
- `concurrency: 1` — serialized (FIFO); one call at a time.
- `concurrency: N` — at most N calls in flight; extras queue until a slot frees.

## 💤 Idle offloading

Both `IsolatedWorker` and every `IsolatedProcessingStrategy` accept an `idleTimeout`. If no work has been in flight for that long, the pool releases its isolates; the next call transparently re-spawns them. Handy for workers that might sit idle for a long time.

```dart
// Request/response worker that naps when unused
final worker = IsolatedWorker<int, int>(
  MultiplyHandler(10),
  idleTimeout: const Duration(minutes: 1),
  onIdleShutdown: () => print('isolates released'),
);

// Stream strategy with idle offloading
stream.isolatedMap(
  handler,
  strategy: IsolatedProcessingStrategy.concurrent(
    concurrency: 3,
    idleTimeout: const Duration(seconds: 30),
  ),
);
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

### Stream API — `isolatedMap`
```dart
stream.isolatedMap(
  handler,           // Your IsolatedHandler instance
  strategy: IsolatedProcessingStrategy.concurrent(
    concurrency: 4,  // How many operations to run in parallel
    isolates: 2,     // How many isolates to distribute work across
    idleTimeout: const Duration(minutes: 1), // optional
  ),
)
```

**Strategies:**
- **`sequential()`** — Process one at a time *(default)*
- **`concurrent(concurrency, isolates)`** — Process multiple simultaneously, preserving input order
- **`droppable(concurrency, isolates)`** — Drop events when busy
- **`restartable(concurrency, isolates)`** — Cancel previous work for new events

All four accept optional `idleTimeout` and `onIdleShutdown`.

**Returns**: `Stream<E>` with transformed elements in input order.

### Request/response API — `IsolatedWorker`
```dart
final worker = IsolatedWorker<T, E>(
  handler,            // Your IsolatedHandler instance
  isolates: 1,        // How many background isolates
  concurrency: null,  // null = unbounded; 1 = sequential; N = bounded
  idleTimeout: null,  // optional: release isolates after inactivity
  onIdleShutdown: null,
);

final result = await worker.compute(value); // Future<E>
worker.dispose();
```

**When to pick which:** use `isolatedMap` when you already have a `Stream<T>` to transform; use `IsolatedWorker` when you have individual values to process on demand and want a long-lived worker you can call like a function.

## 📄 License

MIT License - see the [LICENSE](LICENSE) file for details.

