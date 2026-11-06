## 3.0.0-dev.3
- Added `IsolatedHandler.transferOf`: the buffers of a result a Web Worker moves to the caller (`postMessage` transfer list) instead of copying. No effect on native platforms, where the handler's isolate copies a result as it sends it.

## 3.0.0-dev.2
- Added `dart run isolated_stream:build`: compiles `web/**/*.worker.dart` to JS and Wasm.
- `IsolatedWebWorker('x.worker')` without an extension loads the Wasm build where supported, JS elsewhere.

## 3.0.0-dev.1
- **BREAKING CHANGE**: requires Dart 3.3.
- Web support: a handler runs in a Web Worker if it overrides `webWorker`, else on the main thread.
- Added `IsolatedWebWorker`, `runIsolatedHandler`, `RemoteWorkerError`.

## 2.1.0
- Fixed: an unsendable handler error no longer kills the worker isolate and hangs `compute()` — it is delivered as a `RemoteError` fallback.
- Fixed: worker isolates are monitored via `onExit`/`onError` — unexpected death fails in-flight `compute()` calls with a descriptive error and the pool re-spawns a fresh isolate on the next call.
- Fixed: an unsendable input to `compute()` no longer leaks the reply port or causes an unhandled async error on a later `dispose()`.

## 2.1.0-dev.1
- Added `IsolatedWorker<T, E>`: long-lived request/response API with `compute(value) -> Future<E>`.
- Added `concurrency: int?` on `IsolatedWorker` — `null` is unbounded, `1` serializes calls, `N` caps in-flight at N and queues the rest.
- Added `idleTimeout` + `onIdleShutdown` on `IsolatedWorker` and all `IsolatedProcessingStrategy` factories — workers release their isolates after inactivity and transparently re-spawn on the next call.
- Changed: sequential and concurrent scheduling is now centralized in an internal `_WorkPolicy` primitive shared by `IsolatedWorker` and the stream strategies (no behavior change).
- Fixed: in-flight `compute()` no longer hangs on `dispose()` — it now completes with a `StateError`.
- Fixed: `dispose()` racing an in-progress isolate spawn no longer leaks isolates or hangs.
- Fixed: `Droppable`/`Restartable` strategies could leave the output stream open forever if the source closed after all events had completed.
- Changed: `IsolatedWorker` throws `ArgumentError` (not `AssertionError`) for `isolates < 1`.

## 2.0.2
- Fixed a memory leak during multiple stream reinitialisations.
## 2.0.1
- Updated documentation
## 2.0.0

- **BREAKING CHANGE**: Replaced `concurrency`, and `isolates` parameters with strategy-based API
- Added `IsolatedProcessingStrategy` pattern for better type safety and extensibility
- Added `IsolatedProcessingStrategy.concurrent()`, `.droppable()`, `.restartable()`, `.sequential()` factories
- Improved broadcast stream handling with shared isolates across listeners
- Fixed concurrency tracking issues in isolate boundaries
- Enhanced lazy initialization for better resource management

**Migration example:**
```dart
// Before (v1.x)
stream.isolatedMap(handler, concurrency: 3, isolates: 2)
```

```dart
// After (v2.x)
stream.isolatedMap(
  handler,
  strategy: IsolatedProcessingStrategy.concurrent(
    concurrency: 3,
    isolates: 2,
  ),
);
```

## 1.0.0

- Initial release  
- Added `isolatedMap()` extension for streams
- Added `IsolatedHandler` abstract class
- Support for sync and async transformations
- Configurable concurrency with order preservation
- Isolate pooling for high-throughput scenarios
