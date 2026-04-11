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
