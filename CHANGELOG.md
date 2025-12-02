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
