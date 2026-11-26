@TestOn('dart2js')
library;

import 'dart:js_interop';
import 'dart:js_interop_unsafe';

import 'package:isolated_stream/isolated_stream.dart';
import 'package:test/test.dart';

import 'handlers.dart';

// The worker is compiled before the tests run (tool/test_web.sh).
void main() {
  test('a browser without WebAssembly gets the JavaScript worker', () async {
    globalContext.delete('WebAssembly'.toJS);
    expect(globalContext.has('WebAssembly'), isFalse);
    script = 'worker';
    final worker = IsolatedWorker<int, int>(ScaleHandler(3));
    addTearDown(worker.dispose);

    expect(await worker.compute(2), 6);
  });
}
