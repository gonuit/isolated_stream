@TestOn('vm')
library;

import 'dart:io';

import 'package:test/test.dart';

void main() {
  test('build compiles every web/**/*.worker.dart to JS and WebAssembly',
      () async {
    final project = await Directory.systemTemp.createTemp('isolated_stream');
    addTearDown(() => project.delete(recursive: true));
    await File('${project.path}/web/workers/sum.worker.dart')
        .create(recursive: true)
        .then((file) => file.writeAsString('void main() {}\n'));
    await File('${project.path}/web/app.dart')
        .writeAsString('void main() {}\n');

    final result = await Process.run(
      Platform.resolvedExecutable,
      ['run', File('bin/build.dart').absolute.path],
      workingDirectory: project.path,
    );

    expect(result.exitCode, 0, reason: '${result.stdout}${result.stderr}');
    for (final name in ['sum.worker.js', 'sum.worker.wasm', 'sum.worker.mjs']) {
      expect(File('${project.path}/web/workers/$name').existsSync(), isTrue,
          reason: name);
    }
    expect(File('${project.path}/web/app.js').existsSync(), isFalse);
  }, timeout: const Timeout(Duration(minutes: 2)));
}
