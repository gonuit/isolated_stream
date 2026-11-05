import 'dart:io';

/// Compiles Web Worker scripts to JavaScript and to WebAssembly.
///
/// ```sh
/// dart run isolated_stream:build              # every web/**/*.worker.dart
/// dart run isolated_stream:build lib/a.dart   # the files named
/// ```
///
/// `x.worker.dart` becomes `x.worker.js` and `x.worker.wasm` (with its
/// `x.worker.mjs` loader) beside it; name it `x.worker` in
/// `IsolatedWebWorker` and the browser gets the WebAssembly one if it can
/// run it, the JavaScript one otherwise.
Future<void> main(List<String> args) async {
  if (args.contains('-h') || args.contains('--help')) {
    stdout.writeln('Usage: dart run isolated_stream:build [worker.dart ...]\n'
        'Without files, compiles every web/**/*.worker.dart.');
    return;
  }
  final sources = args.isNotEmpty ? args : _workersIn(Directory('web'));
  if (sources.isEmpty) {
    stderr.writeln('No web/**/*.worker.dart files found.');
    exitCode = 64;
    return;
  }
  for (final source in sources) {
    if (!source.endsWith('.dart')) {
      stderr.writeln('Not a Dart file: $source');
      exitCode = 64;
      return;
    }
    final base = source.substring(0, source.length - '.dart'.length);
    for (final (target, output) in [
      ('js', '$base.js'),
      ('wasm', '$base.wasm')
    ]) {
      stdout.writeln('$source -> $output');
      final result = await Process.run(Platform.resolvedExecutable,
          ['compile', target, '-O2', source, '-o', output]);
      if (result.exitCode != 0) {
        stderr
          ..write(result.stdout)
          ..write(result.stderr);
        exitCode = result.exitCode;
        return;
      }
    }
  }
}

List<String> _workersIn(Directory web) {
  if (!web.existsSync()) return [];
  return [
    for (final entity in web.listSync(recursive: true))
      if (entity is File && entity.path.endsWith('.worker.dart')) entity.path,
  ]..sort();
}
