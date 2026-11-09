#!/bin/sh
# Browser tests: compiles the test worker to JavaScript and to WebAssembly,
# then runs the tests in Chrome with the page compiled to both.
set -e
cd "$(dirname "$0")/.."
dart run isolated_stream:build test/web/worker.dart
dart test -p chrome "$@"
dart test -p chrome -c dart2wasm "$@"
