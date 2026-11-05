#!/bin/sh
# Browser tests: compiles the test worker to JavaScript and to WebAssembly,
# then runs the tests in Chrome with the page compiled to both.
set -e
cd "$(dirname "$0")/.."
dart compile js -O2 test/web/worker.dart -o test/web/worker.dart.js
dart compile wasm -O2 test/web/worker.dart -o test/web/worker.wasm
dart test -p chrome "$@"
dart test -p chrome -c dart2wasm "$@"
