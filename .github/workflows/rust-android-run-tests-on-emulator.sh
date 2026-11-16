#!/bin/bash
set -eo pipefail

adb wait-for-device

while [[ -z "$(adb shell getprop sys.boot_completed | tr -d '\r')" ]]; do sleep 1; done

any_failures=0

# Already built by the "Build unit tests for Android" step, this only collects the paths.
tests=$(cargo ndk -t "$TARGET" test --locked --no-run --message-format=json \
    | jq -r 'select(.executable != null) | .executable')
if [[ -z "$tests" ]]; then
    echo "no test executables found" >&2
    exit 1
fi

for test in $tests; do
    adb push "$test" /data/local/tmp/
    adb shell chmod +x /data/local/tmp/$(basename "$test")
    adb shell API_LEVEL=$API_LEVEL /data/local/tmp/$(basename "$test") || any_failures=1
done

exit $any_failures
