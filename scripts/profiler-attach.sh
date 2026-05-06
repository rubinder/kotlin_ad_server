#!/usr/bin/env bash
set -euo pipefail
#
# Usage: ./scripts/profiler-attach.sh <duration_seconds> [out_file]
# Default duration 60s.
#
# Attaches async-profiler to the running ad-server JVM, captures CPU samples for the duration,
# writes collapsed stacks to the output file. Requirements:
#   - brew install async-profiler   (macOS)
#   - sudo sysctl kernel.perf_event_paranoid=1   (Linux only)

DURATION="${1:-60}"
OUT="${2:-docs/load-test/profile-$(date +%Y%m%d-%H%M%S).collapsed}"

cd "$(dirname "$0")/.."
mkdir -p "$(dirname "$OUT")"

# Find async-profiler — homebrew installs to /opt/homebrew or /usr/local/Cellar
PROFILER_JAR=""
for candidate in \
    /opt/homebrew/lib/async-profiler/async-profiler.jar \
    /usr/local/lib/async-profiler/async-profiler.jar \
    /opt/homebrew/Cellar/async-profiler/*/lib/async-profiler.jar \
    "$HOME/async-profiler/lib/async-profiler.jar"; do
    if [[ -f "$candidate" ]]; then
        PROFILER_JAR="$candidate"
        break
    fi
done
if [[ -z "$PROFILER_JAR" ]]; then
    echo "ERROR: async-profiler not found. brew install async-profiler"
    exit 1
fi

PID=$(jps -l | grep -E 'ApplicationKt|ad-server' | head -1 | awk '{print $1}')
if [[ -z "$PID" ]]; then
    echo "ERROR: ad-server JVM not running. Start with ./gradlew :ad-server:run"
    exit 1
fi
echo "==> Attaching to PID $PID, sampling $DURATION s, output $OUT"

java -jar "$PROFILER_JAR" -d "$DURATION" -e cpu -o collapsed -f "$OUT" "$PID"

echo "==> Captured $(wc -l <"$OUT") stack samples to $OUT"
