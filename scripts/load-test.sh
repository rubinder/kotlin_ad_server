#!/usr/bin/env bash
set -euo pipefail
#
# Usage: ./scripts/load-test.sh <simulation> [output_dir]
# Example: ./scripts/load-test.sh RampUp docs/load-test/baseline-run
#
# Prerequisites: ad-server, frequency-service, postgres, redis are running.

SIMULATION="${1:-RampUp}"
OUTDIR="${2:-load-test/build/reports/$(date +%Y%m%d-%H%M%S)-${SIMULATION}}"

cd "$(dirname "$0")/.."

echo "==> Generating workload feeder..."
./gradlew :load-test:compileGatlingKotlin --quiet
./gradlew :load-test:runWorkloadGenerator --quiet 2>/dev/null || true

mkdir -p "$OUTDIR"

echo "==> Running ${SIMULATION} simulation..."
./gradlew :load-test:gatlingRun \
    -DgatlingSimulationFqn="com.github.robran.adserver.load.${SIMULATION}Simulation" \
    -PgatlingResultsDir="$OUTDIR"

echo "==> Done. Results in $OUTDIR"
echo "    Open the HTML report (look for index.html under that dir)."
