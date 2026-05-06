#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Running full test suite (unit + Testcontainers integration)"
./gradlew test

echo "==> Phase 1+2+3+4 smoke test PASSED. Full observability: metrics + traces + structured logs."
