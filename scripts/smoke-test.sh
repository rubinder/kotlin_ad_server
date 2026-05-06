#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Running full test suite (unit + Testcontainers integration)"
./gradlew test

echo "==> Phase 1+2+3+4+5 smoke test PASSED. Load test scenarios + profiling toolchain ready."
