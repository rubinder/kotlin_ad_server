#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Running full test suite (unit + Testcontainers integration)"
./gradlew test

echo "==> Phase 1+2+3 smoke test PASSED. Full event loop: ad-server → Kafka → Flink → Redis."
