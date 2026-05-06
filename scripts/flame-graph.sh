#!/usr/bin/env bash
set -euo pipefail
#
# Usage: ./scripts/flame-graph.sh <input.collapsed> [output.svg]
# Renders a flame graph from collapsed stacks. Requires: brew install flamegraph

IN="${1:?usage: flame-graph.sh <input.collapsed> [output.svg]}"
OUT="${2:-${IN%.collapsed}.svg}"

if ! command -v flamegraph.pl >/dev/null 2>&1; then
    echo "ERROR: flamegraph.pl not on PATH. brew install flamegraph"
    exit 1
fi

flamegraph.pl --title "ad-server hot path" --countname "samples" "$IN" > "$OUT"
echo "==> Wrote $OUT"
