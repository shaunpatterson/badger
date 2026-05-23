#!/bin/bash
# Run STABLE benchmark subset (excludes BenchmarkDbGrowth & BenchmarkDBOpen which are
# I/O dominated and highly variable). Writes full output to file, prints metric to stdout.
# Usage: ./run_bench.sh <output_file> [benchtime]
set -e
OUT="${1:-/tmp/bench.txt}"
BENCHTIME="${2:-3x}"

# Skip noisy I/O-dominated benchmarks at the package level via -bench regex.
# BenchmarkDbGrowth: writes tens of MB, high variance from FS cache
# BenchmarkDBOpen: requires -benchdir
# BenchmarkValueGCRewriteExpiredOnlyFile: heavy I/O
ROOT_BENCH='BenchmarkReadWrite|BenchmarkIteratePrefixSingleKey'

{
  # Run sub-packages (CPU-bound, stable)
  go test -run='^$' -bench=. -benchtime="$BENCHTIME" -benchmem -timeout=10m ./skl/ ./table/ ./y/
  # Run only stable root benchmarks
  go test -run='^$' -bench="$ROOT_BENCH" -benchtime="$BENCHTIME" -benchmem -timeout=10m .
} > "$OUT" 2>&1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$SCRIPT_DIR/parse_metric.sh" "$OUT"
