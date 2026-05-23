#!/bin/bash
# Parse benchmark output file, emit single metric (sum of all ns/op).
# Matches any line that has the field 'ns/op' (handles split lines from
# benchmarks that emit interleaved stdout).
# Usage: ./parse_metric.sh <bench_output_file>
set -e
INPUT="$1"
awk '
  {
    for (i = 1; i <= NF; i++) {
      if ($i == "ns/op") {
        # The field immediately before "ns/op" is the value.
        val = $(i-1) + 0
        if (val > 0) {
          total += val
          count++
        }
        break
      }
    }
  }
  END {
    printf "%.0f\n", total
  }
' "$INPUT"
