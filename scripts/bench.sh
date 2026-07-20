#!/usr/bin/env bash
# bench.sh — run the shift-bench scenario matrix, collect machine-readable
# JSON, and render a visible results table (docs/bench-M7/results.md).
#
# Each scenario runs with -warmup and -runs (median reported) for stability,
# and keeps its RSS ceiling as a HARD gate (the M1 exit criterion, ADR-0003):
# a scenario that blows its RSS budget fails the whole run.
#
# Env:
#   BENCH_BYTES   input size per scenario   (default 64MiB)
#   BENCH_RUNS    repeats per scenario      (default 3)
#   BENCH_OUT     output dir                (default docs/bench-M7)
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
BYTES="${BENCH_BYTES:-64MiB}"
RUNS="${BENCH_RUNS:-3}"
OUT="${BENCH_OUT:-$ROOT/docs/bench-M7}"
RESULTS="$OUT/results"
BIN="$ROOT/bin/shift-bench"

mkdir -p "$RESULTS" "$ROOT/bin"
(cd engine && go build -o "$BIN" ./cmd/shift-bench)

# scenario|extra-flags|rss-ceiling
matrix=(
	"transform||100MiB"
	"csv||100MiB"
	"aggregate|-watermark 8MiB -groups 100000|120MiB"
	"baseline||1GiB"
)

echo "shift-bench matrix: bytes=$BYTES runs=$RUNS"
for row in "${matrix[@]}"; do
	IFS='|' read -r scenario extra ceiling <<< "$row"
	echo "--- $scenario (rss<=$ceiling)"
	# shellcheck disable=SC2086 # $extra is intentionally word-split into flags
	"$BIN" -scenario "$scenario" -bytes "$BYTES" -runs "$RUNS" -warmup \
		-max-rss "$ceiling" $extra -json > "$RESULTS/$scenario.json" \
		|| { echo "bench: $scenario FAILED (RSS ceiling or error)" >&2; exit 1; }
done

# Render the visible table from the collected JSON.
python3 "$ROOT/scripts/bench_report.py" "$RESULTS" "$BYTES" "$RUNS" > "$OUT/results.md"
echo "bench: wrote $OUT/results.md"
