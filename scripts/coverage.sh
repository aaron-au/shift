#!/usr/bin/env bash
# coverage.sh — per-package Go coverage across all modules, with a hard gate.
#
# Runs coverage-instrumented tests for every module, merges the profiles, and
# emits (into coverage/):
#   merged.cover   — combined coverprofile (all modules)
#   coverage.html  — browsable per-line report (go tool cover)
#   coverage.md    — per-package summary table (for the CI job summary)
#   coverage.json  — {total, packages:{...}} (consumed by the shields badge)
#
# Gate: every *gated* package must meet its floor in coverage.thresholds
# (per-package override, else `default`). Packages matching an exclusion
# pattern are measured and reported but never gated (thin main wiring,
# generated code, telemetry wiring, test helpers). Exits non-zero on any breach.
#
# Coverage runs full `-race` (NOT -short, so pg-backed tests count) but sets
# SHIFT_COVERAGE=1, which makes the connector-SUBPROCESS tests (runner
# leaseloop/service spawning real connectors) skip. Those are timing-dependent:
# the exact lines they cover vary run to run — coverage from them measured
# {0%,43%,85%} for leaseloop across runs, which can't back a hard gate. They
# still run for CORRECTNESS in `make test` (full `-race`, no SHIFT_COVERAGE).
# So `check` depends on BOTH `test` (behavior) and `cover` (coverage gate).
# -short is deliberately NOT used — it would also drop deterministic pg tests
# (e.g. the whole hub API surface), undercounting real coverage.
#
# Env:
#   SHIFT_TEST_PG   Postgres DSN for hub/runner store tests (pgtest falls back
#                   to a throwaway pg_ctl cluster when unset).
#   COVER_NOGATE=1  measure + report but skip the gate (used by cover-bump).
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
MODULES=(engine sdk pkg runner hub connectors)
OUT="$ROOT/coverage"
THRESHOLDS="$ROOT/coverage.thresholds"
MODPATH="github.com/aaron-au/shift"

# Packages excluded from the GATE (still measured + shown). Extended-regex,
# matched against the full import path.
EXCLUDE_RE='/cmd/|/telemetry$|/connectorpb$|/pgtest$|/oidctest$|/sdktest$|/e2e$'

# Skip the flaky connector-subprocess tests (see header). They still run in
# `make test`. Deterministic pg/httptest/e2e tests run normally.
export SHIFT_COVERAGE=1

rm -rf "$OUT"
mkdir -p "$OUT"

raw="$OUT/raw.cover"
: > "$raw"

for m in "${MODULES[@]}"; do
	echo "--- coverage $m"
	prof="$OUT/$m.cover"
	# -coverpkg=./... instruments every package in the module (so 0%-covered
	# packages still surface), not just the one under test. atomic mode is
	# required with -race. SHIFT_COVERAGE (exported above) skips the flaky
	# connector-subprocess tests for deterministic coverage (see header).
	(cd "$m" && go test -race -covermode=atomic -coverpkg=./... -coverprofile="$prof" ./... ) \
		|| { echo "coverage: tests failed in module $m" >&2; exit 1; }
	# Collect raw blocks (drop each profile's own "mode:" header line).
	tail -n +2 "$prof" >> "$raw"
done

# Merge count-mode profiles: with -coverpkg=./... every package's test binary
# emits a block for EVERY package, so the same block appears many times.
# Concatenation duplicates them — a real merge dedupes by block span and SUMS
# the hit counts. Key = "<file>:<start>,<end> <numstmts>" (first two fields).
awk '
{
	key = $1 " " $2
	if (!(key in seen)) { order[++n] = key; seen[key] = 1 }
	cnt[key] += $3
}
END {
	print "mode: atomic"
	for (i = 1; i <= n; i++) print order[i], cnt[order[i]]
}
' "$raw" > "$OUT/merged.cover"

# Browsable HTML.
go tool cover -html="$OUT/merged.cover" -o "$OUT/coverage.html"

# ---- Per-package aggregation + gate (awk over the raw profile) ----------------
# Coverprofile line: <file>:<s.col>,<e.col> <numstmts> <count>
# Package = dirname(file). Sum stmts as total; stmts with count>0 as covered.
awk -v exclude_re="$EXCLUDE_RE" -v thresholds="$THRESHOLDS" \
    -v nogate="${COVER_NOGATE:-0}" -v outdir="$OUT" '
BEGIN {
	# Load thresholds: "default <pct>" and "<import-path> <pct>"; # comments.
	deflt = 0
	while ((getline line < thresholds) > 0) {
		if (line ~ /^[[:space:]]*#/ || line ~ /^[[:space:]]*$/) continue
		n = split(line, f, /[[:space:]]+/)
		if (n < 2) continue
		if (f[1] == "default") deflt = f[2] + 0
		else thr[f[1]] = f[2] + 0
	}
	close(thresholds)
}
NR == 1 { next }                       # skip "mode: count"
{
	line = $0
	pos = index(line, ".go:")
	if (pos == 0) next
	file = substr(line, 1, pos + 2)    # up to and including ".go"
	pkg = file
	sub(/\/[^\/]+$/, "", pkg)          # dirname => package import path
	stmts = $(NF-1) + 0
	cnt   = $NF + 0
	total[pkg] += stmts
	if (cnt > 0) covered[pkg] += stmts
	seen[pkg] = 1
	gtotal += stmts
	if (cnt > 0) gcovered += stmts
}
END {
	# Stable ordering.
	np = 0
	for (p in seen) pkgs[++np] = p
	for (i = 1; i < np; i++) for (j = i+1; j <= np; j++)
		if (pkgs[j] < pkgs[i]) { t = pkgs[i]; pkgs[i] = pkgs[j]; pkgs[j] = t }

	md = outdir "/coverage.md"
	js = outdir "/coverage.json"
	printf "" > md
	printf "" > js

	gpct = (gtotal > 0) ? (100.0 * gcovered / gtotal) : 0
	printf "## Coverage — %.1f%% total\n\n", gpct >> md
	printf "| Package | Coverage | Floor | Status |\n" >> md
	printf "|---|---:|---:|:--|\n" >> md

	printf "{\n  \"total\": %.1f,\n  \"packages\": {\n", gpct >> js

	failed = 0
	for (i = 1; i <= np; i++) {
		p = pkgs[i]
		pct = (total[p] > 0) ? (100.0 * covered[p] / total[p]) : 0
		short = p; sub(/^github.com\/aaron-au\/shift\//, "", short)
		gated = (p ~ exclude_re) ? 0 : 1
		floor = (p in thr) ? thr[p] : deflt
		status = "—"
		if (!gated) status = "not gated"
		else if (pct + 0.05 >= floor) status = "ok"
		else { status = "**BELOW**"; failed = 1 }
		printf "| %s | %.1f%% | %s | %s |\n", short, pct, (gated ? floor "%" : "—"), status >> md
		printf "    \"%s\": %.1f%s\n", short, pct, (i < np ? "," : "") >> js
	}
	printf "  }\n}\n" >> js

	printf "\nTotal coverage: %.1f%%\n", gpct
	if (nogate == "1") { print "cover: gate skipped (COVER_NOGATE=1)"; exit 0 }
	if (failed) { print "cover: FAIL — package(s) below floor (see table)"; exit 1 }
	print "cover: all gated packages meet their floor"
}
' "$OUT/merged.cover" | tee "$OUT/summary.txt"

# Propagate the awk exit status (PIPESTATUS[0] is awk, tee is [1]).
exit "${PIPESTATUS[0]}"
