#!/usr/bin/env bash
# fuzz.sh — run one fuzz target, distinguishing a real finding from the
# end-of-budget flake.
#
# `go test -fuzz` occasionally reports "context deadline exceeded" exactly as
# the -fuzztime budget expires: the coordinator cancels its workers, and a
# worker caught mid-RPC surfaces that cancellation as a test failure. Nothing
# was found; the run simply ended. Seen twice in CI (PR #51 on
# ndjson.FuzzReader, PR #55 on flowdoc.FuzzParse), both times on branches that
# could not have affected the target.
#
# The discriminator is not the exit code or the message — it is whether the
# run produced a CRASHER. Go writes a failing input to
# testdata/fuzz/<Target>/ and leaves it there; that file is the finding, and
# it is what CI uploads. So: no new crasher + deadline message => retry once.
# A crasher, or any other failure, fails immediately and loudly.
#
# Usage: fuzz.sh <module-dir> <package> <target>
set -uo pipefail

module=${1:?module dir}
pkg=${2:?package}
target=${3:?fuzz target}
fuzztime=${FUZZTIME:-30s}

corpus="$module/${pkg#./}/testdata/fuzz/$target"

crashers() { [ -d "$corpus" ] && find "$corpus" -type f 2>/dev/null | wc -l | tr -d ' ' || echo 0; }

before=$(crashers)

run() {
	(cd "$module" && go test "$pkg" -run='^$' -fuzz="^${target}\$" -fuzztime="$fuzztime") 2>&1
}

out=$(run)
status=$?

# A mistyped or renamed target makes `go test -fuzz` print a warning and exit
# 0 — the target silently stops being fuzzed and nothing complains. Treat it
# as a failure so a rename cannot quietly delete coverage.
if grep -q "no fuzz tests to fuzz" <<<"$out"; then
	echo "$out"
	echo "fuzz: no target matching '$target' in $module/$pkg — renamed or mistyped?"
	exit 1
fi

if [ $status -eq 0 ]; then
	echo "$out"
	exit 0
fi

after=$(crashers)
if [ "$after" -gt "$before" ]; then
	echo "$out"
	echo "fuzz: $target FOUND A CRASHER — input written under $corpus"
	exit 1
fi

if ! grep -q "context deadline exceeded" <<<"$out"; then
	echo "$out"
	echo "fuzz: $target failed (no crasher, but not the end-of-budget flake)"
	exit 1
fi

echo "fuzz: $target hit the end-of-budget flake (no crasher produced); retrying once" >&2
out=$(run)
status=$?
echo "$out"
if [ $status -eq 0 ]; then
	exit 0
fi
if [ "$(crashers)" -gt "$before" ]; then
	echo "fuzz: $target FOUND A CRASHER on retry — input written under $corpus"
fi
exit 1
