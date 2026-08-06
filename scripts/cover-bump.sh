#!/usr/bin/env bash
# cover-bump.sh — ratchet coverage.thresholds up to the currently-achieved
# coverage (minus a small epsilon so nondeterministic tests don't flake the
# gate). Floors only ever rise: an existing floor is never lowered.
#
# Usage:
#   ./scripts/cover-bump.sh          # measure (no gate), then rewrite thresholds
#
# Run after adding tests, review the diff, and commit coverage.thresholds.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
MD="$ROOT/coverage/coverage.md"
THRESHOLDS="$ROOT/coverage.thresholds"
EPSILON=2   # percentage points of slack below achieved

# Measure without gating so we get a fresh table even if the gate would fail.
COVER_NOGATE=1 ./scripts/coverage.sh >/dev/null

[ -f "$MD" ] || { echo "cover-bump: $MD missing" >&2; exit 1; }

# Rewrite the thresholds file IN PLACE: each existing package line keeps its
# position (and therefore the comment above it, which is usually the only record
# of WHY a floor is where it is), with only the number raised. New packages are
# appended at the end.
#
# Two bugs this shape exists to prevent, both found on 2026-08-05:
#
#   1. The "never lower" check compared the md table's SHORT package name
#      against floors keyed by FULL import path, so the lookup always missed,
#      `cur` was always 0, and every floor was rewritten to achieved-epsilon —
#      silently LOWERING floors, which is the one thing a ratchet must not do.
#   2. Package lines were dropped and re-emitted at the end, which bunched every
#      explanatory comment at the top of the file, detached from the floor it
#      explained.
awk -v md="$MD" -v eps="$EPSILON" -v prefix="github.com/aaron-au/shift/" '
BEGIN {
	# Parse the coverage.md table: | pkg | pct% | floor | status |
	while ((getline line < md) > 0) {
		if (line !~ /^\| /) continue
		if (line ~ /Package/ || line ~ /^\|---/) continue
		gsub(/ /, "", line)
		split(line, c, "|")         # c[2]=pkg c[3]=pct% c[4]=floor c[5]=status
		if (c[5] == "notgated") continue
		pct = c[3]; sub(/%$/, "", pct); pct += 0
		want = int(pct) - eps
		if (want < 0) want = 0
		# Key by full import path: that is what the thresholds file and the gate
		# both use, and mixing the two forms is how bug 1 happened.
		achieved[prefix c[2]] = want
		gated[prefix c[2]] = 1
	}
	close(md)
}
/^[[:space:]]*#/ { print; next }        # keep comments verbatim, in place
/^[[:space:]]*$/ { print; next }        # keep blank lines
{
	n = split($0, f, /[[:space:]]+/)
	if (n < 2 || f[1] == "default") { print; next }
	pkg = f[1]
	seen[pkg] = 1
	cur = f[2] + 0
	newfloor = (pkg in achieved && achieved[pkg] > cur) ? achieved[pkg] : cur
	printf "%s %d\n", pkg, newfloor
}
END {
	# Packages gated for the first time: append, sorted, so the diff is readable.
	np = 0
	for (p in gated) if (!(p in seen)) pk[++np] = p
	if (np == 0) exit
	for (i = 1; i < np; i++) for (j = i+1; j <= np; j++)
		if (pk[j] < pk[i]) { t = pk[i]; pk[i] = pk[j]; pk[j] = t }
	print ""
	print "# Newly gated packages (added by cover-bump; add a note about each)."
	for (i = 1; i <= np; i++) printf "%s %d\n", pk[i], achieved[pk[i]]
}
' "$THRESHOLDS" > "$THRESHOLDS.tmp"

# Re-attach the header comment block (everything up to and including the first
# `default` line is preserved by the awk above via comment/blank passthrough,
# but the generated body replaces old package lines).
mv "$THRESHOLDS.tmp" "$THRESHOLDS"
echo "cover-bump: rewrote $THRESHOLDS (epsilon=${EPSILON}pp; floors only rise)"
echo "review the diff, then commit coverage.thresholds"
