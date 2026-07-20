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

# Preserve existing explicit floors (to enforce "floors only rise") and the
# `default` line, then merge with newly-achieved values.
awk -v md="$MD" -v eps="$EPSILON" '
BEGIN {
	# Load current floors from the existing thresholds file (stdin below is
	# the thresholds file).
}
/^[[:space:]]*#/ { print; next }        # keep comments verbatim
/^[[:space:]]*$/ { print; next }        # keep blank lines
{
	n = split($0, f, /[[:space:]]+/)
	if (n >= 2) old[f[1]] = f[2] + 0
	if (f[1] == "default") deflt_line = $0
}
END {
	# Parse the coverage.md table: | pkg | pct% | floor | status |
	while ((getline line < md) > 0) {
		if (line !~ /^\| /) continue
		if (line ~ /Package/ || line ~ /^\|---/) continue
		gsub(/ /, "", line)
		m = split(line, c, "|")     # c[2]=pkg c[3]=pct% c[4]=floor c[5]=status
		pkg = c[2]
		status = c[5]
		if (status == "notgated") continue
		pct = c[3]; sub(/%$/, "", pct); pct += 0
		want = int(pct) - eps
		if (want < 0) want = 0
		cur = (pkg in old) ? old[pkg] : 0
		floor[pkg] = (want > cur) ? want : cur   # never lower
		gated[pkg] = 1
	}
	close(md)

	# Emit: default first (kept), then one line per gated package, sorted.
	if (deflt_line != "") print deflt_line; else print "default 0"
	np = 0
	for (p in gated) pk[++np] = p
	for (i = 1; i < np; i++) for (j = i+1; j <= np; j++)
		if (pk[j] < pk[i]) { t = pk[i]; pk[i] = pk[j]; pk[j] = t }
	print ""
	for (i = 1; i <= np; i++) printf "github.com/aaron-au/shift/%s %d\n", pk[i], floor[pk[i]]
}
' "$THRESHOLDS" > "$THRESHOLDS.tmp"

# Re-attach the header comment block (everything up to and including the first
# `default` line is preserved by the awk above via comment/blank passthrough,
# but the generated body replaces old package lines).
mv "$THRESHOLDS.tmp" "$THRESHOLDS"
echo "cover-bump: rewrote $THRESHOLDS (epsilon=${EPSILON}pp; floors only rise)"
echo "review the diff, then commit coverage.thresholds"
