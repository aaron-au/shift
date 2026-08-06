#!/usr/bin/env bash
# fetch-schema-suite.sh — refresh the vendored JSON-Schema-Test-Suite fixtures
# used by engine/schema (ADR-0042 §4c-i).
#
# The suite is VENDORED rather than fetched at test time, deliberately: a test
# that needs the network is a test that fails in CI for reasons unrelated to
# the code, and a validator's conformance evidence should be reviewable in the
# diff that changes it.
#
# Only the files for keywords this subset implements are vendored. Adding a
# keyword means adding its file here, and the conformance test will then hold
# the implementation to the specification's own cases.
#
# Usage: ./scripts/fetch-schema-suite.sh
set -euo pipefail

cd "$(dirname "$0")/.."
DEST="engine/schema/testdata/suite"
BASE="https://raw.githubusercontent.com/json-schema-org/JSON-Schema-Test-Suite/main/tests/draft2020-12"

# Keywords in the supported set.
KEYWORDS=(
	type required properties items enum const
	minimum maximum minLength maxLength pattern
	minItems maxItems additionalProperties
	ref defs
)

# Format assertion cases live under optional/format/. The top-level format.json
# is deliberately NOT vendored: it tests format-as-ANNOTATION (everything
# valid), and this subset asserts formats instead (ADR-0042 §4c).
FORMATS=(date date-time time email uuid)

mkdir -p "$DEST"
for kw in "${KEYWORDS[@]}"; do
	curl -fsSL -o "$DEST/$kw.json" "$BASE/$kw.json"
	echo "  $kw.json"
done
for f in "${FORMATS[@]}"; do
	curl -fsSL -o "$DEST/format-$f.json" "$BASE/optional/format/$f.json"
	echo "  format-$f.json"
done

echo "fetched $(( ${#KEYWORDS[@]} + ${#FORMATS[@]} )) suite files into $DEST"
echo "review the diff, then run: go test ./engine/schema -run Conformance -v"
