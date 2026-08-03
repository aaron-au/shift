#!/usr/bin/env bash
# notice.sh — regenerate NOTICE from the modules that actually ship.
#
# Scope is the BUILD graph (`go list -deps` over every main package), not the
# module graph: go.mod lists modules that never reach a binary, and attribution
# is owed only for what is distributed.
#
# Licences are classified from each module's own LICENSE/COPYING text. A module
# whose licence text is unrecognised is reported as "see upstream" and must be
# resolved by hand — silence there would be a compliance gap, not a pass.
set -euo pipefail
cd "$(dirname "$0")/.."

MODULES=(hub runner connectors)
mods=$(for m in "${MODULES[@]}"; do
	(cd "$m" && go list -deps -f '{{if .Module}}{{.Module.Path}}{{end}}' ./cmd/... 2>/dev/null)
done | sort -u | grep -v '^github.com/aaron-au/shift' | grep '\.')

python3 scripts/notice.py <<< "$mods"
echo "notice: wrote NOTICE ($(grep -c '^  [a-z]' NOTICE) components)"
