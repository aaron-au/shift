#!/usr/bin/env bash
#
# e2e-operational.sh — Level-1 automated operational test (SHIFT test plan).
#
# Drives a RUNNING SHIFT runner end-to-end (pickup -> manipulate -> deliver),
# asserts each result, and captures evidence under evidence/<ts>/. Exits
# non-zero on the first failure so CI/releasers get a hard signal.
#
# It tests the OPERATIONAL system (a real runner over HTTP), complementing the
# in-process Go e2e suite in hub/e2e/. It provisions nothing external — point it
# at a runner you already have (the `make up` bundle, or a standalone runnerd).
#
# Usage:
#   RUNNER_URL=http://localhost:8340 scripts/e2e-operational.sh
#   scripts/e2e-operational.sh                 # defaults to localhost:8340
#
# Optional:
#   RUNNER_AUTH="user:pass"   # if the control surface has Basic auth enabled
#   EVIDENCE_DIR=...          # override evidence output dir
#
set -euo pipefail

RUNNER_URL="${RUNNER_URL:-http://localhost:8340}"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="${EVIDENCE_DIR:-evidence/$TS}"
mkdir -p "$EVIDENCE_DIR"

CURL=(curl -sS --max-time 60)
[ -n "${RUNNER_AUTH:-}" ] && CURL+=(-u "$RUNNER_AUTH")

pass=0 fail=0
step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$1"; pass=$((pass+1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; fail=$((fail+1)); }

# run_sync <name> <flow-json> — POST /api/flows/run, save body+headers as
# evidence, echo the body. Fails hard on non-2xx.
run_sync() {
  local name="$1" flow="$2"
  local body="$EVIDENCE_DIR/$name.body" hdr="$EVIDENCE_DIR/$name.headers"
  local code
  code=$("${CURL[@]}" -D "$hdr" -o "$body" -w '%{http_code}' \
    -X POST "$RUNNER_URL/api/flows/run" -H 'Content-Type: application/json' -d "$flow")
  echo "$flow" > "$EVIDENCE_DIR/$name.flow.json"
  if [ "$code" != "200" ]; then
    bad "$name: expected 200, got $code (see $body)"; return 1
  fi
  cat "$body"
}

# --- 0. reachability -------------------------------------------------------
step "runner reachable ($RUNNER_URL)"
if "${CURL[@]}" -o "$EVIDENCE_DIR/healthz.txt" -w '%{http_code}' "$RUNNER_URL/healthz" | grep -q 200; then
  ok "healthz"
else
  bad "healthz — is a runner up at $RUNNER_URL? (make up, or a standalone runnerd)"; echo; exit 1
fi

"${CURL[@]}" "$RUNNER_URL/api/status" > "$EVIDENCE_DIR/status.json" 2>/dev/null \
  && ok "status endpoint" || bad "status endpoint"

# --- 1. pickup -> manipulate -> deliver (synthetic source) -----------------
# gen source -> filter(active) -> project(id,email) -> @response. Proves the
# full sync request-reply path and the transform ops.
step "integration: gen -> filter -> project -> @response"
FLOW1='{"name":"e2e-gen","source":{"connector":"gen","action":"gen","config":{"records":100}},
"ops":[{"type":"filter","path":"$.active","op":"eq","value":true},
{"type":"project","fields":[{"path":"$.id"},{"path":"$.email"}]}],
"sink":{"connector":"@response"}}'
if OUT=$(run_sync "gen_filter_project" "$FLOW1"); then
  n=$(printf '%s' "$OUT" | grep -c . || true)
  if [ "$n" -gt 0 ] && [ "$n" -lt 100 ] && printf '%s' "$OUT" | head -1 | grep -q '"email"'; then
    ok "filtered+projected $n/100 records, shape {id,email}"
  else
    bad "unexpected output ($n lines) — see evidence"
  fi
fi

# --- 2. real API pickup -> deliver (http source) ---------------------------
# http GET a public JSON array -> project -> @response. Proves REST-JSON pickup
# and delivery back to the caller. (Skips cleanly if egress is unavailable.)
step "integration: http GET (REST JSON array) -> project -> @response"
FLOW2='{"name":"e2e-http","source":{"connector":"http","action":"get",
"config":{"url":"https://jsonplaceholder.typicode.com/users"}},
"ops":[{"type":"project","fields":[{"path":"$.id"},{"path":"$.name"},{"path":"$.email"}]}],
"sink":{"connector":"@response"}}'
body="$EVIDENCE_DIR/http_users.body"
code=$("${CURL[@]}" -o "$body" -w '%{http_code}' -X POST "$RUNNER_URL/api/flows/run" \
  -H 'Content-Type: application/json' -d "$FLOW2" || echo 000)
if [ "$code" = "200" ]; then
  n=$(grep -c . "$body" || true)
  [ "$n" -gt 0 ] && ok "fetched+projected $n users" || bad "empty http result"
else
  printf '  \033[33mSKIP\033[0m http egress unavailable (code %s)\n' "$code"
fi

# --- 3. failure path is reported, not swallowed ----------------------------
step "integration: failing flow returns 422"
FLOWBAD='{"name":"e2e-bad","source":{"connector":"gen","action":"gen","config":{"records":0}},
"sink":{"connector":"@response"}}'
code=$("${CURL[@]}" -o "$EVIDENCE_DIR/bad.body" -w '%{http_code}' -X POST "$RUNNER_URL/api/flows/run" \
  -H 'Content-Type: application/json' -d "$FLOWBAD" || echo 000)
[ "$code" = "422" ] && ok "failing flow -> 422 + error body" || bad "expected 422, got $code"

# --- summary ---------------------------------------------------------------
step "summary"
printf 'evidence: %s\n' "$EVIDENCE_DIR"
printf '\033[1m%d passed, %d failed\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
