package api_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/api"
)

// TC-031. A runner whose credential is fine gets 401 when the hub's DATABASE is
// down, because authRunner is a store round-trip and every error became an
// opaque rejection.
//
// The consequence was checked rather than assumed: hubclient does not treat 401
// as terminal and leaseloop retries with backoff, so the fleet recovers on its
// own. It recovers while telling the operator the wrong story — every runner
// reporting "unauthorized" at once looks like a credential or registration
// problem, and that is where someone will go looking. 503 says the true thing:
// the hub could not answer, as opposed to answering "not you".
func TestARunnerIsToldTheHubIsUnavailableNotThatItIsUnauthorized(t *testing.T) {
	srv, faults := newFaultServer(t, api.Options{})
	secret := registerRunner(t, srv.URL, "r1")

	// Sanity: the credential works before the outage, or the test proves nothing.
	if _, code := call2t(t, http.MethodPost, srv.URL+"/api/v1/lease",
		secret, `{"capacity":1,"wait_seconds":0}`); code != http.StatusOK && code != http.StatusNoContent {
		t.Fatalf("lease before the outage = %d, want 200 or 204: the credential must be good", code)
	}

	faults.killConnections()

	// The first request after a connection drop is the one that sees the
	// outage; the pool reconnects behind it. Look for a 503 among the first few
	// rather than demanding it from exactly one.
	var sawUnavailable, sawUnauthorized bool
	var lastBody string
	for range 10 {
		raw, code := call2t(t, http.MethodPost, srv.URL+"/api/v1/lease",
			secret, `{"capacity":1,"wait_seconds":0}`)
		lastBody = raw
		switch code {
		case http.StatusServiceUnavailable:
			sawUnavailable = true
		case http.StatusUnauthorized:
			sawUnauthorized = true
		case http.StatusOK, http.StatusNoContent:
			// Recovered.
		}
		if sawUnauthorized || sawUnavailable {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if sawUnauthorized {
		t.Fatalf("a runner with a VALID credential was told 401 during a database outage: "+
			"an operator is sent hunting a credential problem that does not exist (body %q)", lastBody)
	}
	if !sawUnavailable {
		t.Skip("the pool reconnected before any request observed the outage; nothing to assert")
	}
	// The 503 still has to be a machine-readable envelope (ADR-0023), like
	// every other error the hub returns.
	wantErrorEnvelope(t, "POST /api/v1/lease", lastBody, http.StatusServiceUnavailable)
	if strings.Contains(strings.ToLower(lastBody), "unauthorized") {
		t.Fatalf("the unavailable response still says unauthorized: %q", lastBody)
	}
}

// TestAGenuinelyBadCredentialIsStill401 is the other half. Turning every auth
// failure into 503 would be the same bug facing the other way: a hub that never
// says "not you" cannot be diagnosed either, and it would hide a real
// misconfiguration behind an apparent outage.
func TestAGenuinelyBadCredentialIsStill401(t *testing.T) {
	srv, _ := newFaultServer(t, api.Options{})

	raw, code := call2t(t, http.MethodPost, srv.URL+"/api/v1/lease",
		"not-a-real-secret", `{"capacity":1,"wait_seconds":0}`)
	if code != http.StatusUnauthorized {
		t.Fatalf("a bad runner secret = %d, want 401 (body %q)", code, raw)
	}
	wantErrorEnvelope(t, "POST /api/v1/lease", raw, http.StatusUnauthorized)
}
