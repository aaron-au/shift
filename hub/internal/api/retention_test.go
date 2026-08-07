package api_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aaron-au/shift/pkg/consign"
)

// registryWithConnectors serves a hub with a publisher key registered, and
// returns a function that publishes one signed artifact.
func registryWithConnectors(t *testing.T) (*httptest.Server, func(name, version string, art []byte)) {
	t.Helper()
	srv := newServer(t)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/publisher-keys", adminToken,
		`{"name":"pub1","public_key":"`+base64.StdEncoding.EncodeToString(pub)+`"}`, nil); c != http.StatusCreated {
		t.Fatalf("publisher key = %d", c)
	}
	return srv, func(name, version string, art []byte) {
		t.Helper()
		sum := sha256.Sum256(art)
		m := consign.Manifest{Name: name, Version: version, OS: "linux", Arch: "amd64"}
		copy(m.Digest[:], sum[:])
		url := fmt.Sprintf("%s/api/v1/connectors/%s/versions/%s?os=linux&arch=amd64", srv.URL, name, version)
		body, c := callHdr(t, http.MethodPut, url, map[string]string{
			"Authorization":         "Bearer " + adminToken,
			"X-Shift-Publisher-Key": "pub1",
			"X-Shift-Signature":     base64.StdEncoding.EncodeToString(consign.Sign(priv, m)),
		}, art)
		if c != http.StatusCreated {
			t.Fatalf("publish %s@%s = %d (%s)", name, version, c, body)
		}
	}
}

// registryWithCompat is registryWithConnectors with the publisher's
// compatibility declaration (ADR-0047 §6) attached to each version.
func registryWithCompat(t *testing.T) (*httptest.Server, func(name, version, compat, notes string)) {
	t.Helper()
	srv, publish := registryWithConnectors(t)
	_ = publish
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/publisher-keys", adminToken,
		`{"name":"pub2","public_key":"`+base64.StdEncoding.EncodeToString(pub)+`"}`, nil); c != http.StatusCreated {
		t.Fatalf("publisher key = %d", c)
	}
	return srv, func(name, version, compat, notes string) {
		t.Helper()
		art := []byte(name + "-" + version)
		sum := sha256.Sum256(art)
		m := consign.Manifest{Name: name, Version: version, OS: "linux", Arch: "amd64"}
		copy(m.Digest[:], sum[:])
		url := fmt.Sprintf("%s/api/v1/connectors/%s/versions/%s?os=linux&arch=amd64", srv.URL, name, version)
		if compat != "" {
			url += "&compat=" + compat
		}
		hdr := map[string]string{
			"Authorization":         "Bearer " + adminToken,
			"X-Shift-Publisher-Key": "pub2",
			"X-Shift-Signature":     base64.StdEncoding.EncodeToString(consign.Sign(priv, m)),
		}
		if notes != "" {
			hdr["X-Shift-Release-Notes"] = notes
		}
		if body, c := callHdr(t, http.MethodPut, url, hdr, art); c != http.StatusCreated {
			t.Fatalf("publish %s@%s = %d (%s)", name, version, c, body)
		}
	}
}

// Yank is a SELECTION rule, not a recall: flows already pinned keep running.
// Whoever yanked almost always expects otherwise, so the response says which
// flows are still on it (ADR-0047 §3).
func TestYankingSaysWhichFlowsAreStillPinnedToIt(t *testing.T) {
	srv, publish := registryWithConnectors(t)
	publish("gen", "1.0.0", []byte("gen-v1"))
	if c := call(t, http.MethodPut, srv.URL+"/api/v1/flows/orders", adminToken, `{"name":"orders",
	  "source":{"connector":"gen","action":"records"},
	  "sink":{"connector":"@discard","action":""}}`, nil); c != http.StatusCreated {
		t.Fatalf("deploy = %d", c)
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/flows/orders/versions/1/publish", adminToken, "", nil); c != http.StatusOK {
		t.Fatalf("publish = %d", c)
	}

	var refs struct {
		References []struct {
			Flow    string `json:"flow"`
			Current bool   `json:"current"`
		} `json:"references"`
	}
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/connectors/gen/versions/1.0.0/references",
		adminToken, "", &refs); c != http.StatusOK {
		t.Fatalf("references = %d", c)
	}
	if len(refs.References) != 1 || refs.References[0].Flow != "orders" || !refs.References[0].Current {
		t.Fatalf("references = %+v", refs.References)
	}

	var yanked struct {
		Yanked        bool `json:"yanked"`
		StillPinnedBy []struct {
			Flow string `json:"flow"`
		} `json:"still_pinned_by"`
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/connectors/gen/versions/1.0.0/yank",
		adminToken, `{"os":"linux","arch":"amd64"}`, &yanked); c != http.StatusOK {
		t.Fatalf("yank = %d", c)
	}
	if !yanked.Yanked {
		t.Fatal("not yanked")
	}
	if len(yanked.StillPinnedBy) != 1 || yanked.StillPinnedBy[0].Flow != "orders" {
		t.Fatalf("still_pinned_by = %+v; a yank that silently leaves flows running it teaches the wrong lesson",
			yanked.StillPinnedBy)
	}
}

// Collection deletes signed artifacts, and the publisher's private key is not
// held server-side — so it reports by default and only deletes when asked.
func TestCollectionReportsBeforeItDeletes(t *testing.T) {
	srv, publish := registryWithConnectors(t)
	publish("gen", "1.0.0", []byte("gen-v1"))

	var report struct {
		Applied  bool `json:"applied"`
		Versions []struct {
			Name, Version string
		} `json:"versions"`
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/connectors/collect", adminToken, "", &report); c != http.StatusOK {
		t.Fatalf("collect = %d", c)
	}
	if report.Applied {
		t.Fatal("collection deleted without being asked to")
	}
	// The only version of a connector is inside the floor, so there is nothing
	// to collect — a registry with one build of everything is not a registry
	// with anything to clean up.
	if len(report.Versions) != 0 {
		t.Fatalf("collectable = %+v, want none", report.Versions)
	}

	var applied struct {
		Applied bool `json:"applied"`
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/connectors/collect?apply=1", adminToken, "", &applied); c != http.StatusOK {
		t.Fatalf("collect apply = %d", c)
	}
	if !applied.Applied {
		t.Fatal("apply=1 did not apply")
	}
	// The artifact is still there: applying only removes what the report named.
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/connectors/gen/resolve?os=linux&arch=amd64",
		adminToken, "", nil); c != http.StatusOK {
		t.Fatalf("resolve after collect = %d, want 200", c)
	}
}

// Retention is admin-realm: a runner that could delete registry artifacts
// could delete the build another runner is about to fetch.
func TestARunnerCannotCollectOrReadReferences(t *testing.T) {
	srv, _ := registryWithConnectors(t)

	var tok struct{ Token string }
	call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{}`, &tok)
	var reg struct {
		Secret string `json:"secret"`
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"r1"}`, &reg); c != http.StatusCreated {
		t.Fatalf("register = %d", c)
	}
	if c := call(t, http.MethodPost, srv.URL+"/api/v1/connectors/collect?apply=1", reg.Secret, "", nil); c != http.StatusUnauthorized {
		t.Fatalf("collect as a runner = %d, want 401", c)
	}
	if c := call(t, http.MethodGet, srv.URL+"/api/v1/connectors/gen/versions/1.0.0/references",
		reg.Secret, "", nil); c != http.StatusUnauthorized {
		t.Fatalf("references as a runner = %d, want 401", c)
	}
}
