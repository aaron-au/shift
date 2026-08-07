package store_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
)

func newGateway(t *testing.T, s *store.Store, name string) store.Gateway {
	t.Helper()
	gw, err := s.CreateGateway(t.Context(), name, "https://"+name+".example", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("create gateway: %v", err)
	}
	return gw
}

func TestRouteLifecycle(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	rt, token, err := s.CreateRoute(ctx, store.Route{
		Path: "/orders", Method: "POST", Flow: "orders-in",
		Selector:      map[string]string{"workload": "api"},
		AuthPrincipal: "acme-erp",
		AllowCIDRs:    []string{"203.0.113.0/24"},
		MaxBodyBytes:  1 << 20,
	}, true)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if token == "" {
		t.Fatal("no caller token was minted, so the route is unauthenticated by accident")
	}
	if rt.AuthTokenSHA256 == "" || len(rt.AuthTokenSHA256) != 64 {
		t.Fatalf("stored hash = %q, want a hex SHA-256", rt.AuthTokenSHA256)
	}
	if rt.AuthTokenSHA256 == token {
		t.Fatal("the plaintext token was stored; only its hash may be")
	}

	list, err := s.ListRoutes(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].Path != "/orders" {
		t.Fatalf("listed %+v", list)
	}
	if list[0].Selector["workload"] != "api" {
		t.Fatalf("selector did not round-trip: %v", list[0].Selector)
	}

	// Rotation replaces the credential without touching anything else.
	newToken, err := s.RotateRouteToken(ctx, rt.ID)
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if newToken == token {
		t.Fatal("rotation returned the same token")
	}
	after, err := s.GetRoute(ctx, rt.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.AuthTokenSHA256 == rt.AuthTokenSHA256 {
		t.Fatal("rotation did not change the stored hash, so the old token still works")
	}
	if after.Flow != "orders-in" || after.AuthPrincipal != "acme-erp" {
		t.Fatal("rotation changed more than the credential")
	}

	if err := s.DeleteRoute(ctx, rt.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetRoute(ctx, rt.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get after delete = %v, want ErrNotFound", err)
	}
	if err := s.DeleteRoute(ctx, rt.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("second delete = %v, want ErrNotFound", err)
	}
	if _, err := s.RotateRouteToken(ctx, rt.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rotate a missing route = %v, want ErrNotFound", err)
	}
}

// A deliberately public route stores no credential at all — the difference
// between "open" and "somebody forgot" has to be visible in the data.
func TestAPublicRouteStoresNoCredential(t *testing.T) {
	s := open(t)
	rt, token, err := s.CreateRoute(t.Context(), store.Route{Path: "/health", Flow: "health"}, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if token != "" || rt.AuthTokenSHA256 != "" {
		t.Fatal("a public route was given a credential")
	}
}

// One handler per method+path per gateway. Catching it here turns a
// whole-config rejection at the gateway into a single failed edit.
func TestDuplicateRoutesAreRefused(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	r := store.Route{Path: "/orders", Method: "POST", Flow: "a", AuthPrincipal: "x"}
	if _, _, err := s.CreateRoute(ctx, r, true); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, _, err := s.CreateRoute(ctx, r, true); err == nil {
		t.Fatal("a duplicate method+path was accepted; the gateway would reject the whole configuration")
	}
	// A different method on the same path is a different handler, and fine.
	r.Method = "GET"
	if _, _, err := s.CreateRoute(ctx, r, true); err != nil {
		t.Fatalf("same path, different method: %v", err)
	}
}

// A route pinned to one gateway must not appear in another's configuration.
func TestRoutesAreScopedToTheirGateway(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	a, b := newGateway(t, s, "a"), newGateway(t, s, "b")

	if _, _, err := s.CreateRoute(ctx, store.Route{Path: "/everywhere", Flow: "f", AuthPrincipal: "p"}, true); err != nil {
		t.Fatalf("create shared: %v", err)
	}
	if _, _, err := s.CreateRoute(ctx, store.Route{
		GatewayID: a.ID, Path: "/only-a", Flow: "f", AuthPrincipal: "p",
	}, true); err != nil {
		t.Fatalf("create pinned: %v", err)
	}

	cfgA, err := s.BuildGatewayConfig(ctx, a.ID)
	if err != nil {
		t.Fatalf("build a: %v", err)
	}
	cfgB, err := s.BuildGatewayConfig(ctx, b.ID)
	if err != nil {
		t.Fatalf("build b: %v", err)
	}
	paths := func(c store.GatewayConfig) map[string]bool {
		m := map[string]bool{}
		for _, r := range c.Routes {
			m[r.Path] = true
		}
		return m
	}
	if !paths(cfgA)["/everywhere"] || !paths(cfgA)["/only-a"] {
		t.Fatalf("gateway a serves %v, want both", paths(cfgA))
	}
	if !paths(cfgB)["/everywhere"] {
		t.Fatal("gateway b lost the shared route")
	}
	if paths(cfgB)["/only-a"] {
		t.Fatal("a route pinned to gateway a was served by gateway b")
	}
}

// The roster is what selectors are matched against, so it must reflect the
// LIVE runner table — a stale one would keep routing to a runner the hub no
// longer vouches for, or stop routing to one it does.
func TestTheRosterCarriesHubAssertedLabels(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	gw := newGateway(t, s, "g")

	token, _, err := s.CreateRegistrationToken(ctx, time.Hour)
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	id, _, err := s.RegisterRunnerCert(ctx, token, "r1")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	cfg, err := s.BuildGatewayConfig(ctx, gw.ID)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(cfg.Runners) != 1 || cfg.Runners[0].ID != id {
		t.Fatalf("roster = %+v, want the registered runner", cfg.Runners)
	}
	if len(cfg.Runners[0].Labels) != 0 {
		t.Fatal("a runner has labels nobody set")
	}

	if err := s.SetRunnerLabels(ctx, id, map[string]string{"workload": "api"}); err != nil {
		t.Fatalf("labels: %v", err)
	}
	cfg, err = s.BuildGatewayConfig(ctx, gw.ID)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if cfg.Runners[0].Labels["workload"] != "api" {
		t.Fatalf("labels = %v, want the ones the hub asserted", cfg.Runners[0].Labels)
	}

	if err := s.SetRunnerLabels(ctx, "00000000-0000-0000-0000-000000000000", nil); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("labelling a missing runner = %v, want ErrNotFound", err)
	}
}

// A runner is told which gateways to poll (ADR-0038 §4). Every case here is a
// gateway it must NOT be told about, because the failure of each is silent: a
// runner polling a gateway that cannot verify it, or one whose routes could
// never select it, costs a parked connection per DMZ host and looks fine.
func TestARunnerPollsOnlyGatewaysThatCouldSendItWork(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	adopted, unadopted := newGateway(t, s, "adopted"), newGateway(t, s, "unadopted")
	adopt(t, s, adopted.ID, fingerprint("ab"))

	token, _, err := s.CreateRegistrationToken(ctx, time.Hour)
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	runnerID, _, err := s.RegisterRunnerCert(ctx, token, "r1")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	urls := func() []string {
		t.Helper()
		gws, err := s.GatewaysForRunner(ctx, runnerID)
		if err != nil {
			t.Fatalf("gateways for runner: %v", err)
		}
		out := make([]string, 0, len(gws))
		for _, g := range gws {
			out = append(out, g.URL)
		}
		return out
	}

	// No routes anywhere: nothing could be sent, so there is nothing to poll.
	if got := urls(); len(got) != 0 {
		t.Fatalf("gateways = %v, want none before any route exists", got)
	}

	// An unselective route makes every ADOPTED gateway worth polling. The
	// unadopted one still is not: it has no runner CA, so it could not verify
	// the certificate this runner would present.
	if _, _, err := s.CreateRoute(ctx, store.Route{Path: "/open", Flow: "f", AuthPrincipal: "p"}, true); err != nil {
		t.Fatalf("create route: %v", err)
	}
	got := urls()
	if len(got) != 1 || got[0] != adopted.URL {
		t.Fatalf("gateways = %v, want only the adopted one (%s)", got, adopted.URL)
	}
	adopt(t, s, unadopted.ID, fingerprint("cd"))
	if got := urls(); len(got) != 2 {
		t.Fatalf("gateways = %v, want both once the second is adopted", got)
	}
}

// Selector matching decides the address list the same way it decides
// placement. If the two disagreed, a runner would either poll a gateway that
// will never pick it or — worse — stop polling one that would.
func TestTheAddressListMatchesSelectorsTheSameWayPlacementDoes(t *testing.T) {
	s := open(t)
	ctx := t.Context()

	api, batch := newGateway(t, s, "api"), newGateway(t, s, "batch")
	adopt(t, s, api.ID, fingerprint("ab"))
	adopt(t, s, batch.ID, fingerprint("cd"))

	token, _, err := s.CreateRegistrationToken(ctx, time.Hour)
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	runnerID, _, err := s.RegisterRunnerCert(ctx, token, "r1")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Each gateway carries one route, selective and pinned to it.
	for gw, workload := range map[string]string{api.ID: "api", batch.ID: "batch"} {
		if _, _, err := s.CreateRoute(ctx, store.Route{
			GatewayID: gw, Path: "/" + workload, Flow: "f", AuthPrincipal: "p",
			Selector: map[string]string{"workload": workload},
		}, true); err != nil {
			t.Fatalf("create route: %v", err)
		}
	}

	ids := func() []string {
		t.Helper()
		gws, err := s.GatewaysForRunner(ctx, runnerID)
		if err != nil {
			t.Fatalf("gateways for runner: %v", err)
		}
		out := make([]string, 0, len(gws))
		for _, g := range gws {
			out = append(out, g.ID)
		}
		return out
	}

	// Unlabelled: matches neither selector.
	if got := ids(); len(got) != 0 {
		t.Fatalf("gateways = %v, want none for an unlabelled runner", got)
	}

	if err := s.SetRunnerLabels(ctx, runnerID, map[string]string{"workload": "api", "zone": "eu"}); err != nil {
		t.Fatalf("labels: %v", err)
	}
	// Containment, not equality: extra labels the selector does not mention
	// must not stop a match, or every selector would have to name every label.
	if got := ids(); len(got) != 1 || got[0] != api.ID {
		t.Fatalf("gateways = %v, want only the api gateway", got)
	}

	// Relabelling moves the runner. This is the whole reason the list is
	// asked for repeatedly rather than read once at startup.
	if err := s.SetRunnerLabels(ctx, runnerID, map[string]string{"workload": "batch"}); err != nil {
		t.Fatalf("relabel: %v", err)
	}
	if got := ids(); len(got) != 1 || got[0] != batch.ID {
		t.Fatalf("after relabelling, gateways = %v, want only the batch gateway", got)
	}
}

func TestTrustedProxiesAreCarriedPerGateway(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	a, b := newGateway(t, s, "a"), newGateway(t, s, "b")

	if err := s.SetGatewayTrustedProxies(ctx, a.ID, []string{"10.0.0.0/8"}); err != nil {
		t.Fatalf("set: %v", err)
	}
	cfgA, err := s.BuildGatewayConfig(ctx, a.ID)
	if err != nil {
		t.Fatalf("build a: %v", err)
	}
	if len(cfgA.TrustedProxies) != 1 || cfgA.TrustedProxies[0] != "10.0.0.0/8" {
		t.Fatalf("gateway a trusts %v", cfgA.TrustedProxies)
	}
	cfgB, err := s.BuildGatewayConfig(ctx, b.ID)
	if err != nil {
		t.Fatalf("build b: %v", err)
	}
	if len(cfgB.TrustedProxies) != 0 {
		t.Fatal("a gateway inherited another's proxy trust; a spoofable forwarded header would defeat its allowlists")
	}
	if err := s.SetGatewayTrustedProxies(ctx, "00000000-0000-0000-0000-000000000000", nil); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("setting proxies on a missing gateway = %v, want ErrNotFound", err)
	}
}

// A built configuration must be byte-stable across passes. An unstable order
// would churn the config version and re-push identical policy forever.
func TestTheBuiltConfigIsStableAcrossPasses(t *testing.T) {
	s := open(t)
	ctx := t.Context()
	gw := newGateway(t, s, "g")
	for _, p := range []string{"/c", "/a", "/b"} {
		if _, _, err := s.CreateRoute(ctx, store.Route{Path: p, Flow: "f", AuthPrincipal: "x"}, true); err != nil {
			t.Fatalf("create %s: %v", p, err)
		}
	}
	first, err := s.BuildGatewayConfig(ctx, gw.ID)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	second, err := s.BuildGatewayConfig(ctx, gw.ID)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if string(a) != string(b) {
		t.Fatalf("two builds differ:\n%s\n%s", a, b)
	}
	if first.Routes[0].Path != "/a" {
		t.Fatalf("routes are not ordered: %s", a)
	}
}

// The twin of TestTheGoldenConfigParsesIntoTheGatewaysModel in
// gateway/internal/config. The hub's document type is a MIRROR of the
// gateway's — separate modules, and the gateway's has no dependencies — so both
// sides parse one fixture and assert the same values. A renamed field would
// otherwise simply vanish on unmarshal, with no error anywhere, and the policy
// it carried would quietly stop being applied.
func TestTheBuiltConfigMatchesTheGatewaysModel(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "gateway-config.golden.json"))
	if err != nil {
		t.Fatalf("golden fixture: %v", err)
	}
	var c store.GatewayConfig
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("the gateway's document does not fit the hub's model: %v", err)
	}
	if c.Version != 7 || len(c.Routes) != 2 || len(c.Runners) != 2 {
		t.Fatalf("golden did not round-trip: %+v", c)
	}
	if c.Routes[0].AuthPrincipal != "acme-erp" || c.Routes[0].Selector["workload"] != "api" {
		t.Fatalf("route 0 = %+v", c.Routes[0])
	}
	if c.Runners[0].Labels["workload"] != "api" {
		t.Fatalf("roster labels = %v", c.Runners[0].Labels)
	}
	if len(c.TrustedProxies) != 1 {
		t.Fatalf("trusted_proxies = %v", c.TrustedProxies)
	}

	// And re-marshalling must reproduce the fixture. This is the half that
	// catches a field the HUB adds and the gateway would silently drop.
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var want, got any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	wantJSON, _ := json.Marshal(want)
	gotJSON, _ := json.Marshal(got)
	if string(wantJSON) != string(gotJSON) {
		t.Fatalf("the hub does not reproduce the golden document\n want %s\n  got %s", wantJSON, gotJSON)
	}
}
