package api_test

import (
	"net/http"
	"testing"
)

// Routes are ingress policy, administered at the hub (ADR-0038 §6). These
// assert what an operator can and cannot get wrong with one.

func TestCreatingARouteValidatesTheInput(t *testing.T) {
	srv := newServer(t)

	cases := []struct {
		name, body string
		want       int
	}{
		{"valid", `{"path":"/orders","method":"post","flow":"f","auth_principal":"acme"}`, http.StatusCreated},
		{"path without a slash", `{"path":"orders","flow":"f","auth_principal":"acme"}`, http.StatusUnprocessableEntity},
		{"no flow", `{"path":"/x","auth_principal":"acme"}`, http.StatusUnprocessableEntity},
		{"not an HTTP method", `{"path":"/x","method":"YEET","flow":"f","auth_principal":"acme"}`, http.StatusUnprocessableEntity},
		{"bad CIDR", `{"path":"/x","flow":"f","auth_principal":"acme","allow_cidrs":["nope"]}`, http.StatusUnprocessableEntity},
		{"negative body cap", `{"path":"/x","flow":"f","auth_principal":"acme","max_body_bytes":-1}`, http.StatusUnprocessableEntity},
		// An authenticated route with no principal authenticates somebody and
		// then cannot say who, which makes every downstream record anonymous.
		{"no principal and not public", `{"path":"/x","flow":"f"}`, http.StatusUnprocessableEntity},
		{"deliberately public", `{"path":"/health","flow":"f","public":true}`, http.StatusCreated},
		{"not json", `{`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := call(t, http.MethodPost, srv.URL+"/api/v1/routes", adminToken, tc.body, nil); got != tc.want {
				t.Fatalf("status = %d, want %d", got, tc.want)
			}
		})
	}
}

// The caller token is served exactly once. Anywhere else would be a standing
// way to read a live credential out of the API.
func TestARouteTokenIsReturnedOnceAndNeverAgain(t *testing.T) {
	srv := newServer(t)

	var created map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/routes", adminToken,
		`{"path":"/orders","method":"POST","flow":"orders-in","auth_principal":"acme"}`, &created); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	token, _ := created["token"].(string)
	if token == "" {
		t.Fatal("no caller token was returned, so nobody could call the route")
	}
	// The method is normalised, not echoed back as typed.
	if created["method"] != "POST" {
		t.Fatalf("method = %v, want POST", created["method"])
	}
	hash, _ := created["auth_token_sha256"].(string)
	if hash == token {
		t.Fatal("the plaintext token was stored")
	}
	id, _ := created["id"].(string)

	var list []map[string]any
	if got := call(t, http.MethodGet, srv.URL+"/api/v1/routes", adminToken, "", &list); got != http.StatusOK {
		t.Fatalf("list = %d", got)
	}
	if len(list) != 1 {
		t.Fatalf("listed %d routes, want 1", len(list))
	}
	if _, ok := list[0]["token"]; ok {
		t.Fatal("the caller token is served by list; it must be returned once and only once")
	}

	// Rotation gives a new one, once.
	var rotated map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/routes/"+id+"/rotate-token", adminToken, "", &rotated); got != http.StatusOK {
		t.Fatalf("rotate = %d", got)
	}
	if newToken, _ := rotated["token"].(string); newToken == "" || newToken == token {
		t.Fatal("rotation did not mint a new token")
	}

	if got := call(t, http.MethodDelete, srv.URL+"/api/v1/routes/"+id, adminToken, "", nil); got != http.StatusNoContent {
		t.Fatalf("delete = %d", got)
	}
	if got := call(t, http.MethodDelete, srv.URL+"/api/v1/routes/"+id, adminToken, "", nil); got != http.StatusNotFound {
		t.Fatalf("second delete = %d, want 404", got)
	}
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/routes/"+id+"/rotate-token", adminToken, "", nil); got != http.StatusNotFound {
		t.Fatalf("rotate a deleted route = %d, want 404", got)
	}
}

// Every change to what a gateway should serve has to raise the generation, or
// gateways keep serving policy the hub no longer believes in and nothing
// notices — the drift the reconcile loop watches for IS this difference.
func TestEveryConfigChangeRaisesTheGeneration(t *testing.T) {
	srv := newServer(t)

	var gw map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"https://gw.example"}`, &gw); got != http.StatusCreated {
		t.Fatalf("create gateway = %d", got)
	}
	gwID, _ := gw["id"].(string)

	version := func() float64 {
		t.Helper()
		var got map[string]any
		if code := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/"+gwID, adminToken, "", &got); code != http.StatusOK {
			t.Fatalf("get = %d", code)
		}
		v, _ := got["config_version"].(float64)
		return v
	}
	start := version()

	var route map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/routes", adminToken,
		`{"path":"/orders","flow":"f","auth_principal":"acme"}`, &route); got != http.StatusCreated {
		t.Fatalf("create route = %d", got)
	}
	afterCreate := version()
	if afterCreate <= start {
		t.Fatal("creating a route did not raise the generation, so no gateway would ever learn about it")
	}
	routeID, _ := route["id"].(string)

	if got := call(t, http.MethodPost, srv.URL+"/api/v1/routes/"+routeID+"/rotate-token", adminToken, "", nil); got != http.StatusOK {
		t.Fatalf("rotate = %d", got)
	}
	afterRotate := version()
	if afterRotate <= afterCreate {
		t.Fatal("rotating a credential did not raise the generation, so the old token would keep working")
	}

	if got := call(t, http.MethodPut, srv.URL+"/api/v1/gateways/"+gwID+"/trusted-proxies", adminToken,
		`{"trusted_proxies":["10.0.0.0/8"]}`, nil); got != http.StatusNoContent {
		t.Fatalf("trusted proxies = %d", got)
	}
	afterProxies := version()
	if afterProxies <= afterRotate {
		t.Fatal("changing proxy trust did not raise the generation")
	}

	if got := call(t, http.MethodDelete, srv.URL+"/api/v1/routes/"+routeID, adminToken, "", nil); got != http.StatusNoContent {
		t.Fatalf("delete = %d", got)
	}
	if version() <= afterProxies {
		t.Fatal("deleting a route did not raise the generation, so the endpoint would stay up")
	}
}

// Labels decide placement, so they are hub-asserted (ADR-0041 §3) and changing
// them changes every gateway's view of the fleet.
func TestSettingRunnerLabelsRaisesTheGeneration(t *testing.T) {
	srv := newServer(t)

	var gw map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"https://gw.example"}`, &gw); got != http.StatusCreated {
		t.Fatalf("create gateway = %d", got)
	}
	gwID, _ := gw["id"].(string)

	var tok struct{ Token string }
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{}`, &tok); code != http.StatusCreated {
		t.Fatalf("runner token = %d", code)
	}
	var reg struct {
		RunnerID string `json:"runner_id"`
		Secret   string `json:"secret"`
	}
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"r1"}`, &reg); code != http.StatusCreated {
		t.Fatalf("register = %d", code)
	}

	var before map[string]any
	call(t, http.MethodGet, srv.URL+"/api/v1/gateways/"+gwID, adminToken, "", &before)
	v0, _ := before["config_version"].(float64)

	if got := call(t, http.MethodPut, srv.URL+"/api/v1/runners/"+reg.RunnerID+"/labels", adminToken,
		`{"labels":{"workload":"api"}}`, nil); got != http.StatusNoContent {
		t.Fatalf("set labels = %d", got)
	}
	var after map[string]any
	call(t, http.MethodGet, srv.URL+"/api/v1/gateways/"+gwID, adminToken, "", &after)
	if v1, _ := after["config_version"].(float64); v1 <= v0 {
		t.Fatal("changing runner labels did not raise the generation; placement would be stale everywhere")
	}

	// A runner cannot label ITSELF — that is the whole point of asserting
	// them at the hub.
	if got := call(t, http.MethodPut, srv.URL+"/api/v1/runners/"+reg.RunnerID+"/labels", reg.Secret,
		`{"labels":{"workload":"privileged"}}`, nil); got != http.StatusUnauthorized {
		t.Fatalf("runner self-labelling = %d, want 401", got)
	}

	if got := call(t, http.MethodPut, srv.URL+"/api/v1/runners/00000000-0000-0000-0000-000000000000/labels",
		adminToken, `{"labels":{}}`, nil); got != http.StatusNotFound {
		t.Fatalf("labelling a missing runner = %d, want 404", got)
	}
}

func TestTrustedProxiesValidateTheirCIDRs(t *testing.T) {
	srv := newServer(t)
	var gw map[string]any
	if got := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"https://gw.example"}`, &gw); got != http.StatusCreated {
		t.Fatalf("create = %d", got)
	}
	gwID, _ := gw["id"].(string)

	// A bad CIDR here would silently believe nobody, which reads as "the
	// allowlist is broken" much later and much less obviously.
	if got := call(t, http.MethodPut, srv.URL+"/api/v1/gateways/"+gwID+"/trusted-proxies", adminToken,
		`{"trusted_proxies":["10.0.0.0/8","garbage"]}`, nil); got != http.StatusUnprocessableEntity {
		t.Fatalf("bad CIDR = %d, want 422", got)
	}
	if got := call(t, http.MethodPut, srv.URL+"/api/v1/gateways/"+gwID+"/trusted-proxies", adminToken,
		`{`, nil); got != http.StatusBadRequest {
		t.Fatalf("malformed body = %d, want 400", got)
	}
	if got := call(t, http.MethodPut, srv.URL+"/api/v1/gateways/00000000-0000-0000-0000-000000000000/trusted-proxies",
		adminToken, `{"trusted_proxies":[]}`, nil); got != http.StatusNotFound {
		t.Fatalf("missing gateway = %d, want 404", got)
	}
}

// The address list is answered for the identity the credential proves. An
// admin token has no runner identity, so there is no list to serve it — and a
// runner asking gets its own, never one it named.
func TestARunnerAsksForItsOwnGatewayList(t *testing.T) {
	srv := newServer(t)

	var tok struct{ Token string }
	call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{}`, &tok)
	var reg struct {
		RunnerID string `json:"runner_id"`
		Secret   string `json:"secret"`
	}
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"r1"}`, &reg); code != http.StatusCreated {
		t.Fatalf("register = %d", code)
	}

	gateways := func() []map[string]any {
		t.Helper()
		var out struct {
			Gateways []map[string]any `json:"gateways"`
		}
		if code := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/sync", reg.Secret, "", &out); code != http.StatusOK {
			t.Fatalf("sync = %d", code)
		}
		return out.Gateways
	}

	// Nothing published yet: an empty list, not an error. A runner with no
	// inbound work is the default deployment (ADR-0038).
	if got := gateways(); len(got) != 0 {
		t.Fatalf("gateways = %v, want none", got)
	}

	// A gateway that has not been adopted cannot verify this runner, so it is
	// still not somewhere to poll even once a route exists.
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/gateways", adminToken,
		`{"name":"dmz","url":"https://gw.example"}`, nil); code != http.StatusCreated {
		t.Fatalf("create gateway = %d", code)
	}
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/routes", adminToken,
		`{"path":"/orders","flow":"f","auth_principal":"acme"}`, nil); code != http.StatusCreated {
		t.Fatalf("create route = %d", code)
	}
	if got := gateways(); len(got) != 0 {
		t.Fatalf("gateways = %v, want none while the gateway is unadopted", got)
	}

	// The admin realm has no runner identity behind it.
	if code := call(t, http.MethodGet, srv.URL+"/api/v1/gateways/sync", adminToken, "", nil); code != http.StatusUnauthorized {
		t.Fatalf("sync as an admin = %d, want 401", code)
	}
}

// Routes are admin-realm. A runner credential must not reach them: a
// compromised runner that could mint a route would have given itself a public
// path into any flow.
func TestARunnerCannotAdministerRoutes(t *testing.T) {
	srv := newServer(t)

	var tok struct{ Token string }
	call(t, http.MethodPost, srv.URL+"/api/v1/runner-tokens", adminToken, `{}`, &tok)
	var reg struct {
		Secret string `json:"secret"`
	}
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/runners/register", "",
		`{"token":"`+tok.Token+`","name":"r1"}`, &reg); code != http.StatusCreated {
		t.Fatalf("register = %d", code)
	}

	if got := call(t, http.MethodPost, srv.URL+"/api/v1/routes", reg.Secret,
		`{"path":"/mine","flow":"f","auth_principal":"x"}`, nil); got != http.StatusUnauthorized {
		t.Fatalf("create as a runner = %d, want 401", got)
	}
	if got := call(t, http.MethodGet, srv.URL+"/api/v1/routes", reg.Secret, "", nil); got != http.StatusUnauthorized {
		t.Fatalf("list as a runner = %d, want 401", got)
	}

	var list []map[string]any
	call(t, http.MethodGet, srv.URL+"/api/v1/routes", adminToken, "", &list)
	if len(list) != 0 {
		t.Fatal("a runner created a route")
	}
}
