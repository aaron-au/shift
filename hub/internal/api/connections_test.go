package api_test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// Connections (ADR-0034): reusable connector config. These tests exercise
// the whole admin surface plus the two guarantees the feature rests on —
// a deploy cannot reference a connection that does not exist or does not
// agree with the node, and a connection in live use cannot be deleted.

const sftpConnection = `{"connector":"sftp","config":{"host":"sftp.example.com","port":22}}`

func putConnection(t *testing.T, srv *httptest.Server, name, body string) int {
	t.Helper()
	return call(t, "PUT", srv.URL+"/api/v1/connections/"+name, adminToken, body, nil)
}

func TestConnectionCRUD(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)

	if code := putConnection(t, srv, "prod-sftp", sftpConnection); code != 201 {
		t.Fatalf("put = %d, want 201", code)
	}

	var got struct {
		Name      string          `json:"name"`
		Connector string          `json:"connector"`
		Config    json.RawMessage `json:"config"`
		Version   int             `json:"version"`
	}
	if code := call(t, "GET", srv.URL+"/api/v1/connections/prod-sftp", adminToken, "", &got); code != 200 {
		t.Fatalf("get = %d, want 200", code)
	}
	if got.Connector != "sftp" || !strings.Contains(string(got.Config), "sftp.example.com") {
		t.Fatalf("get returned %+v, want the stored document", got)
	}
	if got.Version != 1 {
		t.Fatalf("version = %d, want 1 on first write", got.Version)
	}

	// Replace bumps the version — the audit trail for "who changed the
	// endpoint every flow points at".
	if code := putConnection(t, srv, "prod-sftp",
		`{"connector":"sftp","config":{"host":"sftp2.example.com"}}`); code != 201 {
		t.Fatalf("replace = %d, want 201", code)
	}
	if code := call(t, "GET", srv.URL+"/api/v1/connections/prod-sftp", adminToken, "", &got); code != 200 {
		t.Fatalf("get after replace = %d", code)
	}
	if got.Version != 2 || !strings.Contains(string(got.Config), "sftp2") {
		t.Fatalf("after replace = %+v, want version 2 and the new host", got)
	}

	var list struct {
		Connections []struct{ Name string } `json:"connections"`
	}
	if code := call(t, "GET", srv.URL+"/api/v1/connections", adminToken, "", &list); code != 200 {
		t.Fatalf("list = %d, want 200", code)
	}
	if len(list.Connections) != 1 || list.Connections[0].Name != "prod-sftp" {
		t.Fatalf("list = %+v, want [prod-sftp]", list.Connections)
	}

	if code := call(t, "DELETE", srv.URL+"/api/v1/connections/prod-sftp", adminToken, "", nil); code != 204 {
		t.Fatalf("delete = %d, want 204", code)
	}
	if code := call(t, "GET", srv.URL+"/api/v1/connections/prod-sftp", adminToken, "", nil); code != 404 {
		t.Fatalf("get after delete = %d, want 404", code)
	}
	if code := call(t, "DELETE", srv.URL+"/api/v1/connections/prod-sftp", adminToken, "", nil); code != 404 {
		t.Fatalf("delete of a missing connection = %d, want 404", code)
	}
}

func TestConnectionRejectsBadInput(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)

	for _, tc := range []struct {
		name, connName, body string
		want                 int
	}{
		{"name outside the charset", "has space", sftpConnection, 422},
		{"name starting with a separator", "-lead", sftpConnection, 422},
		{"connector missing", "c1", `{"config":{}}`, 422},
		{"connector is a built-in", "c1", `{"connector":"@discard","config":{}}`, 422},
		{"config is not an object", "c1", `{"connector":"sftp","config":[1,2]}`, 422},
		{"body is not JSON", "c1", `not json`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if code := putConnection(t, srv, tc.connName, tc.body); code != tc.want {
				t.Fatalf("put = %d, want %d", code, tc.want)
			}
		})
	}
}

func TestConnectionConfigSizeBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	// Connection config is a form's worth of settings, not payload; the
	// bound keeps the control plane from being used as a data store.
	big := `{"connector":"sftp","config":{"pad":"` + strings.Repeat("a", 70<<10) + `"}}`
	if code := putConnection(t, srv, "huge", big); code != 413 {
		t.Fatalf("put = %d, want 413", code)
	}
}

func TestConnectionRequiresAdmin(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	for _, tc := range []struct{ method, path string }{
		{"PUT", "/api/v1/connections/c1"},
		{"GET", "/api/v1/connections"},
		{"GET", "/api/v1/connections/c1"},
		{"DELETE", "/api/v1/connections/c1"},
	} {
		if code := call(t, tc.method, srv.URL+tc.path, "", sftpConnection, nil); code != 401 {
			t.Errorf("%s %s unauthenticated = %d, want 401", tc.method, tc.path, code)
		}
	}
	// A runner's bearer secret is not an admin credential: connections are
	// managed in the admin realm and only RESOLVED in the runner realm.
	runner := registerRunner(t, srv.URL, "conn-runner")
	if code := call(t, "GET", srv.URL+"/api/v1/connections", runner, "", nil); code == 200 {
		t.Error("a runner secret listed connections through the admin route")
	}
}

// The deploy-time guarantee: a flow cannot be stored naming a connection
// that does not exist. Catching it here beats a task failure at 3 a.m.
func TestDeployRejectsUnknownConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	doc := `{"name":"orders","source":{"connector":"gen","action":"gen","connection":"nope",
	  "config":{"records":1}},"sink":{"connector":"gen","action":"discard"}}`
	var body errEnvelope
	if code := call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, doc, &body); code != 422 {
		t.Fatalf("deploy = %d, want 422", code)
	}
	if !strings.Contains(body.Error.Message, "nope") {
		t.Fatalf("message %q does not name the missing connection", body.Error.Message)
	}
}

func TestDeployRejectsConnectorMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	if code := putConnection(t, srv, "prod-sftp", sftpConnection); code != 201 {
		t.Fatal("setup: put connection")
	}
	// The node says gen, the connection configures sftp: merging them
	// would hand a gen action a host it has no field for.
	doc := `{"name":"orders","source":{"connector":"gen","action":"gen","connection":"prod-sftp",
	  "config":{"records":1}},"sink":{"connector":"gen","action":"discard"}}`
	var body errEnvelope
	if code := call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, doc, &body); code != 422 {
		t.Fatalf("deploy = %d, want 422", code)
	}
	if !strings.Contains(body.Error.Message, "sftp") || !strings.Contains(body.Error.Message, "gen") {
		t.Fatalf("message %q should name both connectors", body.Error.Message)
	}
}

// ADR-0034 §3: a node may not restate what its connection supplies. The
// silent-override this prevents is one node pointing at a different host
// than its siblings, surfacing later as a network fault.
func TestDeployRejectsNodeOverridingConnectionKey(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	if code := putConnection(t, srv, "gen-conn",
		`{"connector":"gen","config":{"records":10}}`); code != 201 {
		t.Fatal("setup: put connection")
	}
	doc := `{"name":"orders","source":{"connector":"gen","action":"gen","connection":"gen-conn",
	  "config":{"records":99}},"sink":{"connector":"gen","action":"discard"}}`
	var body errEnvelope
	if code := call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, doc, &body); code != 422 {
		t.Fatalf("deploy = %d, want 422", code)
	}
	if !strings.Contains(body.Error.Message, "records") {
		t.Fatalf("message %q does not name the colliding key", body.Error.Message)
	}
}

func TestDeployAcceptsValidConnectionReference(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	if code := putConnection(t, srv, "gen-conn",
		`{"connector":"gen","config":{"records":10}}`); code != 201 {
		t.Fatal("setup: put connection")
	}
	doc := `{"name":"orders","source":{"connector":"gen","action":"gen","connection":"gen-conn"},
	  "sink":{"connector":"gen","action":"discard"}}`
	if code := call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, doc, nil); code != 201 {
		t.Fatalf("deploy = %d, want 201", code)
	}
}

// ADR-0034 open question 1, resolved as refuse: deleting a connection a
// published flow still uses would trade one clear error now for an
// opaque failure at the next run.
func TestDeleteRefusedWhileAPublishedFlowUsesIt(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	if code := putConnection(t, srv, "gen-conn",
		`{"connector":"gen","config":{"records":10}}`); code != 201 {
		t.Fatal("setup: put connection")
	}
	doc := `{"name":"orders","source":{"connector":"gen","action":"gen","connection":"gen-conn"},
	  "sink":{"connector":"gen","action":"discard"}}`
	if code := call(t, "PUT", srv.URL+"/api/v1/flows/orders", adminToken, doc, nil); code != 201 {
		t.Fatal("setup: deploy")
	}

	// Deployed but not published: nothing can dispatch it, so it must not
	// hold the connection hostage.
	if code := call(t, "DELETE", srv.URL+"/api/v1/connections/gen-conn", adminToken, "", nil); code != 204 {
		t.Fatalf("delete with only an unpublished reference = %d, want 204", code)
	}

	// Restore, publish, and the refusal takes effect.
	if code := putConnection(t, srv, "gen-conn",
		`{"connector":"gen","config":{"records":10}}`); code != 201 {
		t.Fatal("setup: re-put connection")
	}
	if code := call(t, "POST", srv.URL+"/api/v1/flows/orders/versions/1/publish", adminToken, "", nil); code != 200 {
		t.Fatal("setup: publish")
	}
	var body errEnvelope
	if code := call(t, "DELETE", srv.URL+"/api/v1/connections/gen-conn", adminToken, "", &body); code != 409 {
		t.Fatalf("delete of an in-use connection = %d, want 409", code)
	}
	if body.Error.Code != "connection_in_use" || !strings.Contains(body.Error.Message, "orders") {
		t.Fatalf("409 body = %+v, want the machine code and the flow name", body)
	}
}

// The runner realm's fetch path (ADR-0034 §4): documents only, no
// decryption, and the connection travels with its secret REFERENCES
// intact so plaintext never leaves the hub.
func TestResolveConnectionsIsRunnerRealm(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := newServer(t)
	if code := putConnection(t, srv, "prod-sftp", sftpConnection); code != 201 {
		t.Fatal("setup: put connection")
	}
	secret := registerRunner(t, srv.URL, "conn-runner")

	var out struct {
		Connections map[string]struct {
			Connector string          `json:"connector"`
			Config    json.RawMessage `json:"config"`
		} `json:"connections"`
	}
	if code := call(t, "POST", srv.URL+"/api/v1/connections/resolve", secret,
		`{"names":["prod-sftp"]}`, &out); code != 200 {
		t.Fatalf("resolve = %d, want 200", code)
	}
	c, ok := out.Connections["prod-sftp"]
	if !ok || c.Connector != "sftp" || !strings.Contains(string(c.Config), "sftp.example.com") {
		t.Fatalf("resolve returned %+v, want the stored document", out.Connections)
	}

	// An unknown name is an error, not a silent omission: a runner that
	// merged an empty config would connect somewhere unintended.
	if code := call(t, "POST", srv.URL+"/api/v1/connections/resolve", secret,
		`{"names":["prod-sftp","ghost"]}`, nil); code != 404 {
		t.Fatalf("resolve with an unknown name = %d, want 404", code)
	}

	for _, body := range []string{`{"names":[]}`, `{"names":` + manyNames(101) + `}`} {
		if code := call(t, "POST", srv.URL+"/api/v1/connections/resolve", secret, body, nil); code != 400 {
			t.Errorf("resolve %s = %d, want 400", body[:20], code)
		}
	}

	if code := call(t, "POST", srv.URL+"/api/v1/connections/resolve", "",
		`{"names":["prod-sftp"]}`, nil); code != 401 {
		t.Error("unauthenticated resolve was not rejected")
	}
}

// secretsServer is a hub with a KEK configured, so the secrets endpoints
// (and therefore the task-config bootstrap) exist.
func secretsServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv, _, _ := newOIDCServer(t)
	return srv
}

// errEnvelope is the hub's stable error shape (ADR-0023).
type errEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func manyNames(n int) string {
	names := make([]string, n)
	for i := range names {
		names[i] = "c"
	}
	b, _ := json.Marshal(names)
	return string(b)
}

// The one-round-trip bootstrap (ADR-0035 §3). The runner cannot name the
// secrets a connection references until it has the connection, so asking
// separately would be two sequential calls; the hub folds them because it
// holds both.
func TestResolveTaskConfigReturnsConnectionsAndTheirSecrets(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := secretsServer(t)
	if code := call(t, "PUT", srv.URL+"/api/v1/secrets/sftp-pw", adminToken,
		`{"value":"hunter2"}`, nil); code != 201 {
		t.Fatal("setup: put secret")
	}
	if code := putConnection(t, srv, "prod-sftp",
		`{"connector":"sftp","config":{"host":"h","password":{"$secret":"sftp-pw"}}}`); code != 201 {
		t.Fatal("setup: put connection")
	}
	secret := registerRunner(t, srv.URL, "tc-runner")

	var out struct {
		Connections map[string]struct {
			Connector string          `json:"connector"`
			Config    json.RawMessage `json:"config"`
		} `json:"connections"`
		Secrets map[string]string `json:"secrets"`
	}
	if code := call(t, "POST", srv.URL+"/api/v1/task-config/resolve", secret,
		`{"connections":["prod-sftp"],"secrets":[]}`, &out); code != 200 {
		t.Fatalf("resolve = %d, want 200", code)
	}
	// The connection travels with its reference INTACT — substitution is
	// runner-side, so this adds no new place for plaintext to sit.
	if !strings.Contains(string(out.Connections["prod-sftp"].Config), "$secret") {
		t.Fatalf("connection config = %s, want the reference left in place",
			out.Connections["prod-sftp"].Config)
	}
	// ...but its secret comes back in the same response, unasked for.
	if out.Secrets["sftp-pw"] != "hunter2" {
		t.Fatalf("secrets = %v, want the connection's own reference resolved", out.Secrets)
	}
}

func TestResolveTaskConfigRejectsUnknownNames(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := secretsServer(t)
	secret := registerRunner(t, srv.URL, "tc-runner2")
	if code := call(t, "POST", srv.URL+"/api/v1/task-config/resolve", secret,
		`{"connections":["ghost"],"secrets":[]}`, nil); code != 404 {
		t.Errorf("unknown connection = %d, want 404", code)
	}
	if code := call(t, "POST", srv.URL+"/api/v1/task-config/resolve", secret,
		`{"connections":[],"secrets":["ghost"]}`, nil); code != 404 {
		t.Errorf("unknown secret = %d, want 404", code)
	}
	if code := call(t, "POST", srv.URL+"/api/v1/task-config/resolve", "",
		`{"connections":[],"secrets":[]}`, nil); code != 401 {
		t.Error("unauthenticated resolve was not rejected")
	}
}

// An empty request is legitimate — a flow with neither — and must not error.
func TestResolveTaskConfigEmptyRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("needs postgres")
	}
	srv := secretsServer(t)
	secret := registerRunner(t, srv.URL, "tc-runner3")
	if code := call(t, "POST", srv.URL+"/api/v1/task-config/resolve", secret,
		`{"connections":[],"secrets":[]}`, nil); code != 200 {
		t.Fatalf("empty resolve = %d, want 200", code)
	}
}
