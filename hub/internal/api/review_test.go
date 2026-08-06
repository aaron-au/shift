package api_test

import (
	"net/http"
	"testing"
)

// Design-time review over the wire (ADR-0042 §7). The studio renders these, so
// the shape and the placement — on the deploy and publish responses, not behind
// an extra call nobody makes — are the contract.

const asyncFlow = `{
	"name": "review-async",
	"source": {"connector": "@webhook", "action": "ndjson"},
	"sink": {"connector": "@discard"}
}`

type noticeBody struct {
	Notices []struct {
		Code     string `json:"code"`
		Severity string `json:"severity"`
		Title    string `json:"title"`
		Detail   string `json:"detail"`
		Step     string `json:"step"`
	} `json:"notices"`
}

func (b noticeBody) has(code string) bool {
	for _, n := range b.Notices {
		if n.Code == code {
			return true
		}
	}
	return false
}

// The developer's own request: they should learn at DEPLOY that this endpoint
// will answer 202 rather than the flow's output.
func TestDeployReturnsDesignNotices(t *testing.T) {
	srv := newServer(t)
	var got struct {
		Version int `json:"version"`
		noticeBody
	}
	if code := call(t, http.MethodPut, srv.URL+"/api/v1/flows/review-async", adminToken, asyncFlow, &got); code != http.StatusCreated {
		t.Fatalf("deploy = %d", code)
	}
	if got.Version < 1 {
		t.Fatalf("version = %d; the notices must not have displaced the deploy result", got.Version)
	}
	if !got.has("async-response.asynchronous") {
		t.Errorf("deploy notices = %+v; the async behaviour was not reported", got.Notices)
	}
	if !got.has("unverified-input.no-schema") {
		t.Errorf("deploy notices = %+v; the unverified input was not reported", got.Notices)
	}
}

// A flow that reviews with warnings must still deploy AND still publish.
// The moment an advisory can block, it has become validation in the wrong file.
func TestNoticesNeverBlockDeployOrPublish(t *testing.T) {
	srv := newServer(t)
	var deployed struct {
		Version int `json:"version"`
	}
	if code := call(t, http.MethodPut, srv.URL+"/api/v1/flows/review-async", adminToken, asyncFlow, &deployed); code != http.StatusCreated {
		t.Fatalf("a flow with warnings was refused at deploy: %d", code)
	}
	var published struct {
		PublishedVersion int `json:"published_version"`
		noticeBody
	}
	url := srv.URL + "/api/v1/flows/review-async/versions/1/publish"
	if code := call(t, http.MethodPost, url, adminToken, "", &published); code != http.StatusOK {
		t.Fatalf("a flow with warnings was refused at publish: %d", code)
	}
	if published.PublishedVersion != 1 {
		t.Errorf("published_version = %d", published.PublishedVersion)
	}
	// Deploy and publish are often days apart and often different people, so
	// the notices are repeated rather than shown once.
	if !published.has("async-response.asynchronous") {
		t.Errorf("publish notices = %+v; the last cheap moment to change your mind said nothing", published.Notices)
	}
}

// The canvas calls this while the author is still editing, so it must review a
// document that has never been stored.
func TestReviewOfAnUnsavedDocument(t *testing.T) {
	srv := newServer(t)
	var got noticeBody
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/flows/review", adminToken, asyncFlow, &got); code != http.StatusOK {
		t.Fatalf("review = %d", code)
	}
	if !got.has("async-response.asynchronous") {
		t.Errorf("notices = %+v", got.Notices)
	}
	for _, n := range got.Notices {
		if n.Title == "" || n.Severity == "" {
			t.Errorf("notice %q is missing what the studio renders: %+v", n.Code, n)
		}
	}
}

// Review is not a softer place to learn a document is broken — validation
// still reports that, with the same code the studio already handles.
func TestReviewRefusesAnInvalidDocument(t *testing.T) {
	srv := newServer(t)
	var errBody struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	bad := `{"name": "broken", "source": {"connector": "@webhook", "action": "ndjson"}}`
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/flows/review", adminToken, bad, &errBody); code != http.StatusUnprocessableEntity {
		t.Fatalf("review of an invalid document = %d, want 422", code)
	}
	if errBody.Error.Code != "flow_invalid" {
		t.Errorf("code = %q, want flow_invalid — the studio keys its error rendering off it", errBody.Error.Code)
	}
}

// Opening a flow you did not just author must still surface its notices.
func TestReviewOfAStoredFlow(t *testing.T) {
	srv := newServer(t)
	if code := call(t, http.MethodPut, srv.URL+"/api/v1/flows/review-async", adminToken, asyncFlow, nil); code != http.StatusCreated {
		t.Fatal("deploy failed")
	}
	var got noticeBody
	if code := call(t, http.MethodGet, srv.URL+"/api/v1/flows/review-async/review", adminToken, "", &got); code != http.StatusOK {
		t.Fatalf("review = %d", code)
	}
	if !got.has("async-response.asynchronous") {
		t.Errorf("notices = %+v", got.Notices)
	}
	if code := call(t, http.MethodGet, srv.URL+"/api/v1/flows/nope/review", adminToken, "", nil); code != http.StatusNotFound {
		t.Errorf("review of an unknown flow = %d, want 404", code)
	}
}

// A synchronous flow gets a different answer, not a missing one — the studio
// badge says which of the two shapes was deployed.
func TestReviewReportsSynchronousFlowsToo(t *testing.T) {
	srv := newServer(t)
	sync := `{
		"name": "review-sync",
		"source": {"connector": "@webhook", "action": "ndjson"},
		"sink": {"connector": "@response"}
	}`
	var got noticeBody
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/flows/review", adminToken, sync, &got); code != http.StatusOK {
		t.Fatalf("review = %d", code)
	}
	if !got.has("async-response.synchronous") {
		t.Errorf("notices = %+v", got.Notices)
	}
}

// The checks are served so a notice's provenance can be explained without the
// studio carrying a second copy of the rules — including a rule some other
// deployment registered.
func TestReviewChecksAreListed(t *testing.T) {
	srv := newServer(t)
	var got struct {
		Checks []struct {
			Code    string `json:"code"`
			Summary string `json:"summary"`
		} `json:"checks"`
	}
	if code := call(t, http.MethodGet, srv.URL+"/api/v1/review-checks", adminToken, "", &got); code != http.StatusOK {
		t.Fatalf("review-checks = %d", code)
	}
	if len(got.Checks) < 4 {
		t.Fatalf("checks = %+v, want the built-in set", got.Checks)
	}
	for _, c := range got.Checks {
		if c.Code == "" || c.Summary == "" {
			t.Errorf("check %+v cannot explain itself", c)
		}
	}
}

// Review reads flow documents, which are tenant data.
func TestReviewEndpointsRequireAdmin(t *testing.T) {
	srv := newServer(t)
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/api/v1/flows/review", asyncFlow},
		{http.MethodGet, "/api/v1/flows/review-async/review", ""},
		{http.MethodGet, "/api/v1/review-checks", ""},
	} {
		if code := call(t, tc.method, srv.URL+tc.path, "", tc.body, nil); code != http.StatusUnauthorized {
			t.Errorf("%s %s unauthenticated = %d, want 401", tc.method, tc.path, code)
		}
	}
}

// A specific version is reviewable, and the default is the LATEST draft rather
// than the published one — reviewing a draft is how you decide whether to
// publish it, so defaulting to "published" would make the endpoint useless on
// exactly the flow being looked at.
func TestReviewAddressesVersionsExplicitly(t *testing.T) {
	srv := newServer(t)
	name := "review-versions"
	async := `{"name":"review-versions","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@discard"}}`
	sync := `{"name":"review-versions","source":{"connector":"@webhook","action":"ndjson"},
		"sink":{"connector":"@response"}}`
	if code := call(t, http.MethodPut, srv.URL+"/api/v1/flows/"+name, adminToken, async, nil); code != http.StatusCreated {
		t.Fatal("deploy v1 failed")
	}
	if code := call(t, http.MethodPut, srv.URL+"/api/v1/flows/"+name, adminToken, sync, nil); code != http.StatusCreated {
		t.Fatal("deploy v2 failed")
	}
	// v1 was never published; the default must still review v2, the latest.
	var latest noticeBody
	if code := call(t, http.MethodGet, srv.URL+"/api/v1/flows/"+name+"/review", adminToken, "", &latest); code != http.StatusOK {
		t.Fatalf("review = %d", code)
	}
	if !latest.has("async-response.synchronous") {
		t.Errorf("default review = %+v; it did not review the latest draft", latest.Notices)
	}
	var v1 noticeBody
	if code := call(t, http.MethodGet, srv.URL+"/api/v1/flows/"+name+"/review?version=1", adminToken, "", &v1); code != http.StatusOK {
		t.Fatalf("review of v1 = %d", code)
	}
	if !v1.has("async-response.asynchronous") {
		t.Errorf("v1 review = %+v; an explicit version must address that version", v1.Notices)
	}
	if code := call(t, http.MethodGet, srv.URL+"/api/v1/flows/"+name+"/review?version=99", adminToken, "", nil); code != http.StatusNotFound {
		t.Errorf("review of a version that does not exist = %d, want 404", code)
	}
}

// Fire-and-forget is a distinct deploy-time answer (ADR-0042 §3d), not the
// absence of one.
func TestReviewReportsFireAndForget(t *testing.T) {
	srv := newServer(t)
	doc := `{"name":"events","source":{"connector":"@webhook","action":"ndjson","ack":"none"},
		"sink":{"connector":"@discard"}}`
	var got noticeBody
	if code := call(t, http.MethodPost, srv.URL+"/api/v1/flows/review", adminToken, doc, &got); code != http.StatusOK {
		t.Fatalf("review = %d", code)
	}
	if !got.has("async-response.fire-and-forget") {
		t.Errorf("notices = %+v", got.Notices)
	}
}
