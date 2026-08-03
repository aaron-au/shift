package api_test

import (
	"net/http"
	"testing"
)

// TestSecurityHeadersOnEveryResponse: the hardening headers must be present on
// the dashboard, on JSON responses, and on rejections alike — an unauthorized
// response is still a response a browser will render.
func TestSecurityHeadersOnEveryResponse(t *testing.T) {
	srv := newServer(t)
	for _, tc := range []struct {
		name, method, path, token string
	}{
		{"dashboard", http.MethodGet, "/", ""},
		{"json api", http.MethodGet, "/api/v1/flows", adminToken},
		{"unauthorized", http.MethodGet, "/api/v1/flows", ""},
		{"not found", http.MethodGet, "/nope", adminToken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), tc.method, srv.URL+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()

			for k, want := range map[string]string{
				"X-Content-Type-Options": "nosniff",
				"X-Frame-Options":        "DENY",
				"Referrer-Policy":        "no-referrer",
			} {
				if got := resp.Header.Get(k); got != want {
					t.Errorf("%s = %q, want %q", k, got, want)
				}
			}
			if csp := resp.Header.Get("Content-Security-Policy"); csp == "" {
				t.Error("Content-Security-Policy missing")
			}
		})
	}
}
