package ingress_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aaron-au/shift/gateway/internal/config"
	"github.com/aaron-au/shift/gateway/internal/ingress"
	"github.com/aaron-au/shift/gateway/internal/runners"
)

// Regression: callers intermittently received a correct status with an EMPTY
// or truncated body.
//
// A runner's response Body is a live stream off its own HTTP request, not a
// buffer. The registry used to close the exchange in Dispatch's defer — which
// runs when Dispatch RETURNS, before the ingress handler has copied the body.
// Closing it let Deliver return, which let the runner's handler return, which
// closed the body mid-copy. Ordering, not corruption: the status was always
// right, so nothing downstream looked broken.
//
// Caught by a flaky end-to-end test under `-count`, and only visible at all
// because the body was big enough for the copy to lose the race.
//
// A large body is therefore load-bearing here: with a few bytes the copy
// usually wins and the bug hides.
func TestResponseBodyIsNotTruncated(t *testing.T) {
	const size = 4 << 20 // 4 MiB: big enough that the copy cannot win by luck
	payload := bytes.Repeat([]byte("abcdefgh"), size/8)

	reg := runners.New()
	h := ingress.New(reg, nil)
	if err := h.SetConfig(&config.Config{Version: 1, Routes: []config.Route{
		{Path: "/big", Method: http.MethodPost, Flow: "big"},
	}}); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	ingress.NewDispatch(reg, nil, "").Routes(mux)
	ctrl := httptest.NewServer(mux)
	defer ctrl.Close()
	public := httptest.NewServer(h)
	defer public.Close()

	// A runner that answers over real HTTP, so the response body really is a
	// live request stream — an in-process io.Reader would not reproduce this.
	runnerErr := make(chan error, 1)
	go func() {
		runnerErr <- func() error {
			resp, err := post(t, ctrl.URL+"/api/v1/gw/poll", strings.NewReader(`{"wait_seconds":5}`))
			if err != nil {
				return err
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				return errf("poll status %d", resp.StatusCode)
			}
			id := resp.Header.Get("X-Shift-Request-Id")
			_, _ = io.Copy(io.Discard, resp.Body)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
				ctrl.URL+"/api/v1/gw/deliver/"+id, bytes.NewReader(payload))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", "application/octet-stream")
			dresp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer func() { _ = dresp.Body.Close() }()
			return nil
		}()
	}()

	waitParked(t, reg)

	resp, err := post(t, public.URL+"/big", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response: %v", err)
	}

	select {
	case err := <-runnerErr:
		if err != nil {
			t.Fatalf("runner: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not finish")
	}

	if len(got) != len(payload) {
		t.Fatalf("body was %d bytes, want %d — the runner's request closed mid-copy",
			len(got), len(payload))
	}
	if !bytes.Equal(got, payload) {
		t.Error("body content differs from what the runner sent")
	}
}
