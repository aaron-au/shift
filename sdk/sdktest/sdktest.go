// Package sdktest runs connectors in-process over a real unix socket, so
// connector actions and the wire protocol are exercised without spawning a
// subprocess. Production spawning is covered by host.Launch integration
// tests.
package sdktest

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aaron-au/shift/sdk"
	"github.com/aaron-au/shift/sdk/host"
)

// TestToken is the fixed auth token sdktest servers use.
const TestToken = "sdktest-token"

// Serve starts c on a temp unix socket for the duration of the test and
// returns an attached host Process speaking the real wire protocol.
func Serve(t *testing.T, c sdk.Connector) *host.Process {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "conn.sock")
	errc := make(chan error, 1)
	go func() { errc <- sdk.ServeOn(socket, TestToken, c) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := host.Attach(ctx, socket, TestToken, 5*time.Second)
	if err != nil {
		t.Fatalf("sdktest: attach: %v", err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("sdktest: close: %v", err)
		}
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("sdktest: serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("sdktest: server did not stop")
		}
	})
	return p
}
