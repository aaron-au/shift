package host

import (
	"context"
	"os"
	"testing"
	"time"
)

// Connector cold start used to cost ~1.02 SECONDS, and the figure was
// suspiciously constant across runs — the tell that it was a fixed delay,
// not work.
//
// It was: grpc.NewClient dials lazily, so the first Handshake triggered the
// connection. The child had not bound its socket yet, that dial failed, and
// gRPC's default reconnect BaseDelay is one second. Every launch sat out a
// full second of backoff before a handshake that itself takes microseconds.
//
// Launch now waits for the socket to appear before the first RPC, and the
// backoff is configured for a local UDS rather than a WAN. Cold start on the
// machine this was written on went 1,025ms -> ~6ms.
//
// The bound below is deliberately far above the observed figure (CI is
// slower and shares cores) but far below the old one, so it fails loudly if
// the backoff is ever reintroduced — which is exactly the kind of regression
// nothing else would catch, because everything still WORKS, just slowly.
func TestColdStartIsNotGRPCBackoff(t *testing.T) {
	t.Setenv("SHIFT_HOST_TEST_MODE", "serve")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The first launch also pays page-cache warming for the binary; measure
	// the steady state, which is what a runner actually experiences.
	best := time.Hour
	for range 3 {
		start := time.Now()
		p, err := Launch(ctx, os.Args[0], LaunchOptions{HandshakeTimeout: 15 * time.Second})
		if err != nil {
			t.Fatalf("launch: %v", err)
		}
		if d := time.Since(start); d < best {
			best = d
		}
		_ = p.Close()
	}

	const ceiling = 400 * time.Millisecond
	if best >= ceiling {
		t.Fatalf("connector cold start %s, want < %s — a fixed ~1s delay here means "+
			"gRPC reconnect backoff is being paid again (see waitForSocket / WithConnectParams)",
			best, ceiling)
	}
	t.Logf("connector cold start (best of 3): %s", best)
}
