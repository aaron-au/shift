package redisconn

import (
	"context"
	"net"
	"testing"
	"time"
)

// firstByteOnWire dials the connector at a bare TCP listener and reports the
// first byte the client sends. 0x16 is a TLS record header (ClientHello); a
// RESP command starts with '*'. Reading the wire directly is the only way to
// tell whether TLS was really negotiated — the config alone lies.
func firstByteOnWire(t *testing.T, cfg *config) byte {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	got := make(chan byte, 1)
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
		buf := make([]byte, 1)
		if n, _ := conn.Read(buf); n > 0 {
			got <- buf[0]
		}
	}()

	cfg.Addr = ln.Addr().String()
	cfg.AllowLocal = true // the listener is loopback; the guard is not under test
	c, err := openClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	// Generous: what is asserted is which bytes reach the wire, not how fast.
	// go-redis dials lazily through a pool with its own retry backoff, and the
	// package's other tests keep the race detector busy.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The command fails (this server speaks no RESP and completes no
	// handshake); what is under test is what reached the wire before that.
	_, _ = c.Get(ctx, "probe")

	select {
	case b := <-got:
		return b
	case <-ctx.Done():
		t.Fatal("client never connected to the listener")
		return 0
	}
}

// TestTLSIsActuallyNegotiated is a regression test for a silent-plaintext bug.
//
// go-redis honors Options.TLSConfig only from its own default dialer:
// options.go does `if opt.Dialer == nil { opt.Dialer = NewDialer(opt) }` and
// the tls.DialWithDialer call lives solely inside NewDialer. This connector
// must install a custom Dialer for the network guard, which made TLSConfig
// dead config — with `tls: true` the client dialed plaintext and sent the AUTH
// password in the clear, with no error to reveal it.
func TestTLSIsActuallyNegotiated(t *testing.T) {
	if b := firstByteOnWire(t, &config{TLS: true}); b != 0x16 {
		t.Fatalf("first byte on the wire = %#x, want 0x16 (TLS ClientHello) — the client sent plaintext with tls enabled", b)
	}
}

// TestTLSDisabledStaysPlaintext: the wrap is opt-in and must not fire (or hang
// on a handshake) when the flow did not ask for TLS.
func TestTLSDisabledStaysPlaintext(t *testing.T) {
	if b := firstByteOnWire(t, &config{}); b == 0x16 {
		t.Fatal("client sent a TLS ClientHello with tls disabled")
	}
}
