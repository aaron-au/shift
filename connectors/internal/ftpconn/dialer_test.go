package ftpconn

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"
)

// acceptOne starts a throwaway TCP listener that reads the first byte of the
// one connection it accepts, and reports it. 0x16 is a TLS record header
// (ClientHello); anything else means the client spoke plaintext.
func acceptOne(t *testing.T) (addr string, first <-chan byte) {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	ch := make(chan byte, 1)
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		buf := make([]byte, 1)
		if n, _ := conn.Read(buf); n > 0 {
			ch <- buf[0]
		}
		close(ch)
	}()
	return ln.Addr().String(), ch
}

// TestDataConnectionsGetTLS is the regression test for a silent-plaintext bug.
//
// jlaffaye/ftp's openDataConn returns the configured dialFunc's connection
// BEFORE reaching its own tls.Client branch, so installing a dial func — which
// the SSRF network guard requires — silently disabled data-channel TLS. The
// control channel was still upgraded via AUTH TLS and PROT P was still sent, so
// against a lenient server file contents and listings crossed the wire in the
// clear on a connection configured as FTPS.
//
// The dialer must therefore wrap data connections itself, while leaving the
// control connection plaintext for the library's AUTH TLS upgrade.
func TestDataConnectionsGetTLS(t *testing.T) {
	gd := &guardedDialer{
		dialer: &net.Dialer{Timeout: 3 * time.Second, Control: guard(true)},
		ctx:    context.Background(),
		tls:    &tls.Config{ServerName: "127.0.0.1", InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}, //nolint:gosec // G402: test server uses no certificate
	}

	// First dial = the control connection. It must stay plaintext so the
	// library can drive the AUTH TLS upgrade itself.
	ctrlAddr, ctrlFirst := acceptOne(t)
	ctrl, err := gd.dial("tcp", ctrlAddr)
	if err != nil {
		t.Fatal(err)
	}
	if _, isTLS := ctrl.(*tls.Conn); isTLS {
		t.Error("control connection was TLS-wrapped by the dialer; the library upgrades it via AUTH TLS")
	}
	if _, err := ctrl.Write([]byte("NOOP\r\n")); err != nil {
		t.Fatal(err)
	}
	if b := <-ctrlFirst; b == 0x16 {
		t.Error("control connection sent a TLS ClientHello")
	}
	_ = ctrl.Close()

	// Every subsequent dial is a PASV data connection and must be TLS.
	dataAddr, dataFirst := acceptOne(t)
	data, err := gd.dial("tcp", dataAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = data.Close() }()
	if _, isTLS := data.(*tls.Conn); !isTLS {
		t.Fatal("data connection is not TLS-wrapped — file contents would cross the wire in cleartext")
	}
	// The wrap is lazy (jlaffaye/ftp#282: an eager handshake hangs with
	// proftpd/pureftpd), so the handshake fires on first use.
	_, _ = data.Write([]byte("x"))
	if b, ok := <-dataFirst; ok && b != 0x16 {
		t.Errorf("first byte on the data connection = %#x, want 0x16 (TLS ClientHello)", b)
	}
}

// TestPlaintextFTPStaysPlaintext: without explicit_tls, no connection may be
// wrapped — the wrap is opt-in, and a spurious ClientHello would break plain
// FTP entirely.
func TestPlaintextFTPStaysPlaintext(t *testing.T) {
	gd := &guardedDialer{dialer: &net.Dialer{Timeout: 3 * time.Second, Control: guard(true)}, ctx: context.Background()}
	for _, role := range []string{"control", "data"} {
		addr, _ := acceptOne(t)
		conn, err := gd.dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		if _, isTLS := conn.(*tls.Conn); isTLS {
			t.Errorf("%s connection was TLS-wrapped without explicit_tls", role)
		}
		_ = conn.Close()
	}
}
