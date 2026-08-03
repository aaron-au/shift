package smtpconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/mail"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aaron-au/shift/engine/record"
	"github.com/emersion/go-sasl"
	smtpserver "github.com/emersion/go-smtp"
)

// captured is one delivered message as the in-process server saw it.
type captured struct {
	from  string
	rcpts []string
	data  []byte
}

// backend is a go-smtp Backend that accepts PLAIN auth for a fixed credential
// and records every delivered message.
type backend struct {
	user, pass string
	mu         sync.Mutex
	got        []captured
}

func (b *backend) NewSession(_ *smtpserver.Conn) (smtpserver.Session, error) {
	return &session{be: b}, nil
}

func (b *backend) messages() []captured {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]captured(nil), b.got...)
}

type session struct {
	be    *backend
	from  string
	rcpts []string
}

func (s *session) AuthMechanisms() []string { return []string{sasl.Plain} }

func (s *session) Auth(_ string) (sasl.Server, error) {
	return sasl.NewPlainServer(func(_, username, password string) error {
		if username != s.be.user || password != s.be.pass {
			return errors.New("invalid credentials")
		}
		return nil
	}), nil
}

func (s *session) Mail(from string, _ *smtpserver.MailOptions) error {
	s.from = from
	return nil
}

func (s *session) Rcpt(to string, _ *smtpserver.RcptOptions) error {
	s.rcpts = append(s.rcpts, to)
	return nil
}

func (s *session) Data(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.be.mu.Lock()
	s.be.got = append(s.be.got, captured{from: s.from, rcpts: append([]string(nil), s.rcpts...), data: data})
	s.be.mu.Unlock()
	return nil
}

func (s *session) Reset()        { s.from, s.rcpts = "", nil }
func (s *session) Logout() error { return nil }

// startSMTPServer runs an in-process SMTP server on loopback that advertises
// AUTH over the plaintext connection (AllowInsecureAuth). Returns host, port
// and the backend that captured deliveries. Torn down via t.Cleanup.
func startSMTPServer(t *testing.T) (host string, port int, be *backend) {
	t.Helper()
	be = &backend{user: "mailer", pass: "s3cret"}
	srv := smtpserver.NewServer(be)
	srv.Domain = "localhost"
	srv.AllowInsecureAuth = true
	srv.ReadTimeout = 10 * time.Second
	srv.WriteTimeout = 10 * time.Second
	srv.MaxMessageBytes = 1 << 20
	srv.MaxRecipients = 50

	lc := net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	go func() { _ = srv.Serve(ln) }()
	addr := ln.Addr().(*net.TCPAddr)
	return "127.0.0.1", addr.Port, be
}

func oneRecord(fields map[string]string) *record.Batch {
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	for k, v := range fields {
		bld.KeyLiteral(k)
		bld.StringLiteral(v)
	}
	bld.EndMap()
	b.Append(bld.Finish())
	return b
}

func TestSendDeliversMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("binds a TCP port; skipped under -short")
	}
	host, port, be := startSMTPServer(t)

	cfg := fmt.Appendf(nil,
		`{"host":%q,"port":%d,"username":"mailer","password":"s3cret","from":"Ops <ops@example.com>",`+
			`"to":["alice@example.com"],"cc":["bob@example.com"],"subject":"Hi $name",`+
			`"body_template":"Order $id is ready.","starttls":false,"allow_local":true}`,
		host, port)

	s := &sendSink{}
	ctx := context.Background()
	if err := s.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Write(ctx, oneRecord(map[string]string{"name": "Alice", "id": "42"})); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	msgs := be.messages()
	if len(msgs) != 1 {
		t.Fatalf("delivered %d messages, want 1", len(msgs))
	}
	m := msgs[0]
	if m.from != "ops@example.com" {
		t.Fatalf("envelope from = %q, want ops@example.com", m.from)
	}
	if len(m.rcpts) != 2 || m.rcpts[0] != "alice@example.com" || m.rcpts[1] != "bob@example.com" {
		t.Fatalf("envelope rcpts = %v, want [alice bob]", m.rcpts)
	}
	parsed, err := mail.ReadMessage(strings.NewReader(string(m.data)))
	if err != nil {
		t.Fatalf("parse delivered message: %v", err)
	}
	if got := parsed.Header.Get("Subject"); got != "Hi Alice" {
		t.Fatalf("Subject = %q, want %q", got, "Hi Alice")
	}
	if got := parsed.Header.Get("To"); got != "alice@example.com" {
		t.Fatalf("To = %q", got)
	}
	if got := parsed.Header.Get("Cc"); got != "bob@example.com" {
		t.Fatalf("Cc = %q", got)
	}
	body, _ := io.ReadAll(parsed.Body)
	if strings.TrimSpace(string(body)) != "Order 42 is ready." {
		t.Fatalf("body = %q, want %q", body, "Order 42 is ready.")
	}
}

func TestSendRendersRecordWhenNoTemplate(t *testing.T) {
	if testing.Short() {
		t.Skip("binds a TCP port; skipped under -short")
	}
	host, port, be := startSMTPServer(t)

	cfg := fmt.Appendf(nil,
		`{"host":%q,"port":%d,"from":"ops@example.com","to":["alice@example.com"],`+
			`"subject":"Update","starttls":false,"allow_local":true}`, host, port)

	s := &sendSink{}
	ctx := context.Background()
	if err := s.Open(ctx, cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Write(ctx, oneRecord(map[string]string{"status": "ok"})); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = s.Close()

	msgs := be.messages()
	if len(msgs) != 1 {
		t.Fatalf("delivered %d, want 1", len(msgs))
	}
	if !strings.Contains(string(msgs[0].data), "status: ok") {
		t.Fatalf("rendered body missing field: %q", msgs[0].data)
	}
}

func TestBuildMessageHeadersAndInjectionDefense(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	// A subject carrying CRLF must not inject a header.
	msg := buildMessage("ops@example.com", []string{"a@example.com"}, nil,
		"Alert\r\nBcc: attacker@evil.com", "hello\nworld", now)
	parsed, err := mail.ReadMessage(strings.NewReader(string(msg)))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Header.Get("Bcc") != "" {
		t.Fatal("header injection succeeded via subject")
	}
	if parsed.Header.Get("Subject") != "AlertBcc: attacker@evil.com" {
		t.Fatalf("subject = %q", parsed.Header.Get("Subject"))
	}
	if parsed.Header.Get("Date") == "" || parsed.Header.Get("Message-ID") == "" {
		t.Fatal("missing Date or Message-ID")
	}
	if !strings.Contains(string(msg), "hello\r\nworld") {
		t.Fatalf("body not CRLF-normalized: %q", msg)
	}
}

func TestSubstitute(t *testing.T) {
	rec := oneRecord(map[string]string{"name": "Zoe"}).Records()[0]
	if got := substitute("Hi $name, ok? $missing", rec); got != "Hi Zoe, ok? $missing" {
		t.Fatalf("substitute = %q", got)
	}
	if got := substitute("no placeholders", rec); got != "no placeholders" {
		t.Fatalf("substitute passthrough = %q", got)
	}
}

func TestValidate(t *testing.T) {
	cases := map[string]string{ //nolint:gosec // G101: literal JSON test fixtures, not real credentials
		"missing host":      `{"from":"a@b.com","to":["c@d.com"]}`,
		"missing from":      `{"host":"h","to":["c@d.com"]}`,
		"missing to":        `{"host":"h","from":"a@b.com"}`,
		"pass without user": `{"host":"h","from":"a@b.com","to":["c@d.com"],"password":"p"}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			var c config
			if err := parseConfig([]byte(raw), &c); err == nil {
				t.Fatalf("%s: expected validation error", name)
			}
		})
	}
	t.Run("defaults", func(t *testing.T) {
		var c config
		if err := parseConfig([]byte(`{"host":"h","from":"a@b.com","to":["c@d.com"]}`), &c); err != nil {
			t.Fatalf("valid config rejected: %v", err)
		}
		if c.Port != 587 || c.TimeoutSeconds != 30 || !c.startTLS() {
			t.Fatalf("defaults not applied: port=%d timeout=%d starttls=%v", c.Port, c.TimeoutSeconds, c.startTLS())
		}
	})
}

func TestGuardRefusesInternalUnlessAllowed(t *testing.T) {
	deny := guard(false)
	for _, addr := range []string{"127.0.0.1:25", "10.0.0.1:25", "169.254.169.254:80", "100.64.0.1:25"} {
		if err := deny(addr[:strings.LastIndexByte(addr, ':')], addr, nil); err == nil {
			t.Fatalf("guard allowed internal target %q", addr)
		}
	}
	allow := guard(true)
	if err := allow("127.0.0.1", "127.0.0.1:25", nil); err != nil {
		t.Fatalf("allow_local guard rejected loopback: %v", err)
	}
	// A public address is always permitted.
	if err := deny("8.8.8.8", "8.8.8.8:25", nil); err != nil {
		t.Fatalf("guard rejected public target: %v", err)
	}
}

func TestStartTLSRequiredFailsClosed(t *testing.T) {
	if testing.Short() {
		t.Skip("binds a TCP port; skipped under -short")
	}
	host, port, _ := startSMTPServer(t) // server advertises no STARTTLS
	cfg := fmt.Appendf(nil,
		`{"host":%q,"port":%d,"from":"a@b.com","to":["c@d.com"],"starttls":true,"allow_local":false}`,
		host, port)
	// allow_local=false with no STARTTLS available → refuse. The guard blocks
	// the loopback dial first, which is itself a fail-closed outcome; either
	// way the send must error rather than proceed in cleartext.
	s := &sendSink{}
	if err := s.Open(context.Background(), cfg); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Write(context.Background(), oneRecord(map[string]string{"x": "1"})); err == nil {
		t.Fatal("expected fail-closed error, got nil")
	}
}
