package smtpconn

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/smtp"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// sendSink sends one email per incoming record. Each Write opens a fresh,
// network-guarded SMTP session (STARTTLS + AUTH as configured), sends every
// record in the batch over it, and quits — so a connection never outlives one
// batch. The batch is never retained: each message's bytes are built and
// written synchronously.
type sendSink struct {
	cfg config
	now func() time.Time // injectable clock for deterministic Date in tests
}

func (s *sendSink) Open(_ context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	if s.now == nil {
		s.now = time.Now
	}
	return nil
}

func (s *sendSink) Write(ctx context.Context, b *record.Batch) error {
	recs := b.Records()
	if len(recs) == 0 {
		return nil
	}
	cl, err := s.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = cl.Close() }() // no-op after a clean Quit; closes on error
	rcpts := s.cfg.recipients()
	for _, rec := range recs {
		subject := substitute(s.cfg.Subject, rec)
		body := s.cfg.BodyTemplate
		if body == "" {
			body = renderBody(rec)
		} else {
			body = substitute(body, rec)
		}
		msg := buildMessage(s.cfg.From, s.cfg.To, s.cfg.Cc, subject, body, s.now())
		if err := sendOne(cl, bareAddr(s.cfg.From), rcpts, msg); err != nil {
			return err
		}
	}
	return cl.Quit()
}

func (s *sendSink) Close() error { return nil }

// dial opens a network-guarded TCP connection, wraps it in an SMTP client, and
// performs STARTTLS + AUTH per config. Fail closed: if STARTTLS is requested
// but unavailable (and allow_local is off) or AUTH is requested but
// unadvertised, the send is refused rather than falling back to cleartext.
func (s *sendSink) dial(ctx context.Context) (*smtp.Client, error) {
	dialer := &net.Dialer{Timeout: s.cfg.timeout(), Control: guard(s.cfg.AllowLocal)}
	conn, err := dialer.DialContext(ctx, "tcp", s.cfg.addr())
	if err != nil {
		return nil, fmt.Errorf("smtp: dial %s: %w", s.cfg.addr(), err)
	}
	cl, err := smtp.NewClient(conn, s.cfg.Host)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("smtp: greeting: %w", err)
	}
	if s.cfg.startTLS() {
		if ok, _ := cl.Extension("STARTTLS"); ok {
			tlsCfg := &tls.Config{ServerName: s.cfg.Host, MinVersion: tls.VersionTLS12}
			if err := cl.StartTLS(tlsCfg); err != nil {
				_ = cl.Close()
				return nil, fmt.Errorf("smtp: starttls: %w", err)
			}
		} else if !s.cfg.AllowLocal {
			_ = cl.Close()
			return nil, errors.New("smtp: server does not advertise STARTTLS (set allow_local to send without it)")
		}
	}
	if s.cfg.Username != "" {
		if ok, _ := cl.Extension("AUTH"); !ok {
			_ = cl.Close()
			return nil, errors.New("smtp: server does not advertise AUTH but a username is set")
		}
		// PlainAuth refuses to transmit credentials over an unencrypted, non-
		// localhost connection — the safe failure when STARTTLS was skipped.
		auth := smtp.PlainAuth("", s.cfg.Username, s.cfg.Password, s.cfg.Host)
		if err := cl.Auth(auth); err != nil {
			_ = cl.Close()
			return nil, fmt.Errorf("smtp: auth: %w", err)
		}
	}
	return cl, nil
}

// sendOne runs one SMTP transaction (MAIL/RCPT/DATA) on an established client.
// A completed DATA resets session state, so the client is reusable for the next
// record in the batch.
func sendOne(cl *smtp.Client, from string, rcpts []string, msg []byte) error {
	if err := cl.Mail(from); err != nil {
		return fmt.Errorf("smtp: MAIL FROM: %w", err)
	}
	for _, rcpt := range rcpts {
		if err := cl.Rcpt(bareAddr(rcpt)); err != nil {
			return fmt.Errorf("smtp: RCPT TO: %w", err)
		}
	}
	w, err := cl.Data()
	if err != nil {
		return fmt.Errorf("smtp: DATA: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		_ = w.Close()
		return fmt.Errorf("smtp: write body: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("smtp: complete DATA: %w", err)
	}
	return nil
}
