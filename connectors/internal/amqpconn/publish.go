package amqpconn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/aaron-au/shift/engine/format/ndjson"
	"github.com/aaron-au/shift/engine/record"
)

// contentTypeJSON is set on every published message: bodies are always encoded
// as a single-line JSON document.
const contentTypeJSON = "application/json"

// publishSink publishes one AMQP message per record. Each record is encoded to
// a compact single-line JSON body (via the ndjson encoder, which escapes so
// every newline in its output is a record boundary) and published to the
// configured exchange/routing key.
type publishSink struct {
	cfg  Config
	dial publishDialer // nil in production → bound to the real dialer in Open
	ch   publishChannel
	buf  bytes.Buffer
	w    *ndjson.Writer
	// ordinal counts messages published, suffixing the idempotency MessageId.
	ordinal int64
}

func (s *publishSink) Open(ctx context.Context, config []byte) error {
	if err := json.Unmarshal(config, &s.cfg); err != nil {
		return fmt.Errorf("amqp: bad config: %w", err)
	}
	if err := s.cfg.validateConn(); err != nil {
		return err
	}
	if s.dial == nil {
		s.dial = dialPublish
	}
	ch, err := s.dial(ctx, &s.cfg)
	if err != nil {
		return err
	}
	s.ch = ch
	s.w = ndjson.NewWriter(&s.buf)
	return nil
}

func (s *publishSink) Write(ctx context.Context, b *record.Batch) error {
	if b.Len() == 0 {
		return nil
	}
	// Encode the whole batch to NDJSON once, then split on the newline record
	// separators: each line is one record's compact JSON, i.e. one message
	// body. The encoder escapes control characters, so a newline inside a
	// string value never appears literally — every '\n' is a boundary.
	s.buf.Reset()
	if err := s.w.Write(ctx, b); err != nil {
		return err
	}
	if err := s.w.Close(); err != nil { // flush buffered output into s.buf
		return err
	}
	data := bytes.TrimRight(s.buf.Bytes(), "\n")
	if len(data) == 0 {
		return nil
	}
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		msgID := ""
		if s.cfg.IdempotencyKey != "" {
			msgID = fmt.Sprintf("%s:%d", s.cfg.IdempotencyKey, s.ordinal)
		}
		// line aliases s.buf; PublishBody sends synchronously (the driver
		// copies into its frame buffer before returning), so no copy is needed.
		if err := s.ch.PublishBody(ctx, s.cfg.Exchange, s.cfg.RoutingKey, contentTypeJSON, msgID, line); err != nil {
			return fmt.Errorf("amqp: publish to exchange %q key %q: %w", s.cfg.Exchange, s.cfg.RoutingKey, err)
		}
		s.ordinal++
	}
	return nil
}

func (s *publishSink) Close() error {
	if s.ch != nil {
		return s.ch.Close()
	}
	return nil
}
