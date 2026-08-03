package amqpconn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/aaron-au/shift/engine/record"
)

// consumeSource drains messages from a queue, emitting one record per message:
// {body, routing_key, exchange, headers, delivery_tag}. body is the message
// payload parsed into a typed value when it is valid JSON, otherwise the raw
// bytes as a string. Each message is acked once its record is built into the
// batch.
//
// Termination is guaranteed: GetNext uses basic.get semantics (never blocks),
// so a drained queue reports empty and the source returns io.EOF; max_messages
// caps the total. The source therefore never waits for future deliveries — it
// is a bounded pull, matching the engine's pull-pipeline contract.
type consumeSource struct {
	cfg     Config
	dial    consumeDialer // nil in production → bound to the real dialer in Open
	ch      consumeChannel
	batch   *record.Batch
	scratch *record.Batch // reused to parse each JSON body before copying in
	emitted int
	done    bool
}

func (s *consumeSource) Open(ctx context.Context, config []byte) error {
	if err := json.Unmarshal(config, &s.cfg); err != nil {
		return fmt.Errorf("amqp: bad config: %w", err)
	}
	if err := s.cfg.validateConn(); err != nil {
		return err
	}
	if s.cfg.Queue == "" {
		return errors.New("amqp: queue is required for consume")
	}
	if s.cfg.Prefetch <= 0 {
		s.cfg.Prefetch = defaultPrefetch
	}
	if s.dial == nil {
		s.dial = dialConsume
	}
	ch, err := s.dial(ctx, &s.cfg)
	if err != nil {
		return err
	}
	s.ch = ch
	s.batch = record.NewBatch()
	s.scratch = record.NewBatch()
	return nil
}

func (s *consumeSource) Next(ctx context.Context) (*record.Batch, error) {
	if s.done {
		return nil, io.EOF
	}
	if s.atLimit() {
		s.done = true
		return nil, io.EOF
	}
	s.batch.Reset()
	for s.batch.Len() < s.cfg.Prefetch && !s.atLimit() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		d, ok, err := s.ch.GetNext(ctx)
		if err != nil {
			return nil, fmt.Errorf("amqp: get from queue %q: %w", s.cfg.Queue, err)
		}
		if !ok { // queue drained — no more messages available now
			s.done = true
			break
		}
		s.appendRecord(d)
		if d.Ack != nil {
			if err := d.Ack(); err != nil {
				return nil, fmt.Errorf("amqp: ack delivery %d: %w", d.DeliveryTag, err)
			}
		}
		s.emitted++
	}
	if s.batch.Len() == 0 {
		return nil, io.EOF
	}
	return s.batch, nil
}

func (s *consumeSource) Close() error {
	if s.ch != nil {
		return s.ch.Close()
	}
	return nil
}

// atLimit reports whether the configured max_messages cap has been reached (0
// means no cap — drain the queue).
func (s *consumeSource) atLimit() bool {
	return s.cfg.MaxMessages > 0 && s.emitted >= s.cfg.MaxMessages
}

// appendRecord builds one message record into s.batch.
func (s *consumeSource) appendRecord(d delivery) {
	// Parse the body first (into the scratch batch) so the map below can be
	// built in a single uninterrupted pass.
	bodyVal, structured := s.decodeBody(d.Body)

	bld := s.batch.Builder()
	bld.BeginMap()

	bld.KeyLiteral("body")
	if structured {
		// CopyValue is the sanctioned way to move a value across batches; the
		// scratch batch is reused for the next message.
		bld.Value(record.CopyValue(s.batch, bodyVal))
	} else {
		bld.String(d.Body)
	}

	bld.KeyLiteral("routing_key")
	bld.StringLiteral(d.RoutingKey)

	bld.KeyLiteral("exchange")
	bld.StringLiteral(d.Exchange)

	bld.KeyLiteral("headers")
	buildHeaders(bld, d.Headers)

	bld.KeyLiteral("delivery_tag")
	bld.Int(int64(d.DeliveryTag)) //nolint:gosec // G115: AMQP delivery tag is a per-channel monotonic counter, never exceeds int64

	bld.EndMap()
	s.batch.Append(bld.Finish())
}

// decodeBody parses body into a typed record value when it is valid JSON,
// returning (value, true). Otherwise it returns (_, false) and the caller
// stores the raw bytes as a string. The value lives in s.scratch and must be
// copied before the next call.
func (s *consumeSource) decodeBody(body []byte) (record.Value, bool) {
	if len(body) == 0 || !json.Valid(body) {
		return record.Value{}, false
	}
	s.scratch.Reset()
	bld := s.scratch.Builder()
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := buildJSONValue(dec, bld, maxBodyDepth); err != nil {
		// Depth guard tripped (or an unexpected shape): fall back to string.
		return record.Value{}, false
	}
	return bld.Finish(), true
}

// buildHeaders emits an AMQP header table as a record map. Header values are
// small metadata; each is written via a scalar type switch, and any nested or
// unrecognised value is stringified so the record stays a flat, typed map
// without map[string]interface{} leaking onto the data path.
func buildHeaders(bld *record.Builder, h map[string]any) {
	bld.BeginMap()
	for k, v := range h {
		bld.KeyLiteral(k)
		switch t := v.(type) {
		case string:
			bld.StringLiteral(t)
		case []byte:
			bld.String(t)
		case bool:
			bld.Bool(t)
		case int:
			bld.Int(int64(t))
		case int8:
			bld.Int(int64(t))
		case int16:
			bld.Int(int64(t))
		case int32:
			bld.Int(int64(t))
		case int64:
			bld.Int(t)
		case float32:
			bld.Float(float64(t))
		case float64:
			bld.Float(t)
		case nil:
			bld.Null()
		default:
			bld.StringLiteral(fmt.Sprintf("%v", t))
		}
	}
	bld.EndMap()
}

// buildJSONValue drives a token stream from dec into bld, constructing a typed
// record value without an intermediate map[string]interface{}. depth bounds
// nesting; exceeding it is an error so the caller can fall back to a string.
func buildJSONValue(dec *json.Decoder, bld *record.Builder, depth int) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	return buildFromToken(dec, bld, tok, depth)
}

func buildFromToken(dec *json.Decoder, bld *record.Builder, tok json.Token, depth int) error {
	switch t := tok.(type) {
	case json.Delim:
		if depth <= 0 {
			return errors.New("amqp: json body nesting too deep")
		}
		switch t {
		case '{':
			bld.BeginMap()
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return err
				}
				key, ok := keyTok.(string)
				if !ok {
					return errors.New("amqp: non-string object key")
				}
				bld.KeyLiteral(key)
				if err := buildJSONValue(dec, bld, depth-1); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // consume '}'
				return err
			}
			bld.EndMap()
		case '[':
			bld.BeginList()
			for dec.More() {
				if err := buildJSONValue(dec, bld, depth-1); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // consume ']'
				return err
			}
			bld.EndList()
		default:
			return fmt.Errorf("amqp: unexpected delimiter %q", t)
		}
	case string:
		bld.StringLiteral(t)
	case json.Number:
		if n, err := t.Int64(); err == nil {
			bld.Int(n)
		} else {
			f, err := t.Float64()
			if err != nil {
				return err
			}
			bld.Float(f)
		}
	case bool:
		bld.Bool(t)
	case nil:
		bld.Null()
	default:
		return fmt.Errorf("amqp: unsupported json token %T", t)
	}
	return nil
}
