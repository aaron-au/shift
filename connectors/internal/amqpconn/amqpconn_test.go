package amqpconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
)

// ---- in-memory fakes over the broker seams (no real broker) ----

type fakeMsg struct {
	exchange, key, contentType, messageID, body string
}

type fakePublish struct {
	msgs   []fakeMsg
	err    error // injected publish error
	closed bool
}

func (f *fakePublish) PublishBody(_ context.Context, exchange, key, contentType, messageID string, body []byte) error {
	if f.err != nil {
		return f.err
	}
	f.msgs = append(f.msgs, fakeMsg{exchange, key, contentType, messageID, string(body)})
	return nil
}

func (f *fakePublish) Close() error { f.closed = true; return nil }

type fakeConsume struct {
	deliveries []delivery
	idx        int
	acked      []uint64
	getErr     error
	closed     bool
}

func (f *fakeConsume) GetNext(_ context.Context) (delivery, bool, error) {
	if f.getErr != nil {
		return delivery{}, false, f.getErr
	}
	if f.idx >= len(f.deliveries) {
		return delivery{}, false, nil // queue drained
	}
	d := f.deliveries[f.idx]
	f.idx++
	tag := d.DeliveryTag
	d.Ack = func() error { f.acked = append(f.acked, tag); return nil }
	return d, true, nil
}

func (f *fakeConsume) Close() error { f.closed = true; return nil }

// openPublish builds a publishSink wired to fp and opens it with cfg JSON.
func openPublish(t *testing.T, fp *fakePublish, cfg string) *publishSink {
	t.Helper()
	s := &publishSink{dial: func(context.Context, *Config) (publishChannel, error) { return fp, nil }}
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatalf("open: %v", err)
	}
	return s
}

func openConsume(t *testing.T, fc *fakeConsume, cfg string) *consumeSource {
	t.Helper()
	s := &consumeSource{dial: func(context.Context, *Config) (consumeChannel, error) { return fc, nil }}
	if err := s.Open(context.Background(), []byte(cfg)); err != nil {
		t.Fatalf("open: %v", err)
	}
	return s
}

func mapBatch(t *testing.T, rows ...func(*record.Builder)) *record.Batch {
	t.Helper()
	b := record.NewBatch()
	bld := b.Builder()
	for _, row := range rows {
		bld.BeginMap()
		row(bld)
		bld.EndMap()
		b.Append(bld.Finish())
	}
	return b
}

// ---- publish ----

func TestPublishMapsRecordsToMessages(t *testing.T) {
	fp := &fakePublish{}
	s := openPublish(t, fp, `{"host":"broker","exchange":"ex","routing_key":"rk"}`)

	b := mapBatch(t,
		func(bld *record.Builder) {
			bld.KeyLiteral("id")
			bld.Int(1)
			bld.KeyLiteral("name")
			bld.StringLiteral("alice")
		},
		func(bld *record.Builder) {
			bld.KeyLiteral("id")
			bld.Int(2)
			bld.KeyLiteral("name")
			bld.StringLiteral("bob")
		},
	)
	if err := s.Write(context.Background(), b); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !fp.closed {
		t.Fatal("channel not closed")
	}
	if len(fp.msgs) != 2 {
		t.Fatalf("published %d messages, want 2", len(fp.msgs))
	}
	if fp.msgs[0].body != `{"id":1,"name":"alice"}` {
		t.Fatalf("msg0 body = %q", fp.msgs[0].body)
	}
	if fp.msgs[1].body != `{"id":2,"name":"bob"}` {
		t.Fatalf("msg1 body = %q", fp.msgs[1].body)
	}
	for i, m := range fp.msgs {
		if m.exchange != "ex" || m.key != "rk" || m.contentType != contentTypeJSON {
			t.Fatalf("msg%d routing/content = %+v", i, m)
		}
		if m.messageID != "" {
			t.Fatalf("msg%d unexpected messageID %q (no idempotency key set)", i, m.messageID)
		}
	}
}

func TestPublishIdempotencyKey(t *testing.T) {
	fp := &fakePublish{}
	s := openPublish(t, fp, `{"host":"broker","exchange":"","routing_key":"q","idempotency_key":"task-9"}`)
	b := mapBatch(t,
		func(bld *record.Builder) { bld.KeyLiteral("n"); bld.Int(1) },
		func(bld *record.Builder) { bld.KeyLiteral("n"); bld.Int(2) },
	)
	if err := s.Write(context.Background(), b); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Write a second batch: ordinals continue across batches so a re-dispatch
	// replays the same id sequence.
	if err := s.Write(context.Background(), mapBatch(t, func(bld *record.Builder) { bld.KeyLiteral("n"); bld.Int(3) })); err != nil {
		t.Fatalf("write2: %v", err)
	}
	want := []string{"task-9:0", "task-9:1", "task-9:2"}
	if len(fp.msgs) != 3 {
		t.Fatalf("published %d, want 3", len(fp.msgs))
	}
	for i, w := range want {
		if fp.msgs[i].messageID != w {
			t.Fatalf("msg%d messageID = %q, want %q", i, fp.msgs[i].messageID, w)
		}
	}
}

func TestPublishEmptyBatchNoop(t *testing.T) {
	fp := &fakePublish{}
	s := openPublish(t, fp, `{"host":"broker","routing_key":"q"}`)
	if err := s.Write(context.Background(), record.NewBatch()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(fp.msgs) != 0 {
		t.Fatalf("published %d messages from empty batch", len(fp.msgs))
	}
}

func TestPublishErrorPropagates(t *testing.T) {
	fp := &fakePublish{err: errors.New("broker down")}
	s := openPublish(t, fp, `{"host":"broker","routing_key":"q"}`)
	b := mapBatch(t, func(bld *record.Builder) { bld.KeyLiteral("n"); bld.Int(1) })
	err := s.Write(context.Background(), b)
	if err == nil || !strings.Contains(err.Error(), "broker down") {
		t.Fatalf("err = %v, want broker-down", err)
	}
}

// ---- consume ----

func TestConsumeMapsMessagesAndAcks(t *testing.T) {
	fc := &fakeConsume{deliveries: []delivery{
		{Body: []byte(`{"order":42,"paid":true}`), RoutingKey: "rk1", Exchange: "orders", DeliveryTag: 1,
			Headers: map[string]any{"source": "web", "retry": int32(3)}},
		{Body: []byte("plain text, not json"), RoutingKey: "rk2", Exchange: "orders", DeliveryTag: 2},
		{Body: []byte(`[1,2,3]`), RoutingKey: "rk3", Exchange: "", DeliveryTag: 3},
	}}
	s := openConsume(t, fc, `{"host":"broker","queue":"q"}`)
	defer func() { _ = s.Close() }()

	recs := drain(t, s)
	if len(recs) != 3 {
		t.Fatalf("consumed %d records, want 3", len(recs))
	}

	// Record 0: JSON object body → structured map.
	body0, _ := recs[0].Field("body")
	if body0.Kind() != record.KindMap {
		t.Fatalf("body0 kind = %v, want map", body0.Kind())
	}
	if order, _ := body0.Field("order"); order.Int() != 42 {
		t.Fatalf("body0.order = %d", order.Int())
	}
	if paid, _ := body0.Field("paid"); !paid.Bool() {
		t.Fatal("body0.paid should be true")
	}
	if rk, _ := recs[0].Field("routing_key"); rk.String() != "rk1" {
		t.Fatalf("rk = %q", rk.String())
	}
	if ex, _ := recs[0].Field("exchange"); ex.String() != "orders" {
		t.Fatalf("exchange = %q", ex.String())
	}
	if tag, _ := recs[0].Field("delivery_tag"); tag.Int() != 1 {
		t.Fatalf("delivery_tag = %d", tag.Int())
	}
	hdrs, _ := recs[0].Field("headers")
	if src, _ := hdrs.Field("source"); src.String() != "web" {
		t.Fatalf("headers.source = %q", src.String())
	}
	if retry, _ := hdrs.Field("retry"); retry.Int() != 3 {
		t.Fatalf("headers.retry = %d", retry.Int())
	}

	// Record 1: non-JSON body → string.
	body1, _ := recs[1].Field("body")
	if body1.Kind() != record.KindString || body1.String() != "plain text, not json" {
		t.Fatalf("body1 = %v %q", body1.Kind(), body1.String())
	}

	// Record 2: JSON array body → list.
	body2, _ := recs[2].Field("body")
	if body2.Kind() != record.KindList || body2.Len() != 3 {
		t.Fatalf("body2 kind = %v len = %d, want list of 3", body2.Kind(), body2.Len())
	}
	if body2.Index(0).Int() != 1 || body2.Index(2).Int() != 3 {
		t.Fatalf("body2 elements = %v", body2)
	}

	// All three acked, in order.
	if len(fc.acked) != 3 || fc.acked[0] != 1 || fc.acked[2] != 3 {
		t.Fatalf("acked = %v, want [1 2 3]", fc.acked)
	}
}

func TestConsumeMaxMessages(t *testing.T) {
	var ds []delivery
	for i := 1; i <= 5; i++ {
		ds = append(ds, delivery{Body: fmt.Appendf(nil, `{"n":%d}`, i), DeliveryTag: uint64(i)})
	}
	fc := &fakeConsume{deliveries: ds}
	s := openConsume(t, fc, `{"host":"broker","queue":"q","max_messages":2}`)
	defer func() { _ = s.Close() }()

	recs := drain(t, s)
	if len(recs) != 2 {
		t.Fatalf("consumed %d records, want 2 (capped)", len(recs))
	}
	// Only the two consumed messages are acked; the rest stay on the broker.
	if len(fc.acked) != 2 {
		t.Fatalf("acked %d, want 2", len(fc.acked))
	}
}

func TestConsumeDrainTerminates(t *testing.T) {
	// Empty queue: Next must return EOF immediately, never block.
	fc := &fakeConsume{}
	s := openConsume(t, fc, `{"host":"broker","queue":"q"}`)
	defer func() { _ = s.Close() }()
	if _, err := s.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("empty queue Next = %v, want EOF", err)
	}
}

func TestConsumeGetError(t *testing.T) {
	fc := &fakeConsume{getErr: errors.New("channel closed")}
	s := openConsume(t, fc, `{"host":"broker","queue":"q"}`)
	defer func() { _ = s.Close() }()
	_, err := s.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "channel closed") {
		t.Fatalf("err = %v, want channel-closed", err)
	}
}

func TestConsumePrefetchBatching(t *testing.T) {
	// prefetch=2 caps one Next batch at 2 records; draining takes two batches.
	var ds []delivery
	for i := 1; i <= 3; i++ {
		ds = append(ds, delivery{Body: fmt.Appendf(nil, `{"n":%d}`, i), DeliveryTag: uint64(i)})
	}
	fc := &fakeConsume{deliveries: ds}
	s := openConsume(t, fc, `{"host":"broker","queue":"q","prefetch":2}`)
	defer func() { _ = s.Close() }()

	b1, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("next1: %v", err)
	}
	if b1.Len() != 2 {
		t.Fatalf("batch1 len = %d, want 2 (prefetch)", b1.Len())
	}
	b2, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("next2: %v", err)
	}
	if b2.Len() != 1 {
		t.Fatalf("batch2 len = %d, want 1", b2.Len())
	}
	if _, err := s.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("next3 = %v, want EOF", err)
	}
}

// drain reads a source to EOF and returns copies of every emitted record. The
// batch is reused between Next calls, so records are copied out before the next
// call invalidates them.
func drain(t *testing.T, s *consumeSource) []record.Value {
	t.Helper()
	out := record.NewBatch()
	var recs []record.Value
	for {
		b, err := s.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, rec := range b.Records() {
			recs = append(recs, record.CopyValue(out, rec))
		}
	}
	return recs
}

// ---- config / url / guard ----

func TestConfigValidation(t *testing.T) {
	t.Run("missing url and host", func(t *testing.T) {
		var c Config
		if err := c.validateConn(); err == nil {
			t.Fatal("expected url-or-host error")
		}
	})
	t.Run("consume requires queue", func(t *testing.T) {
		s := &consumeSource{dial: func(context.Context, *Config) (consumeChannel, error) { return &fakeConsume{}, nil }}
		if err := s.Open(context.Background(), []byte(`{"host":"broker"}`)); err == nil {
			t.Fatal("expected queue-required error")
		}
	})
	t.Run("bad json", func(t *testing.T) {
		s := &publishSink{dial: func(context.Context, *Config) (publishChannel, error) { return &fakePublish{}, nil }}
		if err := s.Open(context.Background(), []byte(`{not json`)); err == nil {
			t.Fatal("expected bad-config error")
		}
	})
}

func TestAMQPURLDefaults(t *testing.T) {
	// Explicit url passes through untouched.
	c := Config{URL: "amqp://u:p@h:5672/v"} //nolint:gosec // G101: test fixture URL, not a real credential
	if err := c.validateConn(); err != nil {
		t.Fatal(err)
	}
	if got := c.amqpURL(); got != "amqp://u:p@h:5672/v" { //nolint:gosec // G101: test fixture URL, not a real credential
		t.Fatalf("url = %q", got)
	}

	// Split fields → assembled URL with default plain port and empty (default)
	// vhost path.
	c = Config{Host: "broker", User: "guest", Password: "guest"}
	if err := c.validateConn(); err != nil {
		t.Fatal(err)
	}
	if c.Port != defaultAMQPPort {
		t.Fatalf("port = %d, want %d", c.Port, defaultAMQPPort)
	}
	if got := c.amqpURL(); got != "amqp://guest:guest@broker:5672/" { //nolint:gosec // G101: test fixture URL, not a real credential
		t.Fatalf("assembled url = %q", got)
	}

	// TLS flips the scheme and default port.
	c = Config{Host: "broker", TLS: true, Vhost: "prod"}
	if err := c.validateConn(); err != nil {
		t.Fatal(err)
	}
	if c.Port != defaultAMQPSPort {
		t.Fatalf("tls port = %d, want %d", c.Port, defaultAMQPSPort)
	}
	got := c.amqpURL()
	if !strings.HasPrefix(got, "amqps://") || !strings.HasSuffix(got, ":5671/prod") {
		t.Fatalf("tls url = %q", got)
	}
}

func TestGuardRefusesInternalTargets(t *testing.T) {
	deny := guard(false)
	for _, addr := range []string{"127.0.0.1:5672", "10.0.0.1:5672", "192.168.1.5:5672", "100.64.0.1:5672", "169.254.169.254:5672"} {
		if err := deny("tcp", addr, nil); err == nil {
			t.Fatalf("guard allowed %s", addr)
		}
	}
	// allow_local lifts the restriction.
	allow := guard(true)
	for _, addr := range []string{"127.0.0.1:5672", "10.0.0.1:5672"} {
		if err := allow("tcp", addr, nil); err != nil {
			t.Fatalf("allow_local guard refused %s: %v", addr, err)
		}
	}
	// A public address is allowed either way.
	if err := deny("tcp", "93.184.216.34:5672", nil); err != nil {
		t.Fatalf("guard refused public address: %v", err)
	}
	// A malformed address is refused.
	if err := deny("tcp", "not-an-address", nil); err == nil {
		t.Fatal("guard allowed malformed address")
	}
}

func TestConnectorDescriptor(t *testing.T) {
	c := Connector()
	if c.Name != "amqp" {
		t.Fatalf("name = %q", c.Name)
	}
	if _, ok := c.Sinks["publish"]; !ok {
		t.Fatal("missing publish sink")
	}
	if _, ok := c.Sources["consume"]; !ok {
		t.Fatal("missing consume source")
	}
	// Schemas must be present and mark secret fields.
	for verb, want := range map[string]string{"publish": "x-shift-secret", "consume": "x-shift-secret"} {
		if !strings.Contains(string(c.Schemas[verb]), want) {
			t.Fatalf("%s schema missing %q", verb, want)
		}
	}
}

// TestConnectorFactories exercises every registered action factory and the
// descriptor build, so the studio-facing catalog is proven constructible.
func TestConnectorFactories(t *testing.T) {
	c := Connector()
	for name, f := range c.Sources {
		if f() == nil {
			t.Fatalf("source %q factory returned nil", name)
		}
	}
	for name, f := range c.Sinks {
		if f() == nil {
			t.Fatalf("sink %q factory returned nil", name)
		}
	}
}

func TestConfigHelpers(t *testing.T) {
	c := Config{TimeoutSeconds: 7}
	if c.timeout().Seconds() != 7 {
		t.Fatalf("timeout = %v", c.timeout())
	}
	// serverName from a URL and from the split host.
	if (&Config{URL: "amqp://h:5672/"}).serverName() != "h" {
		t.Fatal("serverName from url")
	}
	if (&Config{Host: "broker"}).serverName() != "broker" {
		t.Fatal("serverName from host")
	}
}

func TestGuardedDial(t *testing.T) {
	// Guard rejects a loopback target pre-connect when allow_local is off.
	c := &Config{TimeoutSeconds: 2}
	if _, err := c.guardedDial("tcp", "127.0.0.1:9"); err == nil {
		t.Fatal("guardedDial allowed loopback without allow_local")
	}
	// With allow_local it dials a real in-process listener (covers the success
	// path, including the handshake deadline).
	lc := net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		if conn, err := ln.Accept(); err == nil {
			_ = conn.Close()
		}
	}()
	ac := &Config{TimeoutSeconds: 2, AllowLocal: true}
	conn, err := ac.guardedDial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("guardedDial to listener: %v", err)
	}
	_ = conn.Close()
}

func TestConsumeHeaderTypes(t *testing.T) {
	fc := &fakeConsume{deliveries: []delivery{{
		Body:        []byte("x"),
		DeliveryTag: 1,
		Headers: map[string]any{
			"s":      "str",
			"raw":    []byte("bytes"),
			"b":      true,
			"i64":    int64(64),
			"i32":    int32(32),
			"f64":    3.5,
			"nil":    nil,
			"nested": map[string]any{"k": "v"}, // unrecognised → stringified
		},
	}}}
	s := openConsume(t, fc, `{"host":"broker","queue":"q"}`)
	defer func() { _ = s.Close() }()
	recs := drain(t, s)
	h, _ := recs[0].Field("headers")
	check := func(key string, want any) {
		v, ok := h.Field(key)
		if !ok {
			t.Fatalf("missing header %q", key)
		}
		switch w := want.(type) {
		case string:
			if v.String() != w {
				t.Fatalf("header %q = %q, want %q", key, v.String(), w)
			}
		case int64:
			if v.Int() != w {
				t.Fatalf("header %q = %d, want %d", key, v.Int(), w)
			}
		case bool:
			if v.Bool() != w {
				t.Fatalf("header %q bool mismatch", key)
			}
		case float64:
			if v.Float() != w {
				t.Fatalf("header %q float mismatch", key)
			}
		}
	}
	check("s", "str")
	check("raw", "bytes")
	check("b", true)
	check("i64", int64(64))
	check("i32", int64(32))
	check("f64", 3.5)
	if v, _ := h.Field("nil"); v.Kind() != record.KindNull {
		t.Fatalf("nil header kind = %v", v.Kind())
	}
	if v, _ := h.Field("nested"); !strings.Contains(v.String(), "map[") {
		t.Fatalf("nested header = %q, want stringified map", v.String())
	}
}

func TestConsumeBodyScalarsAndNesting(t *testing.T) {
	fc := &fakeConsume{deliveries: []delivery{
		{Body: []byte(`3.14`), DeliveryTag: 1},
		{Body: []byte(`null`), DeliveryTag: 2},
		{Body: []byte(`{"a":{"b":[true,"x"]}}`), DeliveryTag: 3},
	}}
	s := openConsume(t, fc, `{"host":"broker","queue":"q"}`)
	defer func() { _ = s.Close() }()
	recs := drain(t, s)

	if b, _ := recs[0].Field("body"); b.Kind() != record.KindFloat || b.Float() != 3.14 {
		t.Fatalf("float body = %v %v", b.Kind(), b.Float())
	}
	if b, _ := recs[1].Field("body"); b.Kind() != record.KindNull {
		t.Fatalf("null body kind = %v", b.Kind())
	}
	b2, _ := recs[2].Field("body")
	a, _ := b2.Field("a")
	inner, _ := a.Field("b")
	if inner.Kind() != record.KindList || inner.Len() != 2 || !inner.Index(0).Bool() || inner.Index(1).String() != "x" {
		t.Fatalf("nested body = %v", b2)
	}
}

func TestConsumeBodyTooDeepFallsBackToString(t *testing.T) {
	const depth = maxBodyDepth + 5
	body := strings.Repeat("[", depth) + "1" + strings.Repeat("]", depth)
	fc := &fakeConsume{deliveries: []delivery{{Body: []byte(body), DeliveryTag: 1}}}
	s := openConsume(t, fc, `{"host":"broker","queue":"q"}`)
	defer func() { _ = s.Close() }()
	recs := drain(t, s)
	if b, _ := recs[0].Field("body"); b.Kind() != record.KindString {
		t.Fatalf("over-deep body kind = %v, want string fallback", b.Kind())
	}
}

func TestOpenDialErrorsPropagate(t *testing.T) {
	boom := errors.New("dial failed")
	ps := &publishSink{dial: func(context.Context, *Config) (publishChannel, error) { return nil, boom }}
	if err := ps.Open(context.Background(), []byte(`{"host":"h","routing_key":"q"}`)); !errors.Is(err, boom) {
		t.Fatalf("publish open err = %v", err)
	}
	cs := &consumeSource{dial: func(context.Context, *Config) (consumeChannel, error) { return nil, boom }}
	if err := cs.Open(context.Background(), []byte(`{"host":"h","queue":"q"}`)); !errors.Is(err, boom) {
		t.Fatalf("consume open err = %v", err)
	}
}

func TestCloseWithoutOpen(t *testing.T) {
	if err := (&publishSink{}).Close(); err != nil {
		t.Fatalf("publish close (unopened) = %v", err)
	}
	if err := (&consumeSource{}).Close(); err != nil {
		t.Fatalf("consume close (unopened) = %v", err)
	}
}

// ---- optional real-broker integration (skipped by default) ----

// TestRealBrokerRoundTrip publishes then consumes against a live broker. It is
// skipped unless SHIFT_AMQP_TEST_URL is set and -short is off, so `make check`
// (which runs -short / SHIFT_COVERAGE) never needs a broker.
func TestRealBrokerRoundTrip(t *testing.T) {
	url := os.Getenv("SHIFT_AMQP_TEST_URL")
	if url == "" || testing.Short() {
		t.Skip("set SHIFT_AMQP_TEST_URL and drop -short to run the real-broker round trip")
	}
	ctx := context.Background()
	queue := "shift-amqp-test"
	pubCfg := fmt.Sprintf(`{"url":%q,"allow_local":true,"exchange":"","routing_key":%q}`, url, queue)
	conCfg := fmt.Sprintf(`{"url":%q,"allow_local":true,"queue":%q,"durable":false,"max_messages":3}`, url, queue)

	// Declare + drain any residue by consuming first (best effort).
	drainSrc := &consumeSource{}
	if err := drainSrc.Open(ctx, []byte(conCfg)); err != nil {
		t.Fatalf("open consume: %v", err)
	}
	_ = drain(t, drainSrc)
	_ = drainSrc.Close()

	sink := &publishSink{}
	if err := sink.Open(ctx, []byte(pubCfg)); err != nil {
		t.Fatalf("open publish: %v", err)
	}
	b := mapBatch(t,
		func(bld *record.Builder) { bld.KeyLiteral("n"); bld.Int(1) },
		func(bld *record.Builder) { bld.KeyLiteral("n"); bld.Int(2) },
		func(bld *record.Builder) { bld.KeyLiteral("n"); bld.Int(3) },
	)
	if err := sink.Write(ctx, b); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close publish: %v", err)
	}

	src := &consumeSource{}
	if err := src.Open(ctx, []byte(conCfg)); err != nil {
		t.Fatalf("open consume: %v", err)
	}
	defer func() { _ = src.Close() }()
	recs := drain(t, src)
	if len(recs) != 3 {
		t.Fatalf("round trip consumed %d, want 3", len(recs))
	}
}
