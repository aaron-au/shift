package sdktest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/stream"
	"github.com/aaron-au/shift/sdk"
	"github.com/aaron-au/shift/sdk/connectorpb"
	"github.com/aaron-au/shift/sdk/host"
	"github.com/aaron-au/shift/sdk/sdktest"
)

// countSource emits {"i": 0..n-1, "name": "rec-i"} in batches of 100.
type countSource struct {
	n, failAt, next int
	batch           *record.Batch
}

func (s *countSource) Open(_ context.Context, config []byte) error {
	var cfg struct {
		N      int `json:"n"`
		FailAt int `json:"fail_at"`
	}
	if err := json.Unmarshal(config, &cfg); err != nil {
		return err
	}
	if cfg.N <= 0 {
		return fmt.Errorf("n must be positive")
	}
	s.n, s.failAt = cfg.N, cfg.FailAt
	s.batch = record.NewBatch()
	return nil
}

func (s *countSource) Next(context.Context) (*record.Batch, error) {
	if s.next >= s.n {
		return nil, io.EOF
	}
	s.batch.Reset()
	bld := s.batch.Builder()
	for range 100 {
		if s.next >= s.n {
			break
		}
		if s.failAt > 0 && s.next == s.failAt {
			return nil, fmt.Errorf("injected source failure at %d", s.next)
		}
		bld.BeginMap()
		bld.KeyLiteral("i")
		bld.Int(int64(s.next))
		bld.KeyLiteral("name")
		bld.StringLiteral(fmt.Sprintf("rec-%d", s.next))
		bld.EndMap()
		s.batch.Append(bld.Finish())
		s.next++
	}
	return s.batch, nil
}

func (s *countSource) Close() error { return nil }

// collectSink verifies ids ascend and counts records.
type collectSink struct {
	seen   *atomic.Int64
	closed *atomic.Bool
	failAt int64
	last   int64
}

func (s *collectSink) Open(_ context.Context, config []byte) error {
	var cfg struct {
		FailAt int64 `json:"fail_at"`
	}
	if len(config) > 0 {
		if err := json.Unmarshal(config, &cfg); err != nil {
			return err
		}
	}
	s.failAt = cfg.FailAt
	s.last = -1
	return nil
}

func (s *collectSink) Write(_ context.Context, b *record.Batch) error {
	for _, rec := range b.Records() {
		v, ok := rec.Field("i")
		if !ok {
			return fmt.Errorf("record missing i")
		}
		if v.Int() <= s.last {
			return fmt.Errorf("ids not ascending: %d after %d", v.Int(), s.last)
		}
		s.last = v.Int()
		n := s.seen.Add(1)
		if s.failAt > 0 && n == s.failAt {
			return fmt.Errorf("injected sink failure at %d", n)
		}
	}
	return nil
}

func (s *collectSink) Close() error {
	s.closed.Store(true)
	return nil
}

type testState struct {
	seen   atomic.Int64
	closed atomic.Bool
}

func testConnector(st *testState) sdk.Connector {
	return sdk.Connector{
		Name:    "test",
		Version: "0.0.1",
		Sources: map[string]func() sdk.SourceAction{
			"count": func() sdk.SourceAction { return &countSource{} },
		},
		Sinks: map[string]func() sdk.SinkAction{
			"collect": func() sdk.SinkAction { return &collectSink{seen: &st.seen, closed: &st.closed} },
		},
	}
}

func TestHandshakeReportsActions(t *testing.T) {
	p := sdktest.Serve(t, testConnector(&testState{}))
	info := p.Info()
	if info.Name != "test" || info.ProtocolVersion != sdk.ProtocolVersion {
		t.Fatalf("info = %+v", info)
	}
	if len(info.SourceActions) != 1 || info.SourceActions[0] != "count" {
		t.Fatalf("sources = %v", info.SourceActions)
	}
	if len(info.SinkActions) != 1 || info.SinkActions[0] != "collect" {
		t.Fatalf("sinks = %v", info.SinkActions)
	}
	if err := p.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPipelineThroughConnector(t *testing.T) {
	st := &testState{}
	p := sdktest.Serve(t, testConnector(st))

	src := p.Source("count", []byte(`{"n":5000}`))
	sink := p.Sink("collect", nil)
	pipe := stream.New(src, "connector-src").
		Filter("evens", func(v record.Value) bool {
			i, _ := v.Field("i")
			return i.Int()%2 == 0
		})
	rep, err := pipe.Run(context.Background(), sink, "connector-sink")
	if err != nil {
		t.Fatal(err)
	}
	if rep.RecordsOut != 2500 {
		t.Fatalf("records out = %d", rep.RecordsOut)
	}
	if sink.Records != 2500 {
		t.Fatalf("connector-confirmed records = %d", sink.Records)
	}
	if st.seen.Load() != 2500 || !st.closed.Load() {
		t.Fatalf("sink state: seen=%d closed=%v", st.seen.Load(), st.closed.Load())
	}
}

func TestWrongTokenRejected(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "conn.sock")
	go func() { _ = sdk.ServeOn(socket, "right-token", testConnector(&testState{})) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := host.Attach(ctx, socket, "wrong-token", 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("err = %v, want token rejection", err)
	}
}

func TestProtocolVersionMismatch(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "conn.sock")
	go func() { _ = sdk.ServeOn(socket, sdktest.TestToken, testConnector(&testState{})) }()

	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client := connectorpb.NewConnectorClient(conn)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "shift-token", sdktest.TestToken)

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err = client.Handshake(ctx, &connectorpb.HandshakeRequest{ProtocolVersions: []uint32{99}})
		if status.Code(err) != codes.Unavailable || time.Now().After(deadline) {
			break // server is up (or we timed out waiting for it)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v, want FailedPrecondition", err)
	}
}

func TestUnknownActions(t *testing.T) {
	p := sdktest.Serve(t, testConnector(&testState{}))
	ctx := context.Background()

	src := p.Source("nope", nil)
	if _, err := src.Next(ctx); status.Code(err) != codes.NotFound {
		t.Fatalf("source err = %v, want NotFound", err)
	}
	sink := p.Sink("nope", nil)
	err := sink.Write(ctx, record.NewBatch())
	if err == nil {
		err = sink.Close()
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("sink err = %v, want NotFound", err)
	}
}

func TestSourceErrorPropagates(t *testing.T) {
	p := sdktest.Serve(t, testConnector(&testState{}))
	src := p.Source("count", []byte(`{"n":1000,"fail_at":250}`))
	ctx := context.Background()
	var err error
	for {
		if _, err = src.Next(ctx); err != nil {
			break
		}
	}
	if !strings.Contains(err.Error(), "injected source failure at 250") {
		t.Fatalf("err = %v", err)
	}
}

func TestSinkErrorPropagates(t *testing.T) {
	st := &testState{}
	p := sdktest.Serve(t, testConnector(st))
	src := p.Source("count", []byte(`{"n":1000}`))
	sink := p.Sink("collect", []byte(`{"fail_at":150}`))
	_, err := stream.New(src, "src").Run(context.Background(), sink, "sink")
	if err == nil || !strings.Contains(err.Error(), "injected sink failure at 150") {
		t.Fatalf("err = %v", err)
	}
}

func TestSinkCloseWithoutWrites(t *testing.T) {
	p := sdktest.Serve(t, testConnector(&testState{}))
	sink := p.Sink("collect", nil)
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if sink.Records != 0 {
		t.Fatalf("records = %d", sink.Records)
	}
}

func TestBadConfigRejected(t *testing.T) {
	p := sdktest.Serve(t, testConnector(&testState{}))
	src := p.Source("count", []byte(`{"n":0}`))
	if _, err := src.Next(context.Background()); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}
