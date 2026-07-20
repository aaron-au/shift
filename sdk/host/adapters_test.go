package host

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/spill"
	"github.com/aaron-au/shift/sdk/connectorpb"
)

// ---- Source stream --------------------------------------------------------

func TestSourceHappyPath(t *testing.T) {
	p := attachConn(t, reexecConnector(&testState{}))
	src := p.Source("count", []byte(`{"n":350}`))
	ctx := context.Background()
	var got int64
	var last int64 = -1
	for {
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, rec := range b.Records() {
			v, ok := rec.Field("i")
			if !ok {
				t.Fatal("record missing i")
			}
			if v.Int() != last+1 {
				t.Fatalf("out of order: %d after %d", v.Int(), last)
			}
			last = v.Int()
			got++
		}
	}
	if got != 350 {
		t.Fatalf("got %d records, want 350", got)
	}
	// Next after EOF stays EOF (done branch).
	if _, err := src.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("post-EOF next = %v, want EOF", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestSourceOpenConfigError(t *testing.T) {
	// n=0 fails Open server-side -> InvalidArgument surfaced on first Recv.
	p := attachConn(t, reexecConnector(&testState{}))
	src := p.Source("count", []byte(`{"n":0}`))
	_, err := src.Next(context.Background())
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
}

func TestSourceUnknownAction(t *testing.T) {
	p := attachConn(t, reexecConnector(&testState{}))
	src := p.Source("nope", nil)
	_, err := src.Next(context.Background())
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v, want NotFound", err)
	}
	// The stream is marked done and re-Next returns EOF.
	if _, err := src.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("re-next = %v, want EOF", err)
	}
}

func TestSourceMidStreamError(t *testing.T) {
	p := attachConn(t, reexecConnector(&testState{}))
	src := p.Source("count", []byte(`{"n":1000,"fail_at":250}`))
	ctx := context.Background()
	var err error
	for {
		if _, err = src.Next(ctx); err != nil {
			break
		}
	}
	if err == nil || !strings.Contains(err.Error(), "injected source failure at 250") {
		t.Fatalf("err = %v", err)
	}
}

func TestSourceContextCanceled(t *testing.T) {
	p := attachConn(t, reexecConnector(&testState{}))
	src := p.Source("count", []byte(`{"n":10000}`))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := src.Next(ctx); err == nil {
		t.Fatal("expected error from canceled context")
	}
}

func TestSourceCloseBeforeNext(t *testing.T) {
	p := attachConn(t, reexecConnector(&testState{}))
	src := p.Source("count", []byte(`{"n":5}`))
	// Close before any Next: cancel is nil, must not panic.
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := src.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("next after close = %v, want EOF", err)
	}
}

// fakeRecv drives SourceStream.Next with canned frames/errors.
type fakeRecv struct {
	steps []func() (*connectorpb.Frame, error)
	i     int
}

func (f *fakeRecv) Recv() (*connectorpb.Frame, error) {
	if f.i >= len(f.steps) {
		return nil, io.EOF
	}
	step := f.steps[f.i]
	f.i++
	return step()
}

func TestSourceDecodeError(t *testing.T) {
	// A frame carrying a corrupt payload must fail decodeInto and mark the
	// stream done. The unexported fields are reachable from this in-package
	// test, so we bypass the gRPC layer with a fake recv stream.
	_, cancel := context.WithCancel(context.Background())
	s := &SourceStream{
		stream: &fakeRecv{steps: []func() (*connectorpb.Frame, error){
			func() (*connectorpb.Frame, error) {
				return &connectorpb.Frame{Payload: []byte{0xff, 0xff, 0xff, 0xff}, Records: 1}, nil
			},
		}},
		cancel: cancel,
		batch:  record.NewBatch(),
	}
	_, err := s.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "decode frame") {
		t.Fatalf("err = %v, want decode error", err)
	}
	if !s.done {
		t.Fatal("stream should be marked done after decode error")
	}
}

func TestSourceRecvError(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	s := &SourceStream{
		action: "boom",
		stream: &fakeRecv{steps: []func() (*connectorpb.Frame, error){
			func() (*connectorpb.Frame, error) { return nil, errors.New("wire broke") },
		}},
		cancel: cancel,
		batch:  record.NewBatch(),
	}
	_, err := s.Next(context.Background())
	if err == nil || !strings.Contains(err.Error(), "wire broke") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want wrapped recv error", err)
	}
	if !s.done {
		t.Fatal("stream should be done after recv error")
	}
}

func TestSourceRecvEOF(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	s := &SourceStream{
		stream: &fakeRecv{}, // no steps -> immediate io.EOF
		cancel: cancel,
		batch:  record.NewBatch(),
	}
	if _, err := s.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
	if !s.done {
		t.Fatal("stream should be done after EOF")
	}
}

func TestSourceDecodeSuccessViaFake(t *testing.T) {
	var buf bytes.Buffer
	enc := spill.NewEncoder(&buf)
	b := record.NewBatch()
	bld := b.Builder()
	for i := range 3 {
		bld.BeginMap()
		bld.KeyLiteral("i")
		bld.Int(int64(i))
		bld.EndMap()
		b.Append(bld.Finish())
	}
	for _, rec := range b.Records() {
		if err := enc.Encode(rec); err != nil {
			t.Fatal(err)
		}
	}
	payload := buf.Bytes()

	_, cancel := context.WithCancel(context.Background())
	s := &SourceStream{
		stream: &fakeRecv{steps: []func() (*connectorpb.Frame, error){
			func() (*connectorpb.Frame, error) {
				return &connectorpb.Frame{Payload: payload, Records: 3}, nil
			},
		}},
		cancel: cancel,
		batch:  record.NewBatch(),
	}
	out, err := s.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if out.Len() != 3 {
		t.Fatalf("decoded %d records, want 3", out.Len())
	}
}

// ---- Sink stream ----------------------------------------------------------

func TestSinkHappyPath(t *testing.T) {
	st := &testState{}
	p := attachConn(t, reexecConnector(st))
	sink := p.Sink("collect", nil)
	ctx := context.Background()

	src := p.Source("count", []byte(`{"n":450}`))
	for {
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if err := sink.Write(ctx, b); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if sink.Records != 450 {
		t.Fatalf("confirmed %d records, want 450", sink.Records)
	}
	if st.seen.Load() != 450 || !st.closed.Load() {
		t.Fatalf("sink state seen=%d closed=%v", st.seen.Load(), st.closed.Load())
	}
}

func TestSinkCloseWithoutWrites(t *testing.T) {
	p := attachConn(t, reexecConnector(&testState{}))
	sink := p.Sink("collect", nil)
	// Close with no writes: nil stream -> no-op.
	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if sink.Records != 0 {
		t.Fatalf("records = %d, want 0", sink.Records)
	}
}

func TestSinkUnknownAction(t *testing.T) {
	p := attachConn(t, reexecConnector(&testState{}))
	sink := p.Sink("nope", nil)
	b := record.NewBatch()
	bld := b.Builder()
	bld.BeginMap()
	bld.KeyLiteral("i")
	bld.Int(1)
	bld.EndMap()
	b.Append(bld.Finish())

	// The server rejects the action; the failure surfaces on Write (via the
	// sendErr EOF->CloseAndRecv path) or on Close.
	err := sink.Write(context.Background(), b)
	if err == nil {
		err = sink.Close()
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v, want NotFound", err)
	}
}

func TestSinkWriteError(t *testing.T) {
	// A sink that fails mid-stream surfaces the connector's error.
	st := &testState{}
	p := attachConn(t, reexecConnector(st))
	sink := p.Sink("collect", []byte(`{"fail_at":150}`))
	ctx := context.Background()
	src := p.Source("count", []byte(`{"n":1000}`))
	var err error
	for {
		b, nerr := src.Next(ctx)
		if errors.Is(nerr, io.EOF) {
			break
		}
		if nerr != nil {
			t.Fatalf("next: %v", nerr)
		}
		if err = sink.Write(ctx, b); err != nil {
			break
		}
	}
	if err == nil {
		err = sink.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "injected sink failure at 150") {
		t.Fatalf("err = %v, want injected sink failure", err)
	}
}

// ---- decodeInto -----------------------------------------------------------

func TestDecodeIntoEmpty(t *testing.T) {
	b := record.NewBatch()
	if err := decodeInto(nil, b); err != nil {
		t.Fatalf("decodeInto(nil) = %v", err)
	}
	if b.Len() != 0 {
		t.Fatalf("len = %d, want 0", b.Len())
	}
}

func TestDecodeIntoCorrupt(t *testing.T) {
	if err := decodeInto([]byte{0x01, 0x02, 0x03}, record.NewBatch()); err == nil {
		t.Fatal("expected decode error on corrupt payload")
	}
}
