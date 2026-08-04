package sdktest_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/sdk"
	"github.com/aaron-au/shift/sdk/sdktest"
)

// resumableCount emits {"i": 0..n-1} one record per batch and reports its
// position as a plain decimal cursor. Minimal on purpose: this exercises the
// WIRE contract (ADR-0037), not a connector's cursor design.
type resumableCount struct {
	n, next int
	batch   *record.Batch
	// resumedWith records what Resume was handed, so a test can prove the
	// bytes crossed the process boundary unchanged.
	resumedWith string
}

func (s *resumableCount) Open(_ context.Context, config []byte) error {
	var cfg struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal(config, &cfg); err != nil {
		return err
	}
	s.n, s.batch = cfg.N, record.NewBatch()
	return nil
}

func (s *resumableCount) Next(context.Context) (*record.Batch, error) {
	if s.next >= s.n {
		return nil, io.EOF
	}
	s.batch.Reset()
	bld := s.batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("i")
	bld.Int(int64(s.next))
	bld.EndMap()
	s.batch.Append(bld.Finish())
	s.next++
	return s.batch, nil
}

func (s *resumableCount) Close() error { return nil }

func (s *resumableCount) Resume(_ context.Context, cur []byte) error {
	s.resumedWith = string(cur)
	if len(cur) == 0 {
		return nil
	}
	n, err := strconv.Atoi(string(cur))
	if err != nil {
		return errors.New("bad cursor")
	}
	s.next = n
	return nil
}

func (s *resumableCount) Checkpoint() []byte { return []byte(strconv.Itoa(s.next)) }

func resumeConnector() sdk.Connector {
	return sdk.Connector{
		Name: "resumetest", Version: "0.0.1",
		Sources: map[string]func() sdk.SourceAction{
			"count": func() sdk.SourceAction { return &resumableCount{} },
			// Wrapping in a struct that embeds only the SourceAction interface
			// strips the optional capability — the shape of every connector
			// that has not opted in.
			"plain": func() sdk.SourceAction { return &struct{ sdk.SourceAction }{&resumableCount{}} },
		},
	}
}

// The cursor must survive the gRPC hop in both directions: out in the Pull
// request, back on each Frame. A drop in either direction would silently
// degrade every resume to a full replay, and nothing downstream would notice.
func TestCursorCrossesTheWireBothWays(t *testing.T) {
	p := sdktest.Serve(t, resumeConnector())

	// Read from the start; the checkpoint rides back on the frames.
	src := p.Source("count", []byte(`{"n":5}`))
	var seen []int64
	for {
		b, err := src.Next(t.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, r := range b.Records() {
			v, _ := r.Field("i")
			seen = append(seen, v.Int())
		}
	}
	_ = src.Close()
	if len(seen) != 5 {
		t.Fatalf("read %d records, want 5", len(seen))
	}
	if got := string(src.Checkpoint()); got != "5" {
		t.Fatalf("checkpoint = %q, want %q — the frame's cursor did not reach the host", got, "5")
	}

	// Resume from the middle: the request carries the cursor outbound.
	again := p.Source("count", []byte(`{"n":5}`)).ResumeFrom([]byte("3"))
	seen = nil
	for {
		b, err := again.Next(t.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		for _, r := range b.Records() {
			v, _ := r.Field("i")
			seen = append(seen, v.Int())
		}
	}
	_ = again.Close()
	if len(seen) != 2 || seen[0] != 3 {
		t.Fatalf("resumed stream = %v, want [3 4]", seen)
	}
}

// An empty cursor must be indistinguishable from not resuming, so the runner
// can forward whatever the hub returned without testing it first.
func TestEmptyCursorIsANoOpOverTheWire(t *testing.T) {
	p := sdktest.Serve(t, resumeConnector())
	src := p.Source("count", []byte(`{"n":3}`)).ResumeFrom(nil)
	n := 0
	for {
		_, err := src.Next(t.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		n++
	}
	if n != 3 {
		t.Fatalf("read %d records, want 3 — an empty cursor must read from the start", n)
	}
}

// Sending a cursor to an action that cannot honour it must FAIL, not quietly
// start over. A silent full replay would let the runner record progress that
// never happened, and the next resume would skip that range for real.
func TestResumingANonResumableSourceFails(t *testing.T) {
	p := sdktest.Serve(t, resumeConnector())
	src := p.Source("plain", []byte(`{"n":3}`)).ResumeFrom([]byte("1"))
	_, err := src.Next(t.Context())
	if err == nil {
		t.Fatal("a non-resumable source accepted a cursor")
	}
	if !strings.Contains(err.Error(), "ResumableSource") {
		t.Fatalf("err = %v, want it to name the missing capability", err)
	}
	_ = src.Close()
}

// A source that reports no safe position yet must not clobber the last good
// cursor — mid-page nil is normal, and overwriting would throw away progress.
func TestNilCheckpointDoesNotClobberTheLastGoodOne(t *testing.T) {
	p := sdktest.Serve(t, sdk.Connector{
		Name: "haltingcheckpoint", Version: "0.0.1",
		Sources: map[string]func() sdk.SourceAction{
			"count": func() sdk.SourceAction { return &nilAfterFirst{batch: record.NewBatch()} },
		},
	})
	src := p.Source("count", []byte(`{}`))
	for {
		_, err := src.Next(t.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
	}
	if got := string(src.Checkpoint()); got != "first" {
		t.Fatalf("checkpoint = %q, want %q retained across the nil reports", got, "first")
	}
	_ = src.Close()
}

// nilAfterFirst reports a cursor on its first batch and nil thereafter.
type nilAfterFirst struct {
	batch *record.Batch
	n     int
}

func (s *nilAfterFirst) Open(context.Context, []byte) error { return nil }

func (s *nilAfterFirst) Next(context.Context) (*record.Batch, error) {
	if s.n >= 3 {
		return nil, io.EOF
	}
	s.batch.Reset()
	bld := s.batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("i")
	bld.Int(int64(s.n))
	bld.EndMap()
	s.batch.Append(bld.Finish())
	s.n++
	return s.batch, nil
}

func (s *nilAfterFirst) Close() error                         { return nil }
func (s *nilAfterFirst) Resume(context.Context, []byte) error { return nil }

func (s *nilAfterFirst) Checkpoint() []byte {
	if s.n == 1 {
		return []byte("first")
	}
	return nil
}
