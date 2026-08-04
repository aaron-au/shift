package host

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/spill"
	"github.com/aaron-au/shift/sdk/connectorpb"
)

// Source returns a stream.Source that pulls batches from a connector
// source action. The Pull stream opens on the first Next call.
func (p *Process) Source(action string, config []byte) *SourceStream {
	return &SourceStream{p: p, action: action, config: config, batch: record.NewBatch()}
}

// ResumeFrom sets the opaque position the stream restarts at (ADR-0037). It
// must be called before the first Next, since the cursor travels in the Pull
// request that Next opens. An empty cursor is a no-op, so a caller can pass
// whatever the hub returned without testing it first.
//
// A cursor sent to an action that cannot resume fails the stream rather than
// silently starting over: the runner asked to continue from a position, and a
// quiet full replay would let it record progress that never happened.
func (s *SourceStream) ResumeFrom(cur []byte) *SourceStream {
	s.resumeFrom = cur
	return s
}

// Checkpoint returns the most recent position the connector reported as safe
// to resume from, or nil if it has reported none. It advances as batches
// arrive, so a caller must pair it with its own record of how far downstream
// processing has actually got — this is source progress, not confirmed
// progress, and persisting it directly would skip records that were read but
// never written.
func (s *SourceStream) Checkpoint() []byte { return s.checkpoint }

// SourceStream adapts a connector Pull stream to stream.Source. The batch
// returned by Next is valid until the next Next/Close call.
type SourceStream struct {
	p          *Process
	action     string
	config     []byte
	resumeFrom []byte
	checkpoint []byte
	stream     frameRecvStream
	cancel     context.CancelFunc
	batch      *record.Batch
	done       bool
}

// frameRecvStream is the minimal recv surface of the Pull stream.
type frameRecvStream interface {
	Recv() (*connectorpb.Frame, error)
}

func (s *SourceStream) Next(ctx context.Context) (*record.Batch, error) {
	if s.done {
		return nil, io.EOF
	}
	if s.stream == nil {
		sctx, cancel := context.WithCancel(s.p.withToken(ctx))
		stream, err := s.p.client.Pull(sctx, &connectorpb.PullRequest{
			Action: s.action, Config: s.config, ResumeFrom: s.resumeFrom,
		})
		if err != nil {
			cancel()
			return nil, fmt.Errorf("host: pull %s: %w", s.action, err)
		}
		s.stream = stream
		s.cancel = cancel
	}
	frame, err := s.stream.Recv()
	if err != nil {
		s.done = true
		s.cancel()
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("host: %s: %w", s.action, err)
	}
	s.batch.Reset()
	if err := decodeInto(frame.GetPayload(), s.batch); err != nil {
		s.done = true
		s.cancel()
		return nil, err
	}
	// Only advance on a frame that actually carries one: a connector reports
	// nil mid-page (no safe position yet), and overwriting the last good
	// cursor with that would throw away resumable progress.
	if cp := frame.GetCheckpoint(); len(cp) > 0 {
		s.checkpoint = cp
	}
	return s.batch, nil
}

func (s *SourceStream) Close() error {
	s.done = true
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

// Sink returns a stream.Sink that pushes batches into a connector sink
// action. The Push stream opens on the first Write call.
func (p *Process) Sink(action string, config []byte) *SinkStream {
	s := &SinkStream{p: p, action: action, config: config}
	s.enc = spill.NewEncoder(&s.buf)
	return s
}

// SinkStream adapts a connector Push stream to stream.Sink.
type SinkStream struct {
	p      *Process
	action string
	config []byte
	stream connectorpb.Connector_PushClient
	cancel context.CancelFunc
	buf    bytes.Buffer
	enc    *spill.Encoder
	// Records reports the connector-confirmed record count after Close.
	Records int64
}

func (s *SinkStream) Write(ctx context.Context, b *record.Batch) error {
	if s.stream == nil {
		sctx, cancel := context.WithCancel(s.p.withToken(ctx))
		stream, err := s.p.client.Push(sctx)
		if err != nil {
			cancel()
			return fmt.Errorf("host: push %s: %w", s.action, err)
		}
		s.stream = stream
		s.cancel = cancel
		open := &connectorpb.PushMessage{Msg: &connectorpb.PushMessage_Open{
			Open: &connectorpb.PushOpen{Action: s.action, Config: s.config},
		}}
		if err := stream.Send(open); err != nil {
			return s.sendErr(err)
		}
	}
	s.buf.Reset()
	for _, rec := range b.Records() {
		if err := s.enc.Encode(rec); err != nil {
			return fmt.Errorf("host: encode frame: %w", err)
		}
	}
	msg := &connectorpb.PushMessage{Msg: &connectorpb.PushMessage_Frame{
		Frame: &connectorpb.Frame{Payload: s.buf.Bytes(), Records: int64(b.Len())},
	}}
	if err := s.stream.Send(msg); err != nil {
		return s.sendErr(err)
	}
	return nil
}

// sendErr surfaces the server's actual error: a failed Send reports io.EOF
// and parks the real status on CloseAndRecv.
func (s *SinkStream) sendErr(err error) error {
	if errors.Is(err, io.EOF) {
		if _, rerr := s.stream.CloseAndRecv(); rerr != nil {
			err = rerr
		}
	}
	return fmt.Errorf("host: %s: %w", s.action, err)
}

func (s *SinkStream) Close() error {
	if s.stream == nil {
		return nil // nothing was written; nothing to flush
	}
	defer s.cancel()
	sum, err := s.stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("host: %s: %w", s.action, err)
	}
	s.Records = sum.GetRecords()
	return nil
}

func decodeInto(payload []byte, batch *record.Batch) error {
	r := bytes.NewReader(payload)
	dec := spill.NewDecoder(r, 0)
	bld := batch.Builder()
	for r.Len() > 0 {
		if err := dec.Decode(bld); err != nil {
			return fmt.Errorf("host: decode frame: %w", err)
		}
		batch.Append(bld.Finish())
	}
	return nil
}
