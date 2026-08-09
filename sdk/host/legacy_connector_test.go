package host

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/engine/spill"
	"github.com/aaron-au/shift/sdk"
	"github.com/aaron-au/shift/sdk/connectorpb"
)

// TC-007 (docs/assurance/test-conformance.md), connector half.
//
// ADR-0007 and ADR-0047 both promise that a connector artifact published
// against an older SDK keeps working: signed versions stay resolvable, and the
// host offers protocol 1 forever precisely so those builds still handshake.
// Until now that promise was only checked against the negotiation CODE — the
// host's own offer list and a fabricated Process struct. Nothing ever ran an
// older connector.
//
// WHAT THIS FILE FAITHFULLY REPRODUCES, and what it does not:
//
//   - The wire: a connector that advertises protocol 1 at handshake, refuses a
//     host whose offer omits its version (the pre-ADR-0051 sdk server's check,
//     verbatim), and encodes its frames with the protocol-1 codec
//     (spill.NewEncoderProtocol1 — the same tags version 1 defined, and only
//     those). It is driven through the CURRENT host: real subprocess spawn,
//     real Launch handshake, real Pull/Push streams.
//   - Not an archived binary. There is no v1 artifact in the tree, and a Go
//     test cannot resurrect one. What is reproduced is the observable
//     behaviour the protocol pins down; a genuine old build could still differ
//     in ways no wire-level test can see (that is what ADR-0047's
//     `behaviour-change` class is for).
//   - Not the hub schema half of TC-007. A populated v(N-1) database upgrade is
//     a store-package concern and is deliberately out of scope here.
const legacyProtocol uint32 = 1

// legacyToken is the spawn token the in-process legacy server expects.
const legacyToken = "legacy-connector-token" //nolint:gosec // G101: test-only value, not a credential

// legacyServer is a connector built the way one was BEFORE ADR-0051: it
// speaks protocol 1, and its codec knows only the tags version 1 defined.
//
// Hand-rolled against connectorpb rather than built on sdk.Serve because
// sdk.Serve necessarily reports the CURRENT protocol version — a connector
// that reports 2 is not the thing under test.
type legacyServer struct {
	connectorpb.UnimplementedConnectorServer
	// protocol is what this connector claims at handshake. Configurable so
	// the "genuinely unsupported version" case is the same code path.
	protocol uint32
	token    string
	done     chan struct{}

	received atomic.Int64
}

func newLegacyServer(protocol uint32, token string) *legacyServer {
	return &legacyServer{protocol: protocol, token: token, done: make(chan struct{})}
}

func (s *legacyServer) checkToken(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get("shift-token")
	if len(vals) == 1 && subtle.ConstantTimeCompare([]byte(vals[0]), []byte(s.token)) == 1 {
		return nil
	}
	return status.Error(codes.Unauthenticated, "missing or invalid connector token")
}

func (s *legacyServer) unaryAuth(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if err := s.checkToken(ctx); err != nil {
		return nil, err
	}
	return h(ctx, req)
}

func (s *legacyServer) streamAuth(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	if err := s.checkToken(ss.Context()); err != nil {
		return err
	}
	return h(srv, ss)
}

// Handshake is the old server's check, unchanged: a connector requires its own
// version to appear in the host's offer, which is exactly why the host may
// never stop offering 1.
func (s *legacyServer) Handshake(_ context.Context, req *connectorpb.HandshakeRequest) (*connectorpb.HandshakeResponse, error) {
	if !slices.Contains(req.GetProtocolVersions(), s.protocol) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"connector speaks protocol %d, host offered %v", s.protocol, req.GetProtocolVersions())
	}
	return &connectorpb.HandshakeResponse{
		ProtocolVersion: s.protocol,
		Name:            "legacy",
		Version:         "0.9.0",
		SourceActions:   []string{"count"},
		SinkActions:     []string{"collect"},
	}, nil
}

func (s *legacyServer) Health(context.Context, *connectorpb.HealthRequest) (*connectorpb.HealthResponse, error) {
	return &connectorpb.HealthResponse{Ok: true}, nil
}

// Pull emits {"i":n,"name":"row-n"} in batches of 100, encoded with the
// protocol-1 codec.
func (s *legacyServer) Pull(req *connectorpb.PullRequest, stream grpc.ServerStreamingServer[connectorpb.Frame]) error {
	if req.GetAction() != "count" {
		return status.Errorf(codes.NotFound, "unknown source action %q", req.GetAction())
	}
	var cfg struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal(req.GetConfig(), &cfg); err != nil {
		return status.Errorf(codes.InvalidArgument, "open count: %v", err)
	}
	batch := record.NewBatch()
	var buf bytes.Buffer
	enc := spill.NewEncoderProtocol1(&buf)
	for next := 0; next < cfg.N; {
		batch.Reset()
		bld := batch.Builder()
		for range 100 {
			if next >= cfg.N {
				break
			}
			bld.BeginMap()
			bld.KeyLiteral("i")
			bld.Int(int64(next))
			bld.KeyLiteral("name")
			bld.StringLiteral(fmt.Sprintf("row-%d", next))
			bld.EndMap()
			batch.Append(bld.Finish())
			next++
		}
		buf.Reset()
		for _, rec := range batch.Records() {
			if err := enc.Encode(rec); err != nil {
				return status.Errorf(codes.Internal, "encode: %v", err)
			}
		}
		if err := stream.Send(&connectorpb.Frame{Payload: buf.Bytes(), Records: int64(batch.Len())}); err != nil {
			return err
		}
	}
	return nil
}

// Push accepts frames and checks that every record it received is one a
// protocol-1 decoder could actually have parsed.
//
// Re-encoding with the protocol-1 encoder is the check: that encoder refuses
// exactly the kinds version 1 has no tag for, so a record that survives the
// round trip is one this connector's real decoder would have understood. A
// host that sent a decimal to a v1 connector would be caught HERE, in the
// connector, which is where the damage would actually land.
func (s *legacyServer) Push(stream grpc.ClientStreamingServer[connectorpb.PushMessage, connectorpb.PushSummary]) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "push stream closed before Open message")
	}
	open := first.GetOpen()
	if open == nil {
		return status.Error(codes.InvalidArgument, "first push message must be Open")
	}
	if open.GetAction() != "collect" {
		return status.Errorf(codes.NotFound, "unknown sink action %q", open.GetAction())
	}
	batch := record.NewBatch()
	var check bytes.Buffer
	v1 := spill.NewEncoderProtocol1(&check)
	var total int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		frame := msg.GetFrame()
		if frame == nil {
			return status.Error(codes.InvalidArgument, "expected frame message")
		}
		batch.Reset()
		r := bytes.NewReader(frame.GetPayload())
		dec := spill.NewDecoder(r, 0)
		bld := batch.Builder()
		for r.Len() > 0 {
			if err := dec.Decode(bld); err != nil {
				return status.Errorf(codes.InvalidArgument, "decode: %v", err)
			}
			batch.Append(bld.Finish())
		}
		for _, rec := range batch.Records() {
			check.Reset()
			if err := v1.Encode(rec); err != nil {
				return status.Errorf(codes.InvalidArgument,
					"the host sent a value this protocol-1 connector cannot decode: %v", err)
			}
		}
		total += int64(batch.Len())
		s.received.Add(int64(batch.Len()))
	}
	return stream.SendAndClose(&connectorpb.PushSummary{Records: total})
}

func (s *legacyServer) Shutdown(context.Context, *connectorpb.ShutdownRequest) (*connectorpb.ShutdownResponse, error) {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return &connectorpb.ShutdownResponse{}, nil
}

// serveLegacyOn runs the legacy connector on socket until Shutdown or the
// listener closes. Used by both the in-process helper and the re-exec mode.
func serveLegacyOn(socket string, s *legacyServer) error {
	// Mirrors sdk.ServeOn: a stale socket from a crashed predecessor would fail
	// the bind. The path is host-provided per the ADR-0007 spawn contract.
	if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) { //nolint:gosec // G703: socket path comes from the spawning host (test harness), never external input
		return err
	}
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", socket)
	if err != nil {
		return err
	}
	gs := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxFrameBytes),
		grpc.MaxSendMsgSize(maxFrameBytes),
		grpc.UnaryInterceptor(s.unaryAuth),
		grpc.StreamInterceptor(s.streamAuth),
	)
	connectorpb.RegisterConnectorServer(gs, s)
	go func() {
		<-s.done
		gs.GracefulStop()
	}()
	return gs.Serve(lis)
}

// serveLegacyInProcess starts the legacy connector on a private socket and
// returns its socket path. Cleanup stops it, so the package-wide goroutine
// leak check (TC-001) still holds.
func serveLegacyInProcess(t *testing.T, s *legacyServer) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "legacy.sock")
	errc := make(chan error, 1)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			select {
			case <-s.done:
			default:
				close(s.done)
			}
		})
	}
	go func() { errc <- serveLegacyOn(socket, s) }()
	t.Cleanup(func() {
		stop()
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("legacy serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("legacy connector did not stop")
		}
	})
	return socket
}

// TestAConnectorSpeakingTheOlderProtocolStillLaunchesAndStreams drives a
// protocol-1 connector, as a real spawned subprocess, through the current
// host's Launch + handshake + Pull path. This is the ADR-0047 promise made
// executable: an artifact published before ADR-0051 keeps running.
func TestAConnectorSpeakingTheOlderProtocolStillLaunchesAndStreams(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess")
	}
	t.Setenv("SHIFT_HOST_TEST_MODE", "serve-legacy")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := Launch(ctx, os.Args[0], LaunchOptions{HandshakeTimeout: 15 * time.Second})
	if err != nil {
		t.Fatalf("launching a protocol-%d connector must still work: %v", legacyProtocol, err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// The host negotiated DOWN, and knows it did: the recorded version is what
	// every later encoding decision turns on.
	if got := p.Info().ProtocolVersion; got != legacyProtocol {
		t.Fatalf("negotiated protocol = %d, want %d", got, legacyProtocol)
	}
	if p.Info().Name != "legacy" || p.Info().Version != "0.9.0" {
		t.Fatalf("identity = %+v", p.Info())
	}
	if err := p.Health(ctx); err != nil {
		t.Fatalf("health: %v", err)
	}

	// The data path works: protocol-1 frames decode in the current host.
	src := p.Source("count", []byte(`{"n":250}`))
	var n int
	for {
		b, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("pulling from a protocol-1 connector: %v", err)
		}
		for i := range b.Len() {
			rec := b.Record(i)
			got, ok := rec.Field("i")
			if !ok || got.Kind() != record.KindInt || got.Int() != int64(n) {
				t.Fatalf("record %d has i=%v (present %v), want %d", n, got, ok, n)
			}
			n++
		}
	}
	if n != 250 {
		t.Fatalf("pulled %d records, want 250", n)
	}
}

// TestTheHostProtectsAnOlderConnectorFromTheExactKinds: negotiating down is
// only half the promise. The other half is that the host then RESTRICTS itself
// — a decimal encoded for a protocol-1 connector would meet a tag its decoder
// never had. The refusal happens host-side, before anything is sent, and says
// which protocol it is protecting.
//
// Driven through a real protocol-1 connector rather than a fabricated Process,
// so the connector itself is the one confirming what it received.
func TestTheHostProtectsAnOlderConnectorFromTheExactKinds(t *testing.T) {
	legacy := newLegacyServer(legacyProtocol, legacyToken)
	socket := serveLegacyInProcess(t, legacy)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := Attach(ctx, socket, legacyToken, 5*time.Second)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if got := p.Info().ProtocolVersion; got != legacyProtocol {
		t.Fatalf("negotiated protocol = %d, want %d", got, legacyProtocol)
	}

	// Ordinary values still flow, and the connector confirms them.
	ordinary := record.NewBatch()
	bld := ordinary.Builder()
	for i := range 3 {
		bld.BeginMap()
		bld.KeyLiteral("id")
		bld.Int(int64(i))
		bld.KeyLiteral("name")
		bld.StringLiteral("ada")
		bld.EndMap()
		ordinary.Append(bld.Finish())
	}
	sink := p.Sink("collect", nil)
	if err := sink.Write(ctx, ordinary); err != nil {
		t.Fatalf("writing ordinary values to a protocol-1 connector: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close sink: %v", err)
	}
	if sink.Records != 3 || legacy.received.Load() != 3 {
		t.Fatalf("connector confirmed %d records (saw %d), want 3", sink.Records, legacy.received.Load())
	}

	// An exact decimal does not. The host refuses it locally: the connector's
	// counter must not move, because nothing was sent.
	exact := record.NewBatch()
	ebld := exact.Builder()
	ebld.BeginMap()
	ebld.KeyLiteral("amount")
	ebld.Decimal(1010, 2)
	ebld.EndMap()
	exact.Append(ebld.Finish())

	exactSink := p.Sink("collect", nil)
	err = exactSink.Write(ctx, exact)
	if err == nil {
		t.Fatal("the host sent an ADR-0051 decimal to a protocol-1 connector")
	}
	if !strings.Contains(err.Error(), "protocol 1") {
		t.Errorf("error = %v; it must name the protocol being protected", err)
	}
	if legacy.received.Load() != 3 {
		t.Errorf("connector received %d records; the refused batch must never leave the host",
			legacy.received.Load())
	}
	_ = exactSink.Close()
}

// TestAConnectorSpeakingAnUnsupportedProtocolIsRejectedWithAClearError is the
// other side of the compatibility matrix: a version the host genuinely cannot
// speak must fail, and fail legibly, rather than half-connecting.
//
// The rejection comes from the CONNECTOR (its own version is absent from the
// host's offer), which is the direction the protocol was designed around — the
// connector is the party that knows what it can decode. The host surfaces that
// status verbatim, so an operator reads the real cause.
//
// Honest limit: the host RETRIES a handshake failure until its deadline rather
// than failing fast on FailedPrecondition, so the error arrives as a timeout
// wrapping the connector's status. The timeout here is short on purpose.
func TestAConnectorSpeakingAnUnsupportedProtocolIsRejectedWithAClearError(t *testing.T) {
	const fromTheFuture uint32 = 99
	if slices.Contains(sdk.SupportedProtocolVersions(), fromTheFuture) {
		t.Fatalf("protocol %d is now supported; this test needs a version the host does not offer", fromTheFuture)
	}
	socket := serveLegacyInProcess(t, newLegacyServer(fromTheFuture, legacyToken))

	_, err := Attach(context.Background(), socket, legacyToken, 300*time.Millisecond)
	if err == nil {
		t.Fatal("the host accepted a connector speaking a protocol it does not support")
	}
	if !strings.Contains(err.Error(), "protocol 99") {
		t.Errorf("error = %v; it must carry the connector's own explanation", err)
	}
}
