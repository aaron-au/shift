package sdk

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/aaron-au/shift/engine/record"
	"github.com/aaron-au/shift/sdk/connectorpb"
)

// maxFrameBytes bounds gRPC messages in both directions: far above the
// ~1 MiB batch target, far below anything that would mask a runaway batch.
const maxFrameBytes = 64 << 20

// tokenMetadataKey carries the spawn token on every RPC.
const tokenMetadataKey = "shift-token"

// Serve runs the connector until the host asks it to stop (Shutdown RPC or
// SIGTERM/SIGINT). It reads the socket path and token from the environment
// per the spawn contract. Call it from the connector's main; it blocks.
//
// As a special non-serving mode, `<binary> describe` prints the connector's
// canonical descriptor (ADR-0018) to stdout and exits — used by publisher
// tooling to extract action config schemas without a gRPC spawn.
func Serve(c Connector) error {
	if len(os.Args) > 1 && os.Args[1] == "describe" {
		return describeToStdout(c)
	}
	socket := os.Getenv(EnvSocket)
	token := os.Getenv(EnvToken)
	if socket == "" || token == "" {
		return fmt.Errorf("sdk: %s and %s must be set (this binary is spawned by a SHIFT runner)", EnvSocket, EnvToken)
	}
	// Orphan watch: runners are disposable at any moment (ADR-0002). If
	// the spawning runner dies without Shutdown (kill -9), this process is
	// re-parented to init — exit instead of lingering on a socket no one
	// owns. Subprocess path only; ServeOn (sdktest, in-process) skips it.
	go func() {
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for range tick.C {
			if os.Getppid() == 1 {
				os.Exit(0)
			}
		}
	}()
	return ServeOn(socket, token, c)
}

// describeToStdout writes the connector's canonical descriptor to stdout.
func describeToStdout(c Connector) error {
	b, err := CanonicalDescriptor(BuildDescriptor(c))
	if err != nil {
		return fmt.Errorf("sdk: describe: %w", err)
	}
	if _, err := os.Stdout.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("sdk: describe: %w", err)
	}
	return nil
}

// ServeOn is Serve with explicit socket and token (used by sdktest to run
// a connector in-process over a real socket; production code uses Serve).
func ServeOn(socket, token string, c Connector) error {
	// A stale socket file from a crashed predecessor would fail the bind.
	// The path comes from the spawning host via the spawn contract — it is
	// trusted input by design.
	if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) { //nolint:gosec // G703: socket path is host-provided per ADR-0007 spawn contract
		return fmt.Errorf("sdk: clear stale socket: %w", err)
	}
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", socket)
	if err != nil {
		return fmt.Errorf("sdk: listen %s: %w", socket, err)
	}
	srv := &server{c: c, token: token, done: make(chan struct{})}
	gs := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxFrameBytes),
		grpc.MaxSendMsgSize(maxFrameBytes),
		grpc.UnaryInterceptor(srv.unaryAuth),
		grpc.StreamInterceptor(srv.streamAuth),
	)
	srv.gs = gs
	connectorpb.RegisterConnectorServer(gs, srv)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		select {
		case <-sig:
		case <-srv.done:
		}
		gs.GracefulStop()
	}()
	defer signal.Stop(sig)
	return gs.Serve(lis)
}

type server struct {
	connectorpb.UnimplementedConnectorServer
	c     Connector
	token string
	gs    *grpc.Server
	done  chan struct{}
}

func (s *server) checkToken(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	vals := md.Get(tokenMetadataKey)
	if len(vals) == 1 && subtle.ConstantTimeCompare([]byte(vals[0]), []byte(s.token)) == 1 {
		return nil
	}
	return status.Error(codes.Unauthenticated, "missing or invalid connector token")
}

func (s *server) unaryAuth(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if err := s.checkToken(ctx); err != nil {
		return nil, err
	}
	return h(ctx, req)
}

func (s *server) streamAuth(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	if err := s.checkToken(ss.Context()); err != nil {
		return err
	}
	return h(srv, ss)
}

func (s *server) Handshake(_ context.Context, req *connectorpb.HandshakeRequest) (*connectorpb.HandshakeResponse, error) {
	if !slices.Contains(req.GetProtocolVersions(), ProtocolVersion) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"connector speaks protocol %d, host offered %v", ProtocolVersion, req.GetProtocolVersions())
	}
	resp := &connectorpb.HandshakeResponse{
		ProtocolVersion: ProtocolVersion,
		Name:            s.c.Name,
		Version:         s.c.Version,
	}
	for name := range s.c.Sources {
		resp.SourceActions = append(resp.SourceActions, name)
	}
	for name := range s.c.Sinks {
		resp.SinkActions = append(resp.SinkActions, name)
	}
	slices.Sort(resp.SourceActions)
	slices.Sort(resp.SinkActions)
	return resp, nil
}

func (s *server) Health(context.Context, *connectorpb.HealthRequest) (*connectorpb.HealthResponse, error) {
	return &connectorpb.HealthResponse{Ok: true}, nil
}

func (s *server) Describe(_ context.Context, _ *connectorpb.DescribeRequest) (*connectorpb.DescribeResponse, error) {
	d := BuildDescriptor(s.c)
	resp := &connectorpb.DescribeResponse{Name: d.Name, Version: d.Version}
	for _, a := range d.Actions {
		resp.Actions = append(resp.Actions, &connectorpb.ActionSchema{
			Action:       a.Action,
			Direction:    a.Direction,
			ConfigSchema: a.ConfigSchema,
		})
	}
	return resp, nil
}

func (s *server) Pull(req *connectorpb.PullRequest, stream grpc.ServerStreamingServer[connectorpb.Frame]) error {
	mk, ok := s.c.Sources[req.GetAction()]
	if !ok {
		return status.Errorf(codes.NotFound, "unknown source action %q", req.GetAction())
	}
	action := mk()
	ctx := stream.Context()
	if err := action.Open(ctx, req.GetConfig()); err != nil {
		return status.Errorf(codes.InvalidArgument, "open %s: %v", req.GetAction(), err)
	}
	defer func() { _ = action.Close() }()

	enc := newFrameEncoder()
	for {
		b, err := action.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return status.Errorf(codes.Internal, "%s: %v", req.GetAction(), err)
		}
		payload, err := enc.encode(b)
		if err != nil {
			return status.Errorf(codes.Internal, "%v", err)
		}
		if err := stream.Send(&connectorpb.Frame{Payload: payload, Records: int64(b.Len())}); err != nil {
			return err
		}
	}
}

func (s *server) Push(stream grpc.ClientStreamingServer[connectorpb.PushMessage, connectorpb.PushSummary]) error {
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "push stream closed before Open message")
	}
	open := first.GetOpen()
	if open == nil {
		return status.Error(codes.InvalidArgument, "first push message must be Open")
	}
	mk, ok := s.c.Sinks[open.GetAction()]
	if !ok {
		return status.Errorf(codes.NotFound, "unknown sink action %q", open.GetAction())
	}
	action := mk()
	if err := action.Open(ctx, open.GetConfig()); err != nil {
		return status.Errorf(codes.InvalidArgument, "open %s: %v", open.GetAction(), err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = action.Close()
		}
	}()

	batch := record.NewBatch()
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
		if err := decodeFrame(frame.GetPayload(), batch); err != nil {
			return status.Errorf(codes.InvalidArgument, "%v", err)
		}
		if err := action.Write(ctx, batch); err != nil {
			return status.Errorf(codes.Internal, "%s: %v", open.GetAction(), err)
		}
		total += int64(batch.Len())
	}
	closed = true
	if err := action.Close(); err != nil {
		return status.Errorf(codes.Internal, "close %s: %v", open.GetAction(), err)
	}
	return stream.SendAndClose(&connectorpb.PushSummary{Records: total})
}

func (s *server) Shutdown(context.Context, *connectorpb.ShutdownRequest) (*connectorpb.ShutdownResponse, error) {
	// Reply first; GracefulStop (triggered via done) drains this RPC.
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return &connectorpb.ShutdownResponse{}, nil
}
