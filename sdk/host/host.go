// Package host is the runner-facing side of the connector protocol: it
// spawns connector binaries, performs the handshake, and adapts their
// actions to the engine's stream.Source/stream.Sink contract so remote and
// in-process operators compose identically (ADR-0007).
package host

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/aaron-au/shift/sdk"
	"github.com/aaron-au/shift/sdk/connectorpb"
)

// maxFrameBytes mirrors the server-side bound.
const maxFrameBytes = 64 << 20

// Info describes a connected connector, as reported by its handshake.
type Info struct {
	Name            string
	Version         string
	ProtocolVersion uint32
	SourceActions   []string
	SinkActions     []string
}

// Process is a live connector: either a spawned child process (Launch) or
// an attached in-process server (Attach, used by tests).
type Process struct {
	cmd    *exec.Cmd
	waitCh chan error // receives cmd.Wait() result once; nil when attached
	dir    string     // socket dir we own (removed on Close); "" when attached
	conn   *grpc.ClientConn
	client connectorpb.ConnectorClient
	token  string
	info   Info
}

// LaunchOptions tune connector spawning.
type LaunchOptions struct {
	// HandshakeTimeout bounds spawn-to-ready (default 10s).
	HandshakeTimeout time.Duration
	// Stderr receives the connector's stderr (default: inherited).
	Stderr *os.File
}

// Launch spawns the connector binary, waits for its socket to serve, and
// completes the handshake. The caller must Close the returned Process.
//
// Security (ADR-0007): the socket lives in a fresh 0700 directory and every
// RPC carries a per-process random token.
func Launch(ctx context.Context, binary string, opts LaunchOptions) (*Process, error) {
	if opts.HandshakeTimeout <= 0 {
		opts.HandshakeTimeout = 10 * time.Second
	}
	dir, err := os.MkdirTemp("", "shift-conn-*")
	if err != nil {
		return nil, fmt.Errorf("host: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil { //nolint:gosec // G302: directories need the execute bit; 0700 is the intended owner-only mode (ADR-0007)
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("host: %w", err)
	}
	socket := filepath.Join(dir, "connector.sock")
	token, err := newToken()
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}

	// The binary path is chosen by the runner from its connector store —
	// spawning it is this package's purpose.
	cmd := exec.CommandContext(ctx, binary) //nolint:gosec // G204: launching connector binaries is the point of this package; callers vet paths (operator Dir or Ed25519-verified via runner connstore)
	cmd.Env = append(os.Environ(),
		sdk.EnvSocket+"="+socket,
		sdk.EnvToken+"="+token,
	)
	if opts.Stderr != nil {
		cmd.Stderr = opts.Stderr
	} else {
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("host: start %s: %w", binary, err)
	}
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	p := &Process{cmd: cmd, waitCh: waitCh, dir: dir, token: token}
	if err := p.connect(ctx, socket, opts.HandshakeTimeout); err != nil {
		_ = p.kill()
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return p, nil
}

// Attach connects to an already-serving connector socket (no spawn). Used
// by sdktest and future pre-warmed connector pools.
func Attach(ctx context.Context, socket, token string, timeout time.Duration) (*Process, error) {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	p := &Process{token: token}
	if err := p.connect(ctx, socket, timeout); err != nil {
		return nil, err
	}
	return p, nil
}

func newToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("host: token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

func (p *Process) connect(ctx context.Context, socket string, timeout time.Duration) error {
	conn, err := grpc.NewClient("unix://"+socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // UDS in a 0700 dir + token auth (ADR-0007)
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxFrameBytes),
			grpc.MaxCallSendMsgSize(maxFrameBytes),
		),
	)
	if err != nil {
		return fmt.Errorf("host: dial: %w", err)
	}
	p.conn = conn
	p.client = connectorpb.NewConnectorClient(conn)

	// Handshake doubles as the readiness probe: retry until the socket
	// serves or the deadline passes.
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		hctx, cancel := context.WithTimeout(p.withToken(ctx), time.Second)
		resp, err := p.client.Handshake(hctx, &connectorpb.HandshakeRequest{
			ProtocolVersions: []uint32{sdk.ProtocolVersion},
		})
		cancel()
		if err == nil {
			p.info = Info{
				Name:            resp.GetName(),
				Version:         resp.GetVersion(),
				ProtocolVersion: resp.GetProtocolVersion(),
				SourceActions:   resp.GetSourceActions(),
				SinkActions:     resp.GetSinkActions(),
			}
			return nil
		}
		lastErr = err
		if p.waitCh != nil {
			select {
			case werr := <-p.waitCh:
				p.waitCh = nil // consumed; Close must not wait again
				return fmt.Errorf("host: connector exited before handshake (%w): %w", werr, lastErr)
			default:
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	_ = conn.Close()
	return fmt.Errorf("host: handshake timed out: %w", lastErr)
}

func (p *Process) withToken(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "shift-token", p.token)
}

// Info returns the handshake-reported connector identity and actions.
func (p *Process) Info() Info { return p.info }

// Health probes connector liveness.
func (p *Process) Health(ctx context.Context) error {
	resp, err := p.client.Health(p.withToken(ctx), &connectorpb.HealthRequest{})
	if err != nil {
		return err
	}
	if !resp.GetOk() {
		return fmt.Errorf("host: connector unhealthy: %s", resp.GetDetail())
	}
	return nil
}

// Describe fetches the connector's action catalog with config schemas and
// assembles a sdk.Descriptor (ADR-0018). Publisher tooling calls this at
// publish time; it is not on the execution path.
func (p *Process) Describe(ctx context.Context) (sdk.Descriptor, error) {
	resp, err := p.client.Describe(p.withToken(ctx), &connectorpb.DescribeRequest{})
	if err != nil {
		return sdk.Descriptor{}, fmt.Errorf("host: describe: %w", err)
	}
	d := sdk.Descriptor{Name: resp.GetName(), Version: resp.GetVersion()}
	for _, a := range resp.GetActions() {
		ad := sdk.ActionDescriptor{Action: a.GetAction(), Direction: a.GetDirection()}
		if len(a.GetConfigSchema()) > 0 {
			ad.ConfigSchema = a.GetConfigSchema()
		}
		d.Actions = append(d.Actions, ad)
	}
	return d, nil
}

// ExtractDescriptor spawns the connector binary, calls Describe, and
// returns its canonical descriptor bytes (ready to hash + sign + upload
// per ADR-0018) alongside the parsed descriptor. Used by publisher
// tooling; it fully launches and closes the connector.
func ExtractDescriptor(ctx context.Context, binary string) ([]byte, sdk.Descriptor, error) {
	p, err := Launch(ctx, binary, LaunchOptions{})
	if err != nil {
		return nil, sdk.Descriptor{}, err
	}
	defer func() { _ = p.Close() }()
	d, err := p.Describe(ctx)
	if err != nil {
		return nil, sdk.Descriptor{}, err
	}
	canon, err := sdk.CanonicalDescriptor(d)
	if err != nil {
		return nil, sdk.Descriptor{}, fmt.Errorf("host: canonicalize descriptor: %w", err)
	}
	return canon, d, nil
}

// Close shuts the connector down: graceful Shutdown RPC, then SIGKILL
// after a grace period, then reaps the process and socket directory.
//
// Close is idempotent: each resource is released once and its handle cleared,
// so a second Close is a genuine no-op. Without this, a second call would
// re-await the already-drained (never-closed) waitCh and block the caller
// forever — a nasty footgun for defer p.Close() plus an explicit close.
// Not safe for concurrent callers (Close is a single-owner operation).
func (p *Process) Close() error {
	var errs []error
	if p.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, _ = p.client.Shutdown(p.withToken(ctx), &connectorpb.ShutdownRequest{})
		cancel()
		p.client = nil
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		p.conn = nil
	}
	if p.waitCh != nil {
		select {
		case <-p.waitCh:
		case <-time.After(5 * time.Second):
			errs = append(errs, p.kill())
			<-p.waitCh
		}
		p.waitCh = nil
	}
	if p.dir != "" {
		if err := os.RemoveAll(p.dir); err != nil { //nolint:gosec // G703: p.dir is our own os.MkdirTemp path (set in Launch), never external input
			errs = append(errs, err)
		}
		p.dir = ""
	}
	return errors.Join(errs...)
}

func (p *Process) kill() error {
	if p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	return p.cmd.Process.Kill()
}
