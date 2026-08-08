// Package ftpconn is the FTP/FTPS connector: classic RFC 959 FTP with
// explicit TLS (FTPS, AUTH TLS). It complements the SSH-based sftp connector.
// One canvas node with a verb dropdown (ADR-0024): get (source) streams a
// remote file into typed record batches; put (sink) serializes batches to a
// remote file (temp name + rename for atomicity); list (source) emits one
// record per directory entry; and the config-driven verbs delete/mkdir/rmdir/
// rename (sources) perform one side-effecting op and emit a status record.
//
// Records are parsed/written via engine/format (ndjson or csv). Credentials
// arrive already-resolved as plaintext (the runner resolves {"$secret":...}
// refs before spawn — ADR-0010); this connector only tags secret fields in its
// schema and never logs a secret value.
package ftpconn

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/aaron-au/shift/connectors/internal/fileformat"
	"github.com/aaron-au/shift/sdk"
	"github.com/jlaffaye/ftp"
)

// Connector returns the ftp connector definition.
func Connector() sdk.Connector {
	return sdk.Connector{
		Name:    "ftp",
		Version: "0.3.0",
		Meta: &sdk.ConnectorMeta{
			Description: "FTP/FTPS file operations: pick a verb (get/put/list/delete/mkdir/rmdir/rename) and a path. Explicit TLS (FTPS) on by default; certificate verified.",
			Category:    "file-transfer",
			Icon:        "📂",
			Tags:        []string{"ftp", "ftps", "file", "ndjson", "csv"},
		},
		// Every verb except put is a source: configure it with a verb + path and
		// it runs standalone (the op verbs emit a single status record). put is
		// the one sink — it consumes the pipeline's records to write a file.
		Sources: map[string]func() sdk.SourceAction{
			"get":    func() sdk.SourceAction { return &getSource{} },
			"list":   func() sdk.SourceAction { return &listSource{} },
			"delete": func() sdk.SourceAction { return &opSource{op: opDelete} },
			"mkdir":  func() sdk.SourceAction { return &opSource{op: opMkdir} },
			"rmdir":  func() sdk.SourceAction { return &opSource{op: opRmdir} },
			"rename": func() sdk.SourceAction { return &opSource{op: opRename} },
		},
		Sinks: map[string]func() sdk.SinkAction{
			"put": func() sdk.SinkAction { return &putSink{} },
		},
		Schemas: map[string][]byte{
			"get":    []byte(fileConfigSchema),
			"put":    []byte(fileConfigSchema),
			"list":   []byte(listConfigSchema),
			"delete": []byte(opPathSchema),
			"mkdir":  []byte(opPathSchema),
			"rmdir":  []byte(rmdirConfigSchema),
			"rename": []byte(renameConfigSchema),
		},
	}
}

// connProps is the shared connection portion of every action's config schema.
// Secret-typed fields carry x-shift-secret so the studio offers a secret picker.
const connProps = `
    "host": {"type": "string", "title": "Host", "description": "FTP server hostname or IP"},
    "port": {"type": "integer", "title": "Port", "default": 21},
    "user": {"type": "string", "title": "Username", "description": "Defaults to 'anonymous' when empty"},
    "password": {"type": "string", "title": "Password", "x-shift-secret": true},
    "explicit_tls": {"type": "boolean", "title": "Explicit TLS (FTPS / AUTH TLS)", "description": "Encrypt the connection and verify the server certificate. On by default; plaintext credentials are refused unless allow_local.", "default": true},
    "allow_local": {"type": "boolean", "title": "Allow local/loopback and private/internal targets (network guard off; also permits plaintext credentials)", "default": false},
    "insecure_tls": {"type": "boolean", "title": "Skip FTPS certificate verification (self-signed dev servers only)", "default": false},
    "timeout_seconds": {"type": "integer", "title": "Connect timeout (seconds)", "default": 30}`

// Per-action schemas. get/put stream a file; list reads a directory; the op
// sources (delete/mkdir/rmdir/rename) take their target(s) from config.
var (
	fileConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"FTP file",
  "required":["host","path"],"properties":{` + connProps + `,
    "path": {"type": "string", "title": "Remote path", "description": "Path to the remote file"},
    "format": ` + fileformat.SchemaEnum() + `,
    "record_element": ` + fileformat.RecordElementProp + `
  }}`

	listConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"FTP list",
  "required":["host","path"],"properties":{` + connProps + `,
    "path": {"type": "string", "title": "Remote directory", "description": "Directory to list; emits one record per entry {name,path,size,type,mod_time}"}
  }}`

	opPathSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"FTP operation",
  "required":["host","path"],"properties":{` + connProps + `,
    "path": {"type": "string", "title": "Remote path", "description": "Target file/directory"}
  }}`

	rmdirConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"FTP rmdir",
  "required":["host","path"],"properties":{` + connProps + `,
    "path": {"type": "string", "title": "Remote directory"},
    "recursive": {"type": "boolean", "title": "Recursive", "description": "Remove non-empty directories and their contents", "default": false}
  }}`

	renameConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"FTP rename",
  "required":["host","from","to"],"properties":{` + connProps + `,
    "from": {"type": "string", "title": "From path"},
    "to": {"type": "string", "title": "To path"}
  }}`
)

// config is the shared source/sink configuration.
type config struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Path     string `json:"path"`
	From     string `json:"from"` // rename: source path
	To       string `json:"to"`   // rename: destination path
	Format   string `json:"format"`
	// RecordElement names the XML element that delimits one record.
	// Ignored by the other formats, which have no equivalent notion.
	RecordElement  string `json:"record_element,omitempty"`
	Recursive      bool   `json:"recursive"`    // rmdir: remove non-empty trees
	ExplicitTLS    *bool  `json:"explicit_tls"` // nil ⇒ default true (FTPS on)
	AllowLocal     bool   `json:"allow_local"`
	InsecureTLS    bool   `json:"insecure_tls"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

// explicitTLS reports whether FTPS (AUTH TLS) is enabled. Absent ⇒ true: secure
// by default, so a misconfiguration fails closed toward encryption.
func (c *config) explicitTLS() bool { return c.ExplicitTLS == nil || *c.ExplicitTLS }

func (c *config) timeout() time.Duration { return time.Duration(c.TimeoutSeconds) * time.Second }

// parseConfig unmarshals and validates the connection fields (shared by every
// action). Action-specific requirements are checked by each action's Open via
// the require* helpers below.
func parseConfig(raw []byte, into *config) error {
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("ftp: bad config: %w", err)
	}
	return into.validateConn()
}

func (c *config) validateConn() error {
	if c.Host == "" {
		return errors.New("ftp: host is required")
	}
	if c.Port == 0 {
		c.Port = 21
	}
	if c.User == "" {
		c.User = "anonymous"
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = 30
	}
	// Refuse to send real credentials over an unencrypted control connection
	// unless the operator has explicitly opted into local/dev use. Anonymous
	// login (no password) over plaintext is permitted.
	if !c.explicitTLS() && c.Password != "" && !c.AllowLocal {
		return errors.New("ftp: refusing to send credentials over plaintext FTP; enable explicit_tls (FTPS) or set allow_local")
	}
	return nil
}

// requireFileFormat validates the get/put config: a remote file path and a
// supported record format (defaulting to ndjson).
func (c *config) requireFileFormat() error {
	if c.Path == "" {
		return errors.New("ftp: path is required")
	}
	return fileformat.Validate("ftp", &c.Format)
}

// requireDir validates the list config: a remote directory path.
func (c *config) requireDir() error {
	if c.Path == "" {
		return errors.New("ftp: path (directory) is required")
	}
	return nil
}

// tlsConfig builds the FTPS client TLS config. The certificate is verified
// against the configured host unless insecure_tls explicitly says otherwise.
//
// Verification is deliberately NOT tied to allow_local. Reaching any on-prem
// FTPS server — the ordinary case for this connector — requires allow_local,
// and conflating the two silently downgraded every internal FTPS connection to
// MITM-able even when the certificate chain would have verified fine. The two
// privileges are now separate: allow_local decides which hosts are reachable,
// insecure_tls decides whether the certificate is trusted.
func (c *config) tlsConfig() *tls.Config {
	cfg := &tls.Config{ServerName: c.Host, MinVersion: tls.VersionTLS12}
	if c.InsecureTLS {
		cfg.InsecureSkipVerify = true //nolint:gosec // G402: verification disabled only under the explicit insecure_tls opt-in (dev/self-signed)
	}
	return cfg
}

// cgNAT is RFC 6598 shared address space (100.64.0.0/10), parsed once. It is
// NOT covered by net.IP.IsPrivate but is an internal range (and hosts some
// cloud metadata), so the guard blocks it alongside the RFC1918/ULA ranges.
var _, cgNAT, _ = net.ParseCIDR("100.64.0.0/10")

// guard returns a net.Dialer.Control hook that refuses loopback/link-local and
// (unless allowLocal) private/internal targets, evaluated on the concrete
// post-DNS IP so a rebind can't slip past. Mirrors the http connector's SSRF
// guard: on a shared/cloud runner an attacker-influenced host must not reach
// internal services or a metadata endpoint. On-prem FTP to an internal server
// sets allow_local. Applied to both control and data connections via the ftp
// library's dial func.
func guard(allowLocal bool) func(string, string, syscall.RawConn) error {
	return func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("ftp: bad dial address %q: %w", address, err)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return fmt.Errorf("ftp: unresolvable address %q", host)
		}
		switch {
		case ip.IsLoopback(), ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(), ip.IsUnspecified():
			if allowLocal {
				return nil
			}
			return fmt.Errorf("ftp: refusing %s (loopback/link-local; set allow_local for dev use)", ip)
		case ip.IsPrivate(), cgNAT.Contains(ip):
			if allowLocal {
				return nil
			}
			return fmt.Errorf("ftp: refusing %s (private/internal range; set allow_local to reach internal targets)", ip)
		}
		return nil
	}
}

// ftpConn is the narrow slice of the FTP client this connector uses. Defining
// it as an interface lets tests inject an in-memory fake — no real FTP server
// or network — while production uses serverConn over *ftp.ServerConn.
type ftpConn interface {
	Login(user, password string) error
	Retr(path string) (io.ReadCloser, error)
	Stor(path string, r io.Reader) error
	List(path string) ([]*ftp.Entry, error)
	Delete(path string) error
	MakeDir(path string) error
	RemoveDir(path string) error
	RemoveDirRecur(path string) error
	Rename(from, to string) error
	Quit() error
}

// serverConn adapts *ftp.ServerConn to ftpConn. Only Retr needs adapting: the
// library returns a concrete *ftp.Response (already an io.ReadCloser).
type serverConn struct{ c *ftp.ServerConn }

func (s serverConn) Login(user, password string) error       { return s.c.Login(user, password) }
func (s serverConn) Retr(path string) (io.ReadCloser, error) { return s.c.Retr(path) }
func (s serverConn) Stor(path string, r io.Reader) error     { return s.c.Stor(path, r) }
func (s serverConn) List(path string) ([]*ftp.Entry, error)  { return s.c.List(path) }
func (s serverConn) Delete(path string) error                { return s.c.Delete(path) }
func (s serverConn) MakeDir(path string) error               { return s.c.MakeDir(path) }
func (s serverConn) RemoveDir(path string) error             { return s.c.RemoveDir(path) }
func (s serverConn) RemoveDirRecur(path string) error        { return s.c.RemoveDirRecur(path) }
func (s serverConn) Rename(from, to string) error            { return s.c.Rename(from, to) }
func (s serverConn) Quit() error                             { return s.c.Quit() }

// dialFunc opens a logged-in connection and returns it with a closer. It is a
// field on every action (default nil ⇒ realDial) so tests substitute a fake.
type dialFunc func(ctx context.Context, c *config) (ftpConn, func() error, error)

func dialOr(d dialFunc) dialFunc {
	if d == nil {
		return realDial
	}
	return d
}

// guardedDialer supplies every connection the FTP client makes — the control
// connection and each PASV data connection — through the SSRF network guard,
// and wraps the DATA connections in TLS itself.
//
// It has to do the TLS wrap, which is the subtle part. jlaffaye/ftp's
// openDataConn returns `c.options.dialFunc(...)` BEFORE it reaches its own
// tls.Client branch, so installing a dial func (which the network guard
// requires) silently disables data-channel TLS. The control channel is still
// upgraded by the library's AUTH TLS, and PROT P is still sent, so the result
// was either a broken transfer against a compliant server or — against a
// lenient one — file contents and directory listings crossing the wire in
// cleartext on a connection the author configured as FTPS.
//
// Control vs data is decided by order: the client dials the control connection
// exactly once, during Dial, before any data connection exists. The control
// conn is therefore left plaintext for the library to upgrade via AUTH TLS.
//
// The wrap mirrors the library's own data path: tls.Client with no explicit
// handshake, letting the first Read or Write trigger it (jlaffaye/ftp#282 —
// an eager handshake hangs with proftpd and pureftpd).
type guardedDialer struct {
	dialer  *net.Dialer
	ctx     context.Context
	tls     *tls.Config
	mu      sync.Mutex
	control bool // the control connection has been dialed
}

func (g *guardedDialer) dial(network, address string) (net.Conn, error) {
	conn, err := g.dialer.DialContext(g.ctx, network, address)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	isControl := !g.control
	g.control = true
	g.mu.Unlock()

	if isControl || g.tls == nil {
		return conn, nil
	}
	return tls.Client(conn, g.tls), nil
}

// realDial connects (network-guarded, FTPS by default with cert verification)
// and logs in. The returned closer sends QUIT and tears the connection down.
func realDial(ctx context.Context, c *config) (ftpConn, func() error, error) {
	addr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	gd := &guardedDialer{
		dialer: &net.Dialer{Timeout: c.timeout(), Control: guard(c.AllowLocal)},
		ctx:    ctx,
	}
	if c.explicitTLS() {
		gd.tls = c.tlsConfig()
	}
	opts := []ftp.DialOption{
		ftp.DialWithTimeout(c.timeout()),
		ftp.DialWithContext(ctx),
		ftp.DialWithDialFunc(gd.dial),
	}
	if c.explicitTLS() {
		opts = append(opts, ftp.DialWithExplicitTLS(c.tlsConfig()))
	}
	sc, err := ftp.Dial(addr, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("ftp: dial %s: %w", addr, err)
	}
	conn := serverConn{sc}
	if err := conn.Login(c.User, c.Password); err != nil {
		_ = sc.Quit()
		return nil, nil, fmt.Errorf("ftp: login: %w", err)
	}
	return conn, func() error { return sc.Quit() }, nil
}

// ignore550 swallows a "file unavailable" (RFC 959 code 550) reply so
// operations stay idempotent under at-least-once redelivery: deleting a missing
// file, removing a missing directory, renaming an already-moved file, or
// creating an existing directory all report success.
func ignore550(err error) error {
	if err == nil {
		return nil
	}
	var te *textproto.Error
	if errors.As(err, &te) && te.Code == ftp.StatusFileUnavailable {
		return nil
	}
	return err
}
