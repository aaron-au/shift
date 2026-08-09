// Package dbconn is the database connector (PostgreSQL first). It exposes three
// verbs under the ADR-0024 operation model, all on one canvas node:
//
//   - query  — SOURCE: run a SELECT and stream each row as a typed record.
//   - upsert  — SINK: INSERT ... ON CONFLICT (...) DO UPDATE from record batches.
//   - exec   — config-driven SOURCE: run a non-returning statement and emit one
//     status record {rows_affected, ok}.
//
// Connections use database/sql over the pgx stdlib driver. Credentials arrive
// already-resolved as plaintext (the runner resolves {"$secret":...} refs before
// spawn — ADR-0010); this connector only tags secret fields in its schema and
// never logs them. Value arguments are ALWAYS parameterized ($1,$2,...); the
// only identifiers interpolated into SQL (table/column names for upsert) are
// validated and double-quoted (see quoteIdent) so they can never carry a value
// injection. Network egress is guarded like the http/sftp connectors: internal
// targets are refused unless allow_local is set (fail closed).
package dbconn

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"syscall"
	"time"

	"github.com/aaron-au/shift/sdk"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Connector returns the db connector definition.
func Connector() sdk.Connector {
	return sdk.Connector{
		Name:    "db",
		Version: "0.3.0",
		// behaviour-change, not compatible: the config surface is untouched, so
		// the compat gate cannot see this — but the OUTPUT moved. A NUMERIC
		// column used to arrive as a string and now arrives as an exact
		// decimal, which in NDJSON turns "10.10" into 10.10 (a quoted string
		// becomes a bare number), and a timestamp column is now a native
		// instant rather than an RFC 3339 string. Same rendered text for the
		// timestamp, same value for the numeric, different JSON type for a
		// downstream consumer that cares. ADR-0047 §6 exists for exactly what
		// the gate cannot see.
		Compat: "behaviour-change",
		Meta: &sdk.ConnectorMeta{
			Description: "PostgreSQL: query rows (SELECT → records), upsert records (INSERT … ON CONFLICT DO UPDATE), or exec a non-returning statement. Parameterized SQL only; network-guarded.",
			Category:    "database",
			Icon:        "🐘",
			Tags:        []string{"database", "postgres", "postgresql", "sql", "cdc"},
		},
		// query + exec are sources (query streams rows; exec performs a
		// statement and emits a status record, so a one-verb flow runs
		// standalone). upsert is the sink — it consumes the pipeline's records.
		Sources: map[string]func() sdk.SourceAction{
			"query": func() sdk.SourceAction { return &querySource{} },
			"exec":  func() sdk.SourceAction { return &execSource{} },
			// Incremental read (ADR-0037): re-runs from the watermark the last
			// run reached, so a scheduled sync moves forward instead of
			// re-reading the table every fire.
			"sync": func() sdk.SourceAction { return &syncSource{} },
		},
		Sinks: map[string]func() sdk.SinkAction{
			"upsert": func() sdk.SinkAction { return &upsertSink{} },
		},
		Schemas: map[string][]byte{
			"query":  []byte(querySchema),
			"exec":   []byte(execSchema),
			"sync":   []byte(syncSchema),
			"upsert": []byte(upsertSchema),
		},
	}
}

// connProps is the shared connection portion of every action's config schema.
// A full dsn OR the discrete host/port/database/user/password/sslmode fields may
// be given. Secret-typed fields carry x-shift-secret so the studio offers a
// secret picker.
const connProps = `
    "dsn": {"type": "string", "title": "DSN", "description": "Full connection string (postgres://user:pass@host:port/db?sslmode=...). Overrides the discrete fields below.", "x-shift-secret": true},
    "host": {"type": "string", "title": "Host", "description": "Database host (used when dsn is empty)"},
    "port": {"type": "integer", "title": "Port", "default": 5432},
    "database": {"type": "string", "title": "Database"},
    "user": {"type": "string", "title": "Username"},
    "password": {"type": "string", "title": "Password", "x-shift-secret": true},
    "sslmode": {"type": "string", "title": "SSL mode", "enum": ["", "disable", "allow", "prefer", "require", "verify-ca", "verify-full"], "default": "prefer"},
    "allow_local": {"type": "boolean", "title": "Allow local/loopback and private/internal targets (network guard off)", "default": false},
    "timeout_seconds": {"type": "integer", "title": "Connect timeout (seconds)", "default": 30}`

var (
	querySchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"DB query",
  "required":["query"],"properties":{` + connProps + `,
    "query": {"type": "string", "title": "SQL query", "description": "A SELECT statement; each row becomes a record. Use $1,$2,... for parameters."},
    "params": {"type": "array", "title": "Parameters", "description": "Positional values bound to $1,$2,... (parameterized, never concatenated)", "items": {}}
  }}`

	syncSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"DB sync",
  "required":["query","cursor_column"],"properties":{` + connProps + `,
    "query": {"type": "string", "title": "SQL query", "description": "A SELECT that references the cursor as $1 and ORDERs BY the cursor column ascending, e.g. SELECT * FROM orders WHERE updated_at >= $1 ORDER BY updated_at. Use >= with an idempotent sink so rows sharing a watermark are not lost."},
    "cursor_column": {"type": "string", "title": "Cursor column", "description": "The monotonic column whose highest delivered value becomes the next run's starting point. Must be selected by the query."},
    "cursor_initial": {"title": "Initial cursor", "description": "Where the FIRST run starts (e.g. \"1970-01-01T00:00:00Z\" or 0). Ignored once a cursor has been stored."}
  }}`

	execSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"DB exec",
  "required":["statement"],"properties":{` + connProps + `,
    "statement": {"type": "string", "title": "SQL statement", "description": "A non-returning statement (INSERT/UPDATE/DELETE/DDL). Emits one status record {rows_affected, ok}. Use $1,$2,... for parameters."},
    "params": {"type": "array", "title": "Parameters", "description": "Positional values bound to $1,$2,...", "items": {}}
  }}`

	upsertSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"DB upsert",
  "required":["table","conflict_columns"],"properties":{` + connProps + `,
    "table": {"type": "string", "title": "Table", "description": "Target table (optionally schema-qualified: schema.table)"},
    "conflict_columns": {"type": "array", "title": "Conflict columns", "description": "Columns forming the ON CONFLICT key (typically the primary/unique key)", "items": {"type": "string"}},
    "columns": {"type": "array", "title": "Columns", "description": "Columns to write. If empty, the keys of each record are used.", "items": {"type": "string"}}
  }}`
)

// config is the union of every action's configuration; action-specific fields
// are validated by each action's Open.
type config struct {
	DSN            string `json:"dsn"`
	Host           string `json:"host"`
	Port           int    `json:"port"`
	Database       string `json:"database"`
	User           string `json:"user"`
	Password       string `json:"password"`
	SSLMode        string `json:"sslmode"`
	AllowLocal     bool   `json:"allow_local"`
	TimeoutSeconds int    `json:"timeout_seconds"`

	// query / exec / sync
	Query     string `json:"query"`
	Statement string `json:"statement"`
	Params    []any  `json:"params"`

	// sync (incremental read)
	CursorColumn  string          `json:"cursor_column,omitempty"`
	CursorInitial json.RawMessage `json:"cursor_initial,omitempty"`

	// upsert
	Table           string   `json:"table"`
	ConflictColumns []string `json:"conflict_columns"`
	Columns         []string `json:"columns"`
}

func (c *config) validateConn() error {
	if c.DSN == "" && (c.Host == "" || c.Database == "") {
		return errors.New("db: provide dsn, or host and database")
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = 30
	}
	if c.Port == 0 {
		c.Port = 5432
	}
	return nil
}

func (c *config) timeout() time.Duration { return time.Duration(c.TimeoutSeconds) * time.Second }

// dsn returns the connection string, either verbatim (if the user supplied one)
// or assembled from the discrete fields. url.URL handles user/password escaping,
// so credentials with reserved characters are encoded correctly.
func (c *config) dsn() (string, error) {
	if c.DSN != "" {
		return c.DSN, nil
	}
	u := url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		Path:   "/" + c.Database,
	}
	if c.User != "" {
		if c.Password != "" {
			u.User = url.UserPassword(c.User, c.Password)
		} else {
			u.User = url.User(c.User)
		}
	}
	if c.SSLMode != "" {
		q := url.Values{}
		q.Set("sslmode", c.SSLMode)
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}

// open dials PostgreSQL through the pgx stdlib driver with the network guard
// installed as the dial hook, and verifies the connection with a ping (so a
// blocked target fails closed at Open, not mid-stream). The returned closer
// shuts the pool down.
// openDB opens the pool for a config. It is a package var (indirecting
// config.open) so tests can inject a sqlmock-backed *sql.DB and exercise the
// query/exec/upsert paths without a live database — mirroring the querySource
// `start` seam. Production always uses the real driver below.
var openDB = func(ctx context.Context, c *config) (*sql.DB, func() error, error) {
	return c.open(ctx)
}

func (c *config) open(ctx context.Context) (*sql.DB, func() error, error) {
	dsn, err := c.dsn()
	if err != nil {
		return nil, nil, err
	}
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		// Do not wrap: a bad DSN string can contain the password; return a
		// fixed, secret-free message instead of echoing the parse error.
		return nil, nil, errors.New("db: invalid connection settings (check dsn/host/port/database/sslmode)")
	}
	dialer := &net.Dialer{Timeout: c.timeout(), Control: guard(c.AllowLocal)}
	connConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, addr)
	}
	db := sql.OpenDB(stdlib.GetConnector(*connConfig))
	db.SetMaxOpenConns(4)
	pingCtx, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("db: connect: %w", err)
	}
	return db, db.Close, nil
}

// cgNAT is RFC 6598 shared address space (100.64.0.0/10), parsed once.
var _, cgNAT, _ = net.ParseCIDR("100.64.0.0/10")

// guard returns a net.Dialer.Control hook that refuses loopback/link-local and
// (unless allowLocal) private/internal targets, evaluated on the concrete
// post-DNS IP so a rebind can't slip past. Mirrors the http/sftp connectors'
// SSRF guard — on a shared/cloud runner an attacker-influenced host must not
// reach internal databases or the cloud metadata endpoint. An on-prem database
// on an internal network sets allow_local.
func guard(allowLocal bool) func(string, string, syscall.RawConn) error {
	return func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("db: bad dial address %q: %w", address, err)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return fmt.Errorf("db: unresolvable address %q", host)
		}
		switch {
		case ip.IsLoopback(), ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(), ip.IsUnspecified():
			if allowLocal {
				return nil
			}
			return fmt.Errorf("db: refusing %s (loopback/link-local; set allow_local for dev use)", ip)
		case ip.IsPrivate(), cgNAT.Contains(ip):
			if allowLocal {
				return nil
			}
			return fmt.Errorf("db: refusing %s (private/internal range; set allow_local to reach internal targets)", ip)
		}
		return nil
	}
}
