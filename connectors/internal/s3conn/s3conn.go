// Package s3conn is the S3 connector: works against AWS S3 and any
// S3-compatible endpoint (MinIO, Ceph, R2). One canvas node, one verb picked
// from a dropdown (ADR-0024):
//
//   - get    (source) — GetObject, stream the body, decode ndjson/csv.
//   - list   (source) — ListObjectsV2 by prefix, one record per object.
//   - delete (source) — DeleteObject, emit a single status record.
//   - put    (sink)   — PutObject, encode the pipeline's records as ndjson/csv.
//
// The client is built from STATIC, tenant-scoped credentials carried in the
// config (access_key_id / secret_access_key). Ambient environment/instance
// credentials are never consulted — config.LoadDefaultConfig is deliberately
// not used. Secret-typed fields arrive already resolved (the runner resolves
// {"$secret":...} refs before spawn — ADR-0010); this connector only tags them
// in its schema and never logs their values.
package s3conn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/aaron-au/shift/connectors/internal/fileformat"
	"github.com/aaron-au/shift/sdk"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Connector returns the s3 connector definition.
func Connector() sdk.Connector {
	return sdk.Connector{
		Name:    "s3",
		Version: "0.6.0",
		// Refuses a `..` path segment in a key when a CUSTOM ENDPOINT is
		// configured (TC-032): S3 keys are opaque, but a normalising proxy in
		// front of an S3-compatible endpoint can resolve one to a different
		// object. AWS proper is unaffected, so no legitimate AWS key is
		// refused — but an S3-compatible deployment using such a key stops
		// working, which is a behaviour change.
		//
		// Also decodes Content-Encoding deliberately and bounds it (TC-020). A
		// gzip-encoded object previously reached the record parser still
		// compressed and failed at byte one with `unexpected character
		// '\x1f'`; it now reads, up to the ratio bound. Nothing that worked
		// stops working, but the outcome for such an object changes, so it is
		// declared as a behaviour change rather than as a widening (ADR-0047 §6).
		Compat: "behaviour-change",
		Meta: &sdk.ConnectorMeta{
			Description: "AWS S3 and S3-compatible (MinIO/Ceph/R2) object storage: pick a verb (get/put/list/delete). Static tenant credentials; SSRF-guarded.",
			Category:    "object-storage",
			Icon:        "🪣",
			Tags:        []string{"s3", "aws", "minio", "object-storage", "ndjson", "csv"},
		},
		// get/list/delete are sources: configure a verb + bucket/key and the
		// node runs standalone (delete emits a single status record). put is
		// the one sink — it consumes the pipeline's records to write an object.
		Sources: map[string]func() sdk.SourceAction{
			"get":    func() sdk.SourceAction { return &getSource{} },
			"list":   func() sdk.SourceAction { return &listSource{} },
			"delete": func() sdk.SourceAction { return &deleteSource{} },
		},
		Sinks: map[string]func() sdk.SinkAction{
			"put": func() sdk.SinkAction { return &putSink{} },
		},
		Schemas: map[string][]byte{
			"get":    []byte(objectConfigSchema),
			"put":    []byte(objectConfigSchema),
			"list":   []byte(listConfigSchema),
			"delete": []byte(deleteConfigSchema),
		},
	}
}

// connProps is the shared connection portion of every action's config schema.
// Secret-typed fields carry x-shift-secret so the studio offers a secret picker.
const connProps = `
    "region": {"type": "string", "title": "Region", "description": "AWS region (e.g. us-east-1); defaults to us-east-1 for S3-compatible endpoints", "default": "us-east-1"},
    "endpoint": {"type": "string", "title": "Endpoint", "description": "Custom endpoint URL for S3-compatible storage (e.g. https://minio.internal:9000). Leave blank for AWS S3."},
    "access_key_id": {"type": "string", "title": "Access key ID", "x-shift-secret": true},
    "secret_access_key": {"type": "string", "title": "Secret access key", "x-shift-secret": true},
    "path_style": {"type": "boolean", "title": "Path-style addressing", "description": "Use path-style bucket addressing (required by most MinIO setups)", "default": false},
    "allow_local": {"type": "boolean", "title": "Allow local/loopback and private/internal endpoints (network guard off)", "default": false},
    "timeout_seconds": {"type": "integer", "title": "Request timeout (seconds)", "default": 300}`

// Per-action schemas.
var (
	objectConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"S3 object",
  "required":["bucket","key"],"properties":{` + connProps + `,
    "bucket": {"type": "string", "title": "Bucket"},
    "key": {"type": "string", "title": "Object key", "description": "Full object key/path within the bucket"},
    "format": ` + fileformat.SchemaEnum() + `,
    "record_element": ` + fileformat.RecordElementProp + `,
    "columns": ` + fileformat.ColumnsProp() + `,
    "max_decompression_ratio": {"type": "integer", "title": "Max decompression ratio", "description": "For an object stored with Content-Encoding: gzip, refuse it if it inflates to more than this many bytes per wire byte", "default": 100}
  }}`

	listConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"S3 list",
  "required":["bucket"],"properties":{` + connProps + `,
    "bucket": {"type": "string", "title": "Bucket"},
    "prefix": {"type": "string", "title": "Prefix", "description": "Only list objects whose key starts with this prefix; emits one record per object {key,size,etag,last_modified}"}
  }}`

	deleteConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"S3 delete",
  "required":["bucket","key"],"properties":{` + connProps + `,
    "bucket": {"type": "string", "title": "Bucket"},
    "key": {"type": "string", "title": "Object key", "description": "Object to delete"}
  }}`
)

// config is the shared source/sink configuration.
type config struct {
	Bucket          string `json:"bucket"`
	Key             string `json:"key"`
	Prefix          string `json:"prefix"`
	Region          string `json:"region"`
	Endpoint        string `json:"endpoint"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Format          string `json:"format"`
	// RecordElement names the XML element that delimits one record.
	// Ignored by the other formats, which have no equivalent notion.
	RecordElement string `json:"record_element,omitempty"`
	// Columns is the fixed-width layout. Required when Format is "fixedw",
	// ignored otherwise: a fixed-width file has no delimiters, so nothing
	// can be read out of it without being told where the fields are.
	Columns        []fileformat.Column `json:"columns,omitempty"`
	PathStyle      bool                `json:"path_style"`
	AllowLocal     bool                `json:"allow_local"`
	TimeoutSeconds int                 `json:"timeout_seconds"`
	// MaxDecompressionRatio bounds inflated bytes / wire bytes for an object
	// stored with Content-Encoding: gzip (decompress.DefaultMaxRatio when
	// zero). See connectors/internal/decompress for why this is a ratio.
	MaxDecompressionRatio int `json:"max_decompression_ratio,omitempty"`
}

// parseConfig unmarshals and validates the connection fields shared by every
// action. Action-specific requirements (a key + format for get/put, a key for
// delete) are checked by each action's Open via the helpers below.
func parseConfig(raw []byte, into *config) error {
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("s3: bad config: %w", err)
	}
	return into.validateConn()
}

func (c *config) validateConn() error {
	if c.Bucket == "" {
		return errors.New("s3: bucket is required")
	}
	// Static, tenant-scoped credentials only — never fall back to ambient
	// environment/instance credentials.
	if c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return errors.New("s3: access_key_id and secret_access_key are required")
	}
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.TimeoutSeconds <= 0 {
		c.TimeoutSeconds = 300
	}
	return nil
}

// requireKeyFormat validates the get/put config: an object key and a supported
// record format (defaulting to ndjson).
func (c *config) requireKeyFormat() error {
	if c.Key == "" {
		return errors.New("s3: key is required")
	}
	if err := c.refuseDotSegments(c.Key, "key"); err != nil {
		return err
	}
	return fileformat.Validate("s3", &c.Format, c.Columns)
}

// requireKey validates the delete config: an object key.
func (c *config) requireKey() error {
	if c.Key == "" {
		return errors.New("s3: key is required")
	}
	return c.refuseDotSegments(c.Key, "key")
}

// refuseDotSegments rejects a `..` path segment, but ONLY when a custom
// endpoint is configured (TC-032).
//
// An S3 key is an opaque byte string: "/" is a display convention with no
// directory semantics, so `data/../x` addresses an object literally called
// `data/../x`. This connector therefore never cleans a key — cleaning would
// address a DIFFERENT object than the flow document names, which is the same
// mistake as following a traversal, inverted. Against AWS proper there is
// nothing to traverse and nothing to refuse.
//
// The risk is the middlebox. The AWS SDK percent-encodes control characters,
// but "." and "/" are legal path characters, so the key reaches the wire as
// written: `GET /bucket/../../etc/passwd`. A reverse proxy in front of an
// S3-compatible endpoint — nginx and friends normalise `..` in paths by
// default — can resolve that to a different resource, possibly outside the
// bucket.
//
// So the refusal is scoped to exactly the case that carries the risk: a custom
// `endpoint`, meaning something other than AWS is being addressed and a proxy
// may sit in front of it. No legitimate AWS key is refused, and the refusal is
// loud rather than silent — the alternative, quietly rewriting the key, would
// read the wrong object and report success.
func (c *config) refuseDotSegments(key, field string) error {
	if c.Endpoint == "" {
		return nil
	}
	for seg := range strings.SplitSeq(key, "/") {
		if seg != ".." {
			continue
		}
		return fmt.Errorf("s3: %s %q contains a %q path segment and a custom endpoint is configured; "+
			"S3 treats keys as opaque, but a normalising proxy in front of an S3-compatible endpoint can "+
			"resolve this to a different object. Address the object by its literal key, or remove the "+
			"custom endpoint to use AWS directly", field, key, "..")
	}
	return nil
}

// s3API is the minimal S3 surface this connector uses. The concrete
// *s3.Client satisfies it; tests substitute an in-memory fake so no network or
// MinIO is required.
type s3API interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// newClient builds the S3 API client from config. It is a package var so tests
// can inject an in-memory fake; production code uses the real builder.
var newClient = func(c *config) (s3API, error) { return c.buildClient() }

// buildClient constructs a real *s3.Client from the static config credentials
// and a network-guarded HTTP client. config.LoadDefaultConfig is intentionally
// not used: credentials are always the tenant-scoped static pair, never
// ambient environment/instance metadata.
func (c *config) buildClient() (s3API, error) {
	opts := s3.Options{
		Region:       c.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, ""),
		HTTPClient:   c.httpClient(),
		UsePathStyle: c.PathStyle,
	}
	if c.Endpoint != "" {
		opts.BaseEndpoint = aws.String(c.Endpoint)
	}
	return s3.New(opts), nil
}

// cgNAT is RFC 6598 shared address space (100.64.0.0/10), parsed once. It is
// not covered by net.IP.IsPrivate but is an internal-network range (and hosts
// some cloud metadata endpoints), so the guard blocks it too.
var cgNAT = func() *net.IPNet { _, n, _ := net.ParseCIDR("100.64.0.0/10"); return n }()

// httpClient builds an *http.Client whose dialer refuses internal-network
// targets unless AllowLocal is set: loopback, link-local (incl.
// 169.254.169.254 cloud metadata), unspecified, RFC1918/ULA private ranges,
// and CGNAT. The check runs post-resolution on the concrete dialed IP, so a DNS
// name — or a rebind — cannot smuggle a blocked IP past it. Mirrors the http
// connector's SSRF guard; default-deny is safe for a multi-tenant cloud hub,
// and a self-hosted runner pointing at internal MinIO sets allow_local.
func (c *config) httpClient() *http.Client {
	dialer := &net.Dialer{
		Timeout: 30 * time.Second,
		Control: guard(c.AllowLocal),
	}
	return &http.Client{
		Timeout: time.Duration(c.TimeoutSeconds) * time.Second,
		Transport: &http.Transport{
			DialContext:         dialer.DialContext,
			MaxIdleConns:        4,
			IdleConnTimeout:     60 * time.Second,
			ForceAttemptHTTP2:   true,
			TLSHandshakeTimeout: 15 * time.Second,
			// Never inflate anything on our behalf. The AWS SDK sets its own
			// Accept-Encoding today, so the transport does not transparently
			// decompress here — but that is a property of a dependency, not of
			// this code, and the identical transport in azureblobconn WAS
			// exposed by an SDK that does not. getSource decodes gzip
			// deliberately and meters it (TC-020).
			DisableCompression: true,
		},
	}
}

// guard returns a net.Dialer.Control hook enforcing the SSRF policy above,
// evaluated on the concrete post-DNS IP. Fail closed.
func guard(allowLocal bool) func(string, string, syscall.RawConn) error {
	return func(_, address string, _ syscall.RawConn) error {
		if allowLocal {
			return nil
		}
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("s3: bad dial address %q: %w", address, err)
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return fmt.Errorf("s3: unresolvable address %q", host)
		}
		switch {
		case ip.IsLoopback(), ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(), ip.IsUnspecified():
			return fmt.Errorf("s3: refusing %s (loopback/link-local; set allow_local for dev use)", ip)
		case ip.IsPrivate(), cgNAT.Contains(ip):
			return fmt.Errorf("s3: refusing %s (private/internal range; set allow_local to reach internal targets)", ip)
		}
		return nil
	}
}
