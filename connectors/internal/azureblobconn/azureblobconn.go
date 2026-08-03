// Package azureblobconn is the Azure Blob Storage connector: one canvas node
// with a verb dropdown (get/put/list/delete). get downloads a blob and streams
// it into typed record batches (ndjson or csv); put encodes batches and streams
// them up as a block blob; list enumerates a prefix, one record per blob; the
// config-driven delete verb removes a blob and emits a single status record.
//
// Auth is STATIC, tenant-scoped config — never ambient/managed-identity by
// default: an account name + account key, a connection string, or a container
// SAS URL. Secret fields arrive already-resolved as plaintext (the runner
// resolves {"$secret":...} refs before spawn — ADR-0010); this connector only
// tags the secret fields in its schema and never logs their values.
package azureblobconn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/aaron-au/shift/sdk"
)

// Connector returns the azureblob connector definition. One node, one verb
// dropdown; the node's source/sink role follows the verb's direction
// (ADR-0024): get/list/delete are sources, put is the sink.
func Connector() sdk.Connector {
	return sdk.Connector{
		Name:    "azureblob",
		Version: "0.1.0",
		Meta: &sdk.ConnectorMeta{
			Description: "Azure Blob Storage: pick a verb (get/put/list/delete). Static-credential auth (account key, connection string, or container SAS). Network-guarded.",
			Category:    "cloud-storage",
			Icon:        "🔷",
			Tags:        []string{"azure", "blob", "storage", "ndjson", "csv"},
		},
		Sources: map[string]func() sdk.SourceAction{
			"get":    func() sdk.SourceAction { return &getSource{} },
			"list":   func() sdk.SourceAction { return &listSource{} },
			"delete": func() sdk.SourceAction { return &deleteSource{} },
		},
		Sinks: map[string]func() sdk.SinkAction{
			"put": func() sdk.SinkAction { return &putSink{} },
		},
		Schemas: map[string][]byte{
			"get":    []byte(blobConfigSchema),
			"put":    []byte(blobConfigSchema),
			"list":   []byte(listConfigSchema),
			"delete": []byte(deleteConfigSchema),
		},
	}
}

// connProps is the shared connection/auth portion of every action's schema.
// Secret-typed fields carry x-shift-secret so the studio offers a secret picker.
const connProps = `
    "account": {"type": "string", "title": "Storage account", "description": "Account name (used with account_key)"},
    "account_key": {"type": "string", "title": "Account key", "description": "Shared key for the account", "x-shift-secret": true},
    "connection_string": {"type": "string", "title": "Connection string", "description": "Full storage connection string (alternative to account+key)", "x-shift-secret": true},
    "sas_url": {"type": "string", "title": "Container SAS URL", "description": "Container-scoped SAS URL (alternative to account credentials; container field then unused)", "x-shift-secret": true},
    "container": {"type": "string", "title": "Container", "description": "Blob container name (required unless a container SAS URL is used)"},
    "endpoint": {"type": "string", "title": "Blob endpoint", "description": "Override service endpoint for Azurite/custom clouds, e.g. http://127.0.0.1:10000/devstoreaccount1"},
    "allow_local": {"type": "boolean", "title": "Allow local/loopback and private/internal targets (network guard off; needed for Azurite)", "default": false}`

// Per-action schemas. get/put stream a single blob; list enumerates a prefix;
// delete removes the configured blob and emits a status record.
var (
	blobConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"Azure blob",
  "required":["blob"],"properties":{` + connProps + `,
    "blob": {"type": "string", "title": "Blob", "description": "Blob name/key within the container"},
    "format": {"type": "string", "title": "Format", "enum": ["ndjson", "csv"], "default": "ndjson"}
  }}`

	listConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"Azure blob list",
  "properties":{` + connProps + `,
    "prefix": {"type": "string", "title": "Prefix", "description": "List blobs whose name starts with this prefix (empty = whole container); emits one record per blob {name,size,etag,last_modified,content_type}"}
  }}`

	deleteConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"Azure blob delete",
  "required":["blob"],"properties":{` + connProps + `,
    "blob": {"type": "string", "title": "Blob", "description": "Blob name/key to delete (idempotent: a missing blob is a success)"}
  }}`
)

// config is the shared source/sink configuration across all verbs.
type config struct {
	Account          string `json:"account"`
	AccountKey       string `json:"account_key"`
	ConnectionString string `json:"connection_string"`
	SASURL           string `json:"sas_url"`
	Container        string `json:"container"`
	Endpoint         string `json:"endpoint"`
	Blob             string `json:"blob"`
	Prefix           string `json:"prefix"`
	Format           string `json:"format"`
	AllowLocal       bool   `json:"allow_local"`
}

// parseConfig unmarshals and validates the auth/connection fields shared by
// every action. Action-specific requirements (a blob name + format for
// get/put/delete) are checked by the action's Open via the helpers below.
func parseConfig(raw []byte, into *config) error {
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("azureblob: bad config: %w", err)
	}
	return into.validateAuth()
}

// validateAuth requires exactly one usable static-credential mode and, for the
// account-credential modes, a container. Managed identity / ambient credentials
// are never used: auth must be explicit tenant-scoped config.
func (c *config) validateAuth() error {
	modes := 0
	if c.Account != "" && c.AccountKey != "" {
		modes++
	}
	if c.ConnectionString != "" {
		modes++
	}
	if c.SASURL != "" {
		modes++
	}
	if modes == 0 {
		return errors.New("azureblob: provide account+account_key, connection_string, or sas_url")
	}
	// A container SAS URL is already container-scoped; every other mode needs
	// the container named explicitly.
	if c.SASURL == "" && c.Container == "" {
		return errors.New("azureblob: container is required")
	}
	return nil
}

// requireBlobFormat validates the get/put config: a blob name and a supported
// record format (defaulting to ndjson).
func (c *config) requireBlobFormat() error {
	if err := c.requireBlob(); err != nil {
		return err
	}
	if c.Format == "" {
		c.Format = "ndjson"
	}
	if c.Format != "ndjson" && c.Format != "csv" {
		return fmt.Errorf("azureblob: unsupported format %q (want ndjson or csv)", c.Format)
	}
	return nil
}

func (c *config) requireBlob() error {
	if c.Blob == "" {
		return errors.New("azureblob: blob is required")
	}
	return nil
}

// serviceURL is the blob service endpoint for the account-key mode: an explicit
// endpoint override (Azurite/custom) wins, otherwise the standard public host.
func (c *config) serviceURL() string {
	if c.Endpoint != "" {
		return c.Endpoint
	}
	return fmt.Sprintf("https://%s.blob.core.windows.net/", c.Account)
}

// blobInfo is the subset of a blob's metadata the list verb emits.
type blobInfo struct {
	Name         string
	Size         int64
	ETag         string
	LastModified time.Time
	ContentType  string
}

// errNotFound is the store-level sentinel for "blob does not exist". The real
// azStore maps Azure's BlobNotFound onto it and the test fake returns it, so
// the idempotent-delete path is testable without a live service.
var errNotFound = errors.New("azureblob: blob not found")

// blobStore is the minimal surface over the Azure Blob client that the verbs
// use. Defining it as an interface lets every verb be unit-tested against an
// in-memory fake — no network, no Azurite (the real impl is azStore).
type blobStore interface {
	// Download opens a blob for streaming reads; errNotFound if it is absent.
	Download(ctx context.Context, blob string) (io.ReadCloser, error)
	// Upload streams r into a (overwriting) block blob. Overwrite makes a
	// re-dispatched put idempotent by blob key (ADR-0002 at-least-once).
	Upload(ctx context.Context, blob string, r io.Reader) error
	// Delete removes a blob, returning errNotFound if it was already absent.
	Delete(ctx context.Context, blob string) error
	// List invokes fn for every blob under prefix (streamed, page by page).
	List(ctx context.Context, prefix string, fn func(blobInfo) error) error
}

// storeOpener builds a blobStore from config. Verbs default to openStore; tests
// swap in a fake by setting the action's open field before Open.
type storeOpener func(ctx context.Context, c *config) (blobStore, error)

// openStore is the production storeOpener: it builds a network-guarded Azure
// container client from the configured static credentials.
func openStore(_ context.Context, c *config) (blobStore, error) {
	cc, err := c.containerClient()
	if err != nil {
		return nil, err
	}
	return &azStore{cc: cc}, nil
}

// containerClient constructs a container-scoped Azure client for the selected
// auth mode (SAS URL > connection string > account key). Construction is
// offline (no dial); every mode routes egress through the network guard.
func (c *config) containerClient() (*container.Client, error) {
	transport := guardedClient(c.AllowLocal)
	switch {
	case c.SASURL != "":
		opts := &container.ClientOptions{}
		opts.Transport = transport
		return container.NewClientWithNoCredential(c.SASURL, opts)
	case c.ConnectionString != "":
		opts := &azblob.ClientOptions{}
		opts.Transport = transport
		svc, err := azblob.NewClientFromConnectionString(c.ConnectionString, opts)
		if err != nil {
			return nil, fmt.Errorf("azureblob: connection string: %w", err)
		}
		return svc.ServiceClient().NewContainerClient(c.Container), nil
	default: // account + account key
		cred, err := azblob.NewSharedKeyCredential(c.Account, c.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("azureblob: shared key: %w", err)
		}
		opts := &azblob.ClientOptions{}
		opts.Transport = transport
		svc, err := azblob.NewClientWithSharedKeyCredential(c.serviceURL(), cred, opts)
		if err != nil {
			return nil, fmt.Errorf("azureblob: shared-key client: %w", err)
		}
		return svc.ServiceClient().NewContainerClient(c.Container), nil
	}
}

// azStore is the production blobStore backed by an Azure container client.
type azStore struct {
	cc *container.Client
}

func (s *azStore) Download(ctx context.Context, blob string) (io.ReadCloser, error) {
	resp, err := s.cc.NewBlobClient(blob).DownloadStream(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, errNotFound
		}
		return nil, err
	}
	return resp.Body, nil
}

func (s *azStore) Upload(ctx context.Context, blob string, r io.Reader) error {
	_, err := s.cc.NewBlockBlobClient(blob).UploadStream(ctx, r, nil)
	return err
}

func (s *azStore) Delete(ctx context.Context, blob string) error {
	_, err := s.cc.NewBlobClient(blob).Delete(ctx, nil)
	if err != nil && bloberror.HasCode(err, bloberror.BlobNotFound) {
		return errNotFound
	}
	return err
}

func (s *azStore) List(ctx context.Context, prefix string, fn func(blobInfo) error) error {
	var opts *container.ListBlobsFlatOptions
	if prefix != "" {
		opts = &container.ListBlobsFlatOptions{Prefix: &prefix}
	}
	pager := s.cc.NewListBlobsFlatPager(opts)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return err
		}
		if page.Segment == nil {
			continue
		}
		for _, item := range page.Segment.BlobItems {
			info := blobInfo{}
			if item.Name != nil {
				info.Name = *item.Name
			}
			if p := item.Properties; p != nil {
				if p.ContentLength != nil {
					info.Size = *p.ContentLength
				}
				if p.ETag != nil {
					info.ETag = string(*p.ETag)
				}
				if p.LastModified != nil {
					info.LastModified = *p.LastModified
				}
				if p.ContentType != nil {
					info.ContentType = *p.ContentType
				}
			}
			if err := fn(info); err != nil {
				return err
			}
		}
	}
	return nil
}

// cgNAT is RFC 6598 shared address space (100.64.0.0/10), not covered by
// net.IP.IsPrivate but an internal-network range (and cloud metadata host).
var cgNAT = func() *net.IPNet { _, n, _ := net.ParseCIDR("100.64.0.0/10"); return n }()

// checkAddr is the network-guard decision, split out so it is unit-testable.
// It refuses loopback/link-local/unspecified and (unless allowLocal) private
// and CGNAT ranges on the concrete post-DNS IP, mirroring the http/sftp SSRF
// guards (fail closed). Azure's public endpoints are unaffected; Azurite and
// internal endpoints require allow_local.
func checkAddr(allowLocal bool, address string) error {
	if allowLocal {
		return nil
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("azureblob: bad dial address %q: %w", address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("azureblob: unresolvable address %q", host)
	}
	switch {
	case ip.IsLoopback(), ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast(), ip.IsUnspecified():
		return fmt.Errorf("azureblob: refusing %s (loopback/link-local; set allow_local for dev/Azurite)", ip)
	case ip.IsPrivate(), cgNAT.Contains(ip):
		return fmt.Errorf("azureblob: refusing %s (private/internal range; set allow_local to reach internal targets)", ip)
	}
	return nil
}

// guardedClient builds the *http.Client the Azure SDK pipeline uses. Its dialer
// runs checkAddr on the concrete dialed IP so a DNS name — or a rebind — cannot
// smuggle a blocked target past the guard. No client-level Timeout: blob
// transfers stream arbitrarily large payloads and are bounded by context.
func guardedClient(allowLocal bool) *http.Client {
	dialer := &net.Dialer{
		Timeout: 30 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			return checkAddr(allowLocal, address)
		},
	}
	return &http.Client{
		Transport: &http.Transport{
			DialContext:         dialer.DialContext,
			MaxIdleConns:        8,
			IdleConnTimeout:     60 * time.Second,
			ForceAttemptHTTP2:   true,
			TLSHandshakeTimeout: 15 * time.Second,
		},
	}
}

// resolveOpener returns the action's injected opener or the production default.
func resolveOpener(o storeOpener) storeOpener {
	if o != nil {
		return o
	}
	return openStore
}
