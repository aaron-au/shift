// Package fsconn is the local/mounted filesystem connector: a streaming source
// that reads a file and emits typed record batches, a sink that atomically
// writes batches to a file (temp-then-rename), a directory-listing source, and
// config-driven file-management verbs (delete/mkdir/rmdir). Records are
// parsed/written via engine/format (ndjson or csv).
//
// Every path is jailed within a configured root directory: the target is
// lexically resolved and rejected if it escapes root (.. traversal or an
// absolute path pointing outside). This mirrors the sftp connector's guard but
// on the local filesystem — a shared/cloud runner must not let an
// attacker-influenced path reach arbitrary host files. Fail closed: root is
// mandatory. stdlib only (os, io, path/filepath).
package fsconn

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aaron-au/shift/sdk"
)

// Connector returns the fs connector definition.
func Connector() sdk.Connector {
	return sdk.Connector{
		Name:    "fs",
		Version: "0.1.0",
		Meta: &sdk.ConnectorMeta{
			Description: "Local/mounted filesystem: pick a verb (get/put/list/delete/mkdir/rmdir) and a path. All paths are jailed within a configured root.",
			Category:    "file-transfer",
			Icon:        "🗂️",
			Tags:        []string{"fs", "file", "filesystem", "local", "ndjson", "csv"},
		},
		// Every verb except put is a source: configure it with a verb + path
		// and it runs standalone (the op verbs emit a single status record).
		// put is the one sink — it consumes the pipeline's records to a file.
		Sources: map[string]func() sdk.SourceAction{
			"get":    func() sdk.SourceAction { return &getSource{} },
			"list":   func() sdk.SourceAction { return &listSource{} },
			"delete": func() sdk.SourceAction { return &opSource{op: opDelete} },
			"mkdir":  func() sdk.SourceAction { return &opSource{op: opMkdir} },
			"rmdir":  func() sdk.SourceAction { return &opSource{op: opRmdir} },
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
		},
	}
}

// rootProp is the shared jail-root portion of every action's config schema.
const rootProp = `
    "root": {"type": "string", "title": "Root directory", "description": "Base directory; every path is resolved within it and rejected if it escapes. Required (fail closed)."}`

// Per-action schemas. get/put stream a file; list reads a directory; the op
// sources (delete/mkdir/rmdir) take their target from config, so their config
// is root + path only.
var (
	fileConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"FS file",
  "required":["root","path"],"properties":{` + rootProp + `,
    "path": {"type": "string", "title": "Path", "description": "File path, relative to root (an absolute path must still resolve within root)"},
    "format": {"type": "string", "title": "Format", "enum": ["ndjson", "csv"], "default": "ndjson"}
  }}`

	listConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"FS list",
  "required":["root","path"],"properties":{` + rootProp + `,
    "path": {"type": "string", "title": "Directory", "description": "Directory to list; emits one record per entry {name,path,size,mode,mod_time,is_dir}"},
    "recursive": {"type": "boolean", "title": "Recursive", "description": "Descend into sub-directories", "default": false}
  }}`

	opPathSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"FS operation",
  "required":["root","path"],"properties":{` + rootProp + `,
    "path": {"type": "string", "title": "Path", "description": "Target file/directory"}
  }}`

	rmdirConfigSchema = `{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"FS rmdir",
  "required":["root","path"],"properties":{` + rootProp + `,
    "path": {"type": "string", "title": "Directory"},
    "recursive": {"type": "boolean", "title": "Recursive", "description": "Remove non-empty directories and their contents", "default": false}
  }}`
)

// config is the shared source/sink configuration.
type config struct {
	Root      string `json:"root"`
	Path      string `json:"path"`
	Format    string `json:"format"`
	Recursive bool   `json:"recursive"`
}

// parseConfig unmarshals and validates the connection-level fields (root).
// Action-specific requirements (a file path + format, a directory) are checked
// by the action's Open via the helpers below.
func parseConfig(raw []byte, into *config) error {
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("fs: bad config: %w", err)
	}
	return into.validateRoot()
}

func (c *config) validateRoot() error {
	if c.Root == "" {
		return errors.New("fs: root is required")
	}
	return nil
}

// requireFileFormat validates the get/put config: a file path and a supported
// record format (defaulting to ndjson).
func (c *config) requireFileFormat() error {
	if c.Path == "" {
		return errors.New("fs: path is required")
	}
	if c.Format == "" {
		c.Format = "ndjson"
	}
	if c.Format != "ndjson" && c.Format != "csv" {
		return fmt.Errorf("fs: unsupported format %q (want ndjson or csv)", c.Format)
	}
	return nil
}

// requireDir validates the list config: a directory path.
func (c *config) requireDir() error {
	if c.Path == "" {
		return errors.New("fs: path (directory) is required")
	}
	return nil
}

// requirePath validates an op verb's config: a target path.
func (c *config) requirePath() error {
	if c.Path == "" {
		return errors.New("fs: path is required")
	}
	return nil
}

// resolve turns a user-supplied path into an absolute host path proven to live
// within root, or an error. It is the jail guard — the local-filesystem
// equivalent of the http/sftp network guard. The check is lexical: p (relative
// to root, or absolute) is cleaned and compared to root via filepath.Rel, so a
// ".." traversal or an absolute path outside root is rejected fail-closed.
//
// Note: this is a lexical containment check; it does not resolve symlinks, so a
// pre-existing symlink under root that points outside root is not followed-and-
// re-checked. Deployments that expose untrusted roots should mount them without
// escaping symlinks (matching the sftp connector's server-side scope).
func (c *config) resolve(p string) (string, error) {
	if c.Root == "" {
		return "", errors.New("fs: root is required")
	}
	root, err := filepath.Abs(c.Root)
	if err != nil {
		return "", fmt.Errorf("fs: bad root %q: %w", c.Root, err)
	}
	clean := filepath.Clean(p)
	full := clean
	if !filepath.IsAbs(clean) {
		full = filepath.Join(root, clean)
	}
	full = filepath.Clean(full)
	rel, err := filepath.Rel(root, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("fs: path %q escapes root %q", p, c.Root)
	}
	return full, nil
}

// resolvedRoot returns the absolute, cleaned root (used by list to relativize
// emitted entry paths back into the jailed view).
func (c *config) resolvedRoot() (string, error) {
	return filepath.Abs(c.Root)
}

// ignoreMissing swallows a not-found error so operations stay idempotent under
// at-least-once redelivery.
func ignoreMissing(err error) error {
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
