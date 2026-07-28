package fsconn

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// listBatch caps how many directory entries one Next emits.
const listBatch = 512

// entry is one directory entry captured at Open. path is relative to the
// configured root (the jailed view), so it can be fed straight back into a
// get/delete verb on the same node's root.
type entry struct {
	name    string
	relPath string
	size    int64
	mode    string
	modTime time.Time
	isDir   bool
}

// listSource lists a directory, emitting one record per entry:
// {name, path, size, mode, mod_time, is_dir}. The directory is read once at
// Open and iterated from memory. With recursive=true it descends the whole
// tree (skipping the listed directory itself).
type listSource struct {
	entries []entry
	idx     int
	batch   *record.Batch
}

func (s *listSource) Open(_ context.Context, raw []byte) error {
	var cfg config
	if err := parseConfig(raw, &cfg); err != nil {
		return err
	}
	if err := cfg.requireDir(); err != nil {
		return err
	}
	full, err := cfg.resolve(cfg.Path)
	if err != nil {
		return err
	}
	root, err := cfg.resolvedRoot()
	if err != nil {
		return err
	}
	entries, err := readEntries(full, root, cfg.Recursive)
	if err != nil {
		return err
	}
	s.entries, s.batch = entries, record.NewBatch()
	return nil
}

// readEntries walks (recursive) or reads (flat) dir, returning entries with
// root-relative paths. It skips dir itself.
func readEntries(dir, root string, recursive bool) ([]entry, error) {
	var out []entry
	add := func(name, path string, info fs.FileInfo) {
		rel, err := filepath.Rel(root, path)
		if err != nil {
			rel = path
		}
		out = append(out, entry{
			name:    name,
			relPath: rel,
			size:    info.Size(),
			mode:    info.Mode().String(),
			modTime: info.ModTime(),
			isDir:   info.IsDir(),
		})
	}
	if recursive {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if path == dir { // skip the listed directory itself
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			add(d.Name(), path, info)
			return nil
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, d := range des {
		info, err := d.Info()
		if err != nil {
			return nil, err
		}
		add(d.Name(), filepath.Join(dir, d.Name()), info)
	}
	return out, nil
}

func (s *listSource) Next(_ context.Context) (*record.Batch, error) {
	if s.idx >= len(s.entries) {
		return nil, io.EOF
	}
	s.batch.Reset()
	bld := s.batch.Builder()
	for n := 0; s.idx < len(s.entries) && n < listBatch; s.idx, n = s.idx+1, n+1 {
		e := s.entries[s.idx]
		bld.BeginMap()
		bld.KeyLiteral("name")
		bld.StringLiteral(e.name)
		bld.KeyLiteral("path")
		bld.StringLiteral(e.relPath)
		bld.KeyLiteral("size")
		bld.Int(e.size)
		bld.KeyLiteral("mode")
		bld.StringLiteral(e.mode)
		bld.KeyLiteral("mod_time")
		bld.StringLiteral(e.modTime.UTC().Format(time.RFC3339))
		bld.KeyLiteral("is_dir")
		bld.Bool(e.isDir)
		bld.EndMap()
		s.batch.Append(bld.Finish())
	}
	return s.batch, nil
}

func (s *listSource) Close() error { return nil }
