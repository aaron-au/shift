package ftpconn

import (
	"context"
	"io"
	"path"
	"time"

	"github.com/aaron-au/shift/engine/record"
	"github.com/jlaffaye/ftp"
)

// listBatch caps how many directory entries one Next emits.
const listBatch = 512

// listSource lists a remote directory, emitting one record per entry:
// {name, path, size, type, mod_time}. The directory is read once at Open (the
// connection closes immediately after) and iterated from memory.
type listSource struct {
	dial    dialFunc // nil ⇒ realDial (test seam)
	cfg     config
	dir     string
	entries []*ftp.Entry
	idx     int
	batch   *record.Batch
}

func (s *listSource) Open(ctx context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	if err := s.cfg.requireDir(); err != nil {
		return err
	}
	conn, closer, err := dialOr(s.dial)(ctx, &s.cfg)
	if err != nil {
		return err
	}
	defer func() { _ = closer() }() // listing is one-shot; don't hold the connection
	entries, err := conn.List(s.cfg.Path)
	if err != nil {
		return err
	}
	s.dir, s.entries, s.batch = s.cfg.Path, entries, record.NewBatch()
	return nil
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
		bld.StringLiteral(e.Name)
		bld.KeyLiteral("path")
		bld.StringLiteral(path.Join(s.dir, e.Name))
		bld.KeyLiteral("size")
		bld.Int(int64(e.Size)) //nolint:gosec // G115: FTP entry sizes fit int64 in practice; record model is int64
		bld.KeyLiteral("type")
		bld.StringLiteral(entryType(e.Type))
		bld.KeyLiteral("mod_time")
		bld.StringLiteral(e.Time.UTC().Format(time.RFC3339))
		bld.EndMap()
		s.batch.Append(bld.Finish())
	}
	return s.batch, nil
}

func (s *listSource) Close() error { return nil }

// entryType maps the FTP entry kind to a stable string in the emitted record.
func entryType(t ftp.EntryType) string {
	switch t {
	case ftp.EntryTypeFile:
		return "file"
	case ftp.EntryTypeFolder:
		return "dir"
	case ftp.EntryTypeLink:
		return "link"
	default:
		return "unknown"
	}
}
