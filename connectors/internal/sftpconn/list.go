package sftpconn

import (
	"context"
	"io"
	"os"
	"path"
	"time"

	"github.com/aaron-au/shift/engine/record"
)

// listBatch caps how many directory entries one Next emits.
const listBatch = 512

// listSource lists a remote directory, emitting one record per entry:
// {name, path, size, mode, mod_time, is_dir}. The directory is read once at
// Open (the connection closes immediately after) and iterated from memory.
type listSource struct {
	cfg     config
	dir     string
	entries []os.FileInfo
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
	sc, closer, err := s.cfg.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = closer() }() // listing is a one-shot; don't hold the connection
	entries, err := sc.ReadDir(s.cfg.Path)
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
		bld.StringLiteral(e.Name())
		bld.KeyLiteral("path")
		bld.StringLiteral(path.Join(s.dir, e.Name()))
		bld.KeyLiteral("size")
		bld.Int(e.Size())
		bld.KeyLiteral("mode")
		bld.StringLiteral(e.Mode().String())
		bld.KeyLiteral("mod_time")
		bld.StringLiteral(e.ModTime().UTC().Format(time.RFC3339))
		bld.KeyLiteral("is_dir")
		bld.Bool(e.IsDir())
		bld.EndMap()
		s.batch.Append(bld.Finish())
	}
	return s.batch, nil
}

func (s *listSource) Close() error { return nil }
