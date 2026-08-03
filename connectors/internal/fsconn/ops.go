package fsconn

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aaron-au/shift/engine/record"
)

// opKind selects which side-effecting file operation an opSource performs. The
// target path comes from config, so a single node runs standalone: pick the
// verb, give the path, deploy. Each op emits one status record.
type opKind int

const (
	opDelete opKind = iota // Remove path (idempotent: missing = ok)
	opMkdir                // MkdirAll path (idempotent)
	opRmdir                // remove path dir (RemoveAll if recursive)
)

func (k opKind) name() string {
	switch k {
	case opDelete:
		return "delete"
	case opMkdir:
		return "mkdir"
	case opRmdir:
		return "rmdir"
	default:
		return "unknown"
	}
}

// opSource performs one config-driven file operation and emits a single status
// record ({op, path, ok}). It is a source so a one-verb flow is runnable on its
// own; all ops are idempotent under at-least-once redelivery.
type opSource struct {
	op    opKind
	cfg   config
	done  bool
	batch *record.Batch
}

func (s *opSource) Open(_ context.Context, raw []byte) error {
	if err := parseConfig(raw, &s.cfg); err != nil {
		return err
	}
	return s.cfg.requirePath()
}

func (s *opSource) Next(_ context.Context) (*record.Batch, error) {
	if s.done {
		return nil, io.EOF
	}
	s.done = true
	full, err := s.cfg.resolve(s.cfg.Path)
	if err != nil {
		return nil, err
	}
	if err := s.perform(full); err != nil {
		return nil, err
	}

	s.batch = record.NewBatch()
	bld := s.batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("op")
	bld.StringLiteral(s.op.name())
	bld.KeyLiteral("path")
	bld.StringLiteral(s.cfg.Path)
	bld.KeyLiteral("ok")
	bld.Bool(true)
	bld.EndMap()
	s.batch.Append(bld.Finish())
	return s.batch, nil
}

func (s *opSource) perform(full string) error {
	switch s.op {
	case opDelete:
		return ignoreMissing(os.Remove(full))
	case opMkdir:
		return os.MkdirAll(full, 0o750)
	case opRmdir:
		if s.cfg.Recursive {
			return ignoreMissing(os.RemoveAll(full))
		}
		// os.Remove on a directory removes it only when empty.
		return ignoreMissing(os.Remove(full))
	default:
		return fmt.Errorf("fs: unknown operation %d", s.op)
	}
}

func (s *opSource) Close() error { return nil }
