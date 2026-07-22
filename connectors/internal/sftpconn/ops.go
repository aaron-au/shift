package sftpconn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aaron-au/shift/engine/record"
	"github.com/pkg/sftp"
)

// opKind selects which side-effecting file operation an opSource performs. The
// target path(s) come from config, so a single node runs standalone: pick the
// verb, give the path, deploy. Each op emits one status record.
type opKind int

const (
	opDelete opKind = iota // Remove path (idempotent: missing = ok)
	opMkdir                // MkdirAll path (idempotent)
	opRmdir                // remove path dir (RemoveAll if recursive)
	opRename               // PosixRename from → to
)

func (k opKind) name() string {
	switch k {
	case opDelete:
		return "delete"
	case opMkdir:
		return "mkdir"
	case opRmdir:
		return "rmdir"
	case opRename:
		return "rename"
	default:
		return "unknown"
	}
}

// requireOpArgs validates the op's target config: rename needs from+to, the
// rest need path.
func (c *config) requireOpArgs(op opKind) error {
	if op == opRename {
		if c.From == "" || c.To == "" {
			return errors.New("sftp: rename requires from and to")
		}
		return nil
	}
	if c.Path == "" {
		return errors.New("sftp: path is required")
	}
	return nil
}

// opSource performs one config-driven file operation and emits a single status
// record ({op, path|from/to, ok}). It is a source so a one-verb flow is
// runnable on its own; all ops are idempotent under at-least-once redelivery.
type opSource struct {
	op    opKind
	cfg   config
	done  bool
	batch *record.Batch
}

func (s *opSource) Open(_ context.Context, config []byte) error {
	if err := parseConfig(config, &s.cfg); err != nil {
		return err
	}
	return s.cfg.requireOpArgs(s.op)
}

func (s *opSource) Next(ctx context.Context) (*record.Batch, error) {
	if s.done {
		return nil, io.EOF
	}
	s.done = true
	sc, closer, err := s.cfg.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer() }()
	if err := s.perform(sc); err != nil {
		return nil, err
	}

	s.batch = record.NewBatch()
	bld := s.batch.Builder()
	bld.BeginMap()
	bld.KeyLiteral("op")
	bld.StringLiteral(s.op.name())
	if s.op == opRename {
		bld.KeyLiteral("from")
		bld.StringLiteral(s.cfg.From)
		bld.KeyLiteral("to")
		bld.StringLiteral(s.cfg.To)
	} else {
		bld.KeyLiteral("path")
		bld.StringLiteral(s.cfg.Path)
	}
	bld.KeyLiteral("ok")
	bld.Bool(true)
	bld.EndMap()
	s.batch.Append(bld.Finish())
	return s.batch, nil
}

func (s *opSource) perform(sc *sftp.Client) error {
	switch s.op {
	case opDelete:
		return ignoreMissing(sc.Remove(s.cfg.Path))
	case opMkdir:
		return sc.MkdirAll(s.cfg.Path)
	case opRmdir:
		if s.cfg.Recursive {
			return ignoreMissing(sc.RemoveAll(s.cfg.Path))
		}
		return ignoreMissing(sc.RemoveDirectory(s.cfg.Path))
	case opRename:
		// Idempotent: a missing source means a prior attempt already renamed.
		return ignoreMissing(sc.PosixRename(s.cfg.From, s.cfg.To))
	default:
		return fmt.Errorf("sftp: unknown operation %d", s.op)
	}
}

func (s *opSource) Close() error { return nil }

// ignoreMissing swallows a not-found error so operations stay idempotent.
func ignoreMissing(err error) error {
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
