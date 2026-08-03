package ftpconn

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aaron-au/shift/engine/record"
)

// opKind selects which side-effecting file operation an opSource performs. The
// target path(s) come from config, so a single node runs standalone: pick the
// verb, give the path, deploy. Each op emits one status record.
type opKind int

const (
	opDelete opKind = iota // Delete path (idempotent: missing = ok)
	opMkdir                // MakeDir path (idempotent: existing = ok)
	opRmdir                // RemoveDir path (RemoveDirRecur if recursive)
	opRename               // Rename from → to
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
			return errors.New("ftp: rename requires from and to")
		}
		return nil
	}
	if c.Path == "" {
		return errors.New("ftp: path is required")
	}
	return nil
}

// opSource performs one config-driven file operation and emits a single status
// record ({op, path|from/to, ok}). It is a source so a one-verb flow is runnable
// on its own; all ops are idempotent under at-least-once redelivery.
type opSource struct {
	op    opKind
	dial  dialFunc // nil ⇒ realDial (test seam)
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
	conn, closer, err := dialOr(s.dial)(ctx, &s.cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer() }()
	if err := s.perform(conn); err != nil {
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

func (s *opSource) perform(conn ftpConn) error {
	switch s.op {
	case opDelete:
		return ignore550(conn.Delete(s.cfg.Path))
	case opMkdir:
		return ignore550(conn.MakeDir(s.cfg.Path))
	case opRmdir:
		if s.cfg.Recursive {
			return ignore550(conn.RemoveDirRecur(s.cfg.Path))
		}
		return ignore550(conn.RemoveDir(s.cfg.Path))
	case opRename:
		// Idempotent: a missing source means a prior attempt already renamed.
		return ignore550(conn.Rename(s.cfg.From, s.cfg.To))
	default:
		return fmt.Errorf("ftp: unknown operation %d", s.op)
	}
}

func (s *opSource) Close() error { return nil }
