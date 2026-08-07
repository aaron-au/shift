package store

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// invalidTextRepresentation is Postgres SQLSTATE 22P02 — the error it raises
// while parsing a value it cannot read as the column's type.
const invalidTextRepresentation = "22P02"

// IsMalformedID reports whether err is Postgres refusing to parse an id.
//
// Every id in this schema is a UUID, and a caller who supplies something else
// has made a CLIENT mistake. Without this the driver error travels up as a
// generic failure and the API answers 500, which tells an operator the hub is
// broken when their URL was merely wrong — and buries a real 500 among the
// typos in whatever they monitor.
func IsMalformedID(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == invalidTextRepresentation
}
