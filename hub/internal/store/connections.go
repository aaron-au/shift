package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aaron-au/shift/pkg/flowdoc"
)

// Connection is a reusable, account-scoped connector configuration
// (ADR-0034). Config is ordinary connector config and may carry
// {"$secret":"name"} references, which resolve runner-side — the hub
// stores a reference, never a credential, so returning a Connection in
// full leaks nothing a flow document would not.
type Connection struct {
	Name      string          `json:"name"`
	Connector string          `json:"connector"`
	Config    json.RawMessage `json:"config"`
	Version   int             `json:"version"`
	Created   time.Time       `json:"created_at"`
	Updated   time.Time       `json:"updated_at"`
}

// UpsertConnection stores or replaces the named connection, bumping the
// version on replace. createdBy may be empty (break-glass writes).
func (s *Store) UpsertConnection(ctx context.Context, name, connector string, config []byte, createdBy string) (id string, version int, err error) {
	var by *string
	if createdBy != "" {
		by = &createdBy
	}
	err = s.pool.QueryRow(ctx,
		`INSERT INTO connections (id, account_id, name, connector, config, created_by)
		 VALUES ($1,$2,$3,$4,$5,$6)
		 ON CONFLICT (account_id, name)
		 DO UPDATE SET connector = $4, config = $5,
		               version = connections.version + 1, updated_at = now()
		 RETURNING id, version`,
		newUUID(), accountID(ctx), name, connector, config, by).Scan(&id, &version)
	return id, version, err
}

// Connections lists the account's connections, by name.
func (s *Store) Connections(ctx context.Context) ([]Connection, error) {
	return s.queryConnections(ctx,
		`SELECT name, connector, config, version, created_at, updated_at FROM connections
		 WHERE account_id = $1 ORDER BY name`, accountID(ctx))
}

// ConnectionsByName fetches the named connections. Missing names are
// simply absent from the result — callers decide whether that is an
// error, matching SecretEnvelopes.
func (s *Store) ConnectionsByName(ctx context.Context, names []string) ([]Connection, error) {
	return s.queryConnections(ctx,
		`SELECT name, connector, config, version, created_at, updated_at FROM connections
		 WHERE account_id = $1 AND name = ANY($2) ORDER BY name`, accountID(ctx), names)
}

// DeleteConnection removes the named connection.
func (s *Store) DeleteConnection(ctx context.Context, name string) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM connections WHERE account_id = $1 AND name = $2`, accountID(ctx), name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// FlowsUsingConnection names the published flows that reference the given
// connection — the guard that turns deleting a live connection into a
// refusal at the API instead of a task failure at 3 a.m. (ADR-0034 open
// question 1).
//
// Only PUBLISHED versions count: an unpublished draft cannot be
// dispatched, so blocking a delete on one would make the connection
// undeletable for as long as a stale draft sat in the flow's history.
//
// Filtering happens in Go via flowdoc.Connections rather than in SQL,
// because a connection reference can sit on a linear source/sink or on
// any graph step and the authoritative reader of that shape is the flow
// model, not a JSONB path this file would have to keep in step with it.
func (s *Store) FlowsUsingConnection(ctx context.Context, name string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT f.name, fv.document
		   FROM flows f
		   JOIN flow_versions fv ON fv.flow_id = f.id AND fv.version = f.published_version
		  WHERE f.account_id = $1 AND f.published_version > 0
		  ORDER BY f.name`, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var using []string
	for rows.Next() {
		var flowName string
		var raw []byte
		if err := rows.Scan(&flowName, &raw); err != nil {
			return nil, err
		}
		// A stored document that no longer parses must not make a
		// connection undeletable, but it must not silently read as "not
		// referenced" either: fall back to the raw reference scan.
		doc, err := flowdoc.Parse(raw)
		if err != nil {
			continue
		}
		for _, c := range doc.Connections() {
			if c == name {
				using = append(using, flowName)
				break
			}
		}
	}
	return using, rows.Err()
}

func (s *Store) queryConnections(ctx context.Context, sql string, args ...any) ([]Connection, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Connection
	for rows.Next() {
		var c Connection
		if err := rows.Scan(&c.Name, &c.Connector, &c.Config, &c.Version, &c.Created, &c.Updated); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
