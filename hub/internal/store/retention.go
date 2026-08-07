package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Connector retention (ADR-0047 §2/§3).
//
// Pinning made published flows immutable about which build they run, which
// raises the obvious question: when may a build ever be deleted? "Never" means
// supporting v0.0.1 forever; "after N months" is a time bomb that breaks a flow
// nobody edited. So retention is by REFERENCE — a build is kept while a flow
// that runs it still exists — with a floor so a rollback has somewhere to land.
//
// Two verbs, deliberately not conflated (§3):
//
//   - yank    — "do not select this for NEW pins". Existing pins keep
//     resolving. It is a selection rule, and it takes effect immediately.
//   - collect — actual deletion. It only ever touches builds nothing
//     references, so by construction it cannot break a published flow.
//
// The reference set is the pins of each flow's CURRENT published version and
// the one before it. Counting every version ever published would keep
// everything forever (a superseded version stays 'published' on purpose, so it
// stays runnable); counting only the current one would let a rollback land on a
// build that had been collected.

// FlowRef is one published flow version that pins a connector build.
type FlowRef struct {
	Flow    string   `json:"flow"`
	Version int      `json:"version"`
	Steps   []string `json:"steps"`
	// Current reports whether this is the flow's published version — the one
	// running now, as opposed to the one it would roll back to.
	Current bool `json:"current"`
}

// ConnectorRef identifies one artifact row.
type ConnectorRef struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	OS      string `json:"os"`
	Arch    string `json:"arch"`
}

// String renders a ref the way an operator would say it.
func (r ConnectorRef) String() string {
	return fmt.Sprintf("%s@%s (%s/%s)", r.Name, r.Version, r.OS, r.Arch)
}

// retainedPins is the reference set: pins held by each flow's published
// version and its predecessor.
const retainedPins = `
	SELECT p.connector, p.version
	  FROM flow_connector_pins p
	  JOIN (SELECT flow_id, version,
	               row_number() OVER (PARTITION BY flow_id ORDER BY published_at DESC) AS rn
	          FROM flow_versions
	         WHERE published_at IS NOT NULL) fv
	    ON fv.flow_id = p.flow_id AND fv.version = p.flow_version
	 WHERE p.account_id = $1 AND fv.rn <= 2`

// versionFloor is the always-retained set: the newest two versions of every
// connector, referenced or not, so a rollback has something to roll back to.
const versionFloor = `
	SELECT connector_id, version FROM (
	  SELECT connector_id, version,
	         row_number() OVER (PARTITION BY connector_id ORDER BY newest DESC) AS rn
	    FROM (SELECT connector_id, version, max(created_at) AS newest
	            FROM connector_versions GROUP BY connector_id, version) v
	) ranked WHERE rn <= 2`

// ConnectorReferences reports which published flow versions pin one connector
// build.
//
// One query behind four features: it warns before a yank, drives the EOL
// notice list, decides what GC may remove, and answers "which flows am I about
// to upgrade?" for a bulk republish. They agree because they read the same
// index rather than each deciding what "in use" means.
func (s *Store) ConnectorReferences(ctx context.Context, connector, version string) ([]FlowRef, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT f.name, p.flow_version, array_agg(p.step_id ORDER BY p.step_id),
		        bool_or(f.published_version = p.flow_version)
		   FROM flow_connector_pins p
		   JOIN flows f ON f.id = p.flow_id
		  WHERE p.account_id = $1 AND p.connector = $2 AND p.version = $3
		  GROUP BY f.name, p.flow_version
		  ORDER BY f.name, p.flow_version`,
		accountID(ctx), connector, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []FlowRef{}
	for rows.Next() {
		var r FlowRef
		if err := rows.Scan(&r.Flow, &r.Version, &r.Steps, &r.Current); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CollectableConnectorVersions reports what GC would remove, without removing
// it.
//
// A dry run is not a courtesy here. Deleting a signed artifact is the one
// irreversible thing the registry does — the publisher's private key is not
// server-side, so a version deleted by mistake cannot be regenerated from
// anything the hub holds. Seeing the list first is the difference between a
// cleanup and an outage.
func (s *Store) CollectableConnectorVersions(ctx context.Context) ([]ConnectorRef, error) {
	rows, err := s.pool.Query(ctx, collectableSQL, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRefs(rows)
}

const collectableSQL = `
	SELECT c.name, v.version, v.os, v.arch
	  FROM connector_versions v
	  JOIN connectors c ON c.id = v.connector_id
	 WHERE c.account_id = $1
	   AND (v.connector_id, v.version) NOT IN (` + versionFloor + `)
	   AND (c.name, v.version) NOT IN (` + retainedPins + `)
	 ORDER BY c.name, v.version, v.os, v.arch`

// CollectConnectorVersions deletes every collectable artifact and returns what
// went, newest-two-per-connector and everything a live flow pins untouched.
//
// Blobs are content-addressed and shared, so one is removed only once no
// version row points at it — deleting the row and the bytes together would
// destroy a build that another version deduped against.
func (s *Store) CollectConnectorVersions(ctx context.Context) ([]ConnectorRef, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, collectableSQL, accountID(ctx))
	if err != nil {
		return nil, err
	}
	collected, err := scanRefs(rows)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(collected) == 0 {
		return collected, tx.Commit(ctx)
	}
	for _, r := range collected {
		if _, err := tx.Exec(ctx,
			`DELETE FROM connector_versions v
			  USING connectors c
			  WHERE v.connector_id = c.id AND c.account_id = $1
			    AND c.name = $2 AND v.version = $3 AND v.os = $4 AND v.arch = $5`,
			accountID(ctx), r.Name, r.Version, r.OS, r.Arch); err != nil {
			return nil, err
		}
	}
	// Orphaned bytes. The blob table is global (dedup is by content across
	// every account), so this is deliberately not account-scoped: a digest is
	// unreferenced or it is not.
	if _, err := tx.Exec(ctx,
		`DELETE FROM connector_blobs b
		  WHERE NOT EXISTS (SELECT 1 FROM connector_versions v WHERE v.digest = b.digest)`); err != nil {
		return nil, err
	}
	return collected, tx.Commit(ctx)
}

func scanRefs(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]ConnectorRef, error) {
	out := []ConnectorRef{}
	for rows.Next() {
		var r ConnectorRef
		if err := rows.Scan(&r.Name, &r.Version, &r.OS, &r.Arch); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetConnectorEOL schedules a version to stop resolving (ADR-0047 §7).
//
// Across every platform, deliberately: yank is per (os, arch) because a bad
// BUILD can be platform-specific, but a poisoned dependency is a property of
// the release, and an EOL that left one platform live would be an EOL that did
// not happen.
//
// A deadline in the past is allowed and takes effect at once. That is the
// emergency shape — "this is being exploited now" — and refusing it would mean
// the one case where speed matters requires two calls.
func (s *Store) SetConnectorEOL(ctx context.Context, name, version string, deadline time.Time, reason, target string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE connector_versions v
		    SET eol_at = $3, eol_reason = $4, eol_target = $5
		   FROM connectors c
		  WHERE v.connector_id = c.id AND c.account_id = $1 AND c.name = $2 AND v.version = $6`,
		accountID(ctx), name, deadline, reason, target, version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ClearConnectorEOL withdraws a scheduled end-of-life.
//
// Possible because declaring one is a human act on a live system and humans
// mistype version numbers. It does NOT resurrect a deadline that has already
// passed for any flow that failed in the meantime — those tasks are gone — but
// it stops the bleeding, which is what somebody who has just realised needs.
func (s *Store) ClearConnectorEOL(ctx context.Context, name, version string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE connector_versions v
		    SET eol_at = NULL, eol_reason = '', eol_target = ''
		   FROM connectors c
		  WHERE v.connector_id = c.id AND c.account_id = $1 AND c.name = $2 AND v.version = $3`,
		accountID(ctx), name, version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// EOLVersion is one scheduled end-of-life and what it will take down.
type EOLVersion struct {
	Connector string    `json:"connector"`
	Version   string    `json:"version"`
	EOLAt     time.Time `json:"eol_at"`
	Reason    string    `json:"eol_reason,omitempty"`
	Target    string    `json:"eol_target,omitempty"`
	Flows     []FlowRef `json:"flows,omitempty"`
	Passed    bool      `json:"passed"`
}

// ScheduledEOLs lists every version with a deadline, soonest first, each with
// the published flows still pinning it.
//
// One call because the two halves are never useful apart: a deadline nobody is
// affected by needs no action, and an affected flow list without the deadline
// does not say when.
func (s *Store) ScheduledEOLs(ctx context.Context) ([]EOLVersion, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT c.name, v.version, v.eol_at, v.eol_reason, v.eol_target
		   FROM connector_versions v
		   JOIN connectors c ON c.id = v.connector_id
		  WHERE c.account_id = $1 AND v.eol_at IS NOT NULL
		  ORDER BY v.eol_at, c.name, v.version`, accountID(ctx))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EOLVersion{}
	for rows.Next() {
		var e EOLVersion
		if err := rows.Scan(&e.Connector, &e.Version, &e.EOLAt, &e.Reason, &e.Target); err != nil {
			return nil, err
		}
		e.Passed = !time.Now().Before(e.EOLAt)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		refs, err := s.ConnectorReferences(ctx, out[i].Connector, out[i].Version)
		if err != nil {
			return nil, err
		}
		out[i].Flows = refs
	}
	return out, nil
}

// EOLFor reports the scheduled end-of-life for one version, if any.
func (s *Store) EOLFor(ctx context.Context, connector, version string) (*EOLVersion, error) {
	var e EOLVersion
	err := s.pool.QueryRow(ctx,
		`SELECT c.name, v.version, v.eol_at, v.eol_reason, v.eol_target
		   FROM connector_versions v
		   JOIN connectors c ON c.id = v.connector_id
		  WHERE c.account_id = $1 AND c.name = $2 AND v.version = $3 AND v.eol_at IS NOT NULL
		  LIMIT 1`, accountID(ctx), connector, version).Scan(
		&e.Connector, &e.Version, &e.EOLAt, &e.Reason, &e.Target)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	e.Passed = !time.Now().Before(e.EOLAt)
	return &e, nil
}
