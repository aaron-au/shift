package store

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// AuditEntry is one audit_log row (M6b).
type AuditEntry struct {
	ID     int64           `json:"id"`
	At     time.Time       `json:"at"`
	Actor  string          `json:"actor"`
	Action string          `json:"action"`
	Entity string          `json:"entity"`
	Detail json.RawMessage `json:"detail,omitempty"`
}

// AuditFilter narrows a ListAudit query. Zero values mean "no filter".
type AuditFilter struct {
	Action   string // exact action, or "prefix." to match a family (trailing dot ⇒ prefix)
	Actor    string // exact actor
	Entity   string // exact entity
	BeforeID int64  // keyset cursor: only rows with id < BeforeID (0 = newest)
	Limit    int    // capped 1..500 (default 100)
}

// ListAudit returns audit rows newest-first for the request's account,
// keyset-paginated by descending id. Account-scoped like every other list.
func (s *Store) ListAudit(ctx context.Context, f AuditFilter) ([]AuditEntry, error) {
	q := strings.Builder{}
	q.WriteString(`SELECT id, at, actor, action, entity, detail FROM audit_log WHERE account_id = $1`)
	args := []any{accountID(ctx)}
	add := func(clause string, v any) {
		args = append(args, v)
		q.WriteString(clause)
		q.WriteString(strconv.Itoa(len(args)))
	}
	if f.Action != "" {
		if strings.HasSuffix(f.Action, ".") {
			// Family prefix: escape LIKE metacharacters so "_"/"%" in the
			// caller's value are literal, not wildcards (the value is already a
			// bind param — this is about match precision, not injection).
			add(` AND action LIKE $`, likeEscape(f.Action)+"%")
			q.WriteString(` ESCAPE '\'`)
		} else {
			add(` AND action = $`, f.Action)
		}
	}
	if f.Actor != "" {
		add(` AND actor = $`, f.Actor)
	}
	if f.Entity != "" {
		add(` AND entity = $`, f.Entity)
	}
	if f.BeforeID > 0 {
		add(` AND id < $`, f.BeforeID)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	q.WriteString(` ORDER BY id DESC LIMIT `)
	q.WriteString(strconv.Itoa(limit))

	rows, err := s.pool.Query(ctx, q.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]AuditEntry, 0, limit)
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.ID, &e.At, &e.Actor, &e.Action, &e.Entity, &e.Detail); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// likeEscape escapes the LIKE wildcards (\ % _) so a caller-supplied prefix
// matches literally. Pair with an `ESCAPE '\'` clause.
func likeEscape(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}
