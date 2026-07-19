package store

import (
	"context"
	"fmt"
	"time"
)

// User is an OIDC-provisioned human identity. (Issuer, Subject) is the
// stable key; email is informational.
type User struct {
	ID        string     `json:"id"`
	AccountID string     `json:"account_id"`
	Issuer    string     `json:"issuer"`
	Subject   string     `json:"subject"`
	Email     string     `json:"email,omitempty"`
	Name      string     `json:"display_name,omitempty"`
	Role      string     `json:"role"`
	Created   time.Time  `json:"created_at"`
	LastLogin *time.Time `json:"last_login_at,omitempty"`
}

// UpsertUserByOIDC provisions a user on first login (JIT) and refreshes
// email/display name/last_login_at on every subsequent one. New users
// join the context's account with the default role.
func (s *Store) UpsertUserByOIDC(ctx context.Context, issuer, subject, email, name string) (User, error) {
	var u User
	err := s.pool.QueryRow(ctx,
		`INSERT INTO users (id, account_id, issuer, subject, email, display_name, last_login_at)
		 VALUES ($1,$2,$3,$4,$5,$6,now())
		 ON CONFLICT (issuer, subject)
		 DO UPDATE SET email = $5, display_name = $6, last_login_at = now()
		 RETURNING id, account_id, issuer, subject, email, display_name, role, created_at, last_login_at`,
		newUUID(), accountID(ctx), issuer, subject, email, name).Scan(
		&u.ID, &u.AccountID, &u.Issuer, &u.Subject, &u.Email, &u.Name, &u.Role, &u.Created, &u.LastLogin)
	return u, err
}

// SetUserRole changes a user's role, addressed by email within the
// context's account.
func (s *Store) SetUserRole(ctx context.Context, email, role string) (int64, error) {
	if role != "admin" && role != "viewer" {
		return 0, fmt.Errorf("store: unknown role %q", role)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE users SET role = $3 WHERE account_id = $1 AND email = $2`,
		accountID(ctx), email, role)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() == 0 {
		return 0, ErrNotFound
	}
	return tag.RowsAffected(), nil
}
