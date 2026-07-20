// Package auth is the runner control surface's authentication and
// authorization (ADR-0016). Once runners are reachable through an ingress
// their API is public and must be guarded. The first method is HTTP Basic
// with bcrypt-hashed passwords behind an Authenticator interface, so richer
// methods (bearer, OIDC, mTLS) drop in later without touching handlers. The
// permission model (roles → allowed operations) is designed in from the
// start even while the credential type is basic.
//
// Auth is opt-in: with no Authenticator the surface is open (loopback dev,
// and every existing caller keeps working); configuring users switches it
// on. The webhook /hooks endpoints are NOT covered here — they authenticate
// by their own per-hook token, since the callers are external systems.
package auth

import (
	"net/http"

	"golang.org/x/crypto/bcrypt"
)

// Permission is a coarse capability a role may hold.
type Permission string

const (
	// PermRead covers status, task, capture, and listing reads.
	PermRead Permission = "read"
	// PermExecute covers running flows and benchmarks.
	PermExecute Permission = "execute"
	// PermManage covers registering/removing webhooks (config changes).
	PermManage Permission = "manage"
)

// Role bundles permissions under a name.
type Role struct {
	Name  string
	perms map[Permission]bool
}

// Can reports whether the role holds the permission.
func (r Role) Can(p Permission) bool { return r.perms[p] }

func role(name string, ps ...Permission) Role {
	m := make(map[Permission]bool, len(ps))
	for _, p := range ps {
		m[p] = true
	}
	return Role{Name: name, perms: m}
}

// Roles is the fixed role set: admin (all), operator (read+execute), viewer
// (read-only).
var Roles = map[string]Role{
	"admin":    role("admin", PermRead, PermExecute, PermManage),
	"operator": role("operator", PermRead, PermExecute),
	"viewer":   role("viewer", PermRead),
}

// Principal is an authenticated caller.
type Principal struct {
	User string
	Role Role
}

// Authenticator resolves a request to a principal. ok is false when the
// request carries no valid credential.
type Authenticator interface {
	Authenticate(r *http.Request) (Principal, bool)
}

// dummyHash is compared against when the user is unknown, so a bad username
// and a bad password take the same time (no user-enumeration oracle).
var dummyHash, _ = bcrypt.GenerateFromPassword([]byte("x"), bcrypt.MinCost)
