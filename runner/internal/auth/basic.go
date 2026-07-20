package auth

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

type basicUser struct {
	hash []byte
	role Role
}

// Basic authenticates HTTP Basic credentials against a fixed user set with
// bcrypt-hashed passwords.
type Basic struct {
	users map[string]basicUser
}

// NewBasic parses a user spec: entries separated by ';', each
// "user:bcrypt-hash:role". bcrypt hashes contain no ':' or ';', so the
// split is unambiguous. Roles must be one of Roles.
func NewBasic(spec string) (*Basic, error) {
	b := &Basic{users: map[string]basicUser{}}
	for entry := range strings.SplitSeq(spec, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		user, rest, ok := strings.Cut(entry, ":")
		if !ok {
			return nil, errors.New("auth: bad user entry (want user:hash:role)")
		}
		hash, roleName, ok := strings.Cut(rest, ":")
		if !ok {
			return nil, fmt.Errorf("auth: user %q: missing role", user)
		}
		role, ok := Roles[roleName]
		if !ok {
			return nil, fmt.Errorf("auth: user %q: unknown role %q", user, roleName)
		}
		if user == "" || hash == "" {
			return nil, errors.New("auth: empty user or hash")
		}
		b.users[user] = basicUser{hash: []byte(hash), role: role}
	}
	if len(b.users) == 0 {
		return nil, errors.New("auth: no users configured")
	}
	return b, nil
}

// Authenticate verifies HTTP Basic credentials in constant-ish time (an
// unknown user still runs one bcrypt compare).
func (b *Basic) Authenticate(r *http.Request) (Principal, bool) {
	user, pass, ok := r.BasicAuth()
	if !ok {
		return Principal{}, false
	}
	u, known := b.users[user]
	hash := u.hash
	if !known {
		hash = dummyHash
	}
	if bcrypt.CompareHashAndPassword(hash, []byte(pass)) != nil || !known {
		return Principal{}, false
	}
	return Principal{User: user, Role: u.role}, true
}
