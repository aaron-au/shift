// Package oidcauth verifies bearer JWTs against any OIDC-compliant IdP
// via issuer discovery and JWKS signature validation (go-oidc). The hub
// implements the protocol, not a provider: Dex, Auth0, Entra, Keycloak
// all plug in through two config values.
package oidcauth

import (
	"context"
	"fmt"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Config identifies the IdP and this hub's client registration.
type Config struct {
	IssuerURL string // SHIFT_HUB_OIDC_ISSUER
	ClientID  string // SHIFT_HUB_OIDC_CLIENT_ID — enforced as the token audience
}

// Identity is the verified subject of a token.
type Identity struct {
	Issuer        string
	Subject       string
	Email         string
	EmailVerified bool
	Name          string
}

// Verifier validates raw bearer tokens.
type Verifier struct {
	issuer   string
	verifier *oidc.IDTokenVerifier
}

// New discovers the issuer (network call) and prepares JWKS validation.
// JWKS keys are cached and refetched on unknown-kid, so IdP key
// rotation needs no hub restart.
func New(ctx context.Context, cfg Config) (*Verifier, error) {
	if cfg.IssuerURL == "" || cfg.ClientID == "" {
		return nil, fmt.Errorf("oidcauth: issuer URL and client ID are both required")
	}
	provider, err := oidc.NewProvider(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("oidcauth: discovering %s: %w", cfg.IssuerURL, err)
	}
	return &Verifier{
		issuer:   cfg.IssuerURL,
		verifier: provider.Verifier(&oidc.Config{ClientID: cfg.ClientID}),
	}, nil
}

// Verify validates signature, issuer, audience, and expiry, and returns
// the token's identity claims.
func (v *Verifier) Verify(ctx context.Context, rawToken string) (Identity, error) {
	tok, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		return Identity{}, fmt.Errorf("oidcauth: %w", err)
	}
	var claims struct {
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
		Name          string `json:"name"`
	}
	if err := tok.Claims(&claims); err != nil {
		return Identity{}, fmt.Errorf("oidcauth: decoding claims: %w", err)
	}
	return Identity{
		Issuer:        tok.Issuer,
		Subject:       tok.Subject,
		Email:         claims.Email,
		EmailVerified: claims.EmailVerified,
		Name:          claims.Name,
	}, nil
}
