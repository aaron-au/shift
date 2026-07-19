package oidcauth

import (
	"context"
	"fmt"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// FlowConfig configures the browser login (authorization-code) flow the
// hub mediates for its dashboard. Bearer-token API clients never touch
// this — it exists so a human can "login via OIDC" without a SPA.
type FlowConfig struct {
	Config
	ClientSecret string // SHIFT_HUB_OIDC_CLIENT_SECRET
	RedirectURL  string // e.g. https://hub.example:8400/auth/callback
}

// Flow runs the authorization-code exchange.
type Flow struct {
	oauth2   oauth2.Config
	verifier *Verifier
}

// NewFlow discovers the issuer and prepares both the code exchange and
// ID-token verification.
func NewFlow(ctx context.Context, cfg FlowConfig) (*Flow, error) {
	if cfg.RedirectURL == "" {
		return nil, fmt.Errorf("oidcauth: redirect URL is required for the login flow")
	}
	provider, err := oidc.NewProvider(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("oidcauth: discovering %s: %w", cfg.IssuerURL, err)
	}
	return &Flow{
		oauth2: oauth2.Config{
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			Endpoint:     provider.Endpoint(),
			RedirectURL:  cfg.RedirectURL,
			Scopes:       []string{oidc.ScopeOpenID, "profile", "email"},
		},
		verifier: &Verifier{
			issuer:   cfg.IssuerURL,
			verifier: provider.Verifier(&oidc.Config{ClientID: cfg.ClientID}),
		},
	}, nil
}

// AuthCodeURL builds the IdP redirect for the given anti-CSRF state.
func (f *Flow) AuthCodeURL(state string) string { return f.oauth2.AuthCodeURL(state) }

// Exchange trades an authorization code for a verified ID token,
// returning the raw token (the dashboard session cookie) and its
// identity.
func (f *Flow) Exchange(ctx context.Context, code string) (rawIDToken string, id Identity, err error) {
	tok, err := f.oauth2.Exchange(ctx, code)
	if err != nil {
		return "", Identity{}, fmt.Errorf("oidcauth: code exchange: %w", err)
	}
	raw, ok := tok.Extra("id_token").(string)
	if !ok {
		return "", Identity{}, fmt.Errorf("oidcauth: token response had no id_token")
	}
	id, err = f.verifier.Verify(ctx, raw)
	if err != nil {
		return "", Identity{}, err
	}
	return raw, id, nil
}

// Verifier returns the flow's token verifier (shared with the bearer
// path so both realms validate identically).
func (f *Flow) Verifier() *Verifier { return f.verifier }
