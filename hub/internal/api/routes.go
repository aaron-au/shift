package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/aaron-au/shift/hub/internal/store"
)

// Gateway routes (ADR-0038 §5/§6).
//
// A route is what makes a flow reachable from outside: a public path, the flow
// it runs, which runners may serve it, and the ingress policy around it. It
// lives HERE rather than in the flow document because it is ingress policy, not
// a data-plane definition — a caller's bearer token, source allowlist and body
// cap have nothing to do with what the flow does with a record, and putting
// them in the same document would mean editing a flow to rotate a credential.

// maxRouteBody bounds an administrator's route document.
const maxRouteBody = 64 << 10

type routeRequest struct {
	GatewayID string            `json:"gateway_id,omitempty"`
	Path      string            `json:"path"`
	Method    string            `json:"method,omitempty"`
	Flow      string            `json:"flow"`
	Selector  map[string]string `json:"selector,omitempty"`

	// Public makes the route deliberately unauthenticated. It must be set
	// explicitly: a route with no credential is otherwise indistinguishable
	// from one where somebody forgot, and the failure is silent and public.
	Public         bool              `json:"public,omitempty"`
	AuthPrincipal  string            `json:"auth_principal,omitempty"`
	AllowCIDRs     []string          `json:"allow_cidrs,omitempty"`
	RequireHeaders map[string]string `json:"require_headers,omitempty"`
	MaxBodyBytes   int64             `json:"max_body_bytes,omitempty"`
}

func (r *routeRequest) normalise() error {
	r.Path = strings.TrimSpace(r.Path)
	r.Method = strings.ToUpper(strings.TrimSpace(r.Method))
	r.Flow = strings.TrimSpace(r.Flow)
	r.AuthPrincipal = strings.TrimSpace(r.AuthPrincipal)

	if !strings.HasPrefix(r.Path, "/") {
		return errors.New("path must start with /")
	}
	if r.Flow == "" {
		return errors.New("flow is required")
	}
	switch r.Method {
	case "", http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodHead, http.MethodOptions:
	default:
		return fmt.Errorf("method %q is not an HTTP method", r.Method)
	}
	for _, c := range r.AllowCIDRs {
		if _, _, err := net.ParseCIDR(c); err != nil {
			return fmt.Errorf("bad CIDR %q: %w", c, err)
		}
	}
	if r.MaxBodyBytes < 0 {
		return errors.New("max_body_bytes cannot be negative")
	}
	if !r.Public && r.AuthPrincipal == "" {
		// The principal is what the runner sees as the caller. An authenticated
		// route without one authenticates somebody and then cannot say who,
		// which makes every downstream audit record anonymous.
		return errors.New("auth_principal is required (or set \"public\": true for a deliberately open route)")
	}
	return nil
}

// routeCreated is the create response: the route, plus the caller's bearer
// token, served EXACTLY ONCE.
type routeCreated struct {
	store.Route
	ID    string `json:"id"`
	Token string `json:"token,omitempty"`
}

// createRoute records a route and mints its caller credential.
func (a *api) createRoute(w http.ResponseWriter, r *http.Request) {
	var req routeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRouteBody)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := req.normalise(); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	route, token, err := a.st.CreateRoute(r.Context(), store.Route{
		GatewayID: req.GatewayID, Path: req.Path, Method: req.Method, Flow: req.Flow,
		Selector: req.Selector, AuthPrincipal: req.AuthPrincipal,
		AllowCIDRs: req.AllowCIDRs, RequireHeaders: req.RequireHeaders,
		MaxBodyBytes: req.MaxBodyBytes,
	}, !req.Public)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	a.gatewayConfigChanged(r, "route.create", route.ID)
	if req.Public {
		// Loud, because a public path on the internet is a decision somebody
		// should be able to find later without reading the route table.
		slog.Warn("route created with no caller authentication",
			"event", "hub.route.public", "flow", route.Flow)
	}
	writeJSON(w, http.StatusCreated, routeCreated{Route: route, ID: route.ID, Token: token})
}

func (a *api) listRoutes(w http.ResponseWriter, r *http.Request) {
	routes, err := a.st.ListRoutes(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	// Give each one its id: store.Route hides it from the pushed document,
	// which is a different audience.
	out := make([]routeCreated, 0, len(routes))
	for _, rt := range routes {
		out = append(out, routeCreated{Route: rt, ID: rt.ID})
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *api) deleteRoute(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := a.st.DeleteRoute(r.Context(), id); err != nil {
		writeLookupErr(w, err)
		return
	}
	a.gatewayConfigChanged(r, "route.delete", id)
	w.WriteHeader(http.StatusNoContent)
}

// rotateRouteToken re-mints a route's caller credential.
//
// The old token stops working as soon as the next configuration reaches the
// gateway, not when this returns — so the response says what was changed, and
// the gateway's acknowledged version says when it took effect.
func (a *api) rotateRouteToken(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	token, err := a.st.RotateRouteToken(r.Context(), id)
	if err != nil {
		writeLookupErr(w, err)
		return
	}
	a.gatewayConfigChanged(r, "route.rotate-token", id)
	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

// syncGateways tells the calling runner which gateways to poll (runner realm).
//
// The runner asks; the hub answers for the identity the CREDENTIAL proves, not
// for one named in the request. That is the same rule as the labels themselves
// (ADR-0041 §3): a runner that could ask "what should runner X poll?" could
// park against a gateway serving somebody else's traffic.
//
// Metadata only — an address list. No payload, no route policy, no credential.
func (a *api) syncGateways(w http.ResponseWriter, r *http.Request) {
	gws, err := a.st.GatewaysForRunner(r.Context(), runnerID(r))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"gateways": gws})
}

type labelsRequest struct {
	Labels map[string]string `json:"labels"`
}

// setRunnerLabels records what a runner IS (ADR-0041 §3) — the facts route
// selectors are matched against.
func (a *api) setRunnerLabels(w http.ResponseWriter, r *http.Request) {
	var req labelsRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRouteBody)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	id := r.PathValue("id")
	if err := a.st.SetRunnerLabels(r.Context(), id, req.Labels); err != nil {
		writeLookupErr(w, err)
		return
	}
	// Labels decide placement, so changing them changes every gateway's view
	// of the fleet.
	a.gatewayConfigChanged(r, "runner.set-labels", id)
	w.WriteHeader(http.StatusNoContent)
}

type proxiesRequest struct {
	TrustedProxies []string `json:"trusted_proxies"`
}

// setGatewayTrustedProxies records whose forwarded headers a gateway believes.
func (a *api) setGatewayTrustedProxies(w http.ResponseWriter, r *http.Request) {
	var req proxiesRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRouteBody)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	for _, c := range req.TrustedProxies {
		if _, _, err := net.ParseCIDR(c); err != nil {
			// A bad CIDR here would silently believe nobody, which reads as
			// "the allowlist is broken" much later and much less obviously.
			writeErr(w, http.StatusUnprocessableEntity, fmt.Errorf("bad CIDR %q: %w", c, err))
			return
		}
	}
	id := r.PathValue("id")
	if err := a.st.SetGatewayTrustedProxies(r.Context(), id, req.TrustedProxies); err != nil {
		writeLookupErr(w, err)
		return
	}
	a.gatewayConfigChanged(r, "gateway.set-trusted-proxies", id)
	w.WriteHeader(http.StatusNoContent)
}

// gatewayConfigChanged audits the edit and raises the configuration generation
// every gateway is expected to run.
//
// One call site per mutation, deliberately: a change that audited but did not
// bump would leave gateways serving policy the hub no longer believes in, and
// nothing would ever notice — the drift the reconcile loop watches for is
// exactly the difference this raises.
func (a *api) gatewayConfigChanged(r *http.Request, action, target string) {
	_ = a.st.Audit(r.Context(), actor(r), action, target, nil)
	if err := a.st.BumpGatewayConfig(r.Context()); err != nil {
		slog.Error("raising the gateway configuration generation",
			"event", "hub.gateway_config.bump_failed", "error", err.Error())
	}
}
