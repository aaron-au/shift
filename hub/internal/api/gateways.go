package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
)

// The gateway realm (ADR-0049).
//
// Everything here is the HUMAN side of adoption: an administrator records a
// gateway, pastes the fingerprint they read off its logs, and the hub does the
// dialling. There is no gateway-facing endpoint in this file and there must
// never be one — a gateway that could call the hub would be a DMZ box holding a
// hub credential, which is the property ADR-0038 §4 exists to deny.

// defaultGatewayTokenTTL bounds how long an unclaimed install token is useful.
// Short, because it is a bootstrap credential that will sit in a deployment
// manifest — and rotate re-issues one in a single call.
const defaultGatewayTokenTTL = time.Hour

func (a *api) gatewayTokenTTL() time.Duration {
	if a.opts.GatewayTokenTTL > 0 {
		return a.opts.GatewayTokenTTL
	}
	return defaultGatewayTokenTTL
}

// maxGatewayBody bounds an administrator's gateway record. Three short
// strings; anything larger is a mistake or an attempt.
const maxGatewayBody = 8 << 10

type gatewayRequest struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

// gatewayCreated is the create response: the record, plus the install token,
// served EXACTLY ONCE.
//
// The token is not part of store.Gateway's JSON and is never returned by list
// or get. An operator who loses it rotates rather than re-reads, which is the
// same shape as a runner registration token (ADR-0009).
type gatewayCreated struct {
	store.Gateway
	InstallToken string     `json:"install_token"`
	TokenExpires *time.Time `json:"token_expires_at,omitempty"`
}

// normalise validates an administrator's input before anything is stored.
func (g *gatewayRequest) normalise() error {
	g.Name = strings.TrimSpace(g.Name)
	g.URL = strings.TrimSpace(strings.TrimRight(g.URL, "/"))

	if g.Name == "" {
		return errors.New("name is required")
	}
	if g.URL == "" {
		return errors.New("url is required")
	}
	u, err := url.Parse(g.URL)
	if err != nil {
		return fmt.Errorf("url: %w", err)
	}
	if u.Scheme != "https" || u.Host == "" {
		// Pairing binds its proofs to the fingerprint of the certificate on the
		// wire. Over plaintext there is no certificate, so there is nothing to
		// bind to and the whole exchange degrades to a token anyone on the path
		// can copy.
		return errors.New("url must be an https:// address — pairing binds its proofs to the gateway's TLS key")
	}
	return nil
}

// createGateway records an intent to adopt and mints the install token.
//
// It does not dial. The reconcile loop pairs on its next pass, once the
// operator has actually deployed a gateway carrying the token — which is why
// recording and pairing are separate steps rather than one synchronous call.
func (a *api) createGateway(w http.ResponseWriter, r *http.Request) {
	var req gatewayRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxGatewayBody)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := req.normalise(); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	gw, err := a.st.CreateGateway(r.Context(), req.Name, req.URL, time.Now().Add(a.gatewayTokenTTL()))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "gateway.create", gw.ID, map[string]string{"url": gw.URL})
	slog.Info("gateway recorded", "event", "gateway.recorded", "gateway", gw.ID)

	token := gw.InstallToken
	gw.InstallToken = ""
	writeJSON(w, http.StatusCreated, gatewayCreated{
		Gateway: gw, InstallToken: token, TokenExpires: gw.TokenExpires,
	})
}

func (a *api) listGateways(w http.ResponseWriter, r *http.Request) {
	gws, err := a.st.ListGateways(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, gws)
}

func (a *api) getGateway(w http.ResponseWriter, r *http.Request) {
	gw, err := a.st.GetGateway(r.Context(), r.PathValue("id"))
	if err != nil {
		writeLookupErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, gw)
}

// adoptGateway pairs with a gateway now rather than waiting for the loop.
//
// The reconcile loop does this on its own, so this exists for the operator who
// has just finished deploying and does not want to wait a tick — and because a
// synchronous failure is far easier to act on than a log line.
func (a *api) adoptGateway(w http.ResponseWriter, r *http.Request) {
	if a.opts.Gateways == nil {
		writeErr(w, http.StatusServiceUnavailable,
			errors.New("this hub has no gateway CA configured, so it cannot issue a gateway identity"))
		return
	}
	gw, err := a.st.GetGateway(r.Context(), r.PathValue("id"))
	if err != nil {
		writeLookupErr(w, err)
		return
	}
	if gw.AdoptedAt != nil {
		writeErr(w, http.StatusConflict, store.ErrAlreadyAdopted)
		return
	}
	if gw.InstallToken == "" {
		writeErr(w, http.StatusConflict,
			errors.New("this gateway's install token has been spent or revoked; rotate to issue a new one"))
		return
	}
	var runnerCA []byte
	if a.opts.RunnerCA != nil {
		runnerCA = a.opts.RunnerCA.CAPEM()
	}
	issued, fingerprint, err := a.opts.Gateways.Pair(r.Context(), gw.URL, gw.InstallToken, gw.ID, runnerCA)
	if err != nil {
		// Unreachable, not deployed yet, or not holding this token. All three
		// are the administrator's to fix, so the reason travels — this is
		// hub-to-gateway detail, not payload and not a credential.
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	if err := a.st.LearnGatewayFingerprint(r.Context(), gw.ID, fingerprint); err != nil {
		if errors.Is(err, store.ErrAlreadyAdopted) {
			writeErr(w, http.StatusConflict, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if err := a.st.MarkGatewayAdopted(r.Context(), gw.ID, issued.Serial, issued.NotAfter); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "gateway.adopt", gw.ID,
		map[string]string{"cert_serial": issued.Serial, "fingerprint": fingerprint})
	slog.Info("gateway adopted", "event", "gateway.adopted",
		"gateway", gw.ID, "fingerprint", fingerprint, "cert_serial", issued.Serial)

	gw, err = a.st.GetGateway(r.Context(), gw.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, gw)
}

// rotateGatewayAdoption issues a fresh install token and returns the record to
// the unadopted state (ADR-0049 §4).
//
// The recovery path for a gateway redeployed without its state directory, and
// the way back from a token that expired before anyone deployed. It preserves
// the record — name, URL, routes, everything configured — and resets only the
// IDENTITY, so the cost of losing a DMZ box's disk is one redeploy rather than
// a rebuild.
//
// Deliberately an explicit, audited action rather than something the hub
// infers: a gateway whose identity vanished must not be silently re-trusted,
// because "presents a fresh key at the right URL" is exactly what an impostor
// also does.
func (a *api) rotateGatewayAdoption(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	token, err := a.st.RotateGatewayAdoption(r.Context(), id, time.Now().Add(a.gatewayTokenTTL()))
	if err != nil {
		writeLookupErr(w, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "gateway.rotate-adoption", id, nil)
	slog.Warn("gateway adoption rotated; a new install token was issued",
		"event", "gateway.adoption_rotated", "gateway", id)
	writeJSON(w, http.StatusOK, map[string]string{"install_token": token})
}

// deleteGateway removes a gateway. Deletion is revocation (ADR-0044 §6): the
// hub stops dialling and stops renewing, and the identity it last issued dies
// on its own short TTL.
func (a *api) deleteGateway(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := a.st.DeleteGateway(r.Context(), id); err != nil {
		writeLookupErr(w, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "gateway.delete", id, nil)
	slog.Info("gateway deleted", "event", "gateway.deleted", "gateway", id)
	w.WriteHeader(http.StatusNoContent)
}
