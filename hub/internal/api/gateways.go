package api

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/aaron-au/shift/hub/internal/store"
)

// The gateway realm (ADR-0049).
//
// Everything here is the HUMAN side of adoption: an administrator records a
// gateway, pastes the fingerprint they read off its logs, and the hub does the
// dialling. There is no gateway-facing endpoint in this file and there must
// never be one — a gateway that could call the hub would be a DMZ box holding a
// hub credential, which is the property ADR-0038 §4 exists to deny.

// fingerprintLen is a hex SHA-256: 64 characters.
const fingerprintLen = 64

// maxGatewayBody bounds an administrator's gateway record. Three short
// strings; anything larger is a mistake or an attempt.
const maxGatewayBody = 8 << 10

type gatewayRequest struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	Fingerprint string `json:"fingerprint"`
}

// normalise validates an administrator's input before anything is stored.
//
// The fingerprint is checked for SHAPE, not for correctness — the hub cannot
// know the right value, only the gateway can prove it. What this prevents is a
// truncated or mistyped paste being stored as a pin, which would fail later at
// a dial with a confusing error rather than immediately at the form.
func (g *gatewayRequest) normalise() error {
	g.Name = strings.TrimSpace(g.Name)
	g.URL = strings.TrimSpace(strings.TrimRight(g.URL, "/"))
	g.Fingerprint = strings.ToLower(strings.TrimSpace(g.Fingerprint))
	// Operators copy fingerprints out of terminals, where they are often
	// colon-separated. Accept that spelling rather than making them edit it.
	g.Fingerprint = strings.ReplaceAll(g.Fingerprint, ":", "")

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
		// Adoption pins a key inside the TLS handshake. Over plaintext there is
		// no handshake to pin, so the whole trust argument evaporates.
		return errors.New("url must be an https:// address — adoption pins the gateway's key during the TLS handshake")
	}
	if len(g.Fingerprint) != fingerprintLen {
		return fmt.Errorf("fingerprint must be %d hex characters (a SHA-256 of the gateway's public key), got %d",
			fingerprintLen, len(g.Fingerprint))
	}
	if _, err := hex.DecodeString(g.Fingerprint); err != nil {
		return errors.New("fingerprint must be hexadecimal")
	}
	return nil
}

// createGateway records an intent to adopt.
//
// It does not dial. Recording and adopting are separate steps so that a wrong
// paste is a failed adoption against a visible record, rather than a silent
// nothing — the administrator can see what the hub believes and correct it.
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
	gw, err := a.st.CreateGateway(r.Context(), req.Name, req.URL, req.Fingerprint)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "gateway.create", gw.ID, map[string]string{"url": gw.URL})
	slog.Info("gateway recorded", "event", "gateway.recorded",
		"gateway", gw.ID, "fingerprint", gw.Fingerprint)
	writeJSON(w, http.StatusCreated, gw)
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

// adoptGateway performs the pinned exchange (ADR-0049 §2).
//
// Synchronous on purpose. An administrator who just pasted a fingerprint is
// standing in front of the result, and "queued for adoption" would hide the
// one error they can actually fix — a wrong paste — behind a background job.
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

	boot, err := a.opts.Gateways.Fetch(r.Context(), gw.URL, gw.Fingerprint)
	if err != nil {
		// The gateway is unreachable or presented the wrong key. Both are the
		// administrator's to fix, so both are 502 with the reason intact —
		// this is hub-to-gateway detail, not payload and not a credential.
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	var runnerCA []byte
	if a.opts.RunnerCA != nil {
		runnerCA = a.opts.RunnerCA.CAPEM()
	}
	issued, err := a.opts.Gateways.Adopt(r.Context(), gw.URL, gw.Fingerprint, gw.ID, runnerCA, boot.CSR)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	if err := a.st.MarkGatewayAdopted(r.Context(), gw.ID, issued.Serial, issued.NotAfter); err != nil {
		if errors.Is(err, store.ErrAlreadyAdopted) {
			writeErr(w, http.StatusConflict, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "gateway.adopt", gw.ID, map[string]string{"cert_serial": issued.Serial})
	slog.Info("gateway adopted", "event", "gateway.adopted",
		"gateway", gw.ID, "cert_serial", issued.Serial)

	gw, err = a.st.GetGateway(r.Context(), gw.ID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, gw)
}

// rotateGatewayAdoption points a record at a new key (ADR-0049 §4).
//
// The recovery path for a gateway redeployed without its state directory. It
// preserves the record — routes, certificates, everything the administrator
// configured — and returns only the IDENTITY to the unadopted state, so the
// cost of losing a DMZ box's disk is one paste rather than a rebuild.
func (a *api) rotateGatewayAdoption(w http.ResponseWriter, r *http.Request) {
	var req gatewayRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxGatewayBody)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	// Only the fingerprint moves; name and URL stay as recorded.
	req.Name, req.URL = "rotate", "https://placeholder"
	if err := req.normalise(); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	id := r.PathValue("id")
	if err := a.st.RotateGatewayAdoption(r.Context(), id, req.Fingerprint); err != nil {
		writeLookupErr(w, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "gateway.rotate-adoption", id, nil)
	slog.Warn("gateway adoption rotated to a new key", "event", "gateway.adoption_rotated",
		"gateway", id, "fingerprint", req.Fingerprint)
	w.WriteHeader(http.StatusNoContent)
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
