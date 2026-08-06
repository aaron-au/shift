package api

import (
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/aaron-au/shift/hub/internal/store"
)

// Certificate issuance for the runner realm (ADR-0044).
//
// Registration is unchanged in shape — a single-use token, spent once — and
// changed in what it hands back: a signed client certificate instead of a
// long-lived bearer secret. The token gets shorter-lived and stays single-use;
// what disappears is the replayable credential that used to follow it.

// RunnerAuthMode decides which runner credentials the hub accepts.
type RunnerAuthMode string

const (
	// RunnerAuthMTLS accepts only client certificates. The end state: "we
	// support both forever" is how the weaker credential stays alive.
	RunnerAuthMTLS RunnerAuthMode = "mtls"
	// RunnerAuthBearer accepts only bearer secrets — the deployment that
	// terminates TLS at a proxy, where a client certificate cannot survive
	// the hop (ADR-0044 §4).
	RunnerAuthBearer RunnerAuthMode = "bearer"
	// RunnerAuthBoth accepts either. The migration default.
	RunnerAuthBoth RunnerAuthMode = "both"
)

// ParseRunnerAuthMode validates a configured mode.
func ParseRunnerAuthMode(s string) (RunnerAuthMode, error) {
	switch RunnerAuthMode(s) {
	case "":
		return RunnerAuthBoth, nil
	case RunnerAuthMTLS, RunnerAuthBearer, RunnerAuthBoth:
		return RunnerAuthMode(s), nil
	default:
		return "", errors.New(`api: runner auth mode must be "mtls", "bearer" or "both"`)
	}
}

func (m RunnerAuthMode) allowsMTLS() bool   { return m == RunnerAuthMTLS || m == RunnerAuthBoth }
func (m RunnerAuthMode) allowsBearer() bool { return m == RunnerAuthBearer || m == RunnerAuthBoth }

// registerCert handles a registration that presents a CSR: the runner is
// created with no secret at all, and its credential is the certificate signed
// here.
func (a *api) registerCert(w http.ResponseWriter, r *http.Request, token, name, csrB64 string) {
	if a.opts.RunnerCA == nil {
		writeErrCode(w, http.StatusBadRequest, "mtls_unavailable",
			errors.New("this hub is not configured to issue runner certificates"))
		return
	}
	csrDER, err := base64.StdEncoding.DecodeString(csrB64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("csr must be base64-encoded DER"))
		return
	}
	id, _, err := a.st.RegisterRunnerCert(r.Context(), token, name)
	if errors.Is(err, store.ErrTokenInvalid) {
		writeErrCode(w, http.StatusUnauthorized, "registration_token_invalid", err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	issued, err := a.opts.RunnerCA.Sign(csrDER, id)
	if err != nil {
		// The token is spent and the row exists but cannot authenticate
		// anything, which is the correct state for a half-finished
		// registration: harmless, and visible.
		slog.Warn("runner registered but its certificate could not be issued",
			"event", "hub.runner_cert.issue_failed", "runner", id, "error", err)
		writeErrCode(w, http.StatusBadRequest, "csr_rejected", err)
		return
	}
	if err := a.st.RecordRunnerCertificate(r.Context(), id, issued.Serial, issued.NotAfter); err != nil {
		// Bookkeeping only — the certificate is valid and the runner can use
		// it. Failing the registration over this would be worse than an
		// unrecorded serial.
		slog.Warn("recording the runner certificate failed",
			"event", "hub.runner_cert.record_failed", "runner", id, "error", err)
	}
	_ = a.st.Audit(r.Context(), "runner:"+id, "runner.register", name,
		map[string]string{"auth": "mtls", "cert_serial": issued.Serial})
	writeJSON(w, http.StatusCreated, map[string]any{
		"runner_id":   id,
		"certificate": string(issued.CertPEM),
		"ca":          string(a.opts.RunnerCA.CAPEM()),
		"not_after":   issued.NotAfter.UTC().Format(time.RFC3339),
	})
}

// renewCertificate issues a fresh certificate to a runner that authenticated
// with its current one (POST /api/v1/runners/certificate, runner realm).
//
// No registration token: a runner that can still present a valid certificate
// has already proven it is the same runner, and requiring an operator-minted
// token for renewal would mean every fleet needs a human in the loop every
// day. That is how a renewal path quietly never gets used, and "the fleet went
// quiet overnight" is a bad way to discover it.
func (a *api) renewCertificate(w http.ResponseWriter, r *http.Request) {
	if a.opts.RunnerCA == nil {
		writeErrCode(w, http.StatusBadRequest, "mtls_unavailable",
			errors.New("this hub is not configured to issue runner certificates"))
		return
	}
	var req struct {
		CSR string `json:"csr"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	csrDER, err := base64.StdEncoding.DecodeString(req.CSR)
	if err != nil || len(csrDER) == 0 {
		writeErr(w, http.StatusBadRequest, errors.New("csr must be base64-encoded DER"))
		return
	}
	// The id comes from the AUTHENTICATED identity, never from the request. A
	// runner renewing on behalf of another runner is the one thing this
	// endpoint must not permit.
	id := runnerID(r)
	issued, err := a.opts.RunnerCA.Sign(csrDER, id)
	if err != nil {
		writeErrCode(w, http.StatusBadRequest, "csr_rejected", err)
		return
	}
	if err := a.st.RecordRunnerCertificate(r.Context(), id, issued.Serial, issued.NotAfter); err != nil {
		slog.Warn("recording the runner certificate failed",
			"event", "hub.runner_cert.record_failed", "runner", id, "error", err)
	}
	_ = a.st.Audit(r.Context(), "runner:"+id, "runner.certificate.renew", id,
		map[string]string{"cert_serial": issued.Serial})
	writeJSON(w, http.StatusOK, map[string]any{
		"runner_id":   id,
		"certificate": string(issued.CertPEM),
		"ca":          string(a.opts.RunnerCA.CAPEM()),
		"not_after":   issued.NotAfter.UTC().Format(time.RFC3339),
	})
}
