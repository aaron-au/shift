package adopt

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
)

// maxBody bounds everything the hub sends. The gateway trusts the hub, but a
// bounded reader costs nothing and the alternative is an unbounded allocation
// driven by whatever is on the other end of a socket.
const maxBody = 1 << 20

// ConfigApplier receives a pushed configuration. It returns the version it
// actually applied so a rejected configuration is visibly rejected rather than
// silently acknowledged.
type ConfigApplier func(raw []byte) error

// Handler wires the control endpoints onto mux.
//
// Authorisation is per-endpoint and depends on adoption state, which is the
// whole design:
//
//   - /pair and /adopt answer only while UNADOPTED, and only to a caller that
//     proves it holds this gateway'S install token. Once adopted they are
//     gone: leaving them open would keep offering a certificate request for a
//     gateway that already has an owner.
//   - /csr, /identity and /config require an ADOPTED gateway and a verified
//     hub certificate.
func Handler(mux *http.ServeMux, s *State, version string, apply ConfigApplier, log *slog.Logger) {
	mux.HandleFunc("POST /pair", func(w http.ResponseWriter, r *http.Request) {
		var c adoptChallenge
		if err := decode(w, r, &c); err != nil {
			http.Error(w, "malformed challenge", http.StatusBadRequest)
			return
		}
		hello, err := s.Pair(Challenge(c), version)
		if err != nil {
			// One opaque status for every failure — a wrong token, no token,
			// and an already-adopted gateway must not be distinguishable to
			// something probing the port.
			log.Warn("a pairing attempt was refused",
				"event", "gateway.pair_refused", "remote", r.RemoteAddr, "error", err.Error())
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		writeJSON(w, hello)
	})

	mux.HandleFunc("POST /adopt", func(w http.ResponseWriter, r *http.Request) {
		if s.Adopted() {
			// Refused, not applied. Overwriting would let anything that reaches
			// this URL inherit a live gateway's routes.
			http.Error(w, "this gateway has already been adopted", http.StatusConflict)
			return
		}
		var a Adoption
		if err := decode(w, r, &a); err != nil {
			http.Error(w, "malformed adoption", http.StatusBadRequest)
			return
		}
		// The proof covers the MATERIAL, not just the exchange: without that a
		// captured proof could be replayed with a substituted CA, and the
		// gateway would trust an issuer nobody chose.
		if err := s.CheckInstall(a); err != nil {
			log.Warn("an adoption attempt was refused",
				"event", "gateway.adopt_refused", "remote", r.RemoteAddr, "error", err.Error())
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if err := s.Install(a); err != nil {
			log.Error("installing the hub's identity", "event", "gateway.adopt_failed", "error", err.Error())
			http.Error(w, "the offered identity was refused", http.StatusUnprocessableEntity)
			return
		}
		log.Info("adopted", "event", "gateway.adopted",
			"gateway", s.GatewayID(), "not_after", s.NotAfter().UTC().Format("2006-01-02T15:04:05Z"))
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /csr", func(w http.ResponseWriter, r *http.Request) {
		if !hub(s, w, r) {
			return
		}
		csr, err := s.CSR()
		if err != nil {
			log.Error("building a certificate request", "event", "gateway.csr_failed", "error", err.Error())
			http.Error(w, "cannot build a certificate request", http.StatusInternalServerError)
			return
		}
		writeJSON(w, Hello{Fingerprint: s.Fingerprint(), CSR: csr, Version: version})
	})

	mux.HandleFunc("POST /identity", func(w http.ResponseWriter, r *http.Request) {
		// Renewal always requires a verified hub certificate — including on the
		// recovery path, where the gateway is serving its ANCHOR because the
		// identity lapsed. That still works: the gateway kept the gateway CA
		// from adoption, so it can verify the hub's client certificate even
		// when its own is dead, and the hub verifies the gateway by the key it
		// pinned. Neither side needs the install token, which is why the token
		// can be burned (ADR-0049 §6).
		if !hub(s, w, r) {
			return
		}
		var a Adoption
		if err := decode(w, r, &a); err != nil {
			http.Error(w, "malformed identity", http.StatusBadRequest)
			return
		}
		if err := s.Install(a); err != nil {
			log.Error("installing a renewed identity", "event", "gateway.renew_failed", "error", err.Error())
			http.Error(w, "the offered identity was refused", http.StatusUnprocessableEntity)
			return
		}
		log.Info("identity renewed", "event", "gateway.renewed",
			"gateway", s.GatewayID(), "not_after", s.NotAfter().UTC().Format("2006-01-02T15:04:05Z"))
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("POST /config", func(w http.ResponseWriter, r *http.Request) {
		if !hub(s, w, r) {
			return
		}
		if apply == nil {
			http.Error(w, "this gateway cannot apply configuration", http.StatusNotImplemented)
			return
		}
		raw, err := body(w, r)
		if err != nil {
			http.Error(w, "malformed configuration", http.StatusBadRequest)
			return
		}
		if err := apply(raw); err != nil {
			// Rejected loudly. Acknowledging a configuration the gateway did
			// not apply would let the hub believe it had converged.
			log.Error("applying a pushed configuration", "event", "gateway.config_rejected", "error", err.Error())
			http.Error(w, "the configuration was rejected", http.StatusUnprocessableEntity)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

// hub gates an endpoint on an adopted gateway and a verified hub certificate.
// adoptChallenge mirrors Challenge for decoding; the alias keeps the wire type
// and the domain type from drifting.
type adoptChallenge Challenge

func hub(s *State, w http.ResponseWriter, r *http.Request) bool {
	if !s.Adopted() {
		http.Error(w, "this gateway has not been adopted", http.StatusForbidden)
		return false
	}
	if role, _ := s.PeerRole(r.TLS); role != RoleHub {
		// One opaque failure whether the caller is a runner, an unknown CA, or
		// nothing at all.
		http.Error(w, "forbidden", http.StatusForbidden)
		return false
	}
	return true
}

func decode(w http.ResponseWriter, r *http.Request, v any) error {
	raw, err := body(w, r)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

func body(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	return io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
