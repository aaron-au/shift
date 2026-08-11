package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// routerErrors puts ROUTER-generated failures into the ADR-0023 envelope
// (TC-030).
//
// The hub registers its routes as method+path patterns, so http.ServeMux
// answers two cases entirely on its own: a path that matches with the wrong
// method (405, plus an Allow header) and a path that matches nothing (404).
// Both are `text/plain` — `Method Not Allowed\n` — because they never reach a
// handler, and every handler is what calls writeErr.
//
// ADR-0023 says "ALL hub error responses use the envelope", without
// qualification, and the studio and any third-party client are entitled to
// parse on that basis. The alternative to this wrapper was narrowing the ADR to
// handler-generated errors, which would make every client's error handling more
// complicated forever to save writing this file.
//
// This is not a theoretical gap: it is reachable by an ordinary client typo. It
// was found by a test of ours that requested `/api/v1/runners/lease` instead of
// `/api/v1/lease` and got `"Method Not Allowed\n"` back.
func routerErrors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&routerErrWriter{ResponseWriter: w}, r)
	})
}

// routerErrWriter rewrites a router-generated 404/405 body as the envelope.
//
// A handler's own 404 is left alone: writeJSON has already set an
// application/json content type by the time WriteHeader runs, which is what
// distinguishes "the mux answered" from "we answered". Every other status
// passes through untouched, so nothing that streams or negotiates a content
// type is affected.
type routerErrWriter struct {
	http.ResponseWriter
	wroteHeader bool
	swallow     bool // discard the router's plain-text body
}

func (w *routerErrWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true

	ct := w.Header().Get("Content-Type")
	if !isRouterError(code) || strings.HasPrefix(ct, "application/json") {
		w.ResponseWriter.WriteHeader(code)
		return
	}

	// The Allow header on a 405 is the mux's and stays: it is the useful part
	// of that response. Content-Length must go, because the body is about to
	// change length.
	w.Header().Del("Content-Length")
	w.Header().Set("Content-Type", "application/json")
	w.swallow = true
	w.ResponseWriter.WriteHeader(code)

	body, err := json.Marshal(map[string]apiErr{"error": {Status: code, Message: routerErrMessage(code)}})
	if err != nil {
		return
	}
	_, _ = w.ResponseWriter.Write(append(body, '\n'))
}

func (w *routerErrWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.swallow {
		// Report the write as accepted. The bytes are the router's plain-text
		// message, which the envelope has already replaced.
		return len(p), nil
	}
	return w.ResponseWriter.Write(p)
}

func isRouterError(code int) bool {
	return code == http.StatusNotFound || code == http.StatusMethodNotAllowed
}

var (
	errNoRoute    = errors.New("no such endpoint")
	errBadMethod  = errors.New("method not allowed for this endpoint")
	errRouterUnkn = errors.New("request could not be routed")
)

func routerErrMessage(code int) string {
	switch code {
	case http.StatusNotFound:
		return errNoRoute.Error()
	case http.StatusMethodNotAllowed:
		return errBadMethod.Error()
	}
	return errRouterUnkn.Error()
}
