package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"runtime"
	"strconv"

	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/pkg/consign"
)

// connectorNameRE mirrors runner/internal/connpool's naming rule — the
// runner refuses anything else, so reject it at publish time.
var connectorNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

var osArchAllow = map[string]bool{
	"linux/amd64": true, "linux/arm64": true,
	"darwin/amd64": true, "darwin/arm64": true,
	"windows/amd64": true,
}

// maxArtifact caps upload/download size (the blob crosses hub RAM).
const defaultMaxArtifact = 128 << 20

// uploadConnector verifies and stores a signed artifact:
// PUT /api/v1/connectors/{name}/versions/{version}?os=&arch=
// headers X-Shift-Publisher-Key (key name), X-Shift-Signature (base64).
// Fail closed: bad signature/unknown key → 403, nothing stored.
func (a *api) uploadConnector(w http.ResponseWriter, r *http.Request) {
	name, version := r.PathValue("name"), r.PathValue("version")
	if !connectorNameRE.MatchString(name) {
		writeErr(w, http.StatusUnprocessableEntity, fmt.Errorf("connector name must match %s", connectorNameRE))
		return
	}
	if version == "" || len(version) > 64 {
		writeErr(w, http.StatusUnprocessableEntity, errors.New("version must be 1-64 characters"))
		return
	}
	osName, arch := r.URL.Query().Get("os"), r.URL.Query().Get("arch")
	if osName == "" {
		osName = runtime.GOOS
	}
	if arch == "" {
		arch = runtime.GOARCH
	}
	if !osArchAllow[osName+"/"+arch] {
		writeErr(w, http.StatusUnprocessableEntity, fmt.Errorf("unsupported os/arch %s/%s", osName, arch))
		return
	}
	keyName := r.Header.Get("X-Shift-Publisher-Key")
	sig, err := base64.StdEncoding.DecodeString(r.Header.Get("X-Shift-Signature"))
	if keyName == "" || err != nil || len(sig) == 0 {
		writeErr(w, http.StatusBadRequest, errors.New("X-Shift-Publisher-Key and base64 X-Shift-Signature headers are required"))
		return
	}
	// Optional descriptor (ADR-0018): the canonical action-catalog bytes,
	// base64 in a header. When present the signature must cover the v2
	// manifest (identity + artifact digest + descriptor digest); absent
	// keeps the byte-identical v1 form.
	var descriptor []byte
	if h := r.Header.Get("X-Shift-Descriptor"); h != "" {
		descriptor, err = base64.StdEncoding.DecodeString(h)
		if err != nil || len(descriptor) == 0 {
			writeErr(w, http.StatusBadRequest, errors.New("X-Shift-Descriptor must be non-empty base64"))
			return
		}
	}

	body := http.MaxBytesReader(w, r.Body, defaultMaxArtifact)
	hasher := sha256.New()
	data, err := io.ReadAll(io.TeeReader(body, hasher))
	if err != nil {
		if tooBig, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeErr(w, http.StatusRequestEntityTooLarge, fmt.Errorf("artifact exceeds %d bytes", tooBig.Limit))
			return
		}
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if len(data) == 0 {
		writeErr(w, http.StatusBadRequest, errors.New("empty artifact"))
		return
	}

	key, err := a.st.PublisherKeyByName(r.Context(), keyName)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusForbidden, fmt.Errorf("unknown or revoked publisher key %q", keyName))
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	m := consign.Manifest{Name: name, Version: version, OS: osName, Arch: arch}
	copy(m.Digest[:], hasher.Sum(nil))
	if len(descriptor) > 0 {
		m.DescriptorDigest = sha256.Sum256(descriptor)
	}
	if err := consign.Verify(key.PublicKey, m, sig); err != nil {
		writeErr(w, http.StatusForbidden, err)
		return
	}

	if err := a.st.PutConnectorVersion(r.Context(), name, version, osName, arch, m.Digest[:], sig, key.ID, data, descriptor); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "connector.publish", name,
		map[string]any{"version": version, "os": osName, "arch": arch, "digest": hex.EncodeToString(m.Digest[:]), "publisher_key": keyName})
	writeJSON(w, http.StatusCreated, map[string]any{
		"name": name, "version": version, "os": osName, "arch": arch,
		"digest": hex.EncodeToString(m.Digest[:]), "size_bytes": len(data),
	})
}

// resolveConnector returns the manifest a runner needs to fetch+verify:
// GET /api/v1/connectors/{name}/resolve?version=latest&os=&arch=
func (a *api) resolveConnector(w http.ResponseWriter, r *http.Request) {
	if !a.opts.ConnectorPolicy.Allowed(r.PathValue("name")) {
		writeErr(w, http.StatusNotFound, store.ErrNotFound) // hidden by policy
		return
	}
	q := r.URL.Query()
	cv, err := a.st.ResolveConnector(r.Context(), r.PathValue("name"),
		q.Get("version"), q.Get("os"), q.Get("arch"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, connectorManifestJSON(cv))
}

// downloadConnector streams the artifact bytes with verification
// headers: GET /api/v1/connectors/{name}/versions/{version}/artifact?os=&arch=
func (a *api) downloadConnector(w http.ResponseWriter, r *http.Request) {
	if !a.opts.ConnectorPolicy.Allowed(r.PathValue("name")) {
		writeErr(w, http.StatusNotFound, store.ErrNotFound) // hidden by policy
		return
	}
	q := r.URL.Query()
	cv, err := a.st.ResolveConnector(r.Context(), r.PathValue("name"),
		r.PathValue("version"), q.Get("os"), q.Get("arch"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	data, err := a.st.ConnectorBlob(r.Context(), cv.Digest)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Shift-Digest", hex.EncodeToString(cv.Digest))
	w.Header().Set("X-Shift-Signature", base64.StdEncoding.EncodeToString(cv.Signature))
	w.Header().Set("X-Shift-Publisher-Key", base64.StdEncoding.EncodeToString(cv.PublisherKey))
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	_, _ = w.Write(data)
}

func (a *api) listConnectors(w http.ResponseWriter, r *http.Request) {
	cvs, err := a.st.Connectors(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]map[string]any, 0, len(cvs))
	for _, cv := range cvs {
		if !a.opts.ConnectorPolicy.Allowed(cv.Name) {
			continue // hidden by policy — "not even visible"
		}
		out = append(out, connectorManifestJSON(cv))
	}
	writeJSON(w, http.StatusOK, map[string]any{"connectors": out})
}

func connectorManifestJSON(cv store.ConnectorVersion) map[string]any {
	m := map[string]any{
		"name": cv.Name, "version": cv.Version, "os": cv.OS, "arch": cv.Arch,
		"digest":        hex.EncodeToString(cv.Digest),
		"signature":     base64.StdEncoding.EncodeToString(cv.Signature),
		"publisher_key": base64.StdEncoding.EncodeToString(cv.PublisherKey),
		"size_bytes":    cv.SizeBytes,
		"created_at":    cv.Created,
	}
	// Descriptor (ADR-0018): base64 of the exact signed canonical bytes so
	// the runner re-digests them byte-for-byte to verify the v2 manifest,
	// and the studio builder decodes them to render config forms. Omitted
	// for pre-descriptor (v1) artifacts.
	if len(cv.Descriptor) > 0 {
		m["descriptor"] = base64.StdEncoding.EncodeToString(cv.Descriptor)
	}
	// yanked_at present only in the version-history listing (M6e).
	if cv.Yanked != nil {
		m["yanked_at"] = cv.Yanked
	}
	return m
}

// listConnectorVersions: GET /api/v1/connectors/{name}/versions — the full
// version history for one connector (all os/arch, including yanked ones so the
// marketplace can show provenance), newest first (M6e).
func (a *api) listConnectorVersions(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !a.opts.ConnectorPolicy.Allowed(name) {
		writeErr(w, http.StatusNotFound, store.ErrNotFound) // hidden by policy
		return
	}
	cvs, err := a.st.ConnectorVersions(r.Context(), name)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]map[string]any, 0, len(cvs))
	for _, cv := range cvs {
		out = append(out, connectorManifestJSON(cv))
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "versions": out})
}

// setConnectorYanked: POST /api/v1/connectors/{name}/versions/{version}/yank
// body {"os":..,"arch":..,"yanked":true|false} (default true). Admin, audited.
// A yanked version is excluded from resolve/download (fail closed) but stays
// in the version history.
func (a *api) setConnectorYanked(w http.ResponseWriter, r *http.Request) {
	name, version := r.PathValue("name"), r.PathValue("version")
	var req struct {
		OS     string `json:"os"`
		Arch   string `json:"arch"`
		Yanked *bool  `json:"yanked"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.OS == "" {
		req.OS = runtime.GOOS
	}
	if req.Arch == "" {
		req.Arch = runtime.GOARCH
	}
	yank := true
	if req.Yanked != nil {
		yank = *req.Yanked
	}
	if err := a.st.SetConnectorYanked(r.Context(), name, version, req.OS, req.Arch, yank); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	action := "connector.yank"
	if !yank {
		action = "connector.unyank"
	}
	_ = a.st.Audit(r.Context(), actor(r), action, name,
		map[string]any{"version": version, "os": req.OS, "arch": req.Arch})
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "version": version, "os": req.OS, "arch": req.Arch, "yanked": yank})
}

// --- publisher keys ----------------------------------------------------------

func (a *api) addPublisherKey(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name      string `json:"name"`
		PublicKey string `json:"public_key"` // base64
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	pub, err := base64.StdEncoding.DecodeString(req.PublicKey)
	if req.Name == "" || err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("name and base64 public_key are required"))
		return
	}
	id, err := a.st.AddPublisherKey(r.Context(), req.Name, pub)
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "publisher-key.add", req.Name, nil)
	writeJSON(w, http.StatusCreated, map[string]string{"id": id, "name": req.Name})
}

func (a *api) listPublisherKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := a.st.TrustedKeys(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{
			"id": k.ID, "name": k.Name,
			"public_key": base64.StdEncoding.EncodeToString(k.PublicKey),
			"created_at": k.Created,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": out})
}

func (a *api) revokePublisherKey(w http.ResponseWriter, r *http.Request) {
	err := a.st.RevokePublisherKey(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "publisher-key.revoke", r.PathValue("id"), nil)
	w.WriteHeader(http.StatusNoContent)
}
