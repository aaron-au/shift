package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aaron-au/shift/hub/internal/secrets"
	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/pkg/flowdoc"
)

// putSecret stores/replaces a secret value. The value is accepted once
// and never returned by any endpoint.
func (a *api) putSecret(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !flowdoc.SecretNameRE.MatchString(name) {
		writeErr(w, http.StatusUnprocessableEntity,
			fmt.Errorf("secret name must match %s", flowdoc.SecretNameRE))
		return
	}
	var req struct {
		Value string `json:"value"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.Value == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("value is required"))
		return
	}
	version, err := a.opts.Secrets.Put(r.Context(), name, []byte(req.Value), requestIdentity(r).id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("storing secret %q failed", name))
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "secret.put", name, map[string]int{"version": version})
	writeJSON(w, http.StatusCreated, map[string]any{"name": name, "version": version})
}

func (a *api) listSecrets(w http.ResponseWriter, r *http.Request) {
	metas, err := a.opts.Secrets.List(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"secrets": metas})
}

func (a *api) deleteSecret(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	err := a.opts.Secrets.Delete(r.Context(), name)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "secret.delete", name, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) rotateKEK(w http.ResponseWriter, r *http.Request) {
	n, err := a.opts.Secrets.RotateKEK(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "kek.rotate", "", map[string]int{"rewrapped": n})
	writeJSON(w, http.StatusOK, map[string]int{"rewrapped": n})
}

// checkSecretRefs validates at deploy time that every {"$secret":...}
// reference in the document names an existing secret. Metadata check
// only — nothing is decrypted. A later delete is caught at execution
// with a clear task failure instead.
// checkConnectorPolicy rejects a deploy that references any connector the
// hub's per-deployment capability policy disallows (cloud hubs hide
// dangerous connectors). No-op when the hub is unrestricted (self-hosted).
func (a *api) checkConnectorPolicy(doc *flowdoc.Document) error {
	if !a.opts.ConnectorPolicy.Restricted() {
		return nil
	}
	var blocked []string
	for _, name := range doc.Connectors() {
		if !a.opts.ConnectorPolicy.Allowed(name) {
			blocked = append(blocked, name)
		}
	}
	if len(blocked) > 0 {
		return fmt.Errorf("connector(s) not permitted on this hub: %s", strings.Join(blocked, ", "))
	}
	return nil
}

func (a *api) checkSecretRefs(r *http.Request, doc *flowdoc.Document) error {
	refs, err := doc.SecretRefs()
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}
	if a.opts.Secrets == nil {
		return fmt.Errorf("document references secrets but the hub has no secret store configured")
	}
	envs, err := a.st.SecretEnvelopes(r.Context(), refs)
	if err != nil {
		return fmt.Errorf("checking secret references: %w", err)
	}
	found := map[string]bool{}
	for _, e := range envs {
		found[e.Name] = true
	}
	var missing []string
	for _, name := range refs {
		if !found[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return &secrets.MissingError{Names: missing}
	}
	return nil
}

// resolveSecrets is the runner realm's decrypt path. Every resolved
// name is audited; errors carry names only.
func (a *api) resolveSecrets(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Names []string `json:"names"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if len(req.Names) == 0 || len(req.Names) > 100 {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("names must list 1-100 secrets"))
		return
	}
	values, err := a.opts.Secrets.Resolve(r.Context(), req.Names)
	if missing, ok := errors.AsType[*secrets.MissingError](err); ok {
		writeErr(w, http.StatusNotFound, missing)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("secret resolution failed"))
		return
	}
	for _, name := range req.Names {
		_ = a.st.Audit(r.Context(), actor(r), "secret.access", name, nil)
	}
	writeJSON(w, http.StatusOK, map[string]any{"secrets": values})
}
