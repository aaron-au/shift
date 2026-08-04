package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/aaron-au/shift/hub/internal/secrets"
	"github.com/aaron-au/shift/hub/internal/store"
	"github.com/aaron-au/shift/pkg/flowdoc"
)

// Connections: reusable connector configuration (ADR-0034). A connection
// is metadata, not payload — its config carries {"$secret":...}
// references exactly as a flow document does, and those resolve
// runner-side, so the hub stores a reference and never a credential.
//
// Admin manages them; the runner realm resolves the ones a task needs,
// mirroring the secrets endpoints.

// maxConnectionConfig bounds a stored connection document. Connection
// config is host/credential-reference settings, not payload; the flow
// document limit (4 MiB) would be three orders of magnitude too generous
// for a form with a dozen fields.
const maxConnectionConfig = 64 << 10

func (a *api) putConnection(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !flowdoc.ConnectionNamePattern.MatchString(name) {
		writeErr(w, http.StatusUnprocessableEntity,
			fmt.Errorf("connection name must match %s", flowdoc.ConnectionNamePattern))
		return
	}
	var req struct {
		Connector string          `json:"connector"`
		Config    json.RawMessage `json:"config"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	// The connector name takes the connection charset for the same reason
	// the connection name does: it is an identifier the registry and the
	// studio address, not prose.
	if !flowdoc.ConnectionNamePattern.MatchString(req.Connector) {
		writeErr(w, http.StatusUnprocessableEntity,
			fmt.Errorf("connector must match %s", flowdoc.ConnectionNamePattern))
		return
	}
	// Built-ins talk to no external system, so a connection for one
	// configures nothing (flowdoc rejects the node-side reference too).
	// The charset above already excludes the "@" prefix; this states the
	// rule so a later charset change cannot quietly admit them.
	if flowdoc.IsBuiltinConnector(req.Connector) {
		writeErr(w, http.StatusUnprocessableEntity,
			fmt.Errorf("built-in %q takes no connection", req.Connector))
		return
	}
	if len(req.Config) > maxConnectionConfig {
		writeErr(w, http.StatusRequestEntityTooLarge,
			fmt.Errorf("connection config exceeds %d bytes", maxConnectionConfig))
		return
	}
	if len(req.Config) == 0 {
		req.Config = json.RawMessage(`{}`)
	}
	// The config must be an object (it is addressed by key when merged
	// into a node's) and its secret references must name real secrets —
	// the same deploy-time check a flow document gets, applied where the
	// author actually types the reference.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(req.Config, &fields); err != nil {
		writeErr(w, http.StatusUnprocessableEntity,
			fmt.Errorf("connection config must be a JSON object: %w", err))
		return
	}
	if err := a.checkConfigSecretRefs(r, req.Config); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}

	_, version, err := a.st.UpsertConnection(r.Context(), name, req.Connector, req.Config, requestIdentity(r).id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("storing connection %q failed", name))
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "connection.put", name, map[string]any{
		"version": version, "connector": req.Connector,
	})
	writeJSON(w, http.StatusCreated, map[string]any{
		"name": name, "connector": req.Connector, "version": version,
	})
}

func (a *api) listConnections(w http.ResponseWriter, r *http.Request) {
	conns, err := a.st.Connections(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"connections": conns})
}

func (a *api) getConnection(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	conns, err := a.st.ConnectionsByName(r.Context(), []string{name})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if len(conns) == 0 {
		writeErr(w, http.StatusNotFound, store.ErrNotFound)
		return
	}
	writeJSON(w, http.StatusOK, conns[0])
}

// deleteConnection refuses while a published flow still references the
// connection (ADR-0034 open question 1, resolved as refuse). Allowing the
// delete would trade one clear 409 at the moment of the change for an
// opaque task failure at the next scheduled run.
func (a *api) deleteConnection(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	using, err := a.st.FlowsUsingConnection(r.Context(), name)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if len(using) > 0 {
		writeErrCode(w, http.StatusConflict, "connection_in_use",
			fmt.Errorf("connection %q is referenced by published flow(s): %s",
				name, strings.Join(using, ", ")))
		return
	}
	err = a.st.DeleteConnection(r.Context(), name)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	_ = a.st.Audit(r.Context(), actor(r), "connection.delete", name, nil)
	w.WriteHeader(http.StatusNoContent)
}

// resolveConnections is the runner realm's fetch path: the runner asks
// for the connections its task's flow references, merges each into the
// node config, and resolves the secret references separately (ADR-0034
// §4). Documents only — no decryption happens here.
func (a *api) resolveConnections(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Names []string `json:"names"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if len(req.Names) == 0 || len(req.Names) > 100 {
		writeErr(w, http.StatusBadRequest, errors.New("names must list 1-100 connections"))
		return
	}
	conns, err := a.st.ConnectionsByName(r.Context(), req.Names)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("connection resolution failed"))
		return
	}
	if missing := missingConnections(req.Names, conns); len(missing) > 0 {
		writeErrCode(w, http.StatusNotFound, "connection_missing",
			fmt.Errorf("unknown connection(s): %s", strings.Join(missing, ", ")))
		return
	}
	out := make(map[string]store.Connection, len(conns))
	for _, c := range conns {
		out[c.Name] = c
	}
	writeJSON(w, http.StatusOK, map[string]any{"connections": out})
}

// checkConfigSecretRefs validates one config document's {"$secret":...}
// references against the account's secrets, like checkSecretRefs does for
// a whole flow document.
func (a *api) checkConfigSecretRefs(r *http.Request, config json.RawMessage) error {
	refs, err := flowdoc.ConfigSecretRefs(config)
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}
	if a.opts.Secrets == nil {
		return errors.New("config references secrets but the hub has no secret store configured")
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
		return fmt.Errorf("unknown secret(s): %s", strings.Join(missing, ", "))
	}
	return nil
}

// checkConnectionRefs validates at deploy time that every connection a
// document names exists, configures the connector the node declares, and
// does not collide with the node's own config.
//
// All three are metadata checks the hub can make from documents it
// already holds — no descriptor is parsed (ADR-0018 keeps the hub out of
// that) and no schema is consulted. The collision rule needs none: the
// keys a connection supplies ARE its connection-level keys.
func (a *api) checkConnectionRefs(r *http.Request, doc *flowdoc.Document) error {
	names := doc.Connections()
	if len(names) == 0 {
		return nil
	}
	conns, err := a.st.ConnectionsByName(r.Context(), names)
	if err != nil {
		return fmt.Errorf("checking connection references: %w", err)
	}
	if missing := missingConnections(names, conns); len(missing) > 0 {
		return fmt.Errorf("unknown connection(s): %s", strings.Join(missing, ", "))
	}
	byName := make(map[string]store.Connection, len(conns))
	for _, c := range conns {
		byName[c.Name] = c
	}
	for _, use := range doc.ConnectionUses() {
		c := byName[use.Connection]
		if c.Connector != use.Connector {
			return fmt.Errorf("%s declares connector %q but connection %q configures %q",
				use.Label, use.Connector, use.Connection, c.Connector)
		}
		if _, err := flowdoc.MergeConnectionConfig(c.Config, use.Config); err != nil {
			return fmt.Errorf("%s: %w", use.Label, err)
		}
	}
	return nil
}

// missingConnections returns the requested names with no stored
// connection, sorted so the message is stable.
func missingConnections(want []string, got []store.Connection) []string {
	found := make(map[string]bool, len(got))
	for _, c := range got {
		found[c.Name] = true
	}
	var missing []string
	for _, n := range want {
		if !found[n] {
			missing = append(missing, n)
		}
	}
	sort.Strings(missing)
	return missing
}

// resolveTaskConfig is the runner's single bootstrap call: it returns the
// connections a task's flow references AND every secret needed to run it,
// in one round trip.
//
// It exists because the naive sequence is two SEQUENTIAL calls — fetch
// connections, then resolve the secrets those connections turn out to
// reference. Against a hub one WAN hop away that doubles a latency the
// runner-direct paths exist to avoid (ADR-0035 §3), on exactly the
// request-reply and webhook paths where it is most visible.
//
// The hub can collapse it because it holds both: it collects the secret
// references out of the connections it is about to return and folds them
// into the set it resolves. Connection configs travel with their
// {"$secret":...} references INTACT — substitution stays runner-side
// (ADR-0010), so this adds no new place for plaintext to sit.
func (a *api) resolveTaskConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Connections []string `json:"connections"`
		Secrets     []string `json:"secrets"`
	}
	if err := readBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if len(req.Connections) > 100 || len(req.Secrets) > 100 {
		writeErr(w, http.StatusBadRequest, errors.New("connections and secrets are limited to 100 each"))
		return
	}

	conns := map[string]store.Connection{}
	if len(req.Connections) > 0 {
		found, err := a.st.ConnectionsByName(r.Context(), req.Connections)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, errors.New("connection resolution failed"))
			return
		}
		if missing := missingConnections(req.Connections, found); len(missing) > 0 {
			writeErrCode(w, http.StatusNotFound, "connection_missing",
				fmt.Errorf("unknown connection(s): %s", strings.Join(missing, ", ")))
			return
		}
		for _, c := range found {
			conns[c.Name] = c
		}
	}

	// Fold in whatever the connections themselves reference: the runner
	// cannot know these names until it has the connections, and making it
	// ask again is the round trip this endpoint removes.
	want := map[string]bool{}
	for _, n := range req.Secrets {
		want[n] = true
	}
	for _, c := range conns {
		refs, err := flowdoc.ConfigSecretRefs(c.Config)
		if err != nil {
			writeErr(w, http.StatusUnprocessableEntity,
				fmt.Errorf("connection %q: %w", c.Name, err))
			return
		}
		for _, n := range refs {
			want[n] = true
		}
	}

	values := map[string]string{}
	if len(want) > 0 {
		if a.opts.Secrets == nil {
			writeErr(w, http.StatusUnprocessableEntity,
				errors.New("task references secrets but the hub has no secret store configured"))
			return
		}
		names := make([]string, 0, len(want))
		for n := range want {
			names = append(names, n)
		}
		sort.Strings(names)
		var err error
		values, err = a.opts.Secrets.Resolve(r.Context(), names)
		if missing, ok := errors.AsType[*secrets.MissingError](err); ok {
			writeErr(w, http.StatusNotFound, missing)
			return
		}
		if err != nil {
			writeErr(w, http.StatusInternalServerError, errors.New("secret resolution failed"))
			return
		}
		for _, n := range names {
			_ = a.st.Audit(r.Context(), actor(r), "secret.access", n, nil)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"connections": conns,
		"secrets":     values,
	})
}
