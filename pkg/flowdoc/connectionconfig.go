package flowdoc

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Connection-level config handling (ADR-0034). A Connection is a named,
// account-scoped config document that several nodes reference instead of
// repeating the host/credential settings on each one. Its config is
// ordinary connector config: it may carry {"$secret":...} refs, which
// resolve runner-side by the existing mechanism (secretref.go), so this
// adds no new path by which plaintext could reach the hub.
//
// These helpers are the config-level counterparts of the document-level
// SecretRefs/ResolveSecrets, so a connection document travels the same
// road as the node config it will be merged with.

// ConnectionUse is one node's reference to a Connection, with everything
// a validator needs to judge it: which connector the node declares (it
// must match the connection's) and the node's own config (which must not
// collide with the connection's).
type ConnectionUse struct {
	// Label identifies the node for an error message — "source", "sink",
	// or "step <id>" — matching the vocabulary of the other document
	// diagnostics.
	Label      string
	Connector  string
	Connection string
	Config     json.RawMessage
}

// ConnectionUses lists every node that references a Connection, in both
// authoring forms. Callers that only need the set of names should use
// Connections; this is for validating each reference against the
// connection it names.
func (d *Document) ConnectionUses() []ConnectionUse {
	var uses []ConnectionUse
	add := func(label, connector, connection string, config json.RawMessage) {
		if connection == "" {
			return
		}
		uses = append(uses, ConnectionUse{label, connector, connection, config})
	}
	if len(d.Steps) > 0 {
		for i := range d.Steps {
			s := &d.Steps[i]
			if isConnectorType(s.Type) {
				add("step "+s.ID, s.Connector, s.Connection, s.Config)
			}
		}
		return uses
	}
	add("source", d.Source.Connector, d.Source.Connection, d.Source.Config)
	add("sink", d.Sink.Connector, d.Sink.Connection, d.Sink.Config)
	return uses
}

// ConfigSecretRefs returns the sorted, de-duplicated secret names
// referenced by a single connector config document (a Connection's
// config, or one node's). Names are validated against SecretNameRE.
func ConfigSecretRefs(raw json.RawMessage) ([]string, error) {
	seen := map[string]bool{}
	if err := walkConfig(raw, func(name string) error {
		if !SecretNameRE.MatchString(name) {
			return fmt.Errorf("invalid secret name %q", name)
		}
		seen[name] = true
		return nil
	}); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names, nil
}

// ResolveConfigSecrets returns a copy of one config document with every
// secret reference replaced by its looked-up value. The input is not
// modified. Used runner-side on a Connection's config before it is
// merged into the node's.
func ResolveConfigSecrets(raw json.RawMessage, lookup func(name string) (string, error)) (json.RawMessage, error) {
	return resolveConfig(raw, lookup)
}

// MergeConnectionConfig computes a node's effective config: the
// Connection's config, then the node's own.
//
// A key present in both is an ERROR, not an override (ADR-0034 §3). The
// failure this rule exists to prevent is one node in a flow quietly
// pointing at a different host than its siblings — which is exactly what
// last-write-wins produces, and it surfaces as a network problem rather
// than a config one. If a node genuinely needs different connection
// settings, it needs a different connection.
//
// The comparison is deliberately top-level only. Recursing would let a
// node override a nested field of a connection-level object (a TLS
// setting, one member of a credential block) without a collision ever
// being detected, reintroducing the silent override through the back
// door.
func MergeConnectionConfig(connection, node json.RawMessage) (json.RawMessage, error) {
	connFields, err := configObject(connection, "connection")
	if err != nil {
		return nil, err
	}
	if len(connFields) == 0 {
		return node, nil
	}
	nodeFields, err := configObject(node, "node")
	if err != nil {
		return nil, err
	}
	var collisions []string
	for k := range nodeFields {
		if _, dup := connFields[k]; dup {
			collisions = append(collisions, k)
		}
	}
	if len(collisions) > 0 {
		sort.Strings(collisions)
		return nil, &ConnectionCollisionError{Keys: collisions}
	}
	merged := make(map[string]json.RawMessage, len(connFields)+len(nodeFields))
	for k, v := range connFields {
		merged[k] = v
	}
	for k, v := range nodeFields {
		merged[k] = v
	}
	return json.Marshal(merged)
}

// ConnectionCollisionError reports node config keys the referenced
// Connection already supplies. Typed so the hub can answer a deploy with
// the offending key names instead of a generic 422.
type ConnectionCollisionError struct {
	Keys []string
}

func (e *ConnectionCollisionError) Error() string {
	noun := "key"
	if len(e.Keys) > 1 {
		noun = "keys"
	}
	return fmt.Sprintf("node config %s %s already supplied by the connection: remove from the node, or reference a different connection",
		noun, quoteList(e.Keys))
}

// configObject decodes a config document into its top-level fields,
// leaving each value as raw JSON. An empty document is an empty object;
// anything that is not a JSON object is an error, since a connector
// config is addressed by key.
func configObject(raw json.RawMessage, what string) (map[string]json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("%s config must be a JSON object: %w", what, err)
	}
	return fields, nil
}

func quoteList(items []string) string {
	out := make([]byte, 0, len(items)*12)
	for i, s := range items {
		if i > 0 {
			out = append(out, ", "...)
		}
		out = append(out, '"')
		out = append(out, s...)
		out = append(out, '"')
	}
	return string(out)
}
