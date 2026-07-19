package flowdoc

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
)

// A secret reference is a JSON object value of exactly the form
// {"$secret": "name"} anywhere inside an endpoint config. The object
// form cannot collide with legitimate string values, and the strict
// one-key shape means objects that merely contain a "$secret" key among
// others are left untouched.
//
// The hub validates references at deploy time (names must exist); the
// runner resolves them just before execution (hub-side documents stay
// inert — plaintext never lands in the queue, lease payloads, or logs).

const secretRefKey = "$secret"

// SecretNameRE mirrors the hub's secrets.name CHECK constraint.
var SecretNameRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

// SecretRefs returns the sorted, de-duplicated secret names referenced by
// the document's connector configs (source and sink in the linear form;
// every connector step — including handlers — in the graph form).
func (d *Document) SecretRefs() ([]string, error) {
	seen := map[string]bool{}
	for _, ep := range d.configRefs() {
		if err := walkConfig(ep.config, func(name string) error {
			if !SecretNameRE.MatchString(name) {
				return fmt.Errorf("flow: %s config: invalid secret name %q", ep.label, name)
			}
			seen[name] = true
			return nil
		}); err != nil {
			return nil, err
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names, nil
}

// ResolveSecrets returns a copy of the document with every secret
// reference replaced by the looked-up value. The receiver is not
// modified (the Steps slice is copied when present). lookup errors
// propagate unchanged.
func (d *Document) ResolveSecrets(lookup func(name string) (string, error)) (*Document, error) {
	out := *d
	if len(d.Steps) > 0 {
		out.Steps = make([]Step, len(d.Steps))
		copy(out.Steps, d.Steps)
	}
	for _, ep := range out.configRefs() {
		resolved, err := resolveConfig(ep.config, lookup)
		if err != nil {
			return nil, fmt.Errorf("flow: %s config: %w", ep.label, err)
		}
		*ep.dst = resolved
	}
	return &out, nil
}

// configRef points at one connector config to walk/rewrite, with a label
// for error messages. dst is nil for read-only callers (SecretRefs).
type configRef struct {
	label  string
	config json.RawMessage
	dst    *json.RawMessage
}

// configRefs enumerates the connector configs to inspect. For the graph
// form it targets the receiver's own Steps slice, so callers that copied
// Steps first (ResolveSecrets) rewrite the copy in place.
func (d *Document) configRefs() []configRef {
	if len(d.Steps) > 0 {
		refs := make([]configRef, 0, len(d.Steps))
		for i := range d.Steps {
			s := &d.Steps[i]
			if isConnectorType(s.Type) {
				refs = append(refs, configRef{"step " + s.ID, s.Config, &s.Config})
			}
		}
		return refs
	}
	return []configRef{
		{"source", d.Source.Config, &d.Source.Config},
		{"sink", d.Sink.Config, &d.Sink.Config},
	}
}

// secretRefName returns the referenced name when v is exactly
// {"$secret": "name"}.
func secretRefName(v map[string]any) (string, bool) {
	if len(v) != 1 {
		return "", false
	}
	name, ok := v[secretRefKey].(string)
	return name, ok
}

func walkConfig(raw json.RawMessage, visit func(name string) error) error {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return fmt.Errorf("invalid config JSON: %w", err)
	}
	return walkValue(v, visit)
}

func walkValue(v any, visit func(name string) error) error {
	switch x := v.(type) {
	case map[string]any:
		if name, ok := secretRefName(x); ok {
			return visit(name)
		}
		for _, child := range x {
			if err := walkValue(child, visit); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range x {
			if err := walkValue(child, visit); err != nil {
				return err
			}
		}
	}
	return nil
}

func resolveConfig(raw json.RawMessage, lookup func(string) (string, error)) (json.RawMessage, error) {
	if len(raw) == 0 {
		return raw, nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("invalid config JSON: %w", err)
	}
	resolved, changed, err := resolveValue(v, lookup)
	if err != nil {
		return nil, err
	}
	if !changed {
		return raw, nil
	}
	return json.Marshal(resolved)
}

func resolveValue(v any, lookup func(string) (string, error)) (out any, changed bool, err error) {
	switch x := v.(type) {
	case map[string]any:
		if name, ok := secretRefName(x); ok {
			value, err := lookup(name)
			if err != nil {
				return nil, false, err
			}
			return value, true, nil
		}
		dst := make(map[string]any, len(x))
		for k, child := range x {
			r, c, err := resolveValue(child, lookup)
			if err != nil {
				return nil, false, err
			}
			dst[k] = r
			changed = changed || c
		}
		return dst, changed, nil
	case []any:
		dst := make([]any, len(x))
		for i, child := range x {
			r, c, err := resolveValue(child, lookup)
			if err != nil {
				return nil, false, err
			}
			dst[i] = r
			changed = changed || c
		}
		return dst, changed, nil
	default:
		return v, false, nil
	}
}
