package schema

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Compile turns a JSON Schema document into an evaluator, or explains why it
// will not.
//
// This runs ONCE, at plan build. It is allowed to be unhurried and to use
// encoding/json and maps freely; none of that is on the request path. What it
// must not do is accept a schema it will then under-enforce.
func Compile(raw []byte) (*Schema, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("%w: not valid JSON: %w", errCompile, err)
	}
	c := &compiler{root: doc}
	n, err := c.node(doc, "")
	if err != nil {
		return nil, err
	}
	return &Schema{root: n}, nil
}

type compiler struct {
	root any
	// resolving is the $ref stack, used to reject recursive schemas — which
	// have no bounded compiled form here, and would otherwise compile into an
	// infinite tree or a stack overflow.
	resolving []string
}

// assertions is the closed set of keywords that CONSTRAIN a document.
// Anything outside this set and annotations below is rejected (ADR-0042 §4c).
var assertions = map[string]bool{
	"type": true, "required": true, "properties": true, "items": true,
	"enum": true, "const": true, "minimum": true, "maximum": true,
	"minLength": true, "maxLength": true, "pattern": true,
	"minItems": true, "maxItems": true, "additionalProperties": true,
	"format": true, "$ref": true, "$defs": true,
}

// annotations describe a schema without asserting anything about a document,
// so ignoring one cannot weaken validation. THIS distinction is the whole
// point of the closed set: a misspelt assertion silently validates nothing,
// whereas a misspelt title is a typo in prose.
var annotations = map[string]bool{
	"$schema": true, "$id": true, "$anchor": true, "$comment": true,
	"title": true, "description": true, "default": true, "examples": true,
	"deprecated": true, "readOnly": true, "writeOnly": true,
	// OpenAPI documents carry components/schemas alongside the schema proper;
	// it holds definitions, which $ref resolves into, not assertions.
	"components": true,
}

func (c *compiler) node(v any, ptr string) (*node, error) {
	switch s := v.(type) {
	case bool:
		// The boolean schemas: true accepts everything, false rejects it.
		if s {
			return &node{}, nil
		}
		// Modelled as a constant that nothing equals, which every value fails.
		return &node{konst: &constVal{kind: constNever}}, nil
	case map[string]any:
		return c.object(s, ptr)
	default:
		return nil, fmt.Errorf("%w: %s: a schema must be an object or a boolean", errCompile, at(ptr))
	}
}

func (c *compiler) object(m map[string]any, ptr string) (*node, error) {
	if err := c.checkKeywords(m, ptr); err != nil {
		return nil, err
	}
	n := &node{}
	for _, kw := range sortedKeys(m) {
		var err error
		switch kw {
		case "type":
			n.types, err = compileType(m[kw], ptr)
		case "required":
			n.required, err = stringList(m[kw], ptr, "required")
		case "properties":
			err = c.properties(n, m[kw], ptr)
		case "items":
			n.items, err = c.node(m[kw], ptr+"/items")
		case "additionalProperties":
			b, ok := m[kw].(bool)
			if !ok {
				// The schema form ({"additionalProperties": {...}}) applies a
				// schema to unlisted properties. Rejected rather than ignored:
				// ignoring it would silently permit what the author restricted.
				err = fmt.Errorf("%w: %s: additionalProperties must be true or false in this subset", errCompile, at(ptr))
				break
			}
			n.hasAddl, n.addlAllowed = true, b
		case "enum":
			n.enum, err = compileEnum(m[kw], ptr)
		case "const":
			var cv constVal
			cv, err = toConst(m[kw], ptr)
			n.konst = &cv
		case "minimum":
			n.minimum, err = numberOf(m[kw], ptr, "minimum")
		case "maximum":
			n.maximum, err = numberOf(m[kw], ptr, "maximum")
		case "minLength":
			n.minLength, err = intOf(m[kw], ptr, "minLength")
		case "maxLength":
			n.maxLength, err = intOf(m[kw], ptr, "maxLength")
		case "minItems":
			n.minItems, err = intOf(m[kw], ptr, "minItems")
		case "maxItems":
			n.maxItems, err = intOf(m[kw], ptr, "maxItems")
		case "pattern":
			n.pattern, err = compilePattern(m[kw], ptr)
		case "format":
			n.format, err = compileFormat(m[kw], ptr)
		case "$ref":
			// In 2020-12 $ref does NOT replace its siblings: {"$ref": "#/$defs/x",
			// "maxItems": 2} must enforce BOTH. Treating $ref as a replacement
			// silently drops every sibling assertion — the exact failure this
			// package exists to prevent, and caught by the conformance suite.
			n.refTarget, err = c.ref(m[kw], ptr)
		}
		if err != nil {
			return nil, err
		}
	}
	return n, nil
}

// checkKeywords is where the closed set is enforced.
func (c *compiler) checkKeywords(m map[string]any, ptr string) error {
	var unknown []string
	for _, k := range sortedKeys(m) {
		if !assertions[k] && !annotations[k] {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	// Naming a near-miss turns the most common real mistake — a typo in an
	// assertion — from a puzzle into an obvious fix.
	hint := ""
	if len(unknown) == 1 {
		if s := nearest(unknown[0]); s != "" {
			hint = fmt.Sprintf(" (did you mean %q?)", s)
		}
	}
	return fmt.Errorf("%w: %s: unsupported keyword(s) %s%s — this subset rejects what it cannot enforce, "+
		"because a keyword that is ignored is a check that never runs",
		errCompile, at(ptr), strings.Join(quoteAll(unknown), ", "), hint)
}

func (c *compiler) properties(n *node, v any, ptr string) error {
	m, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: %s: properties must be an object", errCompile, at(ptr))
	}
	n.props = make(map[string]*node, len(m))
	for _, k := range sortedKeys(m) {
		sub, err := c.node(m[k], ptr+"/properties/"+escapePtr(k))
		if err != nil {
			return err
		}
		n.props[k] = sub
	}
	return nil
}

// ref resolves a LOCAL JSON Pointer reference at compile time and inlines it.
//
// Remote references are refused (ADR-0042 §4c-ii): resolving one means the
// runner fetching a URL chosen by whoever wrote the schema, which is an SSRF
// primitive on the component holding decrypted secrets, plus an availability
// and a supply-chain dependency. If they are ever wanted, the answer is
// publish-time resolution pinned by digest at the hub, not a fetch here.
func (c *compiler) ref(v any, ptr string) (*node, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("%w: %s: $ref must be a string", errCompile, at(ptr))
	}
	if !strings.HasPrefix(s, "#") {
		return nil, fmt.Errorf("%w: %s: $ref %q is not local; remote references are refused "+
			"(a schema-chosen URL fetched by the runner is an SSRF primitive)", errCompile, at(ptr), s)
	}
	for _, seen := range c.resolving {
		if seen == s {
			return nil, fmt.Errorf("%w: %s: $ref %q is recursive; a recursive schema has no bounded compiled form",
				errCompile, at(ptr), s)
		}
	}
	target, err := resolvePointer(c.root, strings.TrimPrefix(s, "#"))
	if err != nil {
		return nil, fmt.Errorf("%w: %s: $ref %q: %w", errCompile, at(ptr), s, err)
	}
	c.resolving = append(c.resolving, s)
	defer func() { c.resolving = c.resolving[:len(c.resolving)-1] }()
	return c.node(target, s)
}

// resolvePointer walks an RFC 6901 JSON Pointer. It accepts any local target,
// not just $defs, because a schema lifted out of an OpenAPI document refers to
// #/components/schemas/... and rejecting that would reject the schemas people
// actually have.
func resolvePointer(root any, ptr string) (any, error) {
	if ptr == "" || ptr == "/" {
		return root, nil
	}
	cur := root
	for _, raw := range strings.Split(strings.TrimPrefix(ptr, "/"), "/") {
		seg := unescapePtr(raw)
		switch t := cur.(type) {
		case map[string]any:
			next, ok := t[seg]
			if !ok {
				return nil, fmt.Errorf("no such member %q", seg)
			}
			cur = next
		case []any:
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(t) {
				return nil, fmt.Errorf("no such index %q", seg)
			}
			cur = t[i]
		default:
			return nil, fmt.Errorf("cannot descend into %q", seg)
		}
	}
	return cur, nil
}

func compileType(v any, ptr string) (typeSet, error) {
	var names []string
	switch t := v.(type) {
	case string:
		names = []string{t}
	case []any:
		for _, e := range t {
			s, ok := e.(string)
			if !ok {
				return 0, fmt.Errorf("%w: %s: type entries must be strings", errCompile, at(ptr))
			}
			names = append(names, s)
		}
	default:
		return 0, fmt.Errorf("%w: %s: type must be a string or an array of strings", errCompile, at(ptr))
	}

	var set typeSet
	for _, name := range names {
		switch name {
		case "null":
			set |= typeNull
		case "boolean":
			set |= typeBool
		case "object":
			set |= typeObject
		case "array":
			set |= typeArray
		case "number":
			set |= typeNumber
		case "integer":
			set |= typeInteger
		case "string":
			set |= typeString
		default:
			return 0, fmt.Errorf("%w: %s: unknown type %q", errCompile, at(ptr), name)
		}
	}
	return set, nil
}

func compileEnum(v any, ptr string) ([]constVal, error) {
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: %s: enum must be an array", errCompile, at(ptr))
	}
	out := make([]constVal, 0, len(list))
	for _, e := range list {
		cv, err := toConst(e, ptr)
		if err != nil {
			return nil, err
		}
		out = append(out, cv)
	}
	return out, nil
}

func compilePattern(v any, ptr string) (*regexp.Regexp, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("%w: %s: pattern must be a string", errCompile, at(ptr))
	}
	// Go's RE2 has no backtracking, so a pattern cannot be a CPU bomb — which
	// is why `pattern` is safe to support against untrusted input at all.
	re, err := regexp.Compile(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: pattern %q: %w", errCompile, at(ptr), s, err)
	}
	return re, nil
}

func stringList(v any, ptr, kw string) ([]string, error) {
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: %s: %s must be an array of strings", errCompile, at(ptr), kw)
	}
	out := make([]string, 0, len(list))
	for _, e := range list {
		s, ok := e.(string)
		if !ok {
			return nil, fmt.Errorf("%w: %s: %s entries must be strings", errCompile, at(ptr), kw)
		}
		out = append(out, s)
	}
	return out, nil
}

func numberOf(v any, ptr, kw string) (*float64, error) {
	f, ok := v.(float64)
	if !ok {
		return nil, fmt.Errorf("%w: %s: %s must be a number", errCompile, at(ptr), kw)
	}
	return &f, nil
}

func intOf(v any, ptr, kw string) (*int, error) {
	f, ok := v.(float64)
	if !ok || f != math.Trunc(f) || f < 0 {
		return nil, fmt.Errorf("%w: %s: %s must be a non-negative integer", errCompile, at(ptr), kw)
	}
	i := int(f)
	return &i, nil
}

func at(ptr string) string {
	if ptr == "" {
		return "at the schema root"
	}
	return "at " + ptr
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func quoteAll(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = strconv.Quote(s)
	}
	return out
}

// nearest finds a supported keyword within one edit of s, which is what a
// typo usually is.
func nearest(s string) string {
	best, bestDist := "", 3
	for kw := range assertions {
		if d := editDistance(strings.ToLower(s), strings.ToLower(kw)); d < bestDist {
			best, bestDist = kw, d
		}
	}
	return best
}

func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}

func escapePtr(s string) string {
	return strings.NewReplacer("~", "~0", "/", "~1").Replace(s)
}

func unescapePtr(s string) string {
	// Order matters: ~01 must decode to ~1, not to /.
	return strings.NewReplacer("~1", "/", "~0", "~").Replace(s)
}
