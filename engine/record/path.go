package record

import (
	"fmt"
	"strconv"
	"strings"
)

// Path is a compiled accessor into a record tree, parsed once at pipeline
// build time so per-record evaluation does no string work (ADR-0004:
// "transforms compile field paths once per stream").
//
// Syntax: `$.field.nested[0].leaf` — `$` is the record root, `.name`
// descends into a map field, `[i]` indexes a list.
type Path struct {
	raw   string
	steps []pathStep
}

type pathStep struct {
	key   string // map field, when index < 0
	index int    // list index, when >= 0
}

// ParsePath compiles a path expression.
func ParsePath(expr string) (Path, error) {
	s := strings.TrimSpace(expr)
	if s == "" || s[0] != '$' {
		return Path{}, fmt.Errorf("record: path %q must start with $", expr)
	}
	p := Path{raw: expr}
	s = s[1:]
	for len(s) > 0 {
		switch s[0] {
		case '.':
			s = s[1:]
			end := 0
			for end < len(s) && s[end] != '.' && s[end] != '[' {
				end++
			}
			if end == 0 {
				return Path{}, fmt.Errorf("record: empty field name in path %q", expr)
			}
			p.steps = append(p.steps, pathStep{key: s[:end], index: -1})
			s = s[end:]
		case '[':
			close := strings.IndexByte(s, ']')
			if close < 0 {
				return Path{}, fmt.Errorf("record: unterminated index in path %q", expr)
			}
			idx, err := strconv.Atoi(s[1:close])
			if err != nil || idx < 0 {
				return Path{}, fmt.Errorf("record: bad list index in path %q", expr)
			}
			p.steps = append(p.steps, pathStep{index: idx})
			s = s[close+1:]
		default:
			return Path{}, fmt.Errorf("record: unexpected %q in path %q", s[0], expr)
		}
	}
	return p, nil
}

// MustParsePath is ParsePath that panics on error, for compile-time paths.
func MustParsePath(expr string) Path {
	p, err := ParsePath(expr)
	if err != nil {
		panic(err)
	}
	return p
}

// String returns the original path expression.
func (p Path) String() string { return p.raw }

// IsRoot reports whether the path is just `$`.
func (p Path) IsRoot() bool { return len(p.steps) == 0 }

// Get evaluates the path against a record root.
func (p Path) Get(root Value) (Value, bool) {
	v := root
	for _, st := range p.steps {
		if st.index >= 0 {
			if v.kind != KindList || st.index >= len(v.kids) {
				return Value{}, false
			}
			v = v.kids[st.index]
			continue
		}
		var ok bool
		v, ok = v.Field(st.key)
		if !ok {
			return Value{}, false
		}
	}
	return v, true
}

// LeafName returns the final field name of the path ("" if it ends in an
// index or is the root) — a convenient default output name for projections.
func (p Path) LeafName() string {
	for i := len(p.steps) - 1; i >= 0; i-- {
		if p.steps[i].index < 0 {
			if i == len(p.steps)-1 {
				return p.steps[i].key
			}
			break
		}
	}
	return ""
}
