package soapconn

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aaron-au/shift/engine/record"
)

// xmlNode is a parsed XML element: its local (namespace-stripped) name, its
// attributes, its child elements in document order, and its accumulated text.
// This tiny tree is the seam for the future engine XML/EDI format (M1.5) — the
// element→record mapping below is the convention that work will formalize.
type xmlNode struct {
	name     string
	attrs    []xmlAttr
	children []*xmlNode
	text     strings.Builder
}

type xmlAttr struct {
	name  string
	value string
}

// parseTree decodes an XML document into an element tree. Namespaces are
// resolved by the decoder; we keep only local names so matching (Envelope,
// Body, Fault) is prefix-agnostic (soap:/soapenv:/s:/env: all work). Namespace
// declaration attributes (xmlns / xmlns:*) are dropped.
func parseTree(doc []byte) (*xmlNode, error) {
	dec := xml.NewDecoder(bytes.NewReader(doc))
	var root *xmlNode
	var stack []*xmlNode
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("soap: xml decode: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			n := &xmlNode{name: t.Name.Local}
			for _, a := range t.Attr {
				if a.Name.Local == "xmlns" || a.Name.Space == "xmlns" {
					continue // namespace declaration, not data
				}
				n.attrs = append(n.attrs, xmlAttr{name: a.Name.Local, value: a.Value})
			}
			if len(stack) > 0 {
				parent := stack[len(stack)-1]
				parent.children = append(parent.children, n)
			} else {
				root = n
			}
			stack = append(stack, n)
		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case xml.CharData:
			if len(stack) > 0 {
				stack[len(stack)-1].text.Write(t)
			}
		}
	}
	if root == nil {
		return nil, errors.New("soap: empty XML response")
	}
	return root, nil
}

// findLocal returns the first element (depth-first, self included) with the
// given local name, or nil.
func findLocal(n *xmlNode, local string) *xmlNode {
	if n.name == local {
		return n
	}
	for _, c := range n.children {
		if r := findLocal(c, local); r != nil {
			return r
		}
	}
	return nil
}

// directChild returns the first direct child with the given local name, or nil.
func directChild(n *xmlNode, local string) *xmlNode {
	for _, c := range n.children {
		if c.name == local {
			return c
		}
	}
	return nil
}

// textOfChild returns the trimmed text of the first direct child with the given
// local name, or "".
func textOfChild(n *xmlNode, local string) string {
	if c := directChild(n, local); c != nil {
		return strings.TrimSpace(c.text.String())
	}
	return ""
}

// soapFault turns a <Fault> element into an error, reading both SOAP 1.1
// (faultcode/faultstring) and SOAP 1.2 (Code/Value, Reason/Text) shapes.
func soapFault(fault *xmlNode) error {
	code := textOfChild(fault, "faultcode")
	reason := textOfChild(fault, "faultstring")
	if code == "" { // SOAP 1.2
		if c := directChild(fault, "Code"); c != nil {
			code = textOfChild(c, "Value")
		}
	}
	if reason == "" { // SOAP 1.2
		if r := directChild(fault, "Reason"); r != nil {
			reason = textOfChild(r, "Text")
		}
	}
	switch {
	case code != "" && reason != "":
		return fmt.Errorf("soap: fault %s: %s", code, reason)
	case reason != "":
		return fmt.Errorf("soap: fault: %s", reason)
	case code != "":
		return fmt.Errorf("soap: fault %s", code)
	default:
		return errors.New("soap: fault (no code or reason)")
	}
}

// build renders one element as a record.Value into the builder, producing
// exactly one top-level value (so it can be Finish()ed). The mapping is the
// generic, lossless XML→record convention (seed for M1.5):
//
//   - A leaf element (no attributes, no child elements) becomes its trimmed
//     text as a string value: <Price>42</Price> → "42".
//   - Otherwise it becomes a map: attributes under "@name" keys, non-empty
//     trimmed text under "#text", child elements keyed by their local name
//     (in first-seen order). Repeated child names collapse into a list.
func (n *xmlNode) build(bld *record.Builder) {
	if len(n.attrs) == 0 && len(n.children) == 0 {
		bld.StringLiteral(strings.TrimSpace(n.text.String()))
		return
	}
	bld.BeginMap()
	for _, a := range n.attrs {
		bld.KeyLiteral("@" + a.name)
		bld.StringLiteral(a.value)
	}
	if txt := strings.TrimSpace(n.text.String()); txt != "" {
		bld.KeyLiteral("#text")
		bld.StringLiteral(txt)
	}
	// Group children by local name, preserving first-seen order; a name seen
	// more than once becomes a list.
	order := make([]string, 0, len(n.children))
	groups := make(map[string][]*xmlNode, len(n.children))
	for _, c := range n.children {
		if _, seen := groups[c.name]; !seen {
			order = append(order, c.name)
		}
		groups[c.name] = append(groups[c.name], c)
	}
	for _, name := range order {
		nodes := groups[name]
		bld.KeyLiteral(name)
		if len(nodes) == 1 {
			nodes[0].build(bld)
			continue
		}
		bld.BeginList()
		for _, c := range nodes {
			c.build(bld)
		}
		bld.EndList()
	}
	bld.EndMap()
}
