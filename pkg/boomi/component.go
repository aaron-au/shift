// Package boomi reads a Boomi component export and reports how much of it
// SHIFT can express today.
//
// It is the read-only half of the process-import adapter (ADR-0032): parse,
// classify, report — no translation. That ordering is deliberate. A migration
// story is only credible if it states up front what will NOT come across, and
// the fastest way to earn that credibility is to measure real customer designs
// before writing a single line of conversion.
//
// The analyzer never phones home and never needs Boomi API credentials: it
// works on an on-disk export, so it can be run against customer designs
// wherever those designs are allowed to live.
package boomi

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Component is one exported Boomi component (a process, map, profile,
// connection, …). Every export file holds exactly one.
type Component struct {
	ID       string
	Name     string
	Type     string // Boomi component type: "process", "transform.map", …
	Version  int
	Folder   string
	Deleted  bool
	Current  bool
	Modified string

	// Shapes is the process canvas, empty for non-process components.
	Shapes []Shape

	// EncryptedValues reports whether the component carries values Boomi
	// encrypted at export (passwords, OAuth tokens, private certificates).
	// These can never be imported — the ciphertext is bound to the source
	// account — so they are counted and reported, not silently dropped.
	EncryptedValues int

	// File is the path this component was read from (for report locations).
	File string
}

// Shape is one node on a Boomi process canvas.
type Shape struct {
	Name  string // canvas-unique id, e.g. "shape16"
	Type  string // shapetype: "decision", "branch", "map", …
	Label string // author's userlabel, when set
	To    []string
}

// Display returns the author's label when there is one, else the canvas id.
// Reports quote this, so an operator recognizes the shape in the Boomi UI.
func (s Shape) Display() string {
	if s.Label != "" {
		return s.Label
	}
	return s.Name
}

// xmlComponent mirrors the export's envelope. Boomi wraps every component in
// bns:Component with the vendor payload under bns:object; only the fields the
// analyzer reasons about are modeled — the rest is deliberately ignored so a
// schema addition upstream cannot break parsing.
type xmlComponent struct {
	ID       string `xml:"componentId,attr"`
	Name     string `xml:"name,attr"`
	Type     string `xml:"type,attr"`
	Version  int    `xml:"version,attr"`
	Folder   string `xml:"folderFullPath,attr"`
	Deleted  bool   `xml:"deleted,attr"`
	Current  bool   `xml:"currentVersion,attr"`
	Modified string `xml:"modifiedDate,attr"`

	Encrypted struct {
		Values []struct{} `xml:"encryptedValue"`
	} `xml:"encryptedValues"`

	Object struct {
		Process struct {
			Shapes []xmlShape `xml:"shapes>shape"`
		} `xml:"process"`
	} `xml:"object"`
}

type xmlShape struct {
	Name       string `xml:"name,attr"`
	Type       string `xml:"shapetype,attr"`
	Label      string `xml:"userlabel,attr"`
	Dragpoints []struct {
		To string `xml:"toShape,attr"`
	} `xml:"dragpoints>dragpoint"`
}

// ParseComponent reads one exported component.
func ParseComponent(r io.Reader) (*Component, error) {
	var xc xmlComponent
	dec := xml.NewDecoder(r)
	// Boomi exports are UTF-8 but occasionally carry stray entities from
	// author-entered text; tolerate them rather than failing a whole export.
	dec.Strict = false
	if err := dec.Decode(&xc); err != nil {
		return nil, fmt.Errorf("boomi: parse component: %w", err)
	}
	if xc.ID == "" && xc.Name == "" {
		return nil, errors.New("boomi: not a component export (no componentId or name)")
	}

	c := &Component{
		ID: xc.ID, Name: xc.Name, Type: xc.Type, Version: xc.Version,
		Folder: xc.Folder, Deleted: xc.Deleted, Current: xc.Current,
		Modified: xc.Modified, EncryptedValues: len(xc.Encrypted.Values),
	}
	for _, s := range xc.Object.Process.Shapes {
		sh := Shape{Name: s.Name, Type: s.Type, Label: s.Label}
		for _, d := range s.Dragpoints {
			if d.To != "" {
				sh.To = append(sh.To, d.To)
			}
		}
		c.Shapes = append(c.Shapes, sh)
	}
	return c, nil
}

// Export is a parsed directory of Boomi components.
type Export struct {
	Root       string
	Components []*Component
	// Skipped records files that could not be parsed, so a report can say so
	// instead of quietly analyzing a subset.
	Skipped []SkippedFile
}

// SkippedFile is one file the walk could not parse.
type SkippedFile struct {
	File   string
	Reason string
}

// skipDirs are export-tooling directories that hold sync bookkeeping and
// archives rather than live component definitions. Counting them would inflate
// every number in the report.
var skipDirs = map[string]bool{
	".sync-state": true,
	".archive":    true,
	".git":        true,
}

// ParseExport walks a Boomi export directory and parses every component in it.
//
// A file that fails to parse is recorded in Skipped rather than aborting the
// walk: a partial export should still yield a report, and the report says how
// much it could not read.
func ParseExport(root string) (*Export, error) {
	ex := &Export{Root: root}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.EqualFold(filepath.Ext(path), ".xml") {
			return nil
		}
		f, err := os.Open(path) //nolint:gosec // G304: analyzing an operator-supplied export directory is this tool's purpose
		if err != nil {
			ex.Skipped = append(ex.Skipped, SkippedFile{File: path, Reason: err.Error()})
			return nil //nolint:nilerr // recorded in Skipped; one unreadable file must not abort the export walk
		}
		defer func() { _ = f.Close() }()

		c, err := ParseComponent(f)
		if err != nil {
			ex.Skipped = append(ex.Skipped, SkippedFile{File: path, Reason: err.Error()})
			return nil //nolint:nilerr // recorded in Skipped; see above
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			rel = path
		}
		c.File = rel
		ex.Components = append(ex.Components, c)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("boomi: walk %s: %w", root, err)
	}
	sort.Slice(ex.Components, func(i, j int) bool { return ex.Components[i].File < ex.Components[j].File })
	sort.Slice(ex.Skipped, func(i, j int) bool { return ex.Skipped[i].File < ex.Skipped[j].File })
	return ex, nil
}

// Processes returns the components that carry a canvas.
func (e *Export) Processes() []*Component {
	var out []*Component
	for _, c := range e.Components {
		if len(c.Shapes) > 0 {
			out = append(out, c)
		}
	}
	return out
}
