package shiftlog_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aaron-au/shift/pkg/shiftlog"
)

// The vocabulary gate (ADR-0046 §3).
//
// The schema tests next door check that the three binaries are SET UP the same
// way. This checks every individual log call, because a shared setup does not
// stop the next call site inventing `task_id`, omitting `event`, or logging a
// token — and a convention nothing verifies decays in about three months.
//
// It parses source rather than importing anything, so it covers the gateway
// too, which deliberately depends on nothing (§2).

// logMethods are the calls treated as log emissions.
var logMethods = map[string]bool{
	"Debug": true, "Info": true, "Warn": true, "Error": true,
	"DebugContext": true, "InfoContext": true, "WarnContext": true, "ErrorContext": true,
	"LogAttrs": true,
}

// attrConstructors are the slog.Attr helpers, whose FIRST argument is a key.
var attrConstructors = map[string]bool{
	"String": true, "Int": true, "Int64": true, "Uint64": true, "Float64": true,
	"Bool": true, "Time": true, "Duration": true, "Any": true, "Group": true,
}

// logCall is one emission found in the source.
type logCall struct {
	file  string
	line  int
	level string
	keys  []string
}

// scanLogCalls walks a directory tree and returns every log emission.
func scanLogCalls(tb testing.TB, root string) []logCall {
	tb.Helper()
	var out []logCall
	fset := token.NewFileSet()

	err := filepath.Walk(filepath.Join("..", "..", root), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			tb.Fatalf("parsing %s: %v", path, perr)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !logMethods[sel.Sel.Name] {
				return true
			}
			// A log emission's first (or second, for LogAttrs) argument is the
			// message string. Requiring a literal there is what separates
			// `log.Warn("thing", …)` from `err.Error()` or `t.Error(…)`.
			args := call.Args
			if sel.Sel.Name == "LogAttrs" && len(args) >= 3 {
				args = args[3:] // ctx, level, msg, attrs...
			} else if len(args) >= 1 {
				if _, isLit := args[0].(*ast.BasicLit); !isLit {
					return true
				}
				args = args[1:]
			}
			if !isLoggerReceiver(sel.X) {
				return true
			}
			out = append(out, logCall{
				file:  strings.TrimPrefix(path, "../../"),
				line:  fset.Position(call.Pos()).Line,
				level: sel.Sel.Name,
				keys:  extractKeys(args),
			})
			return true
		})
		return nil
	})
	if err != nil {
		tb.Fatalf("walking %s: %v", root, err)
	}
	return out
}

// isLoggerReceiver reports whether x looks like slog or a logger held on a
// struct — `slog.Info`, `l.log.Warn`, `h.logger.Error`, `log.Info`.
func isLoggerReceiver(x ast.Expr) bool {
	switch v := x.(type) {
	case *ast.Ident:
		n := strings.ToLower(v.Name)
		return n == "slog" || strings.Contains(n, "log")
	case *ast.SelectorExpr:
		return strings.Contains(strings.ToLower(v.Sel.Name), "log")
	}
	return false
}

// extractKeys pulls the key literals out of a call's arguments, handling both
// the alternating key/value form and the slog.String("k", v) attr form.
func extractKeys(args []ast.Expr) []string {
	var keys []string
	for i := 0; i < len(args); i++ {
		switch a := args[i].(type) {
		case *ast.BasicLit:
			if a.Kind == token.STRING {
				if s, err := strconv.Unquote(a.Value); err == nil {
					keys = append(keys, s)
				}
				i++ // the value that follows is not a key
			}
		case *ast.SelectorExpr:
			// shiftlog.KeyEvent and friends.
			if strings.HasPrefix(a.Sel.Name, "Key") {
				keys = append(keys, keyConstName(a.Sel.Name))
				i++
			}
		case *ast.CallExpr:
			// slog.String("event", …) — the key is the first argument.
			if sel, ok := a.Fun.(*ast.SelectorExpr); ok && attrConstructors[sel.Sel.Name] {
				if len(a.Args) > 0 {
					if lit, ok := a.Args[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						if s, err := strconv.Unquote(lit.Value); err == nil {
							keys = append(keys, s)
						}
					}
				}
			}
		}
	}
	return keys
}

// keyConstName maps shiftlog.KeyDurationMS → duration_ms.
func keyConstName(name string) string {
	switch name {
	case "KeyComponent":
		return shiftlog.KeyComponent
	case "KeyVersion":
		return shiftlog.KeyVersion
	case "KeyEvent":
		return shiftlog.KeyEvent
	case "KeyError":
		return shiftlog.KeyError
	case "KeyRunner":
		return shiftlog.KeyRunner
	case "KeyFlow":
		return shiftlog.KeyFlow
	case "KeyTask":
		return shiftlog.KeyTask
	case "KeyRequest":
		return shiftlog.KeyRequest
	case "KeyGateway":
		return shiftlog.KeyGateway
	case "KeyConnector":
		return shiftlog.KeyConnector
	case "KeyAccount":
		return shiftlog.KeyAccount
	case "KeyDurationMS":
		return shiftlog.KeyDurationMS
	}
	return strings.ToLower(strings.TrimPrefix(name, "Key"))
}

// components are the three long-running binaries. Connectors log nothing (they
// speak gRPC), and the CLI tools are one-shot — prose is right for those.
var components = []string{"hub/cmd/hubd", "hub/internal", "runner/cmd/runnerd",
	"runner/internal", "gateway/cmd", "gateway/internal"}

func allLogCalls(tb testing.TB) []logCall {
	tb.Helper()
	var all []logCall
	for _, c := range components {
		all = append(all, scanLogCalls(tb, c)...)
	}
	if len(all) < 20 {
		tb.Fatalf("only %d log calls found — the scanner is not matching, so this test proves nothing", len(all))
	}
	return all
}

// Every record must be selectable by a stable name. Without this, "alert on
// the event" is advice rather than a property.
func TestEveryLogCallCarriesAnEvent(t *testing.T) {
	for _, c := range allLogCalls(t) {
		hasEvent := false
		for _, k := range c.keys {
			if k == shiftlog.KeyEvent {
				hasEvent = true
				break
			}
		}
		if !hasEvent {
			t.Errorf("%s:%d: %s call has no %q key — it cannot be filtered or alerted on (ADR-0046 §3)",
				c.file, c.line, c.level, shiftlog.KeyEvent)
		}
	}
}

// Two spellings of one idea split every query that uses it, and the split is
// invisible until somebody's filter quietly returns half the records.
func TestLogKeysUseTheCanonicalSpelling(t *testing.T) {
	for _, c := range allLogCalls(t) {
		for _, k := range c.keys {
			if want, wrong := shiftlog.CanonicalKeys[k]; wrong {
				t.Errorf("%s:%d: key %q must be %q (ADR-0046 §3)", c.file, c.line, k, want)
			}
			if k != strings.ToLower(k) {
				t.Errorf("%s:%d: key %q must be lower_snake_case", c.file, c.line, k)
			}
		}
	}
}

// A record may IDENTIFY a credential; it may never carry one.
func TestNoLogKeyNamesACredentialOrPayload(t *testing.T) {
	for _, c := range allLogCalls(t) {
		for _, k := range c.keys {
			if shiftlog.KeyExempt[k] {
				continue
			}
			for _, bad := range shiftlog.ForbiddenKeys {
				if strings.Contains(strings.ToLower(k), bad) {
					t.Errorf("%s:%d: key %q looks like it carries a credential or payload (%q). "+
						"Identify it (cert_serial, a fingerprint) rather than reproducing it (ADR-0046 §7)",
						c.file, c.line, k, bad)
				}
			}
		}
	}
}

// Nothing may write to stdout except the logger: an operator piping stdout to
// a collector must get records, not a stray Println in the middle of them.
func TestNothingElseWritesToStdout(t *testing.T) {
	for _, dir := range []string{"hub/internal", "runner/internal", "gateway/internal"} {
		fset := token.NewFileSet()
		err := filepath.Walk(filepath.Join("..", "..", dir), func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			f, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				t.Fatalf("parsing %s: %v", path, perr)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				pkg, isIdent := sel.X.(*ast.Ident)
				if !isIdent || pkg.Name != "fmt" {
					return true
				}
				if strings.HasPrefix(sel.Sel.Name, "Print") {
					t.Errorf("%s:%d: fmt.%s writes outside the log stream",
						strings.TrimPrefix(path, "../../"), fset.Position(call.Pos()).Line, sel.Sel.Name)
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", dir, err)
		}
	}
}
