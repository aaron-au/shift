package starlarkop

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"

	"github.com/aaron-au/shift/engine/record"
)

// AllowEnv is the deployment opt-in for code steps (ADR-0052 §9).
//
// The sandbox bounds what a script can do; it does not answer who is allowed to
// run one. While the hub is open-access (real RBAC is issue #16), a flow author
// is anyone who can reach the studio, and a code step is CPU, memory and
// network position inside the perimeter. So a deployment says yes explicitly,
// and a runner without it refuses the step type by name.
const AllowEnv = "SHIFT_ALLOW_CODE_STEPS"

// EntryPoint is the function a script must define.
const EntryPoint = "transform"

// Defaults chosen to be generous for real transforms and still bounded. They
// are settings rather than constants because the right numbers come from real
// flows, not from taste (ADR-0052).
const (
	// DefaultFuel bounds execution steps per record. Deterministic, unlike a
	// wall-clock timeout, so a script either always fits or never does.
	DefaultFuel = 200_000
	// DefaultMaxScriptBytes bounds the script itself.
	DefaultMaxScriptBytes = 256 << 10
	// DefaultMaxOutputFields bounds one returned record, at every level.
	DefaultMaxOutputFields = 1_000
	// DefaultMaxOutputDepth bounds nesting in a returned record.
	DefaultMaxOutputDepth = 32
)

// ErrNotAllowed is returned when a deployment has not opted in.
var ErrNotAllowed = fmt.Errorf("starlark: code steps are disabled on this runner; "+
	"set %s=1 to enable them (ADR-0052)", AllowEnv)

// Options configure one starlark step.
type Options struct {
	// Script is the source. Required.
	Script string
	// StepID names the step in errors and telemetry.
	StepID string
	// Fuel overrides DefaultFuel.
	Fuel uint64
	// MaxOutputFields / MaxOutputDepth override the output bounds.
	MaxOutputFields int
	MaxOutputDepth  int
	// Deadline bounds one record's evaluation in wall-clock time — the
	// backstop for what fuel cannot see, since fuel counts steps and a single
	// step can allocate a great deal (see isolate.go). 0 takes
	// DefaultDeadline.
	Deadline time.Duration
	// Allowed overrides the environment opt-in. Nil consults AllowEnv; a
	// non-nil value is used as given, which is how tests exercise both sides
	// of the gate without setting process state.
	Allowed *bool
}

// Allowed reports whether this runner permits code steps.
func Allowed() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(AllowEnv))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// Program is a compiled script, reusable across records and batches.
type Program struct {
	fn       *starlark.Function
	opts     Options
	fuel     uint64
	fields   int
	depth    int
	deadline time.Duration
}

// Compile parses and executes the script once, capturing its `transform`
// function. Executing at compile time is deliberate: a script's top level runs
// exactly once, under the same restrictions, so a module-level side effect
// cannot be smuggled in per record.
func Compile(opts Options) (*Program, error) {
	allowed := Allowed()
	if opts.Allowed != nil {
		allowed = *opts.Allowed
	}
	if !allowed {
		return nil, ErrNotAllowed
	}
	if strings.TrimSpace(opts.Script) == "" {
		return nil, errors.New("starlark: script is empty")
	}
	if len(opts.Script) > DefaultMaxScriptBytes {
		return nil, fmt.Errorf("starlark: script is %d bytes, limit is %d",
			len(opts.Script), DefaultMaxScriptBytes)
	}
	p := &Program{
		opts:     opts,
		fuel:     or(opts.Fuel, DefaultFuel),
		fields:   orInt(opts.MaxOutputFields, DefaultMaxOutputFields),
		depth:    orInt(opts.MaxOutputDepth, DefaultMaxOutputDepth),
		deadline: opts.Deadline,
	}

	thread := p.newThread()
	fileOpts := &syntax.FileOptions{
		// Recursion stays off: bounded loops are fuel-metered, but a recursive
		// function can exhaust the host stack before fuel notices.
		Recursion: false,
		// While loops are allowed; fuel is what bounds them.
		While:           true,
		TopLevelControl: true,
		GlobalReassign:  false,
		Set:             true,
	}
	globals, err := starlark.ExecFileOptions(fileOpts, thread, "flow.star", opts.Script, predeclared())
	if err != nil {
		return nil, fmt.Errorf("starlark: %w", cleanError(err))
	}
	// Freeze so no record can mutate state visible to the next one: without
	// this, a script is a channel between records and results depend on batch
	// boundaries.
	globals.Freeze()

	entry, ok := globals[EntryPoint]
	if !ok {
		return nil, fmt.Errorf("starlark: script must define %s(rec)", EntryPoint)
	}
	fn, ok := entry.(*starlark.Function)
	if !ok {
		return nil, fmt.Errorf("starlark: %s must be a function, got %s", EntryPoint, entry.Type())
	}
	if fn.NumParams() != 1 {
		return nil, fmt.Errorf("starlark: %s takes exactly one argument (the record), got %d",
			EntryPoint, fn.NumParams())
	}
	p.fn = fn
	return p, nil
}

// newThread builds a thread with no I/O and a fuel budget.
func (p *Program) newThread() *starlark.Thread {
	th := &starlark.Thread{
		Name: "starlark:" + p.opts.StepID,
		// load() is disabled by returning an error rather than a loader: with
		// no module loading there is nothing to import, pin, vendor or audit,
		// so tier 1 has no supply chain at all (ADR-0052 §6).
		Load: func(*starlark.Thread, string) (starlark.StringDict, error) {
			return nil, errors.New("load() is not available: a flow script has no modules to import")
		},
		// print() would be an unbounded, unredacted channel from payload to
		// the runner's logs. Scripts report through their return value.
		Print: func(*starlark.Thread, string) {},
	}
	th.SetMaxExecutionSteps(p.fuel)
	return th
}

// predeclared is the script's whole universe beyond Starlark's own builtins.
//
// Deliberately tiny. Nothing here reads the clock, the environment, the
// filesystem or the network, so a script is a pure function of its record —
// which is what makes a retry produce the same answer (ADR-0002 is
// at-least-once, so a non-deterministic transform is a correctness bug).
func predeclared() starlark.StringDict {
	return starlark.StringDict{
		"decimal": starlark.NewBuiltin("decimal", parseDecimalBuiltin),
	}
}

// parseDecimalBuiltin lets a script construct an exact decimal from text, so a
// literal amount in a script is exact too.
func parseDecimalBuiltin(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kw []starlark.Tuple) (starlark.Value, error) {
	var s string
	if err := starlark.UnpackPositionalArgs(b.Name(), args, kw, 1, &s); err != nil {
		return nil, err
	}
	v, err := record.ParseDecimal([]byte(s))
	if err != nil {
		return nil, fmt.Errorf("decimal(%q): %w", s, err)
	}
	return Decimal{v: v}, nil
}

// Run applies the script to one record, building the result into dst.
//
// keep is false when the script returned None, which drops the record — so a
// script is also a filter.
func (p *Program) Run(ctx context.Context, dst *record.Batch, rec record.Value) (out record.Value, keep bool, err error) {
	if err := ctx.Err(); err != nil {
		return record.Value{}, false, err
	}
	thread := p.newThread()
	// On its own goroutine, so an interpreter panic is contained and a script
	// the fuel budget cannot stop can be abandoned. See isolate.go for what
	// that does and — importantly — does not buy.
	res, err := p.callIsolated(ctx, thread, p.fn, wrap(rec))
	if err != nil {
		return record.Value{}, false, p.scriptError(err)
	}
	if res == starlark.None {
		return record.Value{}, false, nil
	}
	built, err := p.build(dst, res, 0)
	if err != nil {
		return record.Value{}, false, fmt.Errorf("starlark %s: %w", p.opts.StepID, err)
	}
	if built.Kind() != record.KindMap {
		return record.Value{}, false, fmt.Errorf("starlark %s: %s must return a record or None, got %s",
			p.opts.StepID, EntryPoint, res.Type())
	}
	return built, true, nil
}

// scriptError wraps a script failure, keeping fuel exhaustion distinguishable
// because it is the one an author can act on directly.
func (p *Program) scriptError(err error) error {
	// Fuel exhaustion is reported as a cancelled computation with no typed
	// error to match on, so the text is sniffed — and pinned by a test, so a
	// library wording change surfaces as a failure rather than as a fuel
	// error that quietly stops being recognisable.
	if isFuelExhausted(err) {
		return fmt.Errorf("starlark %s: script exceeded its budget of %d execution steps per record "+
			"(simplify it, or raise the step's fuel)", p.opts.StepID, p.fuel)
	}
	return fmt.Errorf("starlark %s: %w", p.opts.StepID, cleanError(err))
}

// isFuelExhausted reports whether err is the step-limit cancellation.
func isFuelExhausted(err error) bool {
	var eval *starlark.EvalError
	if !errors.As(err, &eval) {
		return false
	}
	return strings.Contains(eval.Msg, "too many steps")
}

// cleanError strips the backtrace from an EvalError.
//
// The backtrace quotes the values that caused the failure, and this error text
// travels to the hub in an execution report — which would make a debugging
// convenience into a payload leak (ADR-0052 §8). The detail belongs in the
// sampler, which is runner-only, bounded and redacted.
func cleanError(err error) error {
	var eval *starlark.EvalError
	if errors.As(err, &eval) {
		return errors.New(eval.Msg)
	}
	return err
}

func or(v, def uint64) uint64 {
	if v == 0 {
		return def
	}
	return v
}

func orInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}
