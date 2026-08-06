// Package shiftlog configures a SHIFT binary's operational logging
// (ADR-0046): structured records, on stdout, with one field vocabulary across
// the platform.
//
// It is stdlib-only and dependency-free. The gateway deliberately does NOT
// import it — its go.mod has zero dependencies, which is an auditable security
// property of the one component that may sit in a DMZ (ADR-0038 §3). The
// contract between the three binaries is therefore the OUTPUT SCHEMA, checked
// by a conformance test, not shared code.
package shiftlog

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strings"
)

// Field names, spelled one way platform-wide (ADR-0046 §3). Constants rather
// than string literals because two spellings of "runner" is the whole problem.
const (
	// KeyComponent is hub | runner | gateway. On every record.
	KeyComponent = "component"
	// KeyVersion is the build version, so a mixed-version fleet is legible.
	KeyVersion = "version"
	// KeyEvent is the stable dotted name a dashboard keys on — "msg" is what a
	// human reads, and alerting on prose makes an undocumented API of it.
	KeyEvent = "event"

	KeyError      = "error"
	KeyRunner     = "runner"
	KeyFlow       = "flow"
	KeyTask       = "task"
	KeyRequest    = "request"
	KeyGateway    = "gateway"
	KeyConnector  = "connector"
	KeyAccount    = "account"
	KeyDurationMS = "duration_ms"
)

// Component names.
const (
	ComponentHub     = "hub"
	ComponentRunner  = "runner"
	ComponentGateway = "gateway"
)

// Options configure Setup.
type Options struct {
	// Component is hub | runner | gateway. Required.
	Component string
	// Version is the build version stamped on every record.
	Version string
	// Level is "debug" | "info" | "warn" | "error" (default info). An
	// unparseable value falls back to info rather than failing start-up: a
	// typo in a log level must not stop a runner from running.
	Level string
	// Format is "json" | "text" | "" (auto: text on a terminal, JSON
	// otherwise — see §4).
	Format string
	// Out overrides the destination. Defaults to stdout; tests use it.
	Out io.Writer
}

// Setup builds the logger, installs it as slog's default, and bridges the
// stdlib log package into it.
//
// It returns the logger for callers that prefer to hold one explicitly.
func Setup(opts Options) *slog.Logger {
	out := opts.Out
	if out == nil {
		// stdout, deliberately (ADR-0046 §1): the operator decides whether that
		// becomes a file, a pipe, or a collector. A process that writes its own
		// log files has taken that decision away and acquired state.
		out = os.Stdout
	}

	handlerOpts := &slog.HandlerOptions{Level: ParseLevel(opts.Level)}
	var h slog.Handler
	if textFormat(opts.Format, out) {
		h = slog.NewTextHandler(out, handlerOpts)
	} else {
		h = slog.NewJSONHandler(out, handlerOpts)
	}

	attrs := []slog.Attr{slog.String(KeyComponent, opts.Component)}
	if opts.Version != "" {
		attrs = append(attrs, slog.String(KeyVersion, opts.Version))
	}
	logger := slog.New(h.WithAttrs(attrs))
	slog.SetDefault(logger)

	// The stdlib bridge (ADR-0046 §5). Two things at once: existing log.Print
	// call sites become structured without a mass rewrite, and any third-party
	// library logging through the global logger stops writing prose into an
	// otherwise-JSON stream.
	log.SetOutput(bridge{logger})
	log.SetFlags(0) // slog stamps the time; the stdlib prefix would duplicate it

	return logger
}

// ParseLevel maps a level name, defaulting to info.
func ParseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(strings.TrimSpace(s))); err != nil {
		return slog.LevelInfo
	}
	return l
}

// textFormat decides between the two handlers.
//
// Auto-detection is not cleverness: the alternative is a default that is wrong
// for one of the two audiences — a container pipeline that wants JSON, or a
// developer running `make up` who does not.
func textFormat(format string, out io.Writer) bool {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "text":
		return true
	case "json":
		return false
	}
	return isTerminal(out)
}

// isTerminal reports whether out is a character device.
func isTerminal(out io.Writer) bool {
	f, ok := out.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// bridge re-emits stdlib log output as slog records.
type bridge struct{ l *slog.Logger }

func (b bridge) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	// Existing call sites are prefixed by hand ("runnerd: registering…"); the
	// component is already an attribute, so the prefix is noise in the message.
	// Only a single-word prefix is trimmed: "hubd: migrated" loses it, and
	// "something happened: and then more" keeps every word it was given.
	if before, rest, found := strings.Cut(msg, ": "); found && !strings.Contains(before, " ") {
		msg = rest
	}
	b.l.LogAttrs(context.Background(), slog.LevelInfo, msg)
	return len(p), nil
}

// Fatalf logs a start-up or shutdown failure at ERROR and exits 1.
//
// It exists because `log.Fatalf` through the stdlib bridge would emit at INFO:
// a process dying at info level is exactly what makes a log stream
// untrustworthy, since the one record you most need to find looks like every
// other one.
//
// The message also goes to STDERR, which is the one job stderr keeps: when the
// process is dying, the logger itself may be what failed, and the operator
// should still see why.
func Fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	slog.Error(msg, KeyEvent, EventFatal)
	_, _ = os.Stderr.WriteString(msg + "\n")
	os.Exit(1)
}

// EventFatal names the record a process writes on its way out.
const EventFatal = "process.fatal"
