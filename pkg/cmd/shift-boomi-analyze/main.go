// shift-boomi-analyze reports how much of a Boomi export SHIFT can express
// today (ADR-0032, read-only half: parse, classify, report — no translation).
//
//	shift-boomi-analyze [-json] [-v] <export-dir>
//
// It reads an on-disk export and needs no Boomi credentials and no network, so
// it can run wherever the customer's designs are allowed to live.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/aaron-au/shift/pkg/boomi"
)

func main() {
	var (
		asJSON  = flag.Bool("json", false, "emit the report as JSON")
		verbose = flag.Bool("v", false, "include per-process detail")
	)
	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(),
			"usage: shift-boomi-analyze [-json] [-v] <export-dir>\n\n"+
				"Reports how much of a Boomi component export SHIFT can express today:\n"+
				"shape coverage, what will not import, and which unbuilt features would\n"+
				"unblock the most work. Read-only; no credentials, no network.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	if err := run(flag.Arg(0), *asJSON, *verbose); err != nil {
		fmt.Fprintln(os.Stderr, "shift-boomi-analyze:", err)
		os.Exit(1)
	}
}

func run(dir string, asJSON, verbose bool) error {
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("expected an export directory")
	}

	ex, err := boomi.ParseExport(dir)
	if err != nil {
		return err
	}
	if len(ex.Components) == 0 {
		return fmt.Errorf("no Boomi components found under %s", dir)
	}

	rep := boomi.Analyze(ex)
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	return boomi.RenderText(os.Stdout, rep, verbose)
}
