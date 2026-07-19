// Command runnerd is the SHIFT runner: a stateless worker that leases tasks
// from a hub, streams data through connector subprocesses, and reports
// results. See PLAN.md M3 and docs/adr/0002, 0005.
package main

import (
	"fmt"
	"os"
)

// version is stamped via -ldflags at release build time.
var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Println("runnerd", version)
		return
	}
	fmt.Fprintln(os.Stderr, "runnerd: not implemented yet — scaffold only (see PLAN.md M3)")
	os.Exit(1)
}
