// Command hubd is the SHIFT hub: the HA control plane owning identity,
// flow versions, the connector registry, and the durable task queue.
// See PLAN.md M4 and docs/adr/0002.
package main

import (
	"fmt"
	"os"
)

// version is stamped via -ldflags at release build time.
var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Println("hubd", version)
		return
	}
	fmt.Fprintln(os.Stderr, "hubd: not implemented yet — scaffold only (see PLAN.md M4)")
	os.Exit(1)
}
