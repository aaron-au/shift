// Command shift-connector-gen is the synthetic test/benchmark connector.
// It is spawned by a SHIFT runner (see sdk.Serve).
package main

import (
	"fmt"
	"os"

	"github.com/aaron-au/shift/connectors/internal/genconn"
	"github.com/aaron-au/shift/sdk"
)

func main() {
	if err := sdk.Serve(genconn.Connector()); err != nil {
		fmt.Fprintln(os.Stderr, "shift-connector-gen:", err)
		os.Exit(1)
	}
}
