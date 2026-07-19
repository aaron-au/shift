// Command shift-connector-http is the HTTP connector (streaming GET
// source, NDJSON POST sink). It is spawned by a SHIFT runner.
package main

import (
	"fmt"
	"os"

	"github.com/aaron-au/shift/connectors/internal/httpconn"
	"github.com/aaron-au/shift/sdk"
)

func main() {
	if err := sdk.Serve(httpconn.Connector()); err != nil {
		fmt.Fprintln(os.Stderr, "shift-connector-http:", err)
		os.Exit(1)
	}
}
