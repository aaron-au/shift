// Command shift-connector-fs is the local/mounted filesystem connector binary:
// a SHIFT runner spawns it as a gRPC subprocess. `shift-connector-fs describe`
// prints its canonical descriptor (publisher tooling).
package main

import (
	"fmt"
	"os"

	"github.com/aaron-au/shift/connectors/internal/fsconn"
	"github.com/aaron-au/shift/sdk"
)

func main() {
	if err := sdk.Serve(fsconn.Connector()); err != nil {
		fmt.Fprintln(os.Stderr, "shift-connector-fs:", err)
		os.Exit(1)
	}
}
