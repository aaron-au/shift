// Command shift-connector-smtp is the SMTP connector binary: a SHIFT runner
// spawns it as a gRPC subprocess. `shift-connector-smtp describe` prints its
// canonical descriptor (publisher tooling).
package main

import (
	"fmt"
	"os"

	"github.com/aaron-au/shift/connectors/internal/smtpconn"
	"github.com/aaron-au/shift/sdk"
)

func main() {
	if err := sdk.Serve(smtpconn.Connector()); err != nil {
		fmt.Fprintln(os.Stderr, "shift-connector-smtp:", err)
		os.Exit(1)
	}
}
