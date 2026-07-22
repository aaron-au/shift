// Command shift-connector-sftp is the SFTP connector binary: a SHIFT runner
// spawns it as a gRPC subprocess. `shift-connector-sftp describe` prints its
// canonical descriptor (publisher tooling).
package main

import (
	"fmt"
	"os"

	"github.com/aaron-au/shift/connectors/internal/sftpconn"
	"github.com/aaron-au/shift/sdk"
)

func main() {
	if err := sdk.Serve(sftpconn.Connector()); err != nil {
		fmt.Fprintln(os.Stderr, "shift-connector-sftp:", err)
		os.Exit(1)
	}
}
