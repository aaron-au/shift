// Command shift-connector-ftp is the FTP/FTPS connector binary: a SHIFT runner
// spawns it as a gRPC subprocess. `shift-connector-ftp describe` prints its
// canonical descriptor (publisher tooling).
package main

import (
	"fmt"
	"os"

	"github.com/aaron-au/shift/connectors/internal/ftpconn"
	"github.com/aaron-au/shift/sdk"
)

func main() {
	if err := sdk.Serve(ftpconn.Connector()); err != nil {
		fmt.Fprintln(os.Stderr, "shift-connector-ftp:", err)
		os.Exit(1)
	}
}
