// Command shift-connector-s3 is the S3 connector binary: a SHIFT runner spawns
// it as a gRPC subprocess. `shift-connector-s3 describe` prints its canonical
// descriptor (publisher tooling).
package main

import (
	"fmt"
	"os"

	"github.com/aaron-au/shift/connectors/internal/s3conn"
	"github.com/aaron-au/shift/sdk"
)

func main() {
	if err := sdk.Serve(s3conn.Connector()); err != nil {
		fmt.Fprintln(os.Stderr, "shift-connector-s3:", err)
		os.Exit(1)
	}
}
