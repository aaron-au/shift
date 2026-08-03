// Command shift-connector-azureblob is the Azure Blob Storage connector binary:
// a SHIFT runner spawns it as a gRPC subprocess. `shift-connector-azureblob
// describe` prints its canonical descriptor (publisher tooling).
package main

import (
	"fmt"
	"os"

	"github.com/aaron-au/shift/connectors/internal/azureblobconn"
	"github.com/aaron-au/shift/sdk"
)

func main() {
	if err := sdk.Serve(azureblobconn.Connector()); err != nil {
		fmt.Fprintln(os.Stderr, "shift-connector-azureblob:", err)
		os.Exit(1)
	}
}
