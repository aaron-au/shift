// Command shift-connector-db is the database connector binary (PostgreSQL): a
// SHIFT runner spawns it as a gRPC subprocess. `shift-connector-db describe`
// prints its canonical descriptor (publisher tooling).
package main

import (
	"fmt"
	"os"

	"github.com/aaron-au/shift/connectors/internal/dbconn"
	"github.com/aaron-au/shift/sdk"
)

func main() {
	if err := sdk.Serve(dbconn.Connector()); err != nil {
		fmt.Fprintln(os.Stderr, "shift-connector-db:", err)
		os.Exit(1)
	}
}
