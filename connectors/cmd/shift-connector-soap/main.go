// Command shift-connector-soap is the SOAP/XML connector binary: a SHIFT
// runner spawns it as a gRPC subprocess. `shift-connector-soap describe`
// prints its canonical descriptor (publisher tooling).
package main

import (
	"fmt"
	"os"

	"github.com/aaron-au/shift/connectors/internal/soapconn"
	"github.com/aaron-au/shift/sdk"
)

func main() {
	if err := sdk.Serve(soapconn.Connector()); err != nil {
		fmt.Fprintln(os.Stderr, "shift-connector-soap:", err)
		os.Exit(1)
	}
}
