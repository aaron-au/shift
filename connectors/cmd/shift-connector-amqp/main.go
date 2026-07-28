// Command shift-connector-amqp is the AMQP 0-9-1 (RabbitMQ) connector binary:
// a SHIFT runner spawns it as a gRPC subprocess. `shift-connector-amqp
// describe` prints its canonical descriptor (publisher tooling).
package main

import (
	"fmt"
	"os"

	"github.com/aaron-au/shift/connectors/internal/amqpconn"
	"github.com/aaron-au/shift/sdk"
)

func main() {
	if err := sdk.Serve(amqpconn.Connector()); err != nil {
		fmt.Fprintln(os.Stderr, "shift-connector-amqp:", err)
		os.Exit(1)
	}
}
