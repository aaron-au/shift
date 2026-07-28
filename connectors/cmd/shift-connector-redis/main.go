// Command shift-connector-redis is the Redis connector binary: a SHIFT runner
// spawns it as a gRPC subprocess. `shift-connector-redis describe` prints its
// canonical descriptor (publisher tooling).
package main

import (
	"fmt"
	"os"

	"github.com/aaron-au/shift/connectors/internal/redisconn"
	"github.com/aaron-au/shift/sdk"
)

func main() {
	if err := sdk.Serve(redisconn.Connector()); err != nil {
		fmt.Fprintln(os.Stderr, "shift-connector-redis:", err)
		os.Exit(1)
	}
}
