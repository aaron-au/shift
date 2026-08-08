package redisconn_test

import (
	"testing"

	"github.com/aaron-au/shift/connectors/internal/redisconn"
	"github.com/aaron-au/shift/sdk/sdktest"
)

// The compatibility gate (ADR-0047 §8). testdata/surface.json records the
// action surface this connector last shipped; the gate diffs the current
// build against it and refuses a declared class the diff cannot support.
//
// If this fails, read the report before reaching for SHIFT_UPDATE_SURFACE=1:
// it is naming a change that would reach every flow using this connector.
func TestSurfaceStaysCompatible(t *testing.T) {
	sdktest.CheckSurface(t, redisconn.Connector(), "testdata/surface.json")
}
