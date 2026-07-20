package service

import "os"

// coverageRun reports whether we're in the deterministic coverage pass
// (scripts/coverage.sh sets SHIFT_COVERAGE=1). The connector-subprocess tests
// skip there: their timing-dependent execution makes the exact lines they
// cover vary run to run, and flaky coverage can't back a hard gate (ADR-0022).
// They still run for correctness in `make test` (full -race, no SHIFT_COVERAGE).
func coverageRun() bool { return os.Getenv("SHIFT_COVERAGE") != "" }
