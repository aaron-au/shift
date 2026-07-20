package e2e

import "os"

// coverageRun reports whether we're in the deterministic coverage pass
// (scripts/coverage.sh sets SHIFT_COVERAGE=1). The e2e tests spawn real
// runnerd + connector subprocesses, so their timing-dependent execution makes
// the exact lines they cover (here and, via -coverpkg, in the hub packages)
// vary run to run — flaky coverage can't back a hard gate (ADR-0022). They
// still run for correctness in `make test` (full -race, no SHIFT_COVERAGE).
func coverageRun() bool { return os.Getenv("SHIFT_COVERAGE") != "" }
