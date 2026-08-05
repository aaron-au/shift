package service

import (
	"testing"

	"github.com/aaron-au/shift/sdk/host"
)

// A cursor is opaque bytes only its producer understands. Runners are
// replaceable by design, so the build that recorded a position is frequently
// not the build that resumes it — and reading a v0.3 cursor under v0.4 could
// resolve to a DIFFERENT position, resuming at the wrong place with nothing
// downstream able to notice.
func TestResumeAllowedOnlyForTheExactBuildThatProducedTheCursor(t *testing.T) {
	bound := host.Info{Name: "fs", Version: "0.2.0"}

	for name, tc := range map[string]struct {
		opts SubmitOpts
		want bool
	}{
		"same connector and version": {
			SubmitOpts{ResumeConnector: "fs", ResumeVersion: "0.2.0"}, true,
		},
		"different version": {
			SubmitOpts{ResumeConnector: "fs", ResumeVersion: "0.1.0"}, false,
		},
		"different connector": {
			SubmitOpts{ResumeConnector: "sftp", ResumeVersion: "0.2.0"}, false,
		},
		// A cursor recorded before this pinning existed cannot be shown to be
		// safe, so it is treated as untrusted rather than assumed compatible.
		"no recorded identity": {
			SubmitOpts{}, false,
		},
		"connector but no version": {
			SubmitOpts{ResumeConnector: "fs"}, false,
		},
	} {
		if got := resumeAllowed(tc.opts, bound); got != tc.want {
			t.Errorf("%s: resumeAllowed = %v, want %v", name, got, tc.want)
		}
	}
}
