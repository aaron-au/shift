// Package buildinfo exposes version metadata stamped at build time via
// -ldflags, shared by every SHIFT binary.
package buildinfo

import "runtime/debug"

// Version is the semantic version of the build, stamped via
// -ldflags "-X github.com/aaron-au/shift/pkg/buildinfo.Version=v0.1.0".
var Version = "dev"

// String returns "version (vcs-revision)" using whatever is available from
// the build environment.
func String() string {
	rev := "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && len(s.Value) >= 12 {
				rev = s.Value[:12]
			}
		}
	}
	return Version + " (" + rev + ")"
}
