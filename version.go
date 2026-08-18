package main

import (
	_ "embed"
	"strconv"
	"strings"
)

// versionRaw is embedded from the VERSION file at build time, so the version
// is defined in exactly one place. The self-updater reads it locally and
// compares it against the same file in the repository.
//
//go:embed VERSION
var versionRaw string

func appVersion() string {
	return strings.TrimSpace(versionRaw)
}

// parseVersion turns "1.2.3" into a comparable triple, ignoring a leading v.
func parseVersion(s string) [3]int {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "v")
	s = strings.TrimPrefix(s, "V")
	var out [3]int
	for i, p := range strings.SplitN(s, ".", 3) {
		if i >= 3 {
			break
		}
		n, _ := strconv.Atoi(strings.TrimSpace(p))
		out[i] = n
	}
	return out
}

// isNewer reports whether the remote version is newer than the local one.
func isNewer(remote, local string) bool {
	r, l := parseVersion(remote), parseVersion(local)
	for i := 0; i < 3; i++ {
		if r[i] != l[i] {
			return r[i] > l[i]
		}
	}
	return false
}
