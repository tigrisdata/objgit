package objgit

import "runtime/debug"

func init() {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	Version = bi.Main.Version
}

var Version = "(devel)"
