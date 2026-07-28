// Package version exposes build metadata injected via -ldflags.
package version

var (
	version   = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

type BuildInfo struct {
	Version   string
	Commit    string
	BuildTime string
}

func Info() BuildInfo {
	return BuildInfo{Version: version, Commit: commit, BuildTime: buildTime}
}
