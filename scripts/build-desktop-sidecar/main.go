package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const versionPackage = "github.com/cy-kaf/cy-kaf-client/internal/version"

type target struct {
	GOOS   string
	GOARCH string
	Ext    string
}

type options struct {
	Target    string
	OutputDir string
	Version   string
	Commit    string
	BuildTime string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	opts, err := parseOptions(args)
	if err != nil {
		return err
	}
	buildTarget, err := targetForTriple(opts.Target)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return fmt.Errorf("create sidecar output directory: %w", err)
	}

	output := filepath.Join(opts.OutputDir, "cy-kaf-client-"+opts.Target+buildTarget.Ext)
	ldflags := strings.Join([]string{
		"-s", "-w",
		"-X", versionPackage + ".version=" + opts.Version,
		"-X", versionPackage + ".commit=" + opts.Commit,
		"-X", versionPackage + ".buildTime=" + opts.BuildTime,
	}, " ")
	cmd := exec.Command(
		"go", "build", "-trimpath", "-ldflags", ldflags,
		"-o", output, "./cmd/cy-kaf-client",
	)
	cmd.Env = append(
		os.Environ(),
		"CGO_ENABLED=0",
		"GOOS="+buildTarget.GOOS,
		"GOARCH="+buildTarget.GOARCH,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build desktop sidecar for %s: %w", opts.Target, err)
	}
	return nil
}

func parseOptions(args []string) (options, error) {
	var opts options
	flags := flag.NewFlagSet("build-desktop-sidecar", flag.ContinueOnError)
	flags.StringVar(&opts.Target, "target", "", "Rust target triple")
	flags.StringVar(&opts.OutputDir, "output-dir", "", "Tauri externalBin directory")
	flags.StringVar(&opts.Version, "version", "", "application version")
	flags.StringVar(&opts.Commit, "commit", "", "source commit")
	flags.StringVar(&opts.BuildTime, "build-time", "", "RFC3339 build time")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments")
	}
	for name, value := range map[string]string{
		"target":     opts.Target,
		"output-dir": opts.OutputDir,
		"version":    opts.Version,
		"commit":     opts.Commit,
		"build-time": opts.BuildTime,
	} {
		if value == "" {
			return options{}, fmt.Errorf("-%s is required", name)
		}
	}
	if _, err := time.Parse(time.RFC3339, opts.BuildTime); err != nil {
		return options{}, fmt.Errorf("-build-time must use RFC3339")
	}
	return opts, nil
}

func targetForTriple(triple string) (target, error) {
	targets := map[string]target{
		"x86_64-apple-darwin":       {GOOS: "darwin", GOARCH: "amd64"},
		"aarch64-apple-darwin":      {GOOS: "darwin", GOARCH: "arm64"},
		"x86_64-pc-windows-msvc":    {GOOS: "windows", GOARCH: "amd64", Ext: ".exe"},
		"aarch64-pc-windows-msvc":   {GOOS: "windows", GOARCH: "arm64", Ext: ".exe"},
		"x86_64-unknown-linux-gnu":  {GOOS: "linux", GOARCH: "amd64"},
		"aarch64-unknown-linux-gnu": {GOOS: "linux", GOARCH: "arm64"},
	}
	buildTarget, ok := targets[triple]
	if !ok {
		return target{}, fmt.Errorf("unsupported Rust target %q", triple)
	}
	return buildTarget, nil
}
