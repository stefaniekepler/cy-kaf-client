package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	fixedRepository   = "stefaniekepler/cy-kaf-client"
	maxNotesBytes     = 16 * 1024
	maxSignatureBytes = 16 * 1024
)

var safeVersion = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

type options struct {
	Directory  string
	Version    string
	Notes      string
	PubDate    time.Time
	Repository string
	Tag        string
}

type platform struct {
	Signature string `json:"signature"`
	URL       string `json:"url"`
}

type manifest struct {
	Version   string              `json:"version"`
	Notes     string              `json:"notes"`
	PubDate   string              `json:"pub_date"`
	Platforms map[string]platform `json:"platforms"`
}

type artifactTarget struct {
	manifestKey string
	nameSuffix  string
}

func (target artifactTarget) filename(version string) string {
	return "Cy-KafClient_" + version + "_" + target.nameSuffix
}

var artifactTargets = []artifactTarget{
	{manifestKey: "darwin-x86_64", nameSuffix: "macos-x86_64.app.tar.gz"},
	{manifestKey: "darwin-aarch64", nameSuffix: "macos-aarch64.app.tar.gz"},
	{manifestKey: "windows-x86_64", nameSuffix: "windows-x86_64-setup.exe"},
	{manifestKey: "windows-aarch64", nameSuffix: "windows-aarch64-setup.exe"},
	{manifestKey: "linux-x86_64", nameSuffix: "linux-x86_64.AppImage"},
	{manifestKey: "linux-aarch64", nameSuffix: "linux-aarch64.AppImage"},
}

func main() {
	var directory, version, notes, pubDate, output, tag string
	flag.StringVar(&directory, "dir", "release-assets", "directory containing updater artifacts")
	flag.StringVar(&version, "version", "", "release version without v prefix")
	flag.StringVar(&notes, "notes", "", "bounded plain-text release notes")
	flag.StringVar(&pubDate, "pub-date", "", "RFC3339 publication time")
	flag.StringVar(&tag, "tag", "", "release tag")
	flag.StringVar(&output, "output", "latest.json", "manifest output path")
	flag.Parse()

	when, err := time.Parse(time.RFC3339, pubDate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid pub-date: %v\n", err)
		os.Exit(1)
	}
	content, err := generate(options{Directory: directory, Version: version, Notes: notes, PubDate: when, Repository: fixedRepository, Tag: tag})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(output, content, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write manifest: %v\n", err)
		os.Exit(1)
	}
}

func generate(opts options) ([]byte, error) {
	if !safeVersion.MatchString(opts.Version) {
		return nil, fmt.Errorf("version must match X.Y.Z: %q", opts.Version)
	}
	if opts.Tag != "v"+opts.Version {
		return nil, fmt.Errorf("tag %q does not match version %q", opts.Tag, opts.Version)
	}
	if opts.Repository != fixedRepository {
		return nil, fmt.Errorf("repository must be %s", fixedRepository)
	}
	if len(opts.Notes) > maxNotesBytes {
		return nil, fmt.Errorf("notes exceed %d bytes", maxNotesBytes)
	}
	if opts.PubDate.IsZero() {
		return nil, errors.New("pub-date is required")
	}

	entries, err := os.ReadDir(opts.Directory)
	if err != nil {
		return nil, fmt.Errorf("read artifact directory: %w", err)
	}
	allowed := make(map[string]bool)
	for _, target := range artifactTargets {
		name := target.filename(opts.Version)
		allowed[name] = true
		allowed[name+".sig"] = true
	}
	// DMGs remain published for manual installation but are not updater payloads.
	allowed["Cy-KafClient_"+opts.Version+"_macos-x86_64.dmg"] = true
	allowed["Cy-KafClient_"+opts.Version+"_macos-aarch64.dmg"] = true
	for _, entry := range entries {
		if entry.IsDir() || !allowed[entry.Name()] {
			return nil, fmt.Errorf("unexpected updater artifact: %q", entry.Name())
		}
	}

	result := manifest{Version: opts.Version, Notes: opts.Notes, PubDate: opts.PubDate.UTC().Format(time.RFC3339), Platforms: make(map[string]platform, len(artifactTargets))}
	base := "https://github.com/" + fixedRepository + "/releases/download/" + url.PathEscape(opts.Tag) + "/"
	for _, target := range artifactTargets {
		name := target.filename(opts.Version)
		packageInfo, err := os.Stat(filepath.Join(opts.Directory, name))
		if err != nil {
			return nil, fmt.Errorf("missing updater artifact %q: %w", name, err)
		}
		if !packageInfo.Mode().IsRegular() || packageInfo.Size() == 0 {
			return nil, fmt.Errorf("updater artifact %q is empty or not a regular file", name)
		}
		signatureBytes, err := os.ReadFile(filepath.Join(opts.Directory, name+".sig"))
		if err != nil {
			return nil, fmt.Errorf("missing signature for %q: %w", name, err)
		}
		if len(signatureBytes) > maxSignatureBytes {
			return nil, fmt.Errorf("signature for %q exceeds %d bytes", name, maxSignatureBytes)
		}
		signature := strings.TrimSpace(string(signatureBytes))
		if signature == "" {
			return nil, fmt.Errorf("signature for %q is empty", name)
		}
		result.Platforms[target.manifestKey] = platform{Signature: signature, URL: base + url.PathEscape(name)}
	}

	content, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode manifest: %w", err)
	}
	return append(content, '\n'), nil
}
