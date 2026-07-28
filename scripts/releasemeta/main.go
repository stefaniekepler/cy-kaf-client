package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type metadata struct {
	Tag       string
	Version   string
	IsRelease bool
}

var (
	semanticTag     = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
	semanticVersion = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
	cargoVersion    = regexp.MustCompile(`^version\s*=\s*"([^"]+)"\s*$`)
)

func main() {
	var (
		tag          string
		tauriPath    string
		cargoPath    string
		githubOutput string
	)
	flag.StringVar(&tag, "tag", "", "optional release tag")
	flag.StringVar(
		&tauriPath,
		"tauri-config",
		"desktop/src-tauri/tauri.conf.json",
		"path to tauri.conf.json",
	)
	flag.StringVar(
		&cargoPath,
		"cargo-toml",
		"desktop/src-tauri/Cargo.toml",
		"path to Cargo.toml",
	)
	flag.StringVar(&githubOutput, "github-output", "", "optional GitHub Actions output file")
	flag.Parse()

	got, err := resolve(tag, tauriPath, cargoPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if githubOutput != "" {
		if err := writeGitHubOutput(githubOutput, got); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	fmt.Printf(
		"tag=%s\nversion=%s\nis_release=%s\n",
		got.Tag,
		got.Version,
		strconv.FormatBool(got.IsRelease),
	)
}

func resolve(tag, tauriPath, cargoPath string) (metadata, error) {
	tauriVersion, err := readTauriVersion(tauriPath)
	if err != nil {
		return metadata{}, err
	}
	cargoPackageVersion, err := readCargoPackageVersion(cargoPath)
	if err != nil {
		return metadata{}, err
	}
	if tauriVersion != cargoPackageVersion {
		return metadata{}, fmt.Errorf(
			"version mismatch: Tauri=%q Cargo=%q",
			tauriVersion,
			cargoPackageVersion,
		)
	}
	if !semanticVersion.MatchString(tauriVersion) {
		return metadata{}, fmt.Errorf(
			"application version %q must match X.Y.Z",
			tauriVersion,
		)
	}
	got := metadata{Version: tauriVersion}
	if tag == "" {
		return got, nil
	}
	match := semanticTag.FindStringSubmatch(tag)
	if match == nil {
		return metadata{}, fmt.Errorf("tag must match vX.Y.Z: %q", tag)
	}
	if tagVersion := strings.TrimPrefix(tag, "v"); tagVersion != tauriVersion {
		return metadata{}, fmt.Errorf(
			"tag version %q does not match application version %q",
			tagVersion,
			tauriVersion,
		)
	}

	got.Tag = tag
	got.IsRelease = true
	return got, nil
}

func readTauriVersion(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read Tauri config: %w", err)
	}
	var config struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(content, &config); err != nil {
		return "", fmt.Errorf("parse Tauri config: %w", err)
	}
	if config.Version == "" {
		return "", errors.New("tauri version is empty")
	}
	return config.Version, nil
}

func readCargoPackageVersion(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("read Cargo manifest: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	scanner := bufio.NewScanner(file)
	inPackage := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			if inPackage {
				break
			}
			inPackage = line == "[package]"
			continue
		}
		if !inPackage {
			continue
		}
		if match := cargoVersion.FindStringSubmatch(line); match != nil {
			return match[1], nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan Cargo manifest: %w", err)
	}
	return "", errors.New("cargo package version is missing")
}

func writeGitHubOutput(path string, got metadata) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open GitHub output: %w", err)
	}
	_, err = fmt.Fprintf(
		file,
		"tag=%s\nversion=%s\nis_release=%s\n",
		got.Tag,
		got.Version,
		strconv.FormatBool(got.IsRelease),
	)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("write GitHub output: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close GitHub output: %w", err)
	}
	return nil
}
