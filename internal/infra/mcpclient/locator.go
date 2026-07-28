package mcpclient

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

const (
	maxEnvironmentEntries = 512
	maxPATHEntries        = 256
	maxNVMVersions        = 128
)

type platformDirectorySource func(locator) []string

type locator struct {
	env            map[string]string
	userHome       string
	goos           string
	platformSource platformDirectorySource
}

func NewLocator(env []string, userHome string, goos string) Locator {
	return newLocator(env, userHome, goos, productionPlatformDirectories)
}

func NewLocatorWithSnapshot(
	environment EnvironmentSnapshot,
	userHome string,
) Locator {
	if environment == nil {
		environment = newEnvironmentSnapshot(nil, runtime.GOOS)
	}
	return newLocator(
		environment.environmentEntries(),
		userHome,
		environment.environmentGOOS(),
		productionPlatformDirectories,
	)
}

func newLocator(
	env []string,
	userHome string,
	goos string,
	platformSource platformDirectorySource,
) Locator {
	values := make(map[string]string)
	for index, entry := range env {
		if index >= maxEnvironmentEntries {
			break
		}
		key, value, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			continue
		}
		if goos == "windows" {
			key = strings.ToUpper(key)
		}
		values[key] = value
	}
	return locator{
		env:            values,
		userHome:       userHome,
		goos:           goos,
		platformSource: platformSource,
	}
}

func (current locator) Find(client Client) (string, error) {
	executable, _, err := current.FindWithLaunchDirectory(client)
	return executable, err
}

func (current locator) FindWithLaunchDirectory(
	client Client,
) (string, string, error) {
	name, ok := clientBasename(client)
	if !ok {
		return "", "", fmt.Errorf("%w", ErrClientNotFound)
	}

	pathDirs, pathComplete := splitPathBounded(current.value("PATH"), current.goos)
	if pathComplete &&
		current.goos == runtime.GOOS &&
		current.value("PATH") == os.Getenv("PATH") {
		if candidate, err := exec.LookPath(name); err == nil {
			if canonical, validateErr := validateCandidate(client, candidate, current.goos); validateErr == nil {
				return canonical, filepath.Dir(candidate), nil
			}
		}
	}
	if canonical, directory, ok := current.findInDirectories(client, pathDirs); ok {
		return canonical, directory, nil
	}

	platformSource := current.platformSource
	if platformSource == nil {
		platformSource = productionPlatformDirectories
	}
	if canonical, directory, ok := current.findInDirectories(
		client,
		platformSource(current),
	); ok {
		return canonical, directory, nil
	}
	return "", "", fmt.Errorf("%w", ErrClientNotFound)
}

func (current locator) value(key string) string {
	if current.goos == "windows" {
		key = strings.ToUpper(key)
	}
	return current.env[key]
}

func (current locator) findInDirectories(
	client Client,
	directories []string,
) (string, string, bool) {
	seen := make(map[string]struct{}, len(directories))
	for _, directory := range directories {
		if directory == "" || !filepath.IsAbs(directory) {
			continue
		}
		directory = filepath.Clean(directory)
		if _, duplicate := seen[directory]; duplicate {
			continue
		}
		seen[directory] = struct{}{}

		for _, name := range candidateBasenames(client, current.goos) {
			canonical, err := validateCandidate(client, filepath.Join(directory, name), current.goos)
			if err == nil {
				return canonical, directory, true
			}
		}
	}
	return "", "", false
}

func (current locator) platformDirectories() []string {
	directories := make([]string, 0, 8+maxNVMVersions)
	if value := current.value("NVM_BIN"); value != "" {
		directories = append(directories, value)
	}
	if value := current.value("VOLTA_HOME"); value != "" {
		directories = append(directories, filepath.Join(value, "bin"))
	}
	if value := current.value("PNPM_HOME"); value != "" {
		directories = append(directories, value)
	}
	if value := current.value("BUN_INSTALL"); value != "" {
		directories = append(directories, filepath.Join(value, "bin"))
	}

	if current.goos == "windows" {
		if value := current.value("LOCALAPPDATA"); value != "" {
			directories = append(directories, filepath.Join(value, "Programs"))
		}
		if value := current.value("APPDATA"); value != "" {
			directories = append(directories, filepath.Join(value, "npm"))
		}
		if value := current.value("USERPROFILE"); value != "" {
			directories = append(directories, filepath.Join(value, ".local", "bin"))
		}
		if current.userHome != "" {
			directories = append(directories, filepath.Join(current.userHome, ".local", "bin"))
		}
		return directories
	}

	if current.userHome != "" {
		directories = append(
			directories,
			filepath.Join(current.userHome, ".local", "bin"),
			filepath.Join(current.userHome, "bin"),
		)
	}
	directories = append(directories, "/usr/local/bin", "/opt/homebrew/bin")
	if current.userHome != "" {
		directories = append(directories, boundedNVMDirectories(current.userHome)...)
	}
	return directories
}

func productionPlatformDirectories(current locator) []string {
	return current.platformDirectories()
}

func splitPathBounded(pathValue, goos string) ([]string, bool) {
	var separator byte
	if goos == "windows" {
		separator = ';'
	} else {
		separator = ':'
	}

	directories := make([]string, 0, maxPATHEntries)
	remaining := pathValue
	for range maxPATHEntries {
		next := strings.IndexByte(remaining, separator)
		if next < 0 {
			if remaining != "" {
				directories = append(directories, remaining)
			}
			return directories, true
		}
		if next > 0 {
			directories = append(directories, remaining[:next])
		}
		remaining = remaining[next+1:]
	}
	return directories, remaining == ""
}

func boundedNVMDirectories(userHome string) []string {
	root := filepath.Join(userHome, ".nvm", "versions", "node")
	directory, err := os.Open(root)
	if err != nil {
		return nil
	}
	defer func() { _ = directory.Close() }()

	entries, err := directory.ReadDir(maxNVMVersions)
	if err != nil && err != io.EOF {
		return nil
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].Name() < entries[right].Name()
	})

	directories := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			directories = append(directories, filepath.Join(root, entry.Name(), "bin"))
		}
	}
	return directories
}

func validateCandidate(client Client, candidate string, goos string) (string, error) {
	if !filepath.IsAbs(candidate) || !validCandidateBasename(client, filepath.Base(candidate), goos) {
		return "", ErrClientNotFound
	}
	canonical, err := canonicalExecutable(candidate, goos)
	if err != nil {
		return "", ErrClientNotFound
	}
	return canonical, nil
}

func canonicalExecutable(path, goos string) (string, error) {
	if path == "" || !filepath.IsAbs(path) {
		return "", ErrInvalidExecutable
	}
	canonical, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return "", ErrInvalidExecutable
	}
	canonical, err = filepath.Abs(canonical)
	if err != nil {
		return "", ErrInvalidExecutable
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.Mode().IsRegular() {
		return "", ErrInvalidExecutable
	}
	if goos != "windows" && info.Mode().Perm()&0o111 == 0 {
		return "", ErrInvalidExecutable
	}
	return filepath.Clean(canonical), nil
}

func clientBasename(client Client) (string, bool) {
	switch client {
	case ClientCodex:
		return "codex", true
	case ClientClaudeCode:
		return "claude", true
	default:
		return "", false
	}
}

func candidateBasenames(client Client, goos string) []string {
	base, ok := clientBasename(client)
	if !ok {
		return nil
	}
	if goos != "windows" {
		return []string{base}
	}
	return []string{base + ".exe", base + ".com"}
}

func validCandidateBasename(client Client, basename, goos string) bool {
	for _, allowed := range candidateBasenames(client, goos) {
		if basename == allowed || goos == "windows" && strings.EqualFold(basename, allowed) {
			return true
		}
	}
	return false
}
