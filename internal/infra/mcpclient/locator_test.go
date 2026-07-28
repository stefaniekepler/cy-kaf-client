package mcpclient

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocatorPATHWinsOverPlatformFallback(t *testing.T) {
	pathDir := t.TempDir()
	home := t.TempDir()
	pathExecutable := writeTestFile(t, filepath.Join(pathDir, "codex"), 0o755)
	platformDir := filepath.Join(home, ".local", "bin")
	writeTestFile(t, filepath.Join(platformDir, "codex"), 0o755)

	got, err := newTestLocator(
		[]string{"PATH=" + pathDir},
		home,
		runtime.GOOS,
		platformDir,
	).Find(ClientCodex)
	require.NoError(t, err)
	require.Equal(t, pathExecutable, got)
}

func TestLocatorWithSnapshotUsesSealedEnvironmentAndPlatform(t *testing.T) {
	pathDir := t.TempDir()
	executable := writeTestFile(t, filepath.Join(pathDir, "codex"), 0o755)
	entries := []string{"PATH=" + pathDir}
	snapshot := newEnvironmentSnapshot(entries, runtime.GOOS)
	entries[0] = "PATH=" + t.TempDir()

	got, err := NewLocatorWithSnapshot(snapshot, t.TempDir()).Find(ClientCodex)

	require.NoError(t, err)
	require.Equal(t, executable, got)
}

func TestLocatorUsesPlatformCandidateAsFallback(t *testing.T) {
	home := t.TempDir()
	fallback := writeTestFile(t, filepath.Join(home, ".local", "bin", "claude"), 0o755)

	got, err := newTestLocator(
		[]string{"PATH="},
		home,
		runtime.GOOS,
		filepath.Dir(fallback),
	).Find(ClientClaudeCode)
	require.NoError(t, err)
	require.Equal(t, fallback, got)
}

func TestLocatorResolvesExpectedBasenameSymlinkToExecutableTarget(t *testing.T) {
	pathDir := t.TempDir()
	target := writeTestFile(t, filepath.Join(t.TempDir(), "versioned-launcher"), 0o755)
	link := filepath.Join(pathDir, "codex")
	require.NoError(t, makeTestSymlink(target, link))

	got, err := newTestLocator(
		[]string{"PATH=" + pathDir},
		t.TempDir(),
		runtime.GOOS,
	).Find(ClientCodex)
	require.NoError(t, err)
	require.Equal(t, target, got)
}

func TestLocatorRejectsUnsafeCandidates(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix executable mode is not meaningful on Windows")
	}

	for name, arrange := range map[string]func(*testing.T, string){
		"dangling symlink": func(t *testing.T, path string) {
			require.NoError(t, makeTestSymlink(filepath.Join(t.TempDir(), "missing"), path))
		},
		"symlink to directory": func(t *testing.T, path string) {
			require.NoError(t, makeTestSymlink(t.TempDir(), path))
		},
		"directory": func(t *testing.T, path string) {
			require.NoError(t, os.MkdirAll(path, 0o755))
		},
		"non-executable regular file": func(t *testing.T, path string) {
			writeTestFile(t, path, 0o600)
		},
	} {
		t.Run(name, func(t *testing.T) {
			pathDir := t.TempDir()
			arrange(t, filepath.Join(pathDir, "codex"))

			_, err := newTestLocator(
				[]string{"PATH=" + pathDir},
				t.TempDir(),
				runtime.GOOS,
			).Find(ClientCodex)
			require.ErrorIs(t, err, ErrClientNotFound)
		})
	}
}

func TestLocatorRejectsWrongBasenameBeforeFollowingLink(t *testing.T) {
	target := writeTestFile(t, filepath.Join(t.TempDir(), "target"), 0o755)
	wrongName := filepath.Join(t.TempDir(), "not-codex")
	require.NoError(t, makeTestSymlink(target, wrongName))

	_, err := validateCandidate(ClientCodex, wrongName, runtime.GOOS)
	require.Error(t, err)
}

func TestLocatorFindsExplicitNodeManagerPlatformCandidate(t *testing.T) {
	home := t.TempDir()
	nvmBin := filepath.Join(home, ".nvm", "versions", "node", "v24.1.0", "bin")
	executable := writeTestFile(t, filepath.Join(nvmBin, "claude"), 0o755)

	got, err := newTestLocator(
		[]string{"PATH="},
		home,
		runtime.GOOS,
		nvmBin,
	).Find(ClientClaudeCode)
	require.NoError(t, err)
	require.Equal(t, executable, got)
}

func TestLocatorWindowsRejectsBatchOnlyLayout(t *testing.T) {
	appData := t.TempDir()
	directory := filepath.Join(appData, "npm")
	writeTestFile(t, filepath.Join(directory, "claude.cmd"), 0o600)
	writeTestFile(t, filepath.Join(directory, "claude.bat"), 0o600)

	_, err := newTestLocator(
		[]string{"PATH="},
		t.TempDir(),
		"windows",
		directory,
	).
		Find(ClientClaudeCode)
	require.ErrorIs(t, err, ErrClientNotFound)
}

func TestLocatorWindowsPrefersNativeLauncherOverBatchShim(t *testing.T) {
	directory := t.TempDir()
	writeTestFile(t, filepath.Join(directory, "codex.cmd"), 0o600)
	executable := writeTestFile(t, filepath.Join(directory, "codex.exe"), 0o600)

	got, err := newTestLocator(
		[]string{"PATH="},
		t.TempDir(),
		"windows",
		directory,
	).Find(ClientCodex)
	require.NoError(t, err)
	require.Equal(t, executable, got)
}

func TestLocatorBoundsPATHCandidateCount(t *testing.T) {
	const inspectedPATHEntries = 256
	pathEntries := make([]string, 0, inspectedPATHEntries+1)
	for range inspectedPATHEntries {
		pathEntries = append(pathEntries, t.TempDir())
	}
	beyondBound := t.TempDir()
	writeTestFile(t, filepath.Join(beyondBound, "codex"), 0o755)
	pathEntries = append(pathEntries, beyondBound)

	_, err := newTestLocator(
		[]string{"PATH=" + strings.Join(pathEntries, string(os.PathListSeparator))},
		t.TempDir(),
		runtime.GOOS,
	).Find(ClientCodex)
	require.ErrorIs(t, err, ErrClientNotFound)
}

func TestLocatorBoundsEmptyPATHComponentsToo(t *testing.T) {
	beyondBound := t.TempDir()
	writeTestFile(t, filepath.Join(beyondBound, "codex"), 0o755)
	pathValue := strings.Repeat(string(os.PathListSeparator), 256) + beyondBound

	_, err := newTestLocator(
		[]string{"PATH=" + pathValue},
		t.TempDir(),
		runtime.GOOS,
	).Find(ClientCodex)
	require.ErrorIs(t, err, ErrClientNotFound)
}

func TestLocatorReturnsNotFoundForUnknownOrAbsentClient(t *testing.T) {
	locator := newTestLocator([]string{"PATH="}, t.TempDir(), runtime.GOOS)

	for _, client := range []Client{ClientCodex, ClientClaudeCode, Client("other")} {
		_, err := locator.Find(client)
		require.True(t, errors.Is(err, ErrClientNotFound))
	}
}

func TestLocatorNegativeFixturesDoNotConsultUninjectedPlatformDirectory(t *testing.T) {
	sentinelHome := t.TempDir()
	sentinelDir := filepath.Join(sentinelHome, ".local", "bin")
	sentinel := writeTestFile(t, filepath.Join(sentinelDir, "codex"), 0o755)
	validSentinel, err := validateCandidate(ClientCodex, sentinel, runtime.GOOS)
	require.NoError(t, err)
	require.Equal(t, sentinel, validSentinel)

	unsafeDir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(unsafeDir, "codex"), 0o755))
	beyondBound := t.TempDir()
	writeTestFile(t, filepath.Join(beyondBound, "codex"), 0o755)

	for name, pathValue := range map[string]string{
		"absent":        "",
		"unsafe":        unsafeDir,
		"PATH boundary": strings.Repeat(string(os.PathListSeparator), 256) + beyondBound,
	} {
		t.Run(name, func(t *testing.T) {
			_, findErr := newTestLocator(
				[]string{"PATH=" + pathValue},
				sentinelHome,
				runtime.GOOS,
			).Find(ClientCodex)
			require.ErrorIs(t, findErr, ErrClientNotFound)
		})
	}
}

func newTestLocator(
	env []string,
	userHome string,
	goos string,
	platformDirectories ...string,
) Locator {
	directories := append([]string(nil), platformDirectories...)
	return newLocator(
		env,
		userHome,
		goos,
		func(locator) []string {
			return append([]string(nil), directories...)
		},
	)
}

func writeTestFile(t *testing.T, path string, mode os.FileMode) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"), mode))
	canonical, err := filepath.EvalSymlinks(path)
	require.NoError(t, err)
	canonical, err = filepath.Abs(canonical)
	require.NoError(t, err)
	return filepath.Clean(canonical)
}

func makeTestSymlink(target, link string) error {
	return os.Symlink(target, link)
}
