package mcppolicy

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

func TestStoreMissingFileReturnsDisabledDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-policy.json")

	got, err := NewStore(path).Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := Policy{Version: 1, Enabled: false, AllowWrites: false}
	if got != want {
		t.Fatalf("Load() = %#v, want %#v", got, want)
	}
}

func TestStoreRejectsCorruptAndUnknownVersion(t *testing.T) {
	for name, raw := range map[string]string{
		"corrupt":         `{"version":`,
		"unknown version": `{"version":2,"enabled":true,"allowWrites":true}`,
		"unknown field":   `{"version":1,"enabled":true,"allowWrites":false,"extra":true}`,
		"trailing JSON":   `{"version":1,"enabled":true,"allowWrites":false} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "mcp-policy.json")
			if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}

			got, err := NewStore(path).Load()
			if err == nil {
				t.Fatalf("Load() = %#v, want error", got)
			}
			if got != (Policy{}) {
				t.Fatalf("Load() = %#v, want fail-closed %#v", got, Policy{})
			}
		})
	}
}

func TestStoreRejectsUnreadablePolicyFailClosed(t *testing.T) {
	path := t.TempDir()

	got, err := NewStore(path).Load()
	if err == nil {
		t.Fatalf("Load() = %#v, want read error", got)
	}
	if got != (Policy{}) {
		t.Fatalf("Load() = %#v, want fail-closed %#v", got, Policy{})
	}
}

func TestStoreCanonicalizesDisabledWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-policy.json")
	store := NewStore(path)
	if err := store.Save(context.Background(), Policy{Version: 1, Enabled: false, AllowWrites: true}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	got, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := Policy{Version: 1, Enabled: false, AllowWrites: false}
	if got != want {
		t.Fatalf("Load() = %#v, want %#v", got, want)
	}
}

func TestStoreSaveUsesPrivatePermissionsAndAtomicReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix permission bits or inode replacement")
	}

	dir := filepath.Join(t.TempDir(), "policy")
	path := filepath.Join(dir, "mcp-policy.json")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"enabled":true,"allowWrites":false}`), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := NewStore(path).Save(context.Background(), Policy{Version: 1, Enabled: true, AllowWrites: true}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, after) {
		t.Fatal("Save() did not atomically replace the policy file")
	}
	assertMode(t, dir, 0o700)
	assertMode(t, path, 0o600)
}

func TestStoreConcurrentLoadNeverSeesPartialJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-policy.json")
	writer := NewStore(path)
	reader := NewStore(path)
	if err := writer.Save(context.Background(), Policy{Version: 1, Enabled: true, AllowWrites: false}); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 200; i++ {
			if err := writer.Save(context.Background(), Policy{Version: 1, Enabled: true, AllowWrites: i%2 == 0}); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 400; i++ {
			p, err := reader.Load()
			if err != nil {
				errs <- err
				return
			}
			if p.Version != 1 || !p.Enabled {
				errs <- errors.New("Load() observed a policy other than a complete saved policy")
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestStoreReplacementLeavesNoTempFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mcp-policy.json")
	if err := NewStore(path).Save(context.Background(), Policy{Version: 1, Enabled: true, AllowWrites: false}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, ".mcp-policy.json.*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	assertNoPolicyTempFiles(t, matches)
}

func TestStoreReplacementFailureLeavesNoTempFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mcp-policy.json")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}

	err := NewStore(path).Save(context.Background(), Policy{Version: 1, Enabled: true, AllowWrites: false})
	if err == nil {
		t.Fatal("Save() error = nil, want replacement error")
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".mcp-policy.json.*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	assertNoPolicyTempFiles(t, matches)
}

func TestPathForConfigUsesSiblingMCPPolicy(t *testing.T) {
	if got, want := PathForConfig("/cfg/config.yaml"), "/cfg/mcp-policy.json"; got != want {
		t.Fatalf("PathForConfig() = %q, want %q", got, want)
	}
}

func assertMode(t *testing.T, path string, want fs.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("permissions for %s = %#o, want %#o", path, got, want)
	}
}

func assertNoPolicyTempFiles(t *testing.T, matches []string) {
	t.Helper()
	if len(matches) != 0 {
		t.Fatalf("temporary files remain after Save(): %v", matches)
	}
}
