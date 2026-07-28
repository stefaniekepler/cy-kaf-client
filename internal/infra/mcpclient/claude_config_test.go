package mcpclient

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestClaudeConfigReadsExactArgsWithoutSplittingSpaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".claude.json")
	writeTask3File(t, path, []byte(`{
		"other": {"secret": "sibling-secret-marker"},
		"mcpServers": {
			"cy-kaf-client": {
				"type": "stdio",
				"command": "/tmp/cy-kaf-client",
				"args": ["mcp", "--config", "/tmp/config with spaces.yaml"]
			}
		}
	}`), 0o600)

	entry, found, err := NewClaudeEntryReader(8 << 20).ReadClaude(path)
	if err != nil || !found {
		t.Fatalf("ReadClaude() = found %v, err %v", found, err)
	}
	if got, want := entry.Args, []string{"mcp", "--config", "/tmp/config with spaces.yaml"}; !equalStrings(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
	if !entry.Enabled || entry.Transport != "stdio" {
		t.Fatalf("entry = %#v", entry)
	}
}

func TestClaudeConfigTreatsNonStdioAsReadableConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".claude.json")
	writeTask3File(t, path, []byte(`{"mcpServers":{"cy-kaf-client":{"type":"http","url":"https://example.invalid"}}}`), 0o600)

	entry, found, err := NewClaudeEntryReader(8 << 20).ReadClaude(path)
	if err != nil || !found || entry.Transport != "http" {
		t.Fatalf("ReadClaude() = %#v, %v, %v", entry, found, err)
	}
}

func TestClaudeConfigRetainsTargetEnvironmentForConflictClassification(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".claude.json")
	writeTask3File(t, path, []byte(`{"mcpServers":{"cy-kaf-client":{"type":"stdio","command":"/tmp/client","args":["mcp"],"env":{"TOKEN":"marker"}}}}`), 0o600)

	entry, found, err := NewClaudeEntryReader(8 << 20).ReadClaude(path)
	if err != nil || !found || entry.Env["TOKEN"] != "marker" {
		t.Fatalf("ReadClaude() = %#v, %v, %v", entry, found, err)
	}
}

func TestClaudeConfigFailsClosedWithoutLeakingSiblingBytes(t *testing.T) {
	tests := map[string]string{
		"unknown stdio field": `{"sibling":"sibling-secret-marker","mcpServers":{"cy-kaf-client":{"type":"stdio","command":"/tmp/client","args":[],"unknown":"target-secret-marker"}}}`,
		"malformed JSON":      `{"sibling":"sibling-secret-marker","mcpServers":`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), ".claude.json")
			writeTask3File(t, path, []byte(body), 0o600)

			_, _, err := NewClaudeEntryReader(8 << 20).ReadClaude(path)
			if err == nil {
				t.Fatal("ReadClaude() error = nil")
			}
			for _, secret := range []string{"sibling-secret-marker", "target-secret-marker"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error leaked %q: %v", secret, err)
				}
			}
		})
	}
}

func TestClaudeConfigRejectsOversizeInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".claude.json")
	writeTask3File(t, path, []byte(`{"padding":"`+strings.Repeat("x", 128)+`"}`), 0o600)

	_, _, err := NewClaudeEntryReader(32).ReadClaude(path)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v, want ErrInvalidConfig", err)
	}
}

func TestClaudeConfigMissingFileOrTargetIsNotConfigured(t *testing.T) {
	reader := NewClaudeEntryReader(8 << 20)
	if _, found, err := reader.ReadClaude(filepath.Join(t.TempDir(), "missing")); err != nil || found {
		t.Fatalf("missing file = found %v, err %v", found, err)
	}
	path := filepath.Join(t.TempDir(), ".claude.json")
	writeTask3File(t, path, []byte(`{"mcpServers":{"another":{"type":"stdio"}}}`), 0o600)
	if _, found, err := reader.ReadClaude(path); err != nil || found {
		t.Fatalf("missing target = found %v, err %v", found, err)
	}
}

func TestClaudeConfigPathResolverRejectsUnsafeOverridesAndTargets(t *testing.T) {
	home := evalTestPath(t, t.TempDir())
	outside := evalTestPath(t, t.TempDir())
	regular := filepath.Join(home, "regular")
	writeTask3File(t, regular, []byte("not a directory"), 0o600)
	symlinkRoot := filepath.Join(home, "linked")
	if err := os.Symlink(outside, symlinkRoot); err != nil {
		t.Fatal(err)
	}

	tests := map[string][]string{
		"relative override": {"CODEX_HOME=relative"},
		"outside home":      {"CODEX_HOME=" + outside},
		"override is home":  {"CODEX_HOME=" + home},
		"symlink component": {"CODEX_HOME=" + filepath.Join(symlinkRoot, "codex")},
		"non directory":     {"CODEX_HOME=" + filepath.Join(regular, "codex")},
	}
	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NewConfigPathResolver(env, home).Resolve(ClientCodex); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Resolve() error = %v, want ErrInvalidConfig", err)
			}
		})
	}

	target := filepath.Join(home, ".claude.json")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := NewConfigPathResolver(nil, home).Resolve(ClientClaudeCode); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("non-regular target error = %v", err)
	}
}

func TestClaudeConfigPathResolverAllowsMissingFinalComponents(t *testing.T) {
	home := evalTestPath(t, t.TempDir())
	override := filepath.Join(home, "new", "codex-home")
	got, err := NewConfigPathResolver([]string{"CODEX_HOME=" + override}, home).Resolve(ClientCodex)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(override, "config.toml"); got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}
}

func TestClaudeConfigWindowsMixedCaseOverridesAreCaseInsensitive(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows environment semantics")
	}
	home := evalTestPath(t, t.TempDir())
	insideCodex := filepath.Join(home, "mixed-codex")
	insideClaude := filepath.Join(home, "mixed-claude")
	tests := []struct {
		name       string
		client     Client
		env        string
		want       string
		outsideEnv string
	}{
		{
			name:       "Codex_Home",
			client:     ClientCodex,
			env:        "Codex_Home=" + insideCodex,
			want:       filepath.Join(insideCodex, "config.toml"),
			outsideEnv: "Codex_Home=" + evalTestPath(t, t.TempDir()),
		},
		{
			name:       "claude_config_dir",
			client:     ClientClaudeCode,
			env:        "claude_config_dir=" + insideClaude,
			want:       filepath.Join(insideClaude, ".claude.json"),
			outsideEnv: "claude_config_dir=" + evalTestPath(t, t.TempDir()),
		},
	}
	for _, test := range tests {
		t.Run(test.name+"/inside", func(t *testing.T) {
			got, err := NewConfigPathResolver([]string{test.env}, home).Resolve(test.client)
			if err != nil || got != test.want {
				t.Fatalf("Resolve() = %q, %v; want %q", got, err, test.want)
			}
		})
		t.Run(test.name+"/outside", func(t *testing.T) {
			if _, err := NewConfigPathResolver([]string{test.outsideEnv}, home).Resolve(test.client); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Resolve() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func evalTestPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
