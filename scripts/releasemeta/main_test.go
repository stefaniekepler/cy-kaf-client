package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolve(t *testing.T) {
	tests := []struct {
		name       string
		tag        string
		tauri      string
		cargo      string
		want       metadata
		wantErrSub string
	}{
		{
			name:  "ordinary CI reads versions without a release",
			tauri: `{"version":"0.1.0"}`,
			cargo: "[package]\nname = \"desktop\"\nversion = \"0.1.0\"\n",
			want:  metadata{Version: "0.1.0"},
		},
		{
			name:  "release tag matches both version sources",
			tag:   "v0.1.0",
			tauri: `{"version":"0.1.0"}`,
			cargo: "[package]\nname = \"desktop\"\nversion = \"0.1.0\"\n",
			want:  metadata{Tag: "v0.1.0", Version: "0.1.0", IsRelease: true},
		},
		{
			name:       "rejects non-semantic tag",
			tag:        "release-0.1.0",
			tauri:      `{"version":"0.1.0"}`,
			cargo:      "[package]\nversion = \"0.1.0\"\n",
			wantErrSub: "tag must match vX.Y.Z",
		},
		{
			name:       "rejects Tauri and Cargo mismatch",
			tag:        "v0.1.0",
			tauri:      `{"version":"0.1.0"}`,
			cargo:      "[package]\nversion = \"0.2.0\"\n",
			wantErrSub: "version mismatch",
		},
		{
			name:       "rejects tag and application mismatch",
			tag:        "v0.2.0",
			tauri:      `{"version":"0.1.0"}`,
			cargo:      "[package]\nversion = \"0.1.0\"\n",
			wantErrSub: "tag version",
		},
		{
			name:       "rejects missing Tauri version",
			tauri:      `{}`,
			cargo:      "[package]\nversion = \"0.1.0\"\n",
			wantErrSub: "Tauri version is empty",
		},
		{
			name:       "rejects missing Cargo package version",
			tauri:      `{"version":"0.1.0"}`,
			cargo:      "[dependencies]\nserde = \"1\"\n",
			wantErrSub: "Cargo package version",
		},
		{
			name:       "rejects non-semantic application version",
			tauri:      `{"version":"0.1"}`,
			cargo:      "[package]\nversion = \"0.1\"\n",
			wantErrSub: "application version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			tauriPath := filepath.Join(tempDir, "tauri.conf.json")
			cargoPath := filepath.Join(tempDir, "Cargo.toml")
			require.NoError(t, os.WriteFile(tauriPath, []byte(tt.tauri), 0o600))
			require.NoError(t, os.WriteFile(cargoPath, []byte(tt.cargo), 0o600))

			got, err := resolve(tt.tag, tauriPath, cargoPath)
			if tt.wantErrSub != "" {
				require.ErrorContains(t, err, tt.wantErrSub)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestReadCargoPackageVersionIgnoresOtherTables(t *testing.T) {
	cargoPath := filepath.Join(t.TempDir(), "Cargo.toml")
	content := strings.Join([]string{
		"[workspace.package]",
		`version = "9.9.9"`,
		"",
		"[package]",
		`name = "desktop"`,
		`version = "0.1.0"`,
		"",
		"[dependencies]",
		`serde = "1.0.0"`,
	}, "\n")
	require.NoError(t, os.WriteFile(cargoPath, []byte(content), 0o600))

	got, err := readCargoPackageVersion(cargoPath)

	require.NoError(t, err)
	require.Equal(t, "0.1.0", got)
}

func TestWriteGitHubOutput(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "github-output")

	err := writeGitHubOutput(outputPath, metadata{
		Tag:       "v0.1.0",
		Version:   "0.1.0",
		IsRelease: true,
	})

	require.NoError(t, err)
	content, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Equal(t, "tag=v0.1.0\nversion=0.1.0\nis_release=true\n", string(content))
}
