package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGenerateCompleteManifest(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"Cy-KafClient_1.2.3_macos-x86_64.app.tar.gz":       "mac intel",
		"Cy-KafClient_1.2.3_macos-x86_64.app.tar.gz.sig":   "signature-mac-intel\n",
		"Cy-KafClient_1.2.3_macos-aarch64.app.tar.gz":      "mac arm",
		"Cy-KafClient_1.2.3_macos-aarch64.app.tar.gz.sig":  "signature-mac-arm\n",
		"Cy-KafClient_1.2.3_windows-x86_64-setup.exe":      "win intel",
		"Cy-KafClient_1.2.3_windows-x86_64-setup.exe.sig":  "signature-win-intel\n",
		"Cy-KafClient_1.2.3_windows-aarch64-setup.exe":     "win arm",
		"Cy-KafClient_1.2.3_windows-aarch64-setup.exe.sig": "signature-win-arm\n",
		"Cy-KafClient_1.2.3_linux-x86_64.AppImage":         "linux intel",
		"Cy-KafClient_1.2.3_linux-x86_64.AppImage.sig":     "signature-linux-intel\n",
		"Cy-KafClient_1.2.3_linux-aarch64.AppImage":        "linux arm",
		"Cy-KafClient_1.2.3_linux-aarch64.AppImage.sig":    "signature-linux-arm\n",
	}
	writeFixture(t, dir, files)

	content, err := generate(options{
		Directory:  dir,
		Version:    "1.2.3",
		Notes:      "Fixed reconnects & paths.",
		PubDate:    time.Date(2026, 9, 7, 8, 9, 10, 0, time.UTC),
		Repository: "stefaniekepler/cy-kaf-client",
		Tag:        "v1.2.3",
	})
	if err != nil {
		t.Fatal(err)
	}

	var got manifest
	if err := json.Unmarshal(content, &got); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, content)
	}
	if got.Version != "1.2.3" || got.Notes != "Fixed reconnects & paths." || got.PubDate != "2026-09-07T08:09:10Z" {
		t.Fatalf("unexpected metadata: %#v", got)
	}
	if len(got.Platforms) != 6 {
		t.Fatalf("got %d platforms, want 6", len(got.Platforms))
	}
	want := map[string]platform{
		"darwin-x86_64":   {Signature: "signature-mac-intel", URL: "https://github.com/stefaniekepler/cy-kaf-client/releases/download/v1.2.3/Cy-KafClient_1.2.3_macos-x86_64.app.tar.gz"},
		"darwin-aarch64":  {Signature: "signature-mac-arm", URL: "https://github.com/stefaniekepler/cy-kaf-client/releases/download/v1.2.3/Cy-KafClient_1.2.3_macos-aarch64.app.tar.gz"},
		"windows-x86_64":  {Signature: "signature-win-intel", URL: "https://github.com/stefaniekepler/cy-kaf-client/releases/download/v1.2.3/Cy-KafClient_1.2.3_windows-x86_64-setup.exe"},
		"windows-aarch64": {Signature: "signature-win-arm", URL: "https://github.com/stefaniekepler/cy-kaf-client/releases/download/v1.2.3/Cy-KafClient_1.2.3_windows-aarch64-setup.exe"},
		"linux-x86_64":    {Signature: "signature-linux-intel", URL: "https://github.com/stefaniekepler/cy-kaf-client/releases/download/v1.2.3/Cy-KafClient_1.2.3_linux-x86_64.AppImage"},
		"linux-aarch64":   {Signature: "signature-linux-arm", URL: "https://github.com/stefaniekepler/cy-kaf-client/releases/download/v1.2.3/Cy-KafClient_1.2.3_linux-aarch64.AppImage"},
	}
	for target, expected := range want {
		if got.Platforms[target] != expected {
			t.Errorf("%s = %#v, want %#v", target, got.Platforms[target], expected)
		}
	}
}

func TestGenerateRejectsMissingOrEmptySignature(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "empty"}[empty], func(t *testing.T) {
			dir := completeFixture(t, "1.2.3")
			path := filepath.Join(dir, "Cy-KafClient_1.2.3_linux-aarch64.AppImage.sig")
			if empty {
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			_, err := generate(validOptions(dir, "1.2.3"))
			if err == nil || !strings.Contains(err.Error(), "signature") {
				t.Fatalf("got %v, want signature error", err)
			}
		})
	}
}

func TestGenerateRejectsDuplicateOrUnknownTarget(t *testing.T) {
	for _, name := range []string{
		"Cy-KafClient_1.2.3_linux-amd64.AppImage",
		"Cy-KafClient_1.2.3_freebsd-x86_64.tar.gz",
	} {
		t.Run(name, func(t *testing.T) {
			dir := completeFixture(t, "1.2.3")
			writeFixture(t, dir, map[string]string{name: "extra", name + ".sig": "extra-signature"})
			_, err := generate(validOptions(dir, "1.2.3"))
			if err == nil || !strings.Contains(err.Error(), "unexpected updater artifact") {
				t.Fatalf("got %v, want unexpected artifact error", err)
			}
		})
	}
}

func TestGenerateRejectsVersionMismatchAndUnsafeNames(t *testing.T) {
	for _, mutate := range []func(string) error{
		func(dir string) error {
			return os.Rename(filepath.Join(dir, "Cy-KafClient_1.2.3_linux-aarch64.AppImage"), filepath.Join(dir, "Cy-KafClient_9.9.9_linux-aarch64.AppImage"))
		},
		func(dir string) error {
			return os.WriteFile(filepath.Join(dir, "Cy-KafClient_1.2.3_linux-aarch64.AppImage extra"), []byte("bad"), 0o600)
		},
	} {
		dir := completeFixture(t, "1.2.3")
		if err := mutate(dir); err != nil {
			t.Fatal(err)
		}
		if _, err := generate(validOptions(dir, "1.2.3")); err == nil {
			t.Fatal("expected unsafe or mismatched artifact rejection")
		}
	}
	for _, version := range []string{"v1.2.3", "1.2.3/../../bad", "01.2.3"} {
		dir := completeFixture(t, "1.2.3")
		opts := validOptions(dir, version)
		if _, err := generate(opts); err == nil {
			t.Fatalf("version %q accepted", version)
		}
	}
}

func validOptions(dir, version string) options {
	return options{Directory: dir, Version: version, Notes: "notes", PubDate: time.Unix(0, 0).UTC(), Repository: "stefaniekepler/cy-kaf-client", Tag: "v" + version}
}

func completeFixture(t *testing.T, version string) string {
	t.Helper()
	dir := t.TempDir()
	for _, target := range artifactTargets {
		name := target.filename(version)
		writeFixture(t, dir, map[string]string{name: "package", name + ".sig": "signature"})
	}
	return dir
}

func writeFixture(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
