package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Real Tauri signer fixture. Its ephemeral private key was discarded.
const fixturePayload = "Cy KafClient signed update cache fixture\n"
const fixturePublicKey = "dW50cnVzdGVkIGNvbW1lbnQ6IG1pbmlzaWduIHB1YmxpYyBrZXk6IDMwMzY0NjU1MDkyMUVCNTIKUldSUzZ5RUpWVVkyTVBmVWtDSWRVL3d5WTg5WUU3bS9tMWtrL3ZTQkRTNjh4TUNOYm5qc2RTWjEK"
const fixtureSignature = "dW50cnVzdGVkIGNvbW1lbnQ6IHNpZ25hdHVyZSBmcm9tIHRhdXJpIHNlY3JldCBrZXkKUlVSUzZ5RUpWVVkyTVAwVFNPQ1hFcmFEUlZqaUhmVms0WWQ0dkZmSEx2Yi9USXlySUgrRTBCcDRQdXh2eDJPSGlvT0wrVmdCc0d2TXNVQjB5MkpnVTVDamV0c3FxQUtKRVFZPQp0cnVzdGVkIGNvbW1lbnQ6IHRpbWVzdGFtcDoxNzg4NzYxNTEyCWZpbGU6cGFja2FnZS5iaW4KS1BCYUNaNXVVc25CSlZVRlcrRkVidjFuNXhRaEo2SDFOSFNRVmFtdnhhbzBEYkc1WGdBRDJVWXVlTEdyaW01NUZ5VVEvY2RrMGJDZHF6cUdvUmZlQkE9PQo="

var fixtureSuffixes = []string{
	"macos-x86_64.app.tar.gz", "macos-aarch64.app.tar.gz",
	"windows-x86_64-setup.exe", "windows-aarch64-setup.exe",
	"linux-x86_64.AppImage", "linux-aarch64.AppImage",
}

func signedFixture(t *testing.T) (string, string) {
	t.Helper()
	directory := t.TempDir()
	for _, suffix := range fixtureSuffixes {
		name := filepath.Join(directory, "Cy-KafClient_1.2.3_"+suffix)
		writeTestFile(t, name, fixturePayload)
		writeTestFile(t, name+".sig", fixtureSignature+"\n")
	}
	config := filepath.Join(t.TempDir(), "tauri.conf.json")
	writePublicKey(t, config, fixturePublicKey)
	return directory, config
}

func writePublicKey(t *testing.T, path, key string) {
	t.Helper()
	data, err := json.Marshal(map[string]any{"plugins": map[string]any{"updater": map[string]any{"pubkey": key}}})
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, path, string(data))
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyAcceptsSixRealTauriSignedPackages(t *testing.T) {
	directory, config := signedFixture(t)
	if err := verifyDirectory(directory, config, "1.2.3"); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyRejectsTamperedPackageOnEveryTarget(t *testing.T) {
	for _, suffix := range fixtureSuffixes {
		t.Run(suffix, func(t *testing.T) {
			directory, config := signedFixture(t)
			writeTestFile(t, filepath.Join(directory, "Cy-KafClient_1.2.3_"+suffix), "tampered executable")
			if err := verifyDirectory(directory, config, "1.2.3"); err == nil {
				t.Fatal("tampered package was accepted")
			}
		})
	}
}

func TestVerifyRejectsIncorrectPublicKeyAndDamagedSignatures(t *testing.T) {
	for _, key := range []string{"", "private-invalid-key", wrongPublicKey(t)} {
		directory, config := signedFixture(t)
		writePublicKey(t, config, key)
		err := verifyDirectory(directory, config, "1.2.3")
		if err == nil {
			t.Fatal("incorrect public key was accepted")
		}
		if strings.Contains(err.Error(), "private-invalid") {
			t.Fatal("error leaked untrusted key")
		}
	}
	for _, signature := range []string{"", "private-invalid-signature", tamperedSignature(t)} {
		directory, config := signedFixture(t)
		writeTestFile(t, filepath.Join(directory, "Cy-KafClient_1.2.3_linux-aarch64.AppImage.sig"), signature)
		err := verifyDirectory(directory, config, "1.2.3")
		if err == nil {
			t.Fatal("damaged signature was accepted")
		}
		if strings.Contains(err.Error(), "private-invalid") {
			t.Fatal("error leaked untrusted signature")
		}
	}
}

func wrongPublicKey(t *testing.T) string {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(fixturePublicKey)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(decoded), "\n")
	key, err := base64.StdEncoding.DecodeString(lines[1])
	if err != nil {
		t.Fatal(err)
	}
	key[len(key)-1] ^= 1
	lines[1] = base64.StdEncoding.EncodeToString(key)
	return base64.StdEncoding.EncodeToString([]byte(strings.Join(lines, "\n")))
}

func tamperedSignature(t *testing.T) string {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(fixtureSignature)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString([]byte(strings.Replace(string(decoded), "file:package.bin", "file:altered.bin", 1)))
}

func TestVerifyRejectsIncompleteOversizedAndUnsafeArtifacts(t *testing.T) {
	for _, mutate := range []func(*testing.T, string){
		func(t *testing.T, path string) {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		},
		func(t *testing.T, path string) { writeTestFile(t, path, "") },
		func(t *testing.T, path string) {
			file, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = file.Close() }()
			if err := file.Truncate(256<<20 + 1); err != nil {
				t.Fatal(err)
			}
		},
	} {
		directory, config := signedFixture(t)
		mutate(t, filepath.Join(directory, "Cy-KafClient_1.2.3_linux-aarch64.AppImage"))
		if err := verifyDirectory(directory, config, "1.2.3"); err == nil {
			t.Fatal("invalid artifact accepted")
		}
	}
	directory, config := signedFixture(t)
	if err := verifyDirectory(directory, config, "../private-invalid-version"); err == nil {
		t.Fatal("unsafe version accepted")
	}
}
