package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"aead.dev/minisign"
)

const maxPackageBytes int64 = 256 << 20

var safeVersion = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

var packageSuffixes = []string{
	"macos-x86_64.app.tar.gz", "macos-aarch64.app.tar.gz",
	"windows-x86_64-setup.exe", "windows-aarch64-setup.exe",
	"linux-x86_64.AppImage", "linux-aarch64.AppImage",
}

func main() {
	var directory, config, version string
	flag.StringVar(&directory, "dir", "release-assets", "directory containing updater artifacts")
	flag.StringVar(&config, "tauri-config", "desktop/src-tauri/tauri.conf.json", "application configuration containing the embedded public key")
	flag.StringVar(&version, "version", "", "release version without v prefix")
	flag.Parse()
	if err := verifyDirectory(directory, config, version); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Verified all six updater packages against the embedded public key")
}

func verifyDirectory(directory, config, version string) error {
	if len(version) > 256 || !safeVersion.MatchString(version) {
		return errors.New("release version must match X.Y.Z")
	}
	publicKey, err := readPublicKey(config)
	if err != nil {
		return err
	}
	for _, suffix := range packageSuffixes {
		path := filepath.Join(directory, "Cy-KafClient_"+version+"_"+suffix)
		if err := verifyPackage(publicKey, path); err != nil {
			// Suffix is a fixed known platform name, not untrusted file content.
			return fmt.Errorf("verify %s: %w", suffix, err)
		}
	}
	return nil
}

func readPublicKey(path string) (minisign.PublicKey, error) {
	var config struct {
		Plugins struct {
			Updater struct {
				PublicKey string `json:"pubkey"`
			} `json:"updater"`
		} `json:"plugins"`
	}
	var publicKey minisign.PublicKey
	content, err := readSmallRegularFile(path, 256<<10)
	if err != nil || json.Unmarshal(content, &config) != nil {
		return publicKey, errors.New("cannot read updater public key configuration")
	}
	if len(config.Plugins.Updater.PublicKey) > 4096 {
		return publicKey, errors.New("updater public key is invalid")
	}
	decoded, err := base64.StdEncoding.DecodeString(config.Plugins.Updater.PublicKey)
	if err != nil || publicKey.UnmarshalText(decoded) != nil {
		return publicKey, errors.New("updater public key is invalid")
	}
	return publicKey, nil
}

func verifyPackage(publicKey minisign.PublicKey, path string) error {
	signatureText, err := readSmallRegularFile(path+".sig", 16<<10)
	if err != nil {
		return errors.New("signature is missing or invalid")
	}
	signature, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(signatureText)))
	if err != nil {
		return errors.New("signature is invalid")
	}
	// Tauri emits pre-hashed minisign signatures. Hash while streaming so CI
	// memory use remains bounded even for the largest permitted installer.
	var parsed minisign.Signature
	if parsed.UnmarshalText(signature) != nil || parsed.Algorithm != minisign.HashEdDSA {
		return errors.New("signature is not a Tauri updater signature")
	}
	file, size, err := openRegularFile(path, maxPackageBytes)
	if err != nil {
		return errors.New("package is missing, empty, oversized or not a regular file")
	}
	defer func() { _ = file.Close() }()
	reader := minisign.NewReader(io.LimitReader(file, maxPackageBytes+1))
	read, err := io.Copy(io.Discard, reader)
	if err != nil || read != size || read > maxPackageBytes {
		return errors.New("package could not be read within its size limit")
	}
	if !reader.Verify(publicKey, signature) {
		return errors.New("package signature does not match the embedded public key")
	}
	return nil
}

func openRegularFile(path string, limit int64) (*os.File, int64, error) {
	metadata, err := os.Lstat(path)
	if err != nil || !metadata.Mode().IsRegular() || metadata.Size() <= 0 || metadata.Size() > limit {
		return nil, 0, errors.New("file is missing, empty, oversized or not regular")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, errors.New("cannot open file")
	}
	return file, metadata.Size(), nil
}

func readSmallRegularFile(path string, limit int64) ([]byte, error) {
	file, size, err := openRegularFile(path, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	content, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(content)) != size || int64(len(content)) > limit {
		return nil, errors.New("cannot read bounded file")
	}
	return content, nil
}
