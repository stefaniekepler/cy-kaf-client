package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeGitHub struct {
	exists     bool
	draft      bool
	assets     map[string]string
	commands   []string
	failUpload bool
	verified   bool
}

func (fake *fakeGitHub) run(args ...string) ([]byte, error) {
	fake.commands = append(fake.commands, strings.Join(args, " "))
	if args[0] == "api" {
		if !fake.exists {
			return nil, fmt.Errorf("not found")
		}
		if len(fake.assets) > 0 {
			fake.verified = true
		}
		items := make([]string, 0, len(fake.assets))
		for name, digest := range fake.assets {
			items = append(items, fmt.Sprintf(`{"name":%q,"digest":%q}`, name, digest))
		}
		return []byte(fmt.Sprintf(`{"draft":%t,"prerelease":false,"assets":[%s]}`, fake.draft, strings.Join(items, ","))), nil
	}
	if len(args) >= 3 && args[0] == "release" && args[1] == "create" {
		fake.exists, fake.draft = true, true
		return nil, nil
	}
	if len(args) >= 3 && args[0] == "release" && args[1] == "upload" {
		if fake.failUpload {
			return nil, fmt.Errorf("injected upload failure")
		}
		for _, path := range args[3:] {
			if path == "--clobber" || path == "-R" || path == fixedRepository {
				continue
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			sum := sha256.Sum256(content)
			fake.assets[filepath.Base(path)] = fmt.Sprintf("sha256:%x", sum)
		}
		return nil, nil
	}
	if len(args) >= 3 && args[0] == "release" && args[1] == "edit" {
		for _, arg := range args {
			if arg == "--draft=false" {
				if !fake.verified {
					return nil, fmt.Errorf("attempted publication before remote verification")
				}
				fake.draft = false
			}
		}
		return nil, nil
	}
	return nil, fmt.Errorf("unexpected command: %v", args)
}

func TestPublishCreatesDraftUploadsVerifiesThenPublishes(t *testing.T) {
	dir := releaseFixture(t)
	fake := &fakeGitHub{assets: map[string]string{}}
	if err := publish(fake.run, publishOptions{Tag: "v1.2.3", AssetsDirectory: dir, Title: "release", NotesFile: filepath.Join(dir, "notes.txt")}); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(fake.commands, "\n")
	create := strings.Index(joined, "release create")
	upload := strings.Index(joined, "release upload")
	publish := strings.Index(joined, "--draft=false")
	if create < 0 || upload < create || publish < upload {
		t.Fatalf("unsafe command order:\n%s", joined)
	}
	if fake.draft {
		t.Fatal("release remained draft after verified upload")
	}
}

func TestPublishUploadFailureLeavesNewReleaseDraft(t *testing.T) {
	fake := &fakeGitHub{assets: map[string]string{}, failUpload: true}
	err := publish(fake.run, publishOptions{Tag: "v1.2.3", AssetsDirectory: releaseFixture(t), Title: "release", NotesFile: "notes.txt"})
	if err == nil {
		t.Fatal("expected upload failure")
	}
	if !fake.draft {
		t.Fatal("failed upload exposed a public release")
	}
	for _, command := range fake.commands {
		if strings.Contains(command, "--draft=false") {
			t.Fatalf("published after failure: %s", command)
		}
	}
}

func TestPublishRejectsChangedAssetsOnExistingPublicRelease(t *testing.T) {
	dir := releaseFixture(t)
	fake := &fakeGitHub{exists: true, draft: false, assets: map[string]string{"latest.json": "sha256:deadbeef", "package.sig": fileDigest(t, filepath.Join(dir, "package.sig"))}}
	err := publish(fake.run, publishOptions{Tag: "v1.2.3", AssetsDirectory: dir, Title: "release", NotesFile: "notes.txt"})
	if err == nil || !strings.Contains(err.Error(), "differs") {
		t.Fatalf("got %v, want changed asset rejection", err)
	}
	for _, command := range fake.commands {
		if strings.HasPrefix(command, "release upload") {
			t.Fatalf("clobbered public release: %s", command)
		}
	}
}

func TestPublishCompletesExistingDraft(t *testing.T) {
	fake := &fakeGitHub{exists: true, draft: true, assets: map[string]string{}}
	if err := publish(fake.run, publishOptions{Tag: "v1.2.3", AssetsDirectory: releaseFixture(t), Title: "release", NotesFile: "notes.txt"}); err != nil {
		t.Fatal(err)
	}
	if fake.draft {
		t.Fatal("verified draft was not published")
	}
}

func releaseFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range map[string]string{"latest.json": "manifest", "package.sig": "signature"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func fileDigest(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	return fmt.Sprintf("sha256:%x", sum)
}
