package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const fixedRepository = "stefaniekepler/cy-kaf-client"

var safeTag = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

type publishOptions struct {
	Tag             string
	AssetsDirectory string
	Title           string
	NotesFile       string
}

type commandRunner func(args ...string) ([]byte, error)

type releaseState struct {
	TagName    string `json:"tag_name"`
	Draft      bool   `json:"draft"`
	Prerelease bool   `json:"prerelease"`
	Assets     []struct {
		Name   string `json:"name"`
		Digest string `json:"digest"`
	} `json:"assets"`
}

func main() {
	var opts publishOptions
	flag.StringVar(&opts.Tag, "tag", "", "release tag")
	flag.StringVar(&opts.AssetsDirectory, "assets-dir", "release-assets", "complete release asset directory")
	flag.StringVar(&opts.Title, "title", "", "release title")
	flag.StringVar(&opts.NotesFile, "notes-file", "", "release notes file")
	flag.Parse()
	if err := publish(runGitHub, opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runGitHub(args ...string) ([]byte, error) {
	command := exec.Command("gh", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("gh %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func publish(run commandRunner, opts publishOptions) error {
	if !safeTag.MatchString(opts.Tag) {
		return fmt.Errorf("tag must match vX.Y.Z: %q", opts.Tag)
	}
	if opts.Title == "" || opts.NotesFile == "" {
		return errors.New("title and notes-file are required")
	}
	local, paths, err := localAssets(opts.AssetsDirectory)
	if err != nil {
		return err
	}

	state, exists, err := getRelease(run, opts.Tag)
	if err != nil {
		return err
	}
	if exists && !state.Draft {
		if state.Prerelease {
			return errors.New("existing non-draft release is a prerelease")
		}
		return verifyAssets(local, state)
	}
	if !exists {
		_, err = run("release", "create", opts.Tag, "--draft", "--verify-tag", "--title", opts.Title, "--generate-notes", "--notes-file", opts.NotesFile, "-R", fixedRepository)
		if err != nil {
			return fmt.Errorf("create draft release: %w", err)
		}
	} else {
		_, err = run("release", "edit", opts.Tag, "--draft=true", "--prerelease=false", "--title", opts.Title, "--notes-file", opts.NotesFile, "-R", fixedRepository)
		if err != nil {
			return fmt.Errorf("update draft metadata: %w", err)
		}
	}

	uploadArgs := []string{"release", "upload", opts.Tag}
	uploadArgs = append(uploadArgs, paths...)
	uploadArgs = append(uploadArgs, "--clobber", "-R", fixedRepository)
	if _, err := run(uploadArgs...); err != nil {
		return fmt.Errorf("upload draft assets: %w", err)
	}
	state, exists, err = getRelease(run, opts.Tag)
	if err != nil {
		return err
	}
	if !exists || !state.Draft {
		return errors.New("release must remain draft during asset verification")
	}
	if err := verifyAssets(local, state); err != nil {
		return fmt.Errorf("verify draft assets: %w", err)
	}

	if _, err := run("release", "edit", opts.Tag, "--draft=false", "--prerelease=false", "-R", fixedRepository); err != nil {
		return fmt.Errorf("publish verified draft: %w", err)
	}
	state, exists, err = getRelease(run, opts.Tag)
	if err != nil {
		return err
	}
	if !exists || state.Draft || state.Prerelease {
		return errors.New("release did not become a public final release")
	}
	return verifyAssets(local, state)
}

func getRelease(run commandRunner, tag string) (releaseState, bool, error) {
	// The by-tag endpoint only returns published releases. Authenticated listing
	// includes drafts, which must remain discoverable throughout publication.
	output, err := run("api", "repos/"+fixedRepository+"/releases?per_page=100", "--paginate", "--slurp")
	if err != nil {
		return releaseState{}, false, fmt.Errorf("list releases: %w", err)
	}
	var pages [][]releaseState
	if err := json.Unmarshal(output, &pages); err != nil {
		return releaseState{}, false, fmt.Errorf("decode release state: %w", err)
	}
	for _, page := range pages {
		for _, state := range page {
			if state.TagName == tag {
				return state, true, nil
			}
		}
	}
	return releaseState{}, false, nil
}

func localAssets(directory string) (map[string]string, []string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, nil, fmt.Errorf("read assets: %w", err)
	}
	digests := make(map[string]string, len(entries))
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			return nil, nil, fmt.Errorf("asset %q is not a regular file", entry.Name())
		}
		path := filepath.Join(directory, entry.Name())
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, nil, fmt.Errorf("read asset %q: %w", entry.Name(), err)
		}
		if len(content) == 0 {
			return nil, nil, fmt.Errorf("asset %q is empty", entry.Name())
		}
		sum := sha256.Sum256(content)
		digests[entry.Name()] = fmt.Sprintf("sha256:%x", sum)
		paths = append(paths, path)
	}
	if len(paths) == 0 {
		return nil, nil, errors.New("asset directory is empty")
	}
	sort.Strings(paths)
	return digests, paths, nil
}

func verifyAssets(local map[string]string, remote releaseState) error {
	if len(remote.Assets) != len(local) {
		return fmt.Errorf("remote asset count %d differs from local count %d", len(remote.Assets), len(local))
	}
	seen := make(map[string]bool, len(remote.Assets))
	for _, asset := range remote.Assets {
		want, ok := local[asset.Name]
		if !ok {
			return fmt.Errorf("remote asset %q is unexpected", asset.Name)
		}
		if seen[asset.Name] {
			return fmt.Errorf("remote asset %q is duplicated", asset.Name)
		}
		seen[asset.Name] = true
		if asset.Digest != want {
			return fmt.Errorf("remote asset %q digest differs: got %q, want %q", asset.Name, asset.Digest, want)
		}
	}
	return nil
}
