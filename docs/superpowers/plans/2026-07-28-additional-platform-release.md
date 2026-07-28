# Additional Platform GitHub Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the existing release pipeline so `v0.1.1` publishes and verifies Windows ARM64, Linux x86_64, and Linux ARM64 packages alongside the existing three desktop packages.

**Architecture:** Keep `.github/workflows/ci.yml` as the only CI and release workflow. Extend the native desktop matrix with GitHub-hosted ARM runners, add explicit Go-sidecar target mappings, build Linux AppImages on native Linux architectures, and aggregate exactly six installers into one checksum-verified GitHub Release.

**Tech Stack:** GitHub Actions, Go 1.26, Node.js 22/pnpm 10.26.1, Rust 1.95/Tauri 2.11, NSIS, AppImage, Xvfb, Bash, PowerShell, actionlint 1.7.12.

## Global Constraints

- Preserve macOS Intel `x86_64-apple-darwin` DMG support with minimum macOS 13.0.
- Preserve macOS Apple Silicon `aarch64-apple-darwin` DMG support with minimum macOS 12.0.
- Preserve Windows x64 `x86_64-pc-windows-msvc` NSIS support.
- Add Windows ARM64 `aarch64-pc-windows-msvc` NSIS on `windows-11-arm`.
- Add Linux x86_64 `x86_64-unknown-linux-gnu` AppImage on `ubuntu-24.04`.
- Add Linux ARM64 `aarch64-unknown-linux-gnu` AppImage on `ubuntu-24.04-arm`.
- Build every desktop package on a native runner and require its native smoke test before publishing.
- Publish exactly six installers plus `SHA256SUMS.txt`.
- Keep all packages unsigned and document Gatekeeper, SmartScreen, executable-bit, and Linux runtime implications.
- Release only strict `vX.Y.Z` tags whose version equals the Tauri and Cargo package versions.
- Publish the next version as `v0.1.1`.
- Keep default workflow permission at `contents: read`; only the release job receives `contents: write`.
- Do not publish `.deb`, `.rpm`, MSI, stores, plain desktop executables, or no-frontend Go build artifacts.

---

### Task 1: Version and Sidecar Target Contract

**Files:**
- Modify: `scripts/build-desktop-sidecar/main_test.go`
- Modify: `scripts/build-desktop-sidecar/main.go`
- Modify: `desktop/src-tauri/tauri.conf.json`
- Modify: `desktop/src-tauri/Cargo.toml`
- Modify: `desktop/src-tauri/Cargo.lock`
- Modify: `scripts/releasemeta/main_test.go`

**Interfaces:**
- Consumes: Rust target triples passed by the desktop matrix.
- Produces: `target{GOOS, GOARCH, Ext}` mappings and a synchronized application version of `0.1.1`.

- [ ] **Step 1: Add failing sidecar target cases**

Extend `TestTargetForTriple` with:

```go
"aarch64-pc-windows-msvc":   {GOOS: "windows", GOARCH: "arm64", Ext: ".exe"},
"x86_64-unknown-linux-gnu":  {GOOS: "linux", GOARCH: "amd64"},
"aarch64-unknown-linux-gnu": {GOOS: "linux", GOARCH: "arm64"},
```

Change the unsupported example to `riscv64gc-unknown-linux-gnu` so the test still covers rejection.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
go test ./scripts/build-desktop-sidecar -run TestTargetForTriple -count=1
```

Expected: FAIL for all three newly required target mappings.

- [ ] **Step 3: Add the minimal mappings**

Add the exact three target entries to `targetForTriple`:

```go
"aarch64-pc-windows-msvc":   {GOOS: "windows", GOARCH: "arm64", Ext: ".exe"},
"x86_64-unknown-linux-gnu":  {GOOS: "linux", GOARCH: "amd64"},
"aarch64-unknown-linux-gnu": {GOOS: "linux", GOARCH: "arm64"},
```

- [ ] **Step 4: Verify GREEN**

Run:

```bash
go test ./scripts/build-desktop-sidecar -count=1
```

Expected: PASS.

- [ ] **Step 5: Synchronize the release version**

Replace `0.1.0` with `0.1.1` in:

```text
desktop/src-tauri/tauri.conf.json
desktop/src-tauri/Cargo.toml
scripts/releasemeta/main_test.go
```

Run from `desktop/src-tauri`:

```bash
cargo check --locked
```

If `--locked` reports the local package version mismatch, run:

```bash
cargo check
```

and verify that the only lockfile semantic change is:

```toml
name = "cy-kaf-client-desktop"
version = "0.1.1"
```

- [ ] **Step 6: Verify version metadata**

Run:

```bash
go test ./scripts/releasemeta -count=1
go run ./scripts/releasemeta \
  -tag v0.1.1 \
  -tauri-config desktop/src-tauri/tauri.conf.json \
  -cargo-toml desktop/src-tauri/Cargo.toml
```

Expected:

```text
tag=v0.1.1
version=0.1.1
is_release=true
```

- [ ] **Step 7: Run the submission gate and commit**

Classify as `P1` because release version and target behavior change. Impact is the desktop sidecar build for three new architectures; application runtime APIs and data are unchanged.

```bash
git add \
  scripts/build-desktop-sidecar/main.go \
  scripts/build-desktop-sidecar/main_test.go \
  scripts/releasemeta/main_test.go \
  desktop/src-tauri/tauri.conf.json \
  desktop/src-tauri/Cargo.toml \
  desktop/src-tauri/Cargo.lock
git commit
```

Use a full-path commit body containing the observed focused tests, version validator result, security scan, and rollback by reverting this commit.

### Task 2: Failing Six-Platform Release Contract

**Files:**
- Create: `desktop/src-tauri/src/release_contract_tests.rs`
- Modify: `desktop/src-tauri/src/lib.rs`

**Interfaces:**
- Consumes: `.github/workflows/ci.yml`, `README.md`, and `scripts/desktop-smoke-linux.sh`.
- Produces: regression assertions that prevent the six-platform matrix, seven-asset Release contract, or Linux smoke wiring from being silently removed.

- [ ] **Step 1: Register a test-only module**

At the end of `desktop/src-tauri/src/lib.rs`, add:

```rust
#[cfg(test)]
mod release_contract_tests;
```

- [ ] **Step 2: Write the failing workflow contract tests**

Create `release_contract_tests.rs` with helpers that read repository files at runtime:

```rust
use std::{fs, path::PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root")
}

fn repository_file(path: &str) -> String {
    fs::read_to_string(repository_root().join(path)).unwrap_or_default()
}
```

Add three tests:

```rust
#[test]
fn workflow_declares_all_six_native_desktop_targets() {
    let workflow = repository_file(".github/workflows/ci.yml");
    for required in [
        "runner: macos-15-intel",
        "target: x86_64-apple-darwin",
        "runner: macos-15",
        "target: aarch64-apple-darwin",
        "runner: windows-2025",
        "target: x86_64-pc-windows-msvc",
        "runner: windows-11-arm",
        "target: aarch64-pc-windows-msvc",
        "runner: ubuntu-24.04",
        "target: x86_64-unknown-linux-gnu",
        "runner: ubuntu-24.04-arm",
        "target: aarch64-unknown-linux-gnu",
    ] {
        assert!(workflow.contains(required), "missing {required}");
    }
}

#[test]
fn release_requires_six_installers_and_one_checksum_file() {
    let workflow = repository_file(".github/workflows/ci.yml");
    for suffix in [
        "macos-x86_64.dmg",
        "macos-aarch64.dmg",
        "windows-x86_64-setup.exe",
        "windows-aarch64-setup.exe",
        "linux-x86_64.AppImage",
        "linux-aarch64.AppImage",
    ] {
        assert!(workflow.contains(suffix), "missing {suffix}");
    }
    assert!(workflow.contains("\"SHA256SUMS.txt\""));
}

#[test]
fn linux_packages_have_native_smoke_and_user_documentation() {
    let workflow = repository_file(".github/workflows/ci.yml");
    let smoke = repository_file("scripts/desktop-smoke-linux.sh");
    let readme = repository_file("README.md");

    assert!(workflow.contains("Smoke Linux AppImage"));
    assert!(workflow.contains("scripts/desktop-smoke-linux.sh"));
    assert!(smoke.contains("APPIMAGE_EXTRACT_AND_RUN=1"));
    assert!(smoke.contains("--desktop --no-browser"));
    assert!(readme.contains("linux-x86_64.AppImage"));
    assert!(readme.contains("linux-aarch64.AppImage"));
}
```

- [ ] **Step 3: Run the focused tests and verify RED**

Run:

```bash
cd desktop/src-tauri
cargo test --locked release_contract_tests -- --nocapture
```

Expected: all three tests FAIL because the new runners, assets, Linux smoke script, and README entries do not exist.

- [ ] **Step 4: Commit the failing tests with the implementation**

Do not commit a permanently red branch. Keep these tests uncommitted until Tasks 3–5 make them green.

### Task 3: Linux AppImage Native Smoke

**Files:**
- Create: `scripts/desktop-smoke-linux.sh`

**Interfaces:**
- Consumes: exactly one AppImage path and a Linux/X11 environment supplied by `xvfb-run`.
- Produces: `DESKTOP LINUX SMOKE OK` only after the desktop process, native window, Go sidecar, loopback listener, and shutdown lifecycle are verified.

- [ ] **Step 1: Implement strict input and environment validation**

Start the script with:

```bash
#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <appimage>" >&2
  exit 2
fi
if [[ "$(uname -s)" != "Linux" ]]; then
  echo "desktop Linux smoke requires Linux" >&2
  exit 2
fi
for command_name in lsof pgrep ps sed ss xdotool; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "required command not found: $command_name" >&2
    exit 2
  }
done
```

Resolve the AppImage to an absolute path, require it to be executable, create a dedicated `mktemp -d` directory, and install an `EXIT` trap that terminates recorded processes and deletes only that validated temporary directory.

- [ ] **Step 2: Start the AppImage with isolated state**

Write this exact test configuration:

```yaml
kafka:
  clusters: []
```

Start the package with:

```bash
APPIMAGE_EXTRACT_AND_RUN=1 \
CY_KAF_DESKTOP_TEST_CONFIG="$config_path" \
XDG_CONFIG_HOME="$smoke_directory/config" \
XDG_CACHE_HOME="$smoke_directory/cache" \
XDG_DATA_HOME="$smoke_directory/data" \
"$appimage_path" >"$stdout_log" 2>"$stderr_log" &
shell_pid=$!
```

- [ ] **Step 3: Verify window, sidecar, listener, and shutdown**

Within 30 seconds:

- require the shell process to remain alive;
- find one visible X11 window owned by `shell_pid` named `Cy KafClient`;
- find one direct child whose command contains `/cy-kaf-client --desktop --no-browser`;
- use `lsof` to require exactly one `127.0.0.1:<port>` TCP listener owned by the sidecar.

Close the native window with:

```bash
xdotool windowclose "$window_id"
```

Within five seconds require the shell, sidecar, and listener to disappear. Wait for the shell and emit:

```text
DESKTOP LINUX SMOKE OK
```

On failure, print at most the first 120 lines of captured stdout and stderr.

- [ ] **Step 4: Validate script syntax and static contract**

Run:

```bash
bash -n scripts/desktop-smoke-linux.sh
cd desktop/src-tauri
cargo test --locked release_contract_tests::linux_packages_have_native_smoke_and_user_documentation -- --nocapture
```

Expected: the shell syntax command passes; the Rust test still FAILS only because workflow and README wiring are not implemented yet.

### Task 4: Six-Platform Native CI Matrix

**Files:**
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: six native runner/target matrix entries and Linux system dependencies.
- Produces: six normalized desktop artifacts that have each passed a native smoke test.

- [ ] **Step 1: Expand the fast Go build matrix**

Add:

```yaml
- {goos: windows, goarch: arm64}
- {goos: linux, goarch: amd64}
- {goos: linux, goarch: arm64}
```

Keep the existing Darwin and Windows x64 entries.

- [ ] **Step 2: Add the three desktop matrix entries**

Add:

```yaml
- runner: windows-11-arm
  target: aarch64-pc-windows-msvc
  bundles: nsis
  smoke: windows
  macos_min: ""
  asset: windows-aarch64
  file_arch: ""
- runner: ubuntu-24.04
  target: x86_64-unknown-linux-gnu
  bundles: appimage
  smoke: linux
  macos_min: ""
  asset: linux-x86_64
  file_arch: "x86-64"
- runner: ubuntu-24.04-arm
  target: aarch64-unknown-linux-gnu
  bundles: appimage
  smoke: linux
  macos_min: ""
  asset: linux-aarch64
  file_arch: "ARM aarch64"
```

Add an empty `file_arch` field to the existing entries so matrix access is uniform.

- [ ] **Step 3: Install Linux build and smoke dependencies**

Before Rust setup, add a Linux-only Bash step:

```yaml
- name: Install Linux desktop dependencies
  if: matrix.smoke == 'linux'
  shell: bash
  run: |
    sudo apt-get update
    sudo apt-get install -y \
      build-essential \
      curl \
      file \
      libayatana-appindicator3-dev \
      libssl-dev \
      libwebkit2gtk-4.1-dev \
      libxdo-dev \
      librsvg2-dev \
      lsof \
      patchelf \
      wget \
      xdotool \
      xvfb
```

- [ ] **Step 4: Build and smoke Linux AppImages**

Add:

```yaml
- name: Build Linux native package
  if: matrix.smoke == 'linux'
  working-directory: desktop
  shell: bash
  run: >-
    cargo tauri build
    --target "${{ matrix.target }}"
    --bundles "${{ matrix.bundles }}"
```

Then add a smoke step that:

- finds exactly one `bundle/appimage/*.AppImage`;
- makes it executable;
- requires `file` output to contain `${{ matrix.file_arch }}`;
- runs `xvfb-run -a bash scripts/desktop-smoke-linux.sh "$appimage"`.

- [ ] **Step 5: Normalize Linux assets**

Add a Linux-only Bash step that finds exactly one AppImage and copies it to:

```text
release-assets/Cy-KafClient_${version}_${matrix.asset}.AppImage
```

Do not upload Tauri-generated signatures or updater artifacts.

- [ ] **Step 6: Run static workflow checks**

Run:

```bash
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/ci.yml
cd desktop/src-tauri
cargo test --locked release_contract_tests::workflow_declares_all_six_native_desktop_targets -- --nocapture
```

Expected: both pass.

### Task 5: Release Aggregation and User Documentation

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `README.md`

**Interfaces:**
- Consumes: six normalized `release-*` workflow artifacts.
- Produces: a public final Release containing exactly six installers plus `SHA256SUMS.txt`, with accurate compatibility text.

- [ ] **Step 1: Expand both release asset allowlists**

In both the checksum and publish steps, require:

```bash
"Cy-KafClient_${RELEASE_VERSION}_macos-x86_64.dmg"
"Cy-KafClient_${RELEASE_VERSION}_macos-aarch64.dmg"
"Cy-KafClient_${RELEASE_VERSION}_windows-x86_64-setup.exe"
"Cy-KafClient_${RELEASE_VERSION}_windows-aarch64-setup.exe"
"Cy-KafClient_${RELEASE_VERSION}_linux-x86_64.AppImage"
"Cy-KafClient_${RELEASE_VERSION}_linux-aarch64.AppImage"
```

Keep `SHA256SUMS.txt` only in the publish-step expected list because it is generated after installer validation.

- [ ] **Step 2: Update release compatibility notice**

Use a notice that retains the existing unsigned warnings and adds:

```text
- Windows ARM64: Windows 11 on ARM
- Linux x86_64: 64-bit x86 Linux (Ubuntu 24.04 build baseline)
- Linux ARM64: 64-bit ARM Linux (Ubuntu 24.04 build baseline)

Linux AppImages may require `chmod +x`. A compatible graphical session and
WebKitGTK runtime are required.
```

- [ ] **Step 3: Update README**

Change the opening platform description to macOS, Windows, and Linux. Add rows for all three new packages, Linux executable-bit guidance, Windows ARM64 and Linux source-build examples, Linux configuration path, and change “三个原生 runner” to “六个原生 runner”.

Do not claim all Linux distributions or Windows ARM versions are tested.

- [ ] **Step 4: Verify the release contract is GREEN**

Run:

```bash
cd desktop/src-tauri
cargo test --locked release_contract_tests -- --nocapture
cd ../..
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/ci.yml
git diff --check
```

Expected: all commands pass.

- [ ] **Step 5: Run the submission gate and commit Tasks 2–5**

Classify as `P1`: a CI defect could publish missing or unusable installers, but it does not change runtime data or security boundaries. Include:

- failing contract tests observed before implementation;
- passing sidecar, Rust contract, shell syntax, actionlint, and documentation checks;
- security scan result;
- Linux runtime smoke deferred to the native GitHub runners;
- rollback by reverting the CI/script/docs commit.

Stage only:

```text
.github/workflows/ci.yml
README.md
scripts/desktop-smoke-linux.sh
desktop/src-tauri/src/lib.rs
desktop/src-tauri/src/release_contract_tests.rs
```

### Task 6: Full Local Verification and Review

**Files:**
- Verify all changed files; no planned production edits.

**Interfaces:**
- Consumes: the complete `v0.1.1` candidate.
- Produces: local evidence sufficient to push the implementation branch.

- [ ] **Step 1: Run focused checks**

```bash
go test ./scripts/build-desktop-sidecar ./scripts/releasemeta -count=1
bash -n scripts/desktop-smoke-linux.sh
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/ci.yml
cd desktop/src-tauri
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

- [ ] **Step 2: Run the full repository gate**

From the repository root:

```bash
make verify
```

Expected: backend lint/race/coverage, frontend lint/tests/build, generated contract check, Rust tests, Clippy, and builds all pass.

- [ ] **Step 3: Review the complete diff**

Verify:

- only six intended installer names are accepted;
- platform artifacts cannot overwrite one another;
- Linux jobs run on their native architecture;
- Linux-only apt and smoke steps cannot execute on Windows/macOS;
- Windows ARM uses the same tested installer smoke;
- default permissions remain read-only;
- release checkout remains pinned to the validated immutable commit;
- no secrets, tokens, credentials, private endpoints, or debug artifacts were introduced.

- [ ] **Step 4: Apply review fixes with TDD**

For every review finding, add or strengthen a failing regression assertion first, observe RED, apply the smallest fix, and rerun focused plus affected tests.

- [ ] **Step 5: Run the final submission gate**

Inspect staged versus unstaged content, classify final risk and impact, run the security pass, validate commit subjects, and ensure every implementation commit has exact observed evidence.

### Task 7: Push, Native CI, and v0.1.1 Release

**Files:**
- No planned source edits unless remote CI reveals a reproducible defect.

**Interfaces:**
- Consumes: reviewed commits on `main`.
- Produces: a public `v0.1.1` GitHub Release with seven verified assets.

- [ ] **Step 1: Integrate and push**

Fast-forward the reviewed implementation branch into local `main`, confirm:

```bash
git status --short --branch
git rev-parse HEAD
git rev-parse origin/main
```

Push:

```bash
git push origin main
```

- [ ] **Step 2: Monitor ordinary main CI**

Require success for:

- version-check, backend, frontend, contract-check;
- six fast Go target builds;
- six native desktop-package jobs;
- Windows x64 and ARM64 NSIS install/start/uninstall smoke;
- macOS Intel and Apple Silicon native smoke;
- Linux x86_64 and ARM64 AppImage architecture/lifecycle smoke.

If a job fails, inspect its exact failing step, reproduce or encode the failure as a regression test, fix on `main`, rerun submission gates, commit, push, and wait for a complete green run.

- [ ] **Step 3: Create and push the release tag**

Only after ordinary CI is fully green:

```bash
git tag -a v0.1.1 -m "Cy-KafClient v0.1.1"
git push origin refs/tags/v0.1.1
```

- [ ] **Step 4: Monitor the tag run**

Require `run_attempt=1`, `head_branch=v0.1.1`, the expected immutable `head_sha`, and overall `conclusion=success`.

- [ ] **Step 5: Verify Release metadata and exact asset set**

Require:

```text
tag_name=v0.1.1
draft=false
prerelease=false
assets=7
```

The exact assets are the six installer names from Task 5 plus `SHA256SUMS.txt`.

- [ ] **Step 6: Download and independently verify artifacts**

Download all seven assets to a dedicated `mktemp -d` directory and run:

```bash
shasum -a 256 -c SHA256SUMS.txt
```

Compare each local digest to the GitHub asset digest. Inspect:

- both DMGs: main executable and sidecar Mach-O architecture plus minimum OS;
- both Windows packages: NSIS executable format and the successful native Windows smoke evidence;
- both AppImages: ELF x86-64 or ARM aarch64 header;
- Release body: six-platform compatibility and unsigned/executable-bit warnings.

- [ ] **Step 7: Final repository and tag check**

Require:

```bash
git status --short --branch
git rev-parse HEAD
git rev-parse origin/main
git rev-list -n 1 v0.1.1
git ls-remote --tags origin refs/tags/v0.1.1 'refs/tags/v0.1.1^{}'
```

All commit values must agree after peeling the annotated tag.
