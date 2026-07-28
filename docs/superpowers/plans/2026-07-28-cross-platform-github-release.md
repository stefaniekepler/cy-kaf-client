# Cross-Platform GitHub Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the existing GitHub Actions CI so a validated `v0.1.0` tag produces tested macOS Intel, macOS Apple Silicon, and Windows installers in a GitHub Release.

**Architecture:** Keep `.github/workflows/ci.yml` as the single source of build truth. Add a small Go command that validates and exports release metadata, feed its outputs into the existing quality and desktop matrix jobs, then let one permission-scoped release job aggregate normalized installers, generate checksums, and publish only after every dependency succeeds.

**Tech Stack:** GitHub Actions, Go 1.26, Node.js 22/pnpm 10, Rust 1.95/Tauri 2, NSIS, GitHub CLI, actionlint 1.7.12.

## Global Constraints

- Publish macOS Intel `x86_64-apple-darwin` as a DMG targeting macOS 13.0 or newer.
- Publish macOS Apple Silicon `aarch64-apple-darwin` as a DMG targeting macOS 12.0 or newer.
- Publish Windows `x86_64-pc-windows-msvc` as an NSIS `.exe` installer for Windows 10/11.
- Publish exactly three installers plus `SHA256SUMS.txt`.
- Keep installers unsigned and state Gatekeeper/SmartScreen implications in release notes.
- Release only an existing strict semantic tag matching `vX.Y.Z`.
- Require the tag version, Tauri version, Cargo package version, and Go sidecar version to agree.
- Keep default workflow permission at `contents: read`; grant `contents: write` only to the release job.
- Keep one workflow for CI and release; do not add `release.yml` or migrate to `tauri-apps/tauri-action`.
- Do not claim macOS 13 runtime testing; describe Intel as target-compatible with macOS 13+.
- Do not publish `.app` directories, unsigned plain executables, or the no-frontend Go build gate.

---

### Task 1: Release Metadata Validator

**Files:**
- Create: `scripts/releasemeta/main.go`
- Create: `scripts/releasemeta/main_test.go`

**Interfaces:**
- Consumes: `-tag`, `-tauri-config`, `-cargo-toml`, and optional `-github-output` flags.
- Produces: validated `tag`, semantic `version`, and `is_release` values; writes the same keys to the GitHub output file when requested.

- [ ] **Step 1: Write failing unit tests**

Create table-driven tests covering:

```go
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
            want:  metadata{Version: "0.1.0", IsRelease: false},
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
    }
    // Materialize tauri/cargo strings in t.TempDir(), call resolve, and assert.
}

func TestWriteGitHubOutput(t *testing.T) {
    // Write metadata{Tag:"v0.1.0", Version:"0.1.0", IsRelease:true}.
    // Assert exact output:
    // tag=v0.1.0
    // version=0.1.0
    // is_release=true
}
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
go test ./scripts/releasemeta -run 'TestResolve|TestWriteGitHubOutput' -count=1
```

Expected: FAIL because `metadata`, `resolve`, and `writeGitHubOutput` do not exist.

- [ ] **Step 3: Implement the minimal validator**

Implement:

```go
type metadata struct {
    Tag       string
    Version   string
    IsRelease bool
}

var semanticTag = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func resolve(tag, tauriPath, cargoPath string) (metadata, error)
func readTauriVersion(path string) (string, error)
func readCargoPackageVersion(path string) (string, error)
func writeGitHubOutput(path string, got metadata) error
```

Implementation rules:

- Parse `tauri.conf.json` with `encoding/json`.
- Scan only the first `[package]` table in `Cargo.toml`; read its quoted `version`.
- Reject missing/empty versions.
- Reject differing Tauri and Cargo versions.
- When tag is empty, return `IsRelease=false`.
- When tag is present, require strict `vX.Y.Z` and equality with the application version.
- Open the GitHub output file with append-only mode and `0600` for creation.
- Print human-readable metadata to stdout without printing environment secrets.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run:

```bash
go test ./scripts/releasemeta -count=1
go run ./scripts/releasemeta \
  -tag v0.1.0 \
  -tauri-config desktop/src-tauri/tauri.conf.json \
  -cargo-toml desktop/src-tauri/Cargo.toml
```

Expected:

```text
tag=v0.1.0
version=0.1.0
is_release=true
```

- [ ] **Step 5: Run the submission gate and commit**

Classify as `P2`, impact `scripts/releasemeta` and CI version validation. Run the secret scan, `go test ./scripts/releasemeta -count=1`, subject validator, then:

```bash
git add scripts/releasemeta/main.go scripts/releasemeta/main_test.go
git commit \
  -m "ci: 增加发布标签与桌面应用版本一致性校验并输出可信构建元数据" \
  -m "Change Risk: P2 — 新增隔离的发布元数据校验器，已覆盖正常与拒绝路径" \
  -m "Impact: GitHub Actions 发布版本输入；不影响应用运行时"
```

### Task 2: Restore the CI Quality Gate

**Files:**
- Modify: `.golangci.yml`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: golangci-lint v2 configuration and GitHub-hosted Node 24 Actions runtime.
- Produces: a backend quality job that no longer blocks the desktop matrix because of schema-invalid configuration.

- [ ] **Step 1: Reproduce the current config failure**

Run:

```bash
golangci-lint config verify
```

Expected: FAIL because `issues.exclude-files` is not accepted by the v2 schema.

- [ ] **Step 2: Make the minimal golangci-lint v2 migration**

Replace:

```yaml
issues:
  exclude-files: [".*\\.gen\\.go$"]
```

with:

```yaml
linters:
  enable: [depguard, errcheck, govet, staticcheck, ineffassign, unused, misspell]
  exclusions:
    paths:
      - ".*\\.gen\\.go$"
```

Preserve the existing `linters.settings.depguard` rules under the same `linters` map.

- [ ] **Step 3: Upgrade affected Actions to Node 24 releases**

In `.github/workflows/ci.yml`, replace:

```yaml
actions/checkout@v4
actions/setup-go@v5
actions/setup-node@v4
golangci/golangci-lint-action@v8
```

with:

```yaml
actions/checkout@v6
actions/setup-go@v6
actions/setup-node@v6
golangci/golangci-lint-action@v9
```

Pin the lint binary input:

```yaml
with:
  version: v2.12.2
```

- [ ] **Step 4: Verify the repaired gate**

Run:

```bash
golangci-lint config verify
golangci-lint run
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/ci.yml
```

Expected: all commands exit 0.

- [ ] **Step 5: Run the submission gate and commit**

Classify as `P2`, impact backend lint execution and Actions runtime only:

```bash
git add .golangci.yml .github/workflows/ci.yml
git commit \
  -m "ci: 修复 golangci-lint v2 配置并升级受支持的 GitHub Actions 运行时" \
  -m "Change Risk: P2 — 恢复既有质量门禁并更新 Actions 主版本，已通过本地静态检查" \
  -m "Impact: GitHub backend CI 与工作流运行环境；不改变产品运行逻辑"
```

### Task 3: Extend CI into a Tag-Gated Release Pipeline

**Files:**
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `scripts/releasemeta` outputs `tag`, `version`, `is_release`.
- Produces: normalized three-platform Workflow Artifacts and a four-asset GitHub Release.

- [ ] **Step 1: Add a failing structural assertion**

Run:

```bash
rg -n 'workflow_dispatch|version-check|MACOSX_DEPLOYMENT_TARGET|release-assets|SHA256SUMS|gh release create' .github/workflows/ci.yml
```

Expected: FAIL because the existing workflow has none of the release structure.

- [ ] **Step 2: Add triggers, least privilege, and metadata job**

Add:

```yaml
on:
  push:
    branches: [main]
    tags: ["v*"]
  pull_request:
  workflow_dispatch:
    inputs:
      release_tag:
        description: Existing vX.Y.Z tag to build and publish
        required: true
        type: string

permissions:
  contents: read

concurrency:
  group: ci-${{ github.workflow }}-${{ github.event_name == 'workflow_dispatch' && inputs.release_tag || github.ref }}
  cancel-in-progress: ${{ github.event_name != 'workflow_dispatch' && !startsWith(github.ref, 'refs/tags/') }}

env:
  BUILD_REF: ${{ github.event_name == 'workflow_dispatch' && inputs.release_tag || github.ref }}
  RELEASE_TAG: ${{ github.event_name == 'workflow_dispatch' && inputs.release_tag || startsWith(github.ref, 'refs/tags/') && github.ref_name || '' }}
```

Add `version-check` with outputs:

```yaml
outputs:
  tag: ${{ steps.metadata.outputs.tag }}
  version: ${{ steps.metadata.outputs.version }}
  is_release: ${{ steps.metadata.outputs.is_release }}
```

Checkout `BUILD_REF`, run `scripts/releasemeta`, and pass `$GITHUB_OUTPUT`.
Make every other job depend on `version-check` and checkout the same `BUILD_REF`.

- [ ] **Step 3: Encode platform floors and stable artifact names**

Extend each desktop matrix row:

```yaml
- runner: macos-15-intel
  target: x86_64-apple-darwin
  bundles: app,dmg
  macos_min: "13.0"
  asset: macos-x86_64
- runner: macos-15
  target: aarch64-apple-darwin
  bundles: app,dmg
  macos_min: "12.0"
  asset: macos-aarch64
- runner: windows-2025
  target: x86_64-pc-windows-msvc
  bundles: nsis
  macos_min: ""
  asset: windows-x86_64
```

Build macOS with:

```bash
export MACOSX_DEPLOYMENT_TARGET="${{ matrix.macos_min }}"
cargo tauri build \
  --target "${{ matrix.target }}" \
  --bundles "${{ matrix.bundles }}" \
  --config "{\"bundle\":{\"macOS\":{\"minimumSystemVersion\":\"${{ matrix.macos_min }}\"}}}"
```

Build Windows with the existing NSIS command. Pass
`${{ needs.version-check.outputs.version }}` into the Go sidecar builder.

After smoke tests, require exactly one DMG or NSIS installer and copy it to:

```text
release-assets/Cy-KafClient_<version>_macos-x86_64.dmg
release-assets/Cy-KafClient_<version>_macos-aarch64.dmg
release-assets/Cy-KafClient_<version>_windows-x86_64-setup.exe
```

Upload only `release-assets/*`, with one Artifact per matrix row and
`if-no-files-found: error`.

- [ ] **Step 4: Add the single release aggregator**

Add an Ubuntu `release` job that:

```yaml
if: needs.version-check.outputs.is_release == 'true'
needs: [version-check, backend, frontend, contract-check, desktop-package]
permissions:
  contents: write
```

Use `actions/download-artifact@v4` with `pattern: cy-kaf-client-*` and
`merge-multiple: true`. In bash:

```bash
mapfile -t packages < <(
  find release-assets -maxdepth 1 -type f \
    \( -name '*.dmg' -o -name '*-setup.exe' \) | sort
)
test "${#packages[@]}" -eq 3
cd release-assets
sha256sum Cy-KafClient_* > SHA256SUMS.txt
sha256sum -c SHA256SUMS.txt
```

Create the first release only after all files exist:

```bash
notes=$'Unsigned installers. macOS may show Gatekeeper warnings and Windows may show SmartScreen warnings.\n\nCompatibility targets: Intel macOS 13+, Apple Silicon macOS 12+, Windows 10/11 x64.'
if gh release view "$RELEASE_TAG" >/dev/null 2>&1; then
  gh release upload "$RELEASE_TAG" release-assets/* --clobber
else
  gh release create "$RELEASE_TAG" release-assets/* \
    --verify-tag \
    --title "Cy KafClient ${VERSION}" \
    --generate-notes \
    --notes "$notes"
fi
```

Set `GH_TOKEN: ${{ github.token }}`, `RELEASE_TAG`, and `VERSION` in the
release step environment.

- [ ] **Step 5: Verify workflow semantics**

Run:

```bash
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/ci.yml
rg -n 'workflow_dispatch|version-check|MACOSX_DEPLOYMENT_TARGET|minimumSystemVersion|release-assets|SHA256SUMS|contents: write|gh release create' .github/workflows/ci.yml
```

Expected: actionlint exits 0 and every required release element is present.

- [ ] **Step 6: Run the submission gate and commit**

Classify as `P1`: it changes distribution behavior and external GitHub
Release output, but not runtime code. Include full decision/evidence/rollback
content with actionlint and metadata tests. Commit only after all required
fields are proven.

### Task 4: Update User-Facing Release Documentation

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: final asset names and compatibility floors from Task 3.
- Produces: concise download, compatibility, unsigned-package, and release-trigger guidance.

- [ ] **Step 1: Verify the current README lacks release instructions**

Run:

```bash
rg -n 'GitHub Release|macOS 13|macOS 12|SmartScreen|SHA256SUMS' README.md
```

Expected: FAIL because the current README says no formal installer is
available.

- [ ] **Step 2: Replace the obsolete statement**

Document:

- Releases page: `https://github.com/stefaniekepler/cy-kaf-client/releases`;
- Intel DMG targets macOS 13+;
- Apple Silicon DMG targets macOS 12+;
- Windows x64 NSIS targets Windows 10/11;
- packages are unsigned and may trigger Gatekeeper/SmartScreen;
- verify downloads with `SHA256SUMS.txt`;
- maintainers publish by pushing a matching `vX.Y.Z` tag.

Keep the existing local build commands.

- [ ] **Step 3: Verify documentation consistency**

Run:

```bash
rg -n 'macOS 13|macOS 12|Windows 10/11|Gatekeeper|SmartScreen|SHA256SUMS|vX.Y.Z' README.md
! rg -n '暂不提供正式签名的安装包' README.md
git diff --check
```

Expected: all required phrases exist, obsolete claim is absent, whitespace
check exits 0.

- [ ] **Step 4: Run the submission gate and commit**

Classify as `P3`, impact README only:

```bash
git add README.md
git commit \
  -m "docs: 补充三平台安装包下载、系统兼容范围、校验方式与未签名安全提示" \
  -m "Change Risk: P3 — 仅同步已设计的发布与安装说明，已完成关键词一致性检查" \
  -m "Impact: README 发布文档；无运行时影响"
```

### Task 5: Local Full Verification and Push

**Files:**
- Verify only; modify files only when a failing check identifies a real defect.

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces: a locally verified `main` ready for GitHub CI.

- [ ] **Step 1: Run static and focused validation**

Run:

```bash
go test ./scripts/releasemeta -count=1
golangci-lint config verify
golangci-lint run
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows/ci.yml
git diff --check origin/main...HEAD
```

- [ ] **Step 2: Run product test suites**

Run:

```bash
make test
cd frontend && pnpm exec jest --ci
cd ../desktop/src-tauri && \
  TAURI_CONFIG='{"bundle":{"externalBin":[]}}' cargo test --locked
```

Expected: Go coverage gate passes, all frontend suites pass, all Rust desktop
tests pass.

- [ ] **Step 3: Run the final submission/security gate**

Inspect:

```bash
git status --short
git diff --stat origin/main...HEAD
git diff --name-status origin/main...HEAD
git log --oneline origin/main..HEAD
```

Scan changed content for private keys, tokens, credentials, production
endpoints, permission widening, and accidental AI authorship trailers. Validate
every new commit subject.

- [ ] **Step 4: Push `main`**

Run:

```bash
git push origin main
```

Do not create the release tag yet.

- [ ] **Step 5: Monitor ordinary CI to a terminal state**

Poll:

```text
GET https://api.github.com/repos/stefaniekepler/cy-kaf-client/actions/runs
```

Require the push run for the new HEAD to complete successfully. If a job
fails, inspect its annotations/logs, make the smallest tested fix, commit it
through the submission gate, push, and repeat until green.

### Task 6: Publish and Verify `v0.1.0`

**Files:**
- No repository file changes expected.

**Interfaces:**
- Consumes: green `main` HEAD and version `0.1.0`.
- Produces: public `v0.1.0` tag, GitHub Release, three installers, checksum file, and final product evidence.

- [ ] **Step 1: Confirm release preconditions**

Run:

```bash
git status --short --branch
git fetch origin --tags
git ls-remote --tags origin refs/tags/v0.1.0
go run ./scripts/releasemeta \
  -tag v0.1.0 \
  -tauri-config desktop/src-tauri/tauri.conf.json \
  -cargo-toml desktop/src-tauri/Cargo.toml
```

Expected: clean synchronized `main`, no existing remote `v0.1.0`, metadata
validation succeeds.

- [ ] **Step 2: Create and push the release tag**

Run:

```bash
git tag -a v0.1.0 -m "Cy KafClient v0.1.0"
git push origin v0.1.0
```

- [ ] **Step 3: Monitor the tag workflow**

Poll GitHub Actions by tag and HEAD SHA until terminal. Require:

- backend success;
- frontend success;
- contract-check success;
- all three desktop matrix entries success;
- release job success.

Do not delete or move the tag automatically on failure. Diagnose and report
before any history-changing recovery.

- [ ] **Step 4: Verify GitHub Release metadata**

Use the public GitHub REST API and require:

```text
tag_name = v0.1.0
draft = false
prerelease = false
exact asset names:
  Cy-KafClient_0.1.0_macos-x86_64.dmg
  Cy-KafClient_0.1.0_macos-aarch64.dmg
  Cy-KafClient_0.1.0_windows-x86_64-setup.exe
  SHA256SUMS.txt
```

- [ ] **Step 5: Download and verify all release assets**

Create a temporary directory with `mktemp -d`, download the four
`browser_download_url` values, then run:

```bash
shasum -a 256 -c SHA256SUMS.txt
file Cy-KafClient_0.1.0_macos-x86_64.dmg
file Cy-KafClient_0.1.0_macos-aarch64.dmg
file Cy-KafClient_0.1.0_windows-x86_64-setup.exe
```

Expected: all checksums pass; both DMGs are valid disk images; Windows package
is a PE32+ x86-64 installer.

- [ ] **Step 6: Inspect packaged macOS products**

Mount each DMG read-only with `hdiutil attach`. For each `.app`:

- read `Contents/Info.plist`;
- require Intel `LSMinimumSystemVersion=13.0`;
- require Apple Silicon `LSMinimumSystemVersion=12.0`;
- run `file` on the Tauri executable and bundled sidecar;
- require `x86_64` for Intel and `arm64` for Apple Silicon;
- detach the mounted image.

The native GitHub matrix smoke tests are the runtime evidence. Local inspection
is architecture and packaging evidence only.

- [ ] **Step 7: Final verification report**

Report:

- GitHub repository and Release URL;
- release tag and commit SHA;
- successful workflow URL;
- four asset names and byte sizes;
- checksum verification result;
- packaged architecture and minimum macOS version evidence;
- Windows PE architecture evidence;
- explicit unsigned-package limitation;
- any remaining warning or unverified claim.
