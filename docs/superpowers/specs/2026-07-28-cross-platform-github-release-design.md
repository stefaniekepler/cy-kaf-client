# GitHub 跨平台桌面发布设计

日期：2026-07-28

## 目标

扩展现有 `.github/workflows/ci.yml`，在保留普通分支和 Pull Request
质量检查的基础上，为版本标签提供可重复执行的 GitHub Release 流程。

首个版本为 `v0.1.0`，正式 Release 包含：

- macOS Intel DMG，目标兼容 macOS 13.0 及以上；
- macOS Apple Silicon DMG，目标兼容 macOS 12.0 及以上；
- Windows x64 NSIS 安装程序；
- 三个平台安装包对应的 SHA-256 校验清单。

本阶段发布未签名安装包。发布说明必须明确 macOS Gatekeeper 和 Windows
SmartScreen 可能显示安全提示，并为后续接入平台签名凭据保留扩展空间。

## 当前状态

仓库已有 backend、frontend、contract-check、跨平台 Go 编译和 Tauri
桌面打包任务。桌面矩阵已经覆盖：

- `macos-15-intel` / `x86_64-apple-darwin`；
- `macos-15` / `aarch64-apple-darwin`；
- `windows-2025` / `x86_64-pc-windows-msvc`。

现有工作流只在 `main` 推送和 Pull Request 时运行，只上传 Workflow
Artifacts，不创建 GitHub Release。

首次 GitHub CI 运行在 backend job 中失败。原因是 golangci-lint v2
不再接受 `issues.exclude-files`，因此打包矩阵被依赖关系跳过。实现发布
流程前必须将该配置迁移至 `linters.exclusions.paths`。

## 方案选择

### 采用：扩展现有 CI

在现有工作流中增加标签和手动触发入口、版本校验、产物规范化以及单一
Release job。该方案直接复用当前 sidecar 构建、Tauri 打包和平台冒烟测试，
不会维护两套容易漂移的构建逻辑。

### 未采用：独立 release.yml

独立工作流的权限边界更直观，但会复制环境安装、sidecar 构建、Tauri
打包和冒烟测试步骤。对当前单仓库而言，重复维护成本高于隔离收益。

### 未采用：tauri-apps/tauri-action 直接发布

官方 Action 能缩短常规 Tauri 发布配置，但当前工程需要先生成 React
前端、交叉编译 Go sidecar，并执行自定义 macOS 和 Windows 冒烟测试。
保留现有显式构建步骤更易审查和诊断。

## 触发与版本规则

工作流保留以下普通 CI 入口：

- 推送到 `main`；
- Pull Request。

增加以下发布入口：

- 推送符合 `v*` 的 Git 标签；
- `workflow_dispatch` 手动触发，且必须提供已经存在的版本标签。

发布标签必须严格符合 `vX.Y.Z`。去掉 `v` 后的版本必须同时等于：

- `desktop/src-tauri/tauri.conf.json` 中的 `version`；
- `desktop/src-tauri/Cargo.toml` 中的 package version。

任何不一致都在开始平台构建前失败。Tauri 配置是用户可见应用版本的权威
来源，Cargo package version 必须与其同步。前端 `package.json` 的版本不参与
桌面 Release 校验。

手动运行必须 checkout 输入标签，而不是默认分支 HEAD。发布任务通过
`gh release view` 判断同一标签的 Release 是否已存在：不存在时创建，存在时
覆盖同名附件，从而支持失败后的幂等重跑。

## 工作流结构

```text
main / PR ──> backend + frontend + contract-check ──> desktop matrix ──> CI artifacts
v* 标签   ──> version-check ───────────────────────> desktop matrix ──> release
手动标签  ──> version-check ───────────────────────> desktop matrix ──> release
```

### 质量检查

- 修复 `.golangci.yml` 的 v2 配置结构；
- 将 `actions/checkout`、`actions/setup-go`、`actions/setup-node` 和
  `golangci/golangci-lint-action` 升级到使用 Node 24 的受支持主版本；
- 保留现有 Go race/覆盖率门槛、前端 lint/Jest/build 和契约生成一致性检查；
- 标签发布必须依赖全部质量检查，不允许绕过。

### 平台构建矩阵

| 平台 | Runner | Rust target | 最低系统 | Release 产物 |
| --- | --- | --- | --- | --- |
| macOS Intel | `macos-15-intel` | `x86_64-apple-darwin` | macOS 13.0 | DMG |
| macOS Apple Silicon | `macos-15` | `aarch64-apple-darwin` | macOS 12.0 | DMG |
| Windows x64 | `windows-2025` | `x86_64-pc-windows-msvc` | Windows 10/11 | NSIS EXE |

Intel 构建同时设置：

```text
MACOSX_DEPLOYMENT_TARGET=13.0
bundle.macOS.minimumSystemVersion=13.0
```

Apple Silicon 构建同时设置：

```text
MACOSX_DEPLOYMENT_TARGET=12.0
bundle.macOS.minimumSystemVersion=12.0
```

双重设置确保 Rust 链接目标与 Tauri `Info.plist` 声明一致。GitHub 当前没有
macOS 13 Intel runner，因此 Release 描述为“目标兼容 macOS 13+”，不宣称已在
macOS 13 实机验证。Apple Silicon 硬件从 macOS 11 开始可用，但本应用捆绑的
Go 1.26 sidecar 官方最低要求是 macOS 12，因此完整应用不能声明支持 macOS 11。

每个平台继续执行已有冒烟测试。任一平台失败时，汇总发布任务不会启动。

## 产物处理

平台 job 在冒烟成功后，将安装包复制到稳定名称：

```text
Cy-KafClient_0.1.0_macos-x86_64.dmg
Cy-KafClient_0.1.0_macos-aarch64.dmg
Cy-KafClient_0.1.0_windows-x86_64-setup.exe
```

普通 `main` 和 Pull Request 运行继续将这些文件保存为 Workflow Artifacts。
版本发布运行由单一 Release job 下载三个 Artifact，验证每个平台恰好有一个
安装包，然后生成：

```text
SHA256SUMS.txt
```

Release 不直接上传 `.app` 目录，也不发布没有前端资源的裸 Go 编译门禁产物。

## Release 创建

Release job 必须在所有质量检查和平台 job 成功后运行，并执行：

1. 下载三平台安装包；
2. 校验文件数量、名称和版本；
3. 生成并复核 SHA-256 清单；
4. 创建或更新与标签同名的正式 GitHub Release；
5. 上传三个安装包和 `SHA256SUMS.txt`；
6. 使用 GitHub 自动生成的 Release Notes，并追加未签名提示和兼容性说明。

工作流默认权限为 `contents: read`。只有 Release job 设置
`contents: write`。不引入长期 PAT，使用当前运行自动生成的
`GITHUB_TOKEN`。

同一标签使用 GitHub Actions concurrency group 串行化，且不取消已经开始的
发布，避免两个运行同时覆盖附件。

## 失败处理

- 标签格式或版本不一致：在构建前失败，不创建 Release；
- lint、测试、构建或冒烟失败：不运行 Release job；
- 平台产物缺失或数量异常：Release job 失败，不创建新 Release；
- 上传过程中失败：保留可观察的失败运行；重新执行同一标签时覆盖同名附件；
- 已存在同名 Release：不创建重复 Release；
- 不自动删除标签或 Release，避免失败处理扩大为不可逆操作。

## 验证与发布顺序

本地实现验证包括：

- YAML 语法和 GitHub Actions 静态检查；
- golangci-lint 配置校验和项目 lint；
- 版本校验脚本的成功、标签格式错误、版本不一致测试；
- Go、前端和桌面现有测试；
- 对产物规范化与文件计数逻辑进行 shell/PowerShell 静态检查。

远端发布顺序固定为：

1. 提交并推送 CI 修改到 `main`；
2. 监控普通 CI，确认所有 job 成功；
3. 创建并推送 `v0.1.0` 标签；
4. 监控标签流水线直至完成；
5. 读取 GitHub Release 元数据，确认标签和四个附件；
6. 下载 Release 附件并重新计算 SHA-256，与清单逐项比对。

## 不在本次范围

- Apple Developer ID 签名和公证；
- Windows Authenticode 签名；
- 自动更新器；
- MSI、Microsoft Store 或 Mac App Store 发布；
- Windows ARM64；
- 宣称 macOS 13 实机验证。

## 参考资料

- GitHub-hosted runner：
  <https://docs.github.com/en/actions/how-tos/write-workflows/choose-where-workflows-run/choose-the-runner-for-a-job>
- GitHub Actions 权限：
  <https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax>
- Tauri macOS 最低系统版本：
  <https://v2.tauri.app/distribute/macos-application-bundle/>
- Tauri 版本管理：
  <https://v2.tauri.app/distribute/>
- Go 平台最低要求：
  <https://go.dev/wiki/MinimumRequirements>
- Rust Apple 平台支持：
  <https://doc.rust-lang.org/rustc/platform-support/apple-darwin.html>
