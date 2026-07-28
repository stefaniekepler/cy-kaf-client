# Additional Platform GitHub Release Design

日期：2026-07-28

## 目标

在现有 GitHub Actions 桌面发布流程中增加以下正式 Release 产物：

- Windows ARM64 NSIS 安装程序；
- Linux x86_64 AppImage；
- Linux ARM64 AppImage。

保留现有 macOS Intel、macOS Apple Silicon 和 Windows x64 产物。下一版本为
`v0.1.1`，正式 Release 必须精确包含 6 个安装包和 1 个
`SHA256SUMS.txt`。

## 现状

当前 `.github/workflows/ci.yml` 已具备：

- 版本标签、Tauri 配置和 Cargo package 版本一致性校验；
- 固定到不可变提交的 backend、frontend、contract 和桌面构建任务；
- macOS Intel、macOS Apple Silicon、Windows x64 原生 runner；
- macOS 可执行程序和 Windows NSIS 安装/启动/卸载冒烟；
- 稳定资产命名、SHA-256 清单和幂等 GitHub Release 发布。

`scripts/build-desktop-sidecar` 当前只识别三个既有 Rust target。Release
资产列表和兼容性说明同样固定为三个平台。

## 方案比较

### 采用：三个新增平台均使用原生 GitHub-hosted runner

- Windows ARM64 使用 `windows-11-arm`；
- Linux x86_64 使用 `ubuntu-24.04`；
- Linux ARM64 使用 `ubuntu-24.04-arm`。

对应 Rust target 为：

- `aarch64-pc-windows-msvc`；
- `x86_64-unknown-linux-gnu`；
- `aarch64-unknown-linux-gnu`。

该方案能在目标 CPU 架构上完成 Tauri 构建和原生冒烟。Linux ARM
AppImage 也必须原生构建，因为 Tauri 使用的 `linuxdeploy` 不支持从 x86_64
直接交叉编译 ARM AppImage。

### 未采用：x86_64 runner 交叉编译并用 QEMU 验收

该方案减少 runner label 数量，但 Windows ARM 图形应用验证复杂，Linux ARM
AppImage 工具链不支持直接交叉编译。QEMU 构建耗时更长，诊断也更困难。

### 未采用：只发布裸二进制或压缩包

裸二进制可减少系统依赖，但会失去现有桌面安装体验、图标、资源文件和 sidecar
捆绑，不构成与现有三平台相同等级的桌面支持。

## 构建矩阵

桌面矩阵扩展为六项：

| 平台 | Runner | Rust target | Bundle | 稳定资产后缀 |
| --- | --- | --- | --- | --- |
| macOS Intel | `macos-15-intel` | `x86_64-apple-darwin` | `app,dmg` | `macos-x86_64.dmg` |
| macOS Apple Silicon | `macos-15` | `aarch64-apple-darwin` | `app,dmg` | `macos-aarch64.dmg` |
| Windows x64 | `windows-2025` | `x86_64-pc-windows-msvc` | `nsis` | `windows-x86_64-setup.exe` |
| Windows ARM64 | `windows-11-arm` | `aarch64-pc-windows-msvc` | `nsis` | `windows-aarch64-setup.exe` |
| Linux x86_64 | `ubuntu-24.04` | `x86_64-unknown-linux-gnu` | `appimage` | `linux-x86_64.AppImage` |
| Linux ARM64 | `ubuntu-24.04-arm` | `aarch64-unknown-linux-gnu` | `appimage` | `linux-aarch64.AppImage` |

普通 Go 编译矩阵同步增加 Windows ARM64、Linux x86_64 和 Linux ARM64，
确保无桌面环境时也能快速检查对应 sidecar/server target。

## Linux 构建

Linux runner 在安装 Rust 和 Tauri CLI 前安装 Tauri v2 所需的 Ubuntu
依赖：

```text
libwebkit2gtk-4.1-dev
libappindicator3-dev
librsvg2-dev
patchelf
libfuse2
xvfb
```

每个 Linux job 在自身原生架构上：

1. 构建 React 前端；
2. 用 `scripts/build-desktop-sidecar` 生成同架构 Go sidecar；
3. 生成一个 AppImage；
4. 检查 AppImage 文件数量和 ELF 架构；
5. 在 Xvfb 中使用 AppImage 的 extract-and-run 模式启动桌面程序；
6. 确认桌面进程派生 `--desktop --no-browser` sidecar；
7. 确认 sidecar 仅监听 loopback；
8. 正常终止桌面进程并确认 sidecar 一并退出；
9. 规范化并上传稳定名称的 Release 资产。

Linux 冒烟使用独立脚本，避免给现有 macOS 脚本加入大量平台分支。

## Windows ARM64 构建

Windows ARM64 复用现有 NSIS 构建与 `scripts/desktop-smoke.ps1`。由于 runner
本身为 ARM64，冒烟会原生执行安装程序和应用，覆盖：

- 静默安装；
- 安装后的内部可执行文件；
- 产品注册和桌面快捷方式；
- 窗口与 sidecar 启动；
- 正常关闭后的进程清理；
- 静默卸载。

sidecar 构建工具增加 `aarch64-pc-windows-msvc` 到
`GOOS=windows/GOARCH=arm64` 的显式映射。

## 资产与版本

`v0.1.1` 的安装包名称固定为：

```text
Cy-KafClient_0.1.1_macos-x86_64.dmg
Cy-KafClient_0.1.1_macos-aarch64.dmg
Cy-KafClient_0.1.1_windows-x86_64-setup.exe
Cy-KafClient_0.1.1_windows-aarch64-setup.exe
Cy-KafClient_0.1.1_linux-x86_64.AppImage
Cy-KafClient_0.1.1_linux-aarch64.AppImage
```

Release job 必须拒绝缺失、多余或名称不匹配的资产，并为这六个文件生成
`SHA256SUMS.txt`。Tauri 配置、Cargo package 和 Cargo lock 中当前应用
package 的版本同步提升到 `0.1.1`。

## 发布说明

Release 和 README 保留现有 macOS/Windows 未签名提示，并增加：

- Windows ARM64：Windows 11 on ARM；
- Linux x86_64：64-bit x86 Linux；
- Linux ARM64：64-bit ARM Linux；
- Linux AppImage 可能需要先添加可执行权限；
- Linux 桌面运行时依赖兼容的 WebKitGTK/图形会话。

不宣称已覆盖所有 Linux 发行版。构建基线为 Ubuntu 24.04，AppImage 的真实
兼容范围以目标机器的 glibc、WebKitGTK 和图形环境为准。

## 测试策略

实现遵循测试先行：

- 先扩展 sidecar target 单元测试，观察新增 target 因未实现而失败，再增加映射；
- 先增加 CI 契约测试，要求六项桌面矩阵、六个安装包名称、Linux 原生 runner、
  Linux 冒烟和新版兼容性说明存在，再修改工作流；
- Linux 冒烟脚本通过 shell 静态检查，并在两个 Linux 原生 runner 上执行；
- Windows ARM64 复用已经过 Windows x64 验证的同一冒烟脚本；
- 运行 Go、Rust、前端、工作流静态检查和 `make verify`；
- 推送 `main` 后先确认普通 CI 六个平台全部成功，再创建 `v0.1.1` 标签；
- 标签流水线成功后下载 7 个 Release 资产，重新计算 SHA-256；
- 检查 Windows 和 Linux 安装包架构，确认 GitHub Release 为公开正式版本。

## 失败与回滚

- 任一质量检查、平台构建或冒烟失败时不创建 Release；
- 新 runner 无法分配时保留可观察的失败运行，不降级为未经验证的交叉编译包；
- Release 资产集合异常时在上传前失败；
- 同标签重跑继续使用现有幂等发布逻辑；
- 不自动移动或删除远端标签；
- 如新增平台持续不可用，可回滚对应矩阵项、sidecar 映射、资产契约和文档，不影响
  原有三个平台。

## 不在本次范围

- `.deb`、`.rpm`、MSI 或商店包；
- 代码签名、公证和自动更新；
- Windows x86 32-bit；
- Linux GUI 发行版的穷举兼容测试；
- 私有或自托管 ARM runner。

## 参考资料

- GitHub-hosted runner：
  <https://docs.github.com/en/actions/reference/runners/github-hosted-runners>
- Tauri Windows installer：
  <https://v2.tauri.app/distribute/windows-installer/>
- Tauri AppImage：
  <https://v2.tauri.app/distribute/appimage/>
