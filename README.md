# cy-kaf-client

`cy-kaf-client` 是面向 macOS、Windows 和 Linux 的本地 Kafka 客户端，可通过原生桌面窗口连接 Kafka、Schema Registry、Kafka Connect 和 ksqlDB，无需额外部署服务端。

项目由 React 前端、Go 后端和 Tauri v2 桌面宿主组成。桌面应用会在本机启动仅监听 loopback 的
Go sidecar；CLI 与 MCP server 复用同一套后端能力。

## 核心能力

- 查看和管理集群、Broker、Topic、Consumer Group、消息、ACL 与 Client Quota。
- 支持 Schema Registry、Kafka Connect、ksqlDB、序列化和 Topic 数据分析。
- 提供桌面应用、CLI 以及面向 Codex、Claude Code 的 STDIO MCP server。
- 集群可配置为只读；MCP 默认关闭，启用后默认仅开放只读操作。
- 支持 SASL/PLAIN、SCRAM-SHA-256、SCRAM-SHA-512、SSL 与自定义 truststore。

## 快速开始

从 [GitHub Releases](https://github.com/stefaniekepler/cy-kaf-client/releases) 下载对应安装包：

| 平台 | 支持范围 | 安装包 |
| --- | --- | --- |
| macOS Intel | macOS 13 及以上 | `Cy-KafClient_<版本>_macos-x86_64.dmg` |
| macOS Apple Silicon | macOS 12 及以上 | `Cy-KafClient_<版本>_macos-aarch64.dmg` |
| Windows x64 | Windows 10/11 | `Cy-KafClient_<版本>_windows-x86_64-setup.exe` |
| Windows ARM64 | Windows 11 on ARM | `Cy-KafClient_<版本>_windows-aarch64-setup.exe` |
| Linux x86_64 | 64-bit x86 Linux，Ubuntu 24.04 构建基线 | `Cy-KafClient_<版本>_linux-x86_64.AppImage` |
| Linux ARM64 | 64-bit ARM Linux，Ubuntu 24.04 构建基线 | `Cy-KafClient_<版本>_linux-aarch64.AppImage` |

发布包暂未进行代码签名，因此 macOS Gatekeeper 或 Windows SmartScreen
可能显示安全提示。Linux AppImage 可能需要先执行 `chmod +x <文件名>`，并要求兼容的
图形会话和 WebKitGTK 运行时。安装或运行前请用 Release 中的 `SHA256SUMS.txt`
校验下载文件。

如需从源码构建，请安装 Go 1.26、Node.js 22、pnpm 10.26.1、Rust 1.95.0 和
Tauri CLI 2.11.4。

macOS：

```bash
make desktop-tools
APPLE_SIGNING_IDENTITY=- make desktop-package \
  RUST_TARGET="$(rustc --print host-tuple)" \
  DESKTOP_BUNDLES=app,dmg
```

Windows：

```powershell
make desktop-package RUST_TARGET=x86_64-pc-windows-msvc DESKTOP_BUNDLES=nsis
make desktop-package RUST_TARGET=aarch64-pc-windows-msvc DESKTOP_BUNDLES=nsis
```

Linux：

```bash
make desktop-package \
  RUST_TARGET="$(rustc --print host-tuple)" \
  DESKTOP_BUNDLES=appimage
```

维护者创建并推送与应用版本一致的 `vX.Y.Z` 标签后，CI 会在六个原生 runner
完成构建、安装冒烟测试和 Release 发布。

桌面客户端默认读取：

- macOS：`~/Library/Application Support/cy-kaf-client/config.yaml`
- Windows：`%APPDATA%\cy-kaf-client\config.yaml`
- Linux：`$XDG_CONFIG_HOME/cy-kaf-client/config.yaml`，未设置时使用
  `~/.config/cy-kaf-client/config.yaml`

也可以通过 CLI 启动：

```bash
./cy-kaf-client --config config.yaml
```

常用参数包括 `--port`、`--config`、`--no-browser` 和 `--debug`。

## MCP 集成

在桌面应用的 `Settings` 中启用 MCP，然后选择 `Configure Codex` 或
`Configure Claude Code` 写入 STDIO server 配置。修改配置或权限后，需要重新打开对应客户端。

也可以手动启动：

```text
cy-kaf-client mcp
cy-kaf-client mcp --config /absolute/path/config.yaml
```

MCP 的 STDOUT 仅用于 JSON-RPC，日志写入 STDERR。开启写操作需要单独确认，且不会覆盖集群自身的
只读设置。

## 示例配置

```yaml
server:
  port: 8080

kafka:
  clusters:
    - name: local
      bootstrapServers: localhost:9092
      readOnly: false
      # properties:
      #   security.protocol: SSL
      # schemaRegistry: http://localhost:8081
      # kafkaConnect:
      #   - name: local-connect
      #     address: http://localhost:8083
      # ksqldbServer: http://localhost:8088
```

## 开发与构建

```bash
make verify            # lint、单测、覆盖率、契约检查和构建
make test              # Go 单测与覆盖率门禁
make test-fe           # 前端 Jest 测试
make test-integration  # 集成测试，需要可用的容器运行时
make build             # 构建当前平台 CLI
make release-build     # 构建多平台 CLI/sidecar
```

架构约束、测试要求和环境说明见 [CLAUDE.md](CLAUDE.md)。

## 许可证

项目基于 [Apache License 2.0](LICENSE) 发布。第三方组件与修改说明见 [NOTICE](NOTICE)。
