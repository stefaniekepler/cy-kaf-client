# Provenance（上游来源记录）

本文件记录 cy-kaf-client 中 vendored 内容的逐目录来源，以及对 vendored 文件的本地修改日志，满足 Apache-2.0 衍生作品的合规追溯要求。另见根目录 `NOTICE`。

## 1. 上游基线

- 项目：kafbat/kafka-ui（<https://github.com/kafbat/kafka-ui>）
- 版本：`v1.5.0`（tag）
- Commit：`afc9c918e13c4422268a3a5b7933c7b448746c82`
- 许可证：Apache License, Version 2.0
- 版权：Kafbat project contributors and the original Provectus kafka-ui authors
- Vendoring 方式：`scripts/vendor-upstream.sh`（`make vendor-upstream`）—— 一次性脚本，仅在初始 vendoring 或上游基线升级时重跑

## 2. 逐目录来源

| 本仓库路径 | 上游路径 | 来源 commit | 说明 |
|---|---|---|---|
| `frontend/` | `frontend/` | `afc9c918e13c4422268a3a5b7933c7b448746c82` | 前端源码以该 commit 作为初始 vendored 基线；后续本地修改见 §3。含 `src/generated-sources`（上游 TypeSpec 生成的 TS client；上游 `.gitignore` 忽略此目录、本仓库改为入库，见 §3） |
| `contract/openapi.yaml` | `contract-typespec/build/tsp/api/openapi.yaml`（TypeSpec 编译产物） | `afc9c918e13c4422268a3a5b7933c7b448746c82` | 一次性编译入库，作为本仓库唯一契约真源；后端 `oapi-codegen`（`make gen-go`）与前端 `generated-sources` 均指向它 |
| `e2e/` | `e2e-playwright/` | `afc9c918e13c4422268a3a5b7933c7b448746c82` | Playwright e2e 套件以该 commit 作为初始 vendored 基线；后续本地修改见 §3 |
| `deploy/compose/` | `documentation/compose/` | `afc9c918e13c4422268a3a5b7933c7b448746c82` | compose 环境原样 vendored；P1 接线时（替换 kafbat-ui 服务为本项目镜像/二进制）在本文件 §3 追加改动记录 |
| `LICENSE`（根目录） | `LICENSE` | `afc9c918e13c4422268a3a5b7933c7b448746c82` | 上游 Apache-2.0 全文原样覆盖根 `LICENSE` |

## 3. 本地修改日志

初始 vendoring 时上游运行时代码未作修改；此后的品牌、构建接线与运行时本地偏差均须在下表逐项登记。每次对 vendored 文件的改动，都在下表追加一行。

| 日期 | 文件 | 改动 | 原因 |
|---|---|---|---|
| 2026-07-02 | `frontend/`、`contract/openapi.yaml`、`e2e/`、`deploy/compose/`、`LICENSE`、`frontend/.gitignore` | 初始 vendoring：从上游 v1.5.0（commit `afc9c918e13c`）导入前端全部源码、TypeSpec 编译后契约、e2e 套件、compose 环境、`LICENSE` 全文；同时删除 `frontend/.gitignore` 中的 `generated-sources` 忽略行 | 建立本仓库可复用的上游基线（Task 3，vendoring）；`generated-sources` 需要入库以移除日常开发对 JDK/TypeSpec 工具链的依赖，详见设计规格 §5.1 Vendoring 清单 |
| 2026-07-02 | `scripts/vendor-upstream.sh` | 增加 sed 后置校验与 java 前置守卫 | Task 3 审查 Important+Minor 修复 |
| 2026-07-02 | `frontend/index.html` | `<title>Kafbat UI</title>` → `<title>cy-kaf-client</title>` | 规格 §5.2 白名单（品牌） |
| 2026-07-02 | `frontend/public/manifest.json` | `"name"` → `cy-kaf-client` | 规格 §5.2 白名单（品牌） |
| 2026-07-02 | `frontend/src/components/NavBar/NavBar.tsx` | 顶栏文案 `kafbat UI` → `cy-kaf-client`；github 外链 → `https://github.com/cy-kaf/cy-kaf-client`；删除 discord/producthunt 两项社区外链及对应 icon import | 规格 §5.2 白名单（品牌/外链，见 D9） |
| 2026-07-02 | `frontend/src/components/NavBar/__tests__/NavBar.spec.tsx` | 断言文案 `kafbat UI` → `cy-kaf-client` | 规格 §5.2 白名单（品牌断言同步） |
| 2026-07-02 | `frontend/src/components/common/Logo/Logo.tsx` | 内联 SVG 子节点（上游 logo path）替换为 `cy-kaf` 文字标 `<text>`；外层 `<svg>` 尺寸/viewBox/styled 包装不变 | 规格 §5.2 白名单（品牌，零布局影响） |
| 2026-07-02 | `frontend/src/components/AuthPage/Header/HeaderLogo.tsx` | 内联 SVG 子节点（上游 rect+kafbat 字标 path）替换为 `cy-kaf` 文字标 `<text>`；外层 `<svg>` 尺寸/viewBox/styled 包装不变 | 规格 §5.2 白名单（品牌，零布局影响） |
| 2026-07-02 | `frontend/public/favicon/favicon.svg` | 整文件替换为 `cy` 文字标 SVG | 规格 §5.2 白名单（品牌） |
| 2026-07-02 | `frontend/public/favicon/apple-touch-icon.png`、`icon-192.png`、`icon-512.png` | **未改动（已知遗留）**：PWA 装饰位图仍为上游图标，替换排入 P3 打包精装 | 规格 §5.2 白名单第 5 项遗留项登记 |
| 2026-07-02 | `frontend/src/lib/constants.ts` | `GIT_REPO_LINK`/`GIT_REPO_LATEST_RELEASE_LINK` → `cy-kaf/cy-kaf-client`；localStorage 键前缀（`kafbat-ui`）保持不动 | 规格 §5.2 白名单（外链常量；键前缀显式保留避免行为差异） |
| 2026-07-02 | `frontend/openapitools.json` | generator 输入 glob `../contract-typespec/build/tsp/api/openapi.yaml` → `../contract/openapi.yaml` | 规格 §5.2 白名单（构建接线，指向本仓库契约真源） |
| 2026-07-02 | `frontend/package.json` | `build` 移除 `gen:sources` 前置（client 已入库）；`gen:sources` 去掉 TypeSpec 编译步骤、仅执行 `openapi-generator-cli generate` | 规格 §5.2 白名单（构建接线，脱离 Java/TypeSpec） |
| 2026-07-02 | `frontend/.env.development` | 新建：`VITE_DEV_PROXY=http://localhost:8080`（Go dev 端口） | 规格 §5.2 白名单（构建接线） |
| 2026-07-02 | `frontend/src/components/common/Logo/Logo.tsx` | 文本内容 `cy-kaf` → `cy`；fontSize `14` → `12`；删除 `fill="currentColor"` 属性改为继承 styled 包装的主题 fill | Task 4 审查 Important 修复 |
| 2026-07-03 | `e2e/config/cucumber.js` | `default`/`rerun` 两个 profile 的 `require` 列表中，`src/support/customWorld.ts`（不存在）→ `src/support/PlaywrightWorld.ts`（实际存在、`hooks.ts` 已在用的 world 类） | 规格 §2.4 已知坑修复，vendoring 预授权范围（Task 7）；判据：`npx cucumber-js --config config/cucumber.js --dry-run src/features/Brokers.feature` 能列出场景而不报模块缺失 |
| 2026-07-14 | `deploy/compose/e2e-authz.override.yaml`、`Makefile` | 新增仅 `e2e-p2c` 使用的 StandardAuthorizer compose overlay 与 ACL/Quota curl smoke；未引入上游 `Acls.feature` | P2c Task 8：先作者化 3 个 Cucumber 场景实测，在线集群未暴露 `KAFKA_ACL_VIEW/EDIT` feature，导航与直接 URL 均未挂载 ACL route；按执行期兜底裁定顺延浏览器场景，以真二进制 + authorizer broker smoke 验证装配通路，避免不可达 feature 污染默认 Cucumber paths |
| 2026-07-24 | `frontend/src/components/NavBar/`、`frontend/src/components/common/Select/` | 删除顶栏 `cy` 图标、GitHub 外链与 commit 版本入口；主题下拉菜单改为贴右展开、按内容定宽，并保留普通 Select 的贴左行为 | 用户界面精简与主题菜单截断修复；同步更新对应 Jest 回归断言 |
| 2026-07-24 | `desktop/src-tauri/icons/**` | 以 SHA-256 `83a8216fb9cdbeb5faa5276ccaa7972e37b8c935d96c9d6e894339f3ce6a49c8` 的用户提供 ICNS 为唯一素材，裁去等量透明外边距并统一放大原图（不重绘）；使用 Tauri CLI 2.11.4 重建 desktop、Windows Store、Android、iOS 全部图标；使用 oxipng 10.1.1 `--opt max --strip safe` 无损压缩 48 个 PNG，并对 ICNS 内 8 个 PNG 图层执行无损 Zopfli 压缩 | 桌面应用品牌图标替换及空间优化；PNG 由 2,068,161 B 降至 1,882,179 B，ICNS 由 Tauri 生成的 1,490,947 B 降至 1,347,940 B，合计节省 328,613 B；透明安全边距由约 10% 缩至约 5%，中间图案约放大 11% |
| 2026-07-24 | `frontend/src/lib/hooks/api/topicMessages.tsx`、`frontend/src/lib/hooks/api/__tests__/topicMessages.spec.ts` | 将 SSE `MESSAGE` 的逐条 React 状态更新改为同一动画帧内批量提交；流关闭时立即提交，中止/请求切换时取消待提交批次，`TAILING` 仍保持最新消息在前 | 用户明确批准的大 Topic Messages 性能优化；这是本仓库本地运行时偏差，不代表上游源码同步。验证：focused Jest 9/9、全量 Jest 771/771、`make verify` 均通过 |
| 2026-07-24 | `frontend/src/components/Topics/Topic/Messages/Message.tsx`、`MessagesTable.tsx` 及对应测试 | 将公共时区订阅提升到表格层并下传，使用 `React.memo` 跳过未变化行，仅在首次悬停时挂载复制、保存和重发操作；原消息内容、时间格式与权限组件保持不变 | 用户明确批准的大 Topic Messages 性能优化；这是本仓库本地运行时偏差，不代表上游源码同步。验证：相关 Jest 24/24、全量 Jest 771/771、`make verify` 均通过；严格只读 WebKit 三次呈现 100 行的耗时为 614/461/526 ms，中位数 526 ms，控制样本 439 ms，未读取消息内容且未产生变更请求 |
| 2026-07-25 | `frontend/src/components/Topics/Topic/Messages/Filters/Filters.tsx`、`Filters.styled.ts`、`__tests__/Filters.spec.tsx` | 仅调整工具栏 DOM/换行布局，筛选语义不变 | 将 Add Filters、激活条件、Refresh 与 Search 归入可换行的消息顶部工具栏，移除重复的下方筛选行；同步加入 DOM 归属回归断言 |
| 2026-07-25 | `frontend/src/components/Topics/Topic/Messages/Filters/Filters.tsx`、`Filters.styled.ts`、`__tests__/Filters.spec.tsx` | 为超长无断点激活筛选名增加局部收缩、截断及完整名称 `title`，筛选语义与持久化数据不变 | Final Review Important 修复：防止既有持久化长名称撑宽消息工具栏和页面；以 1024×700、1440×900 Chromium 断言验证无横向溢出或控件重叠 |
| 2026-07-25 | `e2e/package.json`、`e2e/src/playwright.config.ts`、`e2e/src/tests/messages-toolbar.spec.ts` | 保留真实前端路由上的消息工具栏 Playwright 回归；仅使用确定性只读 GET/SSE fixtures，拒绝变更请求且禁用截图、视频和 trace | Final Review Long Filter Fix Round 2：`cd e2e && npm run test:messages-toolbar` 可独立复现 1024×700 必测与 1440×900 spot-check，并断言超长持久化筛选名不会造成页面/工具栏横向溢出、控件重叠或可访问名称丢失 |
| 2026-07-25 | `frontend/index.html`、`frontend/public/manifest.json`、`frontend/src/components/NavBar/NavBar.tsx`、`frontend/src/components/NavBar/__tests__/NavBar.spec.tsx` | 浏览器标题、PWA 名称、顶栏文案及其断言 `cy-kaf-client` → `Cy KafClient` | 桌面产品显示名称统一；保留工程标识、配置目录和 sidecar 名称不变 |
| 2026-07-25 | `desktop/src-tauri/icons/**` | 撤销此前约 5% 透明边距和约 11% 图案放大；以用户提供 ICNS 的未修改 `icon_512x512@2x.png`（1024×1024 RGBA）为唯一母版，使用 Tauri CLI 2.11.4 无视觉变换重建 desktop、Windows Store、Android、iOS 全部图标；最终 `icon.icns` 与 SHA-256 `83a8216fb9cdbeb5faa5276ccaa7972e37b8c935d96c9d6e894339f3ce6a49c8` 的源文件字节一致 | 恢复用户原始素材的透明边距与图案尺度，避免任何裁切、主体放大、重绘或额外留白 |
| 2026-07-25 | `desktop/src-tauri/icons/android/**`、`desktop/src-tauri/icons/ios/**` | Fix Round 1：Android legacy/round/foreground 各密度位均由未修改 1024×1024 母版等比生成，hdpi legacy 与 round 改为 72×72，去除 Tauri 2.11.4 legacy mask 带来的额外边距；iOS 保持母版的画面位置和主体尺度，但以白色背景作唯一的全不透明合成，18 个 AppIcon 的 alpha 均为 255 | Apple 要求 iOS App Icon 不含透明区域；透明角在 App Store 上传中无效。该合成是平台合规所必需的例外，不是裁切、主体缩放、重绘或额外 padding；macOS `icon.icns` 继续与 SHA-256 `83a8216fb9cdbeb5faa5276ccaa7972e37b8c935d96c9d6e894339f3ce6a49c8` 的用户源文件字节一致 |
| 2026-07-25 | `desktop/src-tauri/src/lib.rs`、`host.rs`、`host_tests.rs`、`scripts/desktop-smoke.sh` | macOS 桌面 smoke 为原生 Cocoa/WebKit 状态设置隔离的 `CFFIXED_USER_HOME`，并在显式测试配置旁使用独立运行目录；正常启动仍使用既有 `cy-kaf-client` 配置目录 | 防止历史窗口标题与已保存端口污染 `Cy KafClient` 原生标题验收；新构建 smoke 观察到精确标题、动态 loopback 端口及正常关闭，用户运行目录哈希保持不变 |
| 2026-07-25 | `desktop/src-tauri/src/lib.rs`、`scripts/desktop-smoke.ps1` | Windows NSIS smoke 恢复启动 Cargo 主目标对应的内部文件 `cy-kaf-client-desktop.exe`，并分别断言产品注册名、桌面快捷方式和原生窗口标题均为 `Cy KafClient` | Tauri CLI 2.11.4 在未设置 `mainBinaryName` 时保留 Cargo 二进制名，`productName` 独立驱动可见产品与快捷方式名称；保持内部二进制、sidecar、bundle 标识及配置目录不变 |
| 2026-07-25 | `frontend/src/components/Settings/**`、`frontend/src/components/NavBar/**`、`frontend/src/lib/hooks/api/desktopMcp.ts` | 新增桌面专用 Settings 入口、MCP 默认关闭/只读与写权限确认、Codex/Claude Code 有限配置状态和固定日志目录动作；浏览器模式保持不可用，前端请求不携带可执行文件、路径或环境变量 | Task 8：记录相对 vendored Kafbat UI v1.5.0 的桌面 MCP 设置偏差；自动配置仅提交固定客户端枚举与 replace 布尔值，冲突替换需显式确认 |
| 2026-07-26 | `internal/infra/mcpclient/**`、`frontend/src/components/Settings/**`、`README.md` | Codex/Claude 配置改为有界私有 staging 与单次原子发布；Windows 本版自动配置 fail-closed，batch-only `.cmd`/`.bat` 安装仅显示手动命令，未来自动配置需原生 `.exe`/`.com` launcher | Final whole-branch review：防止并发外部编辑被回滚覆盖及路径组件 TOCTOU；当前 macOS 主机仅完成 Windows 静态交叉编译，不声称 Windows 原生执行或 ACL/launcher 行为已原生验收 |
| 2026-09-04 | `frontend/src/components/Nav/**`、`frontend/src/components/Dashboard/**`、`frontend/src/components/common/Icons/AllClustersIcon.tsx`、`e2e/src/features/navigation.feature`、`e2e/src/pages/{Panel,Dashboard}/**`、`e2e/src/steps/navigation.steps.ts` | 将侧栏 `Dashboard` 明确为带概览图标和首页激活态的 `All clusters`，增加 Overview/Clusters 分组层级，并将列表页标题同步为 `Clusters`；端到端场景改为从集群页返回列表 | 用户批准的导航可发现性优化；不改变路由、集群查询、新增入口权限或移动端侧栏行为；聚焦 Jest、lint、构建及 Cucumber dry-run 通过后提交 |
| 2026-09-05 | `frontend/src/components/PageContainer/**`、`frontend/src/components/Nav/**`、`frontend/src/components/NavBar/**`、`e2e/src/tests/all-clusters-reminder.spec.ts`、`e2e/package.json` | 为 `All clusters` 提供常驻柔和灰阶背景，并仅在进入集群路由时于入口右侧显示四秒中文定位提醒；窄屏等待用户通过具名可访问侧栏开关打开侧栏后再显示；以确定性只读 fixtures 验证明暗主题、提醒生命周期与 1024×700 视口 | 提升返回及管理全部集群入口的可发现性；不改变路由、API、权限、持久化或 Kafka 行为 |
| 2026-09-06 | `frontend/src/components/Nav/**`、`frontend/src/components/PageContainer/__tests__/PageContainer.spec.tsx`、`frontend/src/theme/theme.ts`、`e2e/src/tests/all-clusters-reminder.spec.ts` | 去掉 `All clusters` 上方的 `Overview` 分组标签；定位提醒删除标题行，仅保留中文说明，并改为明暗主题自适应的低透明淡蓝底色、边框和文字且移除阴影；同步更新单元与浏览器回归断言 | 按用户反馈降低进入集群时提醒的视觉突兀感，同时让全部集群入口保持清晰、柔和且不改变既有路由、展示时机、四秒自动消失或 Kafka 行为 |
| 2026-09-06 | `frontend/src/components/Nav/AllClustersReminder/**`、`frontend/src/components/{Nav,PageContainer}/__tests__/**`、`frontend/src/theme/theme.ts`、`e2e/src/tests/all-clusters-reminder.spec.ts` | 恢复定位提醒的“返回全部集群”标题和两级文字层次；提示表面改用低饱和灰蓝中性色、细边框与轻阴影，使其与主界面有柔和差异；继续保留已移除的 `Overview` 标签 | 根据用户复核降低提示与主界面的色彩冲突并保留明确语义；不改变入口、路由、展示时机、四秒自动消失、窄屏行为或 Kafka 行为 |
