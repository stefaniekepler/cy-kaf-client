# CLAUDE.md — cy-kaf-client

跨平台本地 Kafka 客户端：React 前端 + Go 后端 + Tauri v2 桌面宿主，共用一份
OpenAPI 契约（97 个端点，勘误 E1）。桌面端在原生窗口中运行，仅在 loopback 启动
Go sidecar；CLI 与 STDIO MCP server 复用同一后端能力。

当前已完成 ACL、Client Quota、Topic Analysis 与 KSQL 等批准范围；契约矩阵中 92 项已交付，
Graphs、PrometheusExpose 与登录相关的 5 项保持 `exempt`。设计决策、阶段记录与能力矩阵见
`docs/superpowers/specs/`、`docs/superpowers/plans/` 和 `docs/parity/feature-matrix.md`。

## 命令（一切以 make 为入口）
- make verify        # 合并/声明完成前必须全绿：lint+单测(含覆盖率门禁)+契约检查+构建
- make test          # Go 单测 + 覆盖率门禁(domain+app≥90%, 全仓≥85%)
- make test-fe       # 前端 Jest 套件（必须保持全绿；已是 `pnpm exec jest --ci`）
- make test-integration  # 需 colima 运行中（ADR-0001；本机 env var 见「已知坑」）
- make e2e-p1a        # 一键 e2e（compose 起 Kafka + 构建 + 跑 Brokers.feature；需 DOCKER_HOST 指向 colima）
- make e2e-p1b        # P1b 面 e2e（navigation/Topics/TopicsActions + Brokers 回归，12 场景；同需 DOCKER_HOST 指向 colima）
- make e2e-p1c        # P1c 消息面 e2e（TopicsMessages 5 场景：produce/browse/serde/clear/smartfilter；同需 DOCKER_HOST 指向 colima）
- make e2e-p2a        # P2a Schema Registry 面 e2e（SchemaRegistry.feature 4 场景：create/view/delete avro+json+protobuf；起 kafka0+schemaregistry0；同需 DOCKER_HOST 指向 colima）
- make e2e-p2b        # P2b Kafka Connect 面 e2e（KafkaConnect.feature 4 场景：search/main page/connector page/connector page functions；起 kafka0+schemaregistry0+kafka-connect0+postgres-db+create-connectors 种子；connect 名固定 "first"；同需 DOCKER_HOST 指向 colima）
- make e2e-p2c        # P2c ACL/Quota 装配 smoke（非浏览器 E2E；真二进制 + 仅此目标叠 StandardAuthorizer override；ACL create/delete exact present/absent + quota exact upsert/list 均 eventual-read；同需 DOCKER_HOST 指向 colima）
- make e2e-p3-ksql     # P3 KSQL 四场景 Compose/Cucumber 验收（visibility/clear/query/cancel；同需 DOCKER_HOST 指向 colima）
- make build / release-build / build-fe / gen-go / contract-check

## 架构铁律（lint 强制，违规=CI 失败）
- 四层：internal/{domain,app,infra,api}；依赖方向 api→app→domain←infra；组装仅在 cmd/。
- domain 零第三方依赖、app 仅 domain+标准库（depguard allowlist 强制，测试文件额外放行 testify）；api 不 import infra（deny 强制）。
- 契约优先：contract/openapi.yaml 是唯一真源；internal/api/generated/ 勿手改（make gen-go 再生）。
- 内置 React 前端运行时代码零改动；允许改动仅限规格 §5.2 白名单，且必须登记 docs/provenance.md。

## 工作纪律
- TDD：先失败测试再实现；Conventional Commits；小步提交，禁止加入 agent-authorship trailer。
- 声明"完成"前必须附 make verify 输出（superpowers:verification-before-completion）。
- 每合并一个里程碑：更新本文件"当前阶段"、parity matrix、必要时新增 ADR（docs/adr/）。

## 已知坑
- webui/static 为空时二进制只输出占位页——先 make build-fe。
- 前端构建产物 index.html 含 PUBLIC-PATH-VARIABLE 字面量，由后端 serve 时替换（internal/api/static.go）。
- kin-openapi 契约校验是端点行为的最终裁判；generated 标识符对照见 ADR-0002。
- franz-go 能力缺口与 kmsg 回退见 ADR-0003；集群安全 P0 仅 PLAINTEXT/SSL，P1a 起支持 SASL
  PLAIN/SCRAM-SHA-256/SCRAM-SHA-512 + 自定义 truststore PEM/JKS（GSSAPI/OAUTHBEARER 仍豁免，
  见 ADR-0004 §3、parity matrix 豁免表 #11）。
- 大文件勿全文读取：contract/openapi.yaml(5547 行，`wc -l` 实测；早期规格/计划文档估算的
  4829 行已过期)、frontend/src/generated-sources/、webui/static/。
- colima 本机跑 `make test-integration` 需要两个环境变量：`DOCKER_HOST=unix://$HOME/.colima/default/docker.sock`
  与 `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`（testcontainers-go 默认探测不到
  colima 的 socket，Ryuk 挂载会失败）——**未持久化进 Makefile/CI，每个新 shell 需手动 `export`**；
  详见 .superpowers/sdd/task-8-report.md 与 ADR-0003 §3。
- P3 KSQL 的真实验收先检查 MCP；当前没有适用的 Kafka/ksqlDB MCP 时，使用上述 Colima
  Compose/Testcontainers 回退。KSQL 客户端拒绝 URL userinfo、错误响应只保留安全摘要，禁止把
  SQL、凭据或 Authorization 写入日志/SSE；`make e2e-p3-ksql` 的 4 个场景必须用精确
  `--name` 锚定，避免 Cucumber 配置的默认 feature 路径扩跑其它面。有限的 `/ksql` 与
  `LIST` 响应受 16 MiB 聚合体上限保护；`/query` 保持总流可持续读取，但每个顶层 JSON
  frame 受 8 MiB 上限保护。同一集群的 URL、认证或 TLS 轮换会淘汰旧 client 并关闭其
  idle 连接；Resolver 删除集群后的缓存清理仍是后续生命周期集成项。
- `go get`/`go run` 偶发 GOSUMDB 默认镜像连接层 `EOF`：优先重试 `GOSUMDB=sum.golang.org`
  （不要一上来就用 `off`）——ADR-0002 记录过这样一次性解决；ADR-0003 §1 遇到过同类问题表现为
  其它错误（如 504）的情况，那种情况下才考虑 `off` 作为最后手段。
- 前端 `pnpm test` 是常驻的 `jest --watch`、不会自行退出；一次性运行（本地或 CI）必须用
  `pnpm exec jest --ci`（Makefile `test-fe` 目标已是这个写法，勿手动改回 `pnpm test`）。
- kadm 的 `AlterBrokerConfigsState`（全量替换，之前配置全部丢失）**≠** `AlterBrokerConfigs`
  （增量修改）——P1 实现"改一个配置项"类端点必须选后者，选错会清空该 broker/topic 的其它配置；
  详见 ADR-0003 §5「完整对照表」与 §6「额外发现」。P1b 起 Topics 侧同一陷阱同样适用
  （`AlterTopicConfigsState` 全量替换 vs `AlterTopicConfig` 增量），两处集成测试灵魂用例锁死这条
  铁律：`internal/infra/kafka/state_integration_test.go` 的 `TestAlterBrokerConfigIsIncremental`
  （broker）、`internal/infra/kafka/topics_integration_test.go` 的
  `TestTopicsWriteRoundTripAgainstRealKafka`（soul test#2：连续设置两个 topic 配置 key，断言二者
  共存而非后者覆盖前者）。
- **写端点必须携带 readOnly 403 测试**：`internal/api/middleware.go` 的 `readOnlyGuard` 拦截全部
  写方法（POST/PUT/PATCH/DELETE），对 `readOnly` 集群一律 403，除非命中 `readOnlyWhitelist`
  （同文件，当前仅 `"POST /cache"`——强制刷新缓存不改变集群本身状态）；新增任何写端点都必须补一条
  针对 readOnly 集群的 403 用例；若新写端点其实是只读语义（P1c 预告：smartfilters/
  testexecutions），要改的是 `readOnlyWhitelist` 这张白名单表，而不是绕开守卫。
- **多 logdir 集成测试夹具**：`confluentinc/confluent-local` 镜像只在 `KAFKA_LOG_DIRS` 环境变量
  *未设置* 时才默认单目录（直接读镜像 `/etc/confluent/docker/configure`/
  `kafka-propertiesSpec.json` 源码确认，非猜测——该变量落在通用 `KAFKA_*` 透传规则内）：
  `testcontainers.WithEnv(map[string]string{"KAFKA_LOG_DIRS": dirA + "," + dirB})` 即可让容器
  格式化多个日志目录，是 `MoveReplicaLogDir` 成功路径测试
  （`internal/infra/kafka/state_integration_test.go`）不可缺的夹具，见该文件中段（该测试函数上方，约
  131-144 行）注释的完整推导。
- **read-your-writes 写后刷新**：Topics 的 6 个写方法（Create/Delete/Recreate/Clone/
  IncreasePartitions/ChangeReplicationFactor）成功后都会同步调 `StateCache.Refresh`
  （`internal/app/cluster/topic.go` 的 `refreshAfterWrite`），否则写入要等最长 30 秒的周期刷新才会
  反映到 List/Details 等读路径——新增任何"写入活集群、读走周期缓存"模式的端点（P1c 消息发送等）都
  要评估是否需要同样的写后刷新；注意 `StateCache.Refresh` 内部会先 `lifecycle.Invalidate` 关闭并
  重建连接（ADR-0005「决策 3」记录的已知成本：每次成功写多付一次重连+scrape 往返，高频写场景可能
  需要 refresh-without-invalidate 优化）。
- 契约测试必须先 `doc.Servers = nil` 再建路由（gorillamux 对非空 `servers` 按 host 匹配，httptest
  监听 `127.0.0.1:<随机端口>`，不清空会导致全部端点 `FindRoute` 失配）——见 internal/api/contract_test.go。
- **跨架构运行时冒烟仍是发布前置项**：最终 HEAD 已用 `make release-build` 产出并以 `file` 核验
  darwin/amd64、darwin/arm64、windows/amd64 三种带前端工件；当前 Intel Mac 也已重新启动
  darwin/amd64 工件，验证 `/actuator/health`、`/api/info` 与 SPA 占位符替换。CI
  `build-matrix` 仍是编译门禁，正式发布前必须在真实 arm64 Mac 与 Windows 机器上手动重跑 P0
  启动冒烟（尤其 Windows 防火墙提示/路径差异）。
- **实现新端点 = 在 `apiServer` 覆写生成的同名方法**（如 `getBrokers → GetBrokers`，方法名=
  operationId 的 UpperCamel），写在对应 `internal/api/handlers_*.go` 里——**不要手动加路由**，
  `scripts/genstub` 生成的 501 桩（`internal/api/generated/unimplemented.gen.go`）在编译期被
  同名覆写自动让位；`contract/openapi.yaml` 变更后重跑 `make gen-go` 会连带用 `genstub` 重新
  生成桩文件，未覆写的新端点自动落回 501，无需手动同步。见 ADR-0004 §1。
- 新用到的 kadm 符号（方法/类型/字段）必须先加进 `internal/infra/kafka/capability_audit_test.go`，
  靠 `go vet ./internal/infra/kafka/...`（编译期证明，而非读文档/猜测）背书其真实存在与签名——
  ADR-0003 确立的方法论，P1a 起对新增 kadm 调用全程强制。
- `cucumber-js` 的 `e2e/config/cucumber.js` 里 `paths=["src/features/"]` 与 CLI 传入的 feature
  路径参数是**叠加**关系、不是覆盖——只传一个 `.feature` 文件路径仍会连带跑 `paths` 里全部 feature
  的全部 scenario；跑子集必须额外加 `--name '^精确 scenario 名$'` 锚定（`make e2e-p1a` 已这样做，
  见 Makefile `e2e-p1a` 目标注释）。P1b/P1c 扩 e2e 覆盖面（Topics/TopicsActions/navigation 等）时
  同样适用，忘记锚定会把尚未实现端点的 scenario 一并跑挂。
- 若宿主机 `~/.docker/config.json` 残留 `"credsStore": "desktop"`（指向不存在的
  `docker-credential-desktop` 二进制，常见于未装 Docker Desktop 的纯 colima 环境），**任何**
  `docker pull`/`docker compose up` 都会报 `error getting credentials - err: exec:
  "docker-credential-desktop": executable file not found` 而失败，与具体镜像无关——删掉该
  `credsStore` 键（CLI 回退到普通 `auths` 查找）即可一次性解决。
- 端点错误响应一律经 `errorResponse(status, msg)` 助手构造，禁止裸 `generated.ErrorResponse{}`
  字面量；已知集群名对应的后端调用失败统一 500，未知集群名（`errors.Is(err,
  appcluster.ErrUnknownCluster)`）分流 404——P1a Task 5 两轮 review 修复后定型的铁律，全部
  Brokers/Clusters 端点与后续 P1b/P1c 新端点必须遵守。
- **（P1c）SSE flusher 必须穿透 middleware**：`getTopicMessagesV2` 走 `text/event-stream` 逐帧 `Flush`
  （`internal/api/sse.go`）；chi `middleware.Recoverer` 等不得用不透传 `http.Flusher` 的 wrapper 包裹
  底层 ResponseWriter，否则静默降级为缓冲响应——新增流式端点先做「穿透校验」（真 `NewServer` 路由发一帧、
  断言 client 立即收到）再用，见 ADR-0006 §1 与 Task 9。
- **（P1c）CEL `record` 语义**：filter id=`sha256(code)[:8]`（确定性、幂等；前端拿 id 回放为 `smartFilterId`，
  故 id 稳定性是前端契约，勿改哈希）；`record.key`/`record.value` 对**合法 JSON 暴露为可导航结构**
  （`record.value.a.b`），非 JSON 回退字符串，另有 `record.keyAsText`/`valueAsText` 恒为原始串供
  `.contains()` 子串——前端智能过滤生成 `record.value.value.internalValue==N` 依赖此，见 ADR-0006 §3。
- **（P1c）readOnly 白名单变量段用正则**：含活变量段的只读语义写端点（如 `registerFilter` 的
  `.../topics/{topicName}/smartfilters`）精确字符串 map 匹配不到，改由 `middleware.go` 的
  `readOnlyWhitelistPatterns` 正则放行；新增此类端点改这张正则表而非精确 map，见 ADR-0006 §9。
- **（P1c 订正 ADR-0005 §3）produce/delete 触发 refresh-without-invalidate**：`MessageService.Send`/`Delete`
  成功后 best-effort 调 `StateCache.RefreshWithoutInvalidate`——复用现有连接重抓一次（**不** Invalidate
  重连），使 topic messagesCount ~1s 内可见（而非最长 30s）。区别于 Topics 6 个写方法用的全量 `Refresh`
  （含 Invalidate）。e2e-p1c「Produce messages clear messages」场景锁死，见 ADR-0006 §8。
- **（P2a）SR serde 是单个 "SchemaRegistry"、按 schema type 内部分派**：不是两个 Avro/JSON serde
  （规格草拟「两个实现」，契约与现有前端行为均为单条目 serde 按拉到的类型分派——实现前裁定采纳单
  serde，见 ADR-0007 §6）；wire format = magic `0x00`+4B big-endian schema id+payload；Deserialize 按
  id 拉 schema 分派（AVRO hamba/avro→JSON 文本 / JSON 直取 `data[5:]` / **PROTOBUF 不支持**→error）；
  **任何解码失败回退原始字节**（沿用 P1c decodeField 铁律，Browse 不崩），新增 SR 相关解码路径都要保持这
  条回退，见 ADR-0007 §5/§9。
- **（P2a）Provider 动态候选，无 SR 集群零回归**：`NewProviderWithSchemaRegistry(reg, sr)` 才注入 SR
  serde 候选；`def.SchemaRegistry.URL != ""` 时 `candidates` 追加 per-cluster `SchemaRegistrySerde`，
  preferred 四层（config-bound / **SR subject 有 schema 则自动 prefer** / Default*Serde / String）；
  `NewProvider(reg)`（sr=nil）与无 SR 集群行为**完全不变**（8 内置 codec 候选/顺序/preferred 全不动，
  `provider_test.go` 既有断言未改）——改 Provider 时先跑该文件确认零回归，见 ADR-0007 §7。
- **（P2a）checkCompatibility 进 readOnly 白名单正则**：`POST .../schemas/{subject}/check` 是干跑校验
  （只读语义、不注册不改），含活 subject 变量段，加 `middleware.go` `readOnlyWhitelistPatterns` 的
  `^POST /schemas/[^/]+/check$` 放行只读集群（同 P1c smartfilters 变量段机制）；其余 SR 写端点
  （create/delete/update-compat）均 genuine 写、各带 readOnly-403。新增只读语义写端点改这张正则表，见
  ADR-0007 §8、ADR-0006 §9。
- **（P2a）getAllVersionsBySubject 返回完整 `[]SchemaSubject`（非版本号数组）**：契约（唯一真源）实测
  `items: SchemaSubject`，计划占位清单的 `[]int32` 猜测被推翻；app `AllVersions` 组合 `Versions`+逐版本
  `SchemaByVersion` materialize——「契约是端点行为最终裁判」，新 SR 端点的响应形状一律 grep 契约核实，勿信
  计划插图，见 ADR-0007 §4。
- **（P2a）SR 认证/SSL 复用 kafka truststore 装载器**：`kafka.LoadTruststore`（原 `loadTruststore` 为此
  导出）；SR 只实现 truststore（校验服务端证书），`SRSSL.Keystore*` 客户端证书 mTLS 暂未消费（同 kafka 侧
  `tlsConfigFor`）；config 字段 `schemaRegistryAuth`/`schemaRegistrySsl`（Task 1），见 ADR-0007 §2。
- **（P2b）Connect 客户端 = 标准库 `net/http` Pool（非第三方）**：Connect REST 是简单 JSON over HTTP，
  无需像 P2a SR 那样引 franz-go `pkg/sr` 式专属客户端；`internal/infra/connect/client.go` 的 `Pool`
  按 `poolKey{cluster,connect}` 缓存 `*http.Client`；内建两处对真实容器观测到的瞬时行为的有界重试
  （`CreateConnector`/`SetConnectorConfig` 后 `GET status` 约 1 秒 404、任意调用可能在 worker
  rebalance 期间收到 500，均 10×300ms 有界重试），见 ADR-0008 §1(D1)。
- **（P2b）跨 Connect 聚合 skip-bad**：`Connects`/`AllConnectors` 遇到坏、不可达的单个 Connect worker
  时跳过该条目（记日志）而非整体失败——新增任何跨多 Connect 聚合的端点都要沿用这条语义，不要让一个
  down 掉的 worker 打垮整个聚合响应；集成用死地址 `http://127.0.0.1:1` 验证 `len==1`，见
  ADR-0008 §3(D3)。
- **（P2b）`validateConnectorPluginConfig` 进 readOnly 白名单正则**：`PUT
  .../connects/{connectName}/plugins/{pluginName}/config/validate` 是干跑校验（只读语义、PUT
  方法、含两段活变量），加 `middleware.go` `readOnlyWhitelistPatterns` 的 `^PUT
  /connects/[^/]+/plugins/[^/]+/config/validate$` 放行只读集群（同 P1c/P2a 变量段机制）；其余 6 个
  Connect 写端点（create/delete/setConfig/updateState/resetOffsets/restartTask）均 genuine 写、各带
  readOnly-403，不进白名单，见 ADR-0008 §6(D6)。
- **（P2b）Connect 认证/SSL config 字段是扁平数组项**：`kafkaConnect[]` 每个条目直接挂
  `username`/`password`/`keystore*`/`truststore*`（非嵌套 auth/ssl 子对象）——其中
  `username`/`password`/`keystore*` 由**契约声明**（`models.gen.go:1253-1260` 的 `KafkaConnect` 结构体，
  **无 truststore 字段**），嵌套写法违反契约形状；`truststore*` 则是 config 层（`ConnectCfg`，
  `internal/infra/config/config.go`）在契约**之外**的扩展，沿用 P2a `SRSSLCfg`/schemaRegistrySsl 既有约定
  （勿把它当契约字段去 grep models.gen.go）。TLS 只实现 truststore（校验 Connect 服务端证书），
  keystore/客户端证书 mTLS 暂未消费（同 kafka/SR 侧既有取舍），见 ADR-0008 §2(D2)。
- **（P2b）`updateConnectorState` 6 动作映射（含 STOP/KIP-875）**：单端点 `action` 路径参数 → 6 个
  `ConnectorAction`（`RESTART`/`RESTART_ALL_TASKS`/`RESTART_FAILED_TASKS`/`PAUSE`/`RESUME`/
  `STOP`）→ Connect REST 调用，映射表在 infra 层（`internal/infra/connect/client.go`）；
  `resetConnectorOffsets` 透传 Connect 的 `DELETE .../offsets`，要求 connector 处于 **STOPPED**
  （KIP-875，`cp-kafka-connect:7.8.0`/Kafka 3.8.x 支持），客户端不自行门控该前置条件，Connect 拒绝时
  错误必须原样上抛、不被吞（集成测试 STOP→reset 成功 / RESUME 后 reset→`require.Error` 双路径锁定），
  见 ADR-0008 §4(D4)/§7(D7)。
- **（P2b）`getConnects` 的 `withStats` / `getAllConnectors`/`csv` 的 `orderBy` 顺延**：`withStats`
  查询参数被接受但未填充 `generated.Connect.stats`（domain `ConnectCluster`/端口 `Connects` 都无
  stats 数据源，需扩 domain 面才能真填）；`orderBy`/`sortOrder` 契约声明、handler 忽略（前端按 name
  客户端重排，无当前消费者依赖）——两者均记为 P2b-follow-up，见 ADR-0008「顺延项」②③。
- **（P2c）ACL/Quota 复用既有 Kafka Pool，零新连接**：生产组装把同一个
  `internal/infra/kafka.Pool` 注入 ACL 与 Quota service；契约→kadm 的反向 enum mapper 留在 infra，
  operation 虽用 `kmsg.ParseACLOperation` 解析，app/infra 仍须按契约精确白名单拒绝 unknown/别名。
  `USER` resource type 必须明确报 unsupported，禁止降级 `AnyResource()`；CLUSTER binding 只接受
  `kafka-cluster` + `LITERAL`，create 只接受 `LITERAL/PREFIXED`，delete 才额外接受 `MATCH`，见
  ADR-0009 D1/D4。
- **（P2c）ACL helper 在 app 展开、infra per-binding create**：consumer / producer-idempotent /
  streamapp 展开束都在 `AclService`；prefix 必须用 `PREFIXED`，idempotent producer 必须补
  `CLUSTER kafka-cluster LITERAL IDEMPOTENT_WRITE`。每个 binding 单独 `CreateACLs`，否则 kadm
  builder 会把多个维度做笛卡尔积产生多余 ACL。principal/host、资源 names/prefix 与 applicationId
  在构造和稳定去重前 trim，principal 必须是 `type:name`；资源滤空，整批 binding 在 mutation 前
  预校验，空展开/无效 batch 返回 400 且不产生部分写，见 ADR-0009 D3。
- **（P2c）ACL list 回滤与排序是契约语义**：`MATCH` 对 LITERAL 做 exact/`*`、对 PREFIXED 做查询名
  prefix 匹配；resourceName 为空时在 resourceType backstop 后保留存量 LITERAL/PREFIXED。principal/fts
  大小写不敏感，fts 仅搜索七字段 substring，不走 ngram。排序逐字段比较，`AclSortKey` 是
  collision-safe length-prefixed 编码，不能退回 delimiter 拼接，见 ADR-0009 D2。
- **（P2c）ACL CSV format/parse 单点归 app**：固定七列由 `FormatAclCSV`/`ParseAclCSV` 共同维护，
  首个非空行必须是大小写敏感的 exact header；所有数据 cell 在判空、enum 校验、binding 构造与去重前
  trim，header-only 明确清空。API 先全量 parse，app/infra 再做语义防御；`SyncCSV` 以 cluster keyed
  lock 串行化 List→create(toAdd)→delete(toDelete)。锁只在当前进程内生效，外部 Kafka writer 不受
  保护。`ErrBadAclCSV` 只映射解析/校验 400，Kafka 后端失败保持 500，见 ADR-0009 D4。
- **（P2c）Quota upsert 是 full-replace，不是 merge**：先 `DescribeClientQuotas`，对请求 keys Set、
  对同 entity 现存但请求缺省的 keys Remove；POST 用共享 10 MiB strict decoder，必须指定至少一个
  非空 user/clientId/ip，`{}` 返回 400，default entity 只在 GET 折叠为只读响应。entity identity 是
  保留 Type、Name nilness/value 与重复次数的 order-independent typed multiset；同 cluster+entity 用
  进程内 keyed lock 串行化 Describe→diff→Alter→100ms exact readback（caller context/30s 先到为准）。
  空 quotas 删除已有 keys，仅已为空才 no-op；锁不覆盖外部 writer，见 ADR-0009 D5。
- **（P2c）authorizer fixture 与 e2e 只用于测试**：`testsupport_authz_test.go` / `e2e-authz.override.yaml`
  启用 `StandardAuthorizer` + `User:ANONYMOUS` super user；7 个 genuine 写端点全部 readOnly-403，均不进
  白名单。后端现已暴露 `KAFKA_ACL_VIEW`，并仅为非 readOnly 集群暴露 `KAFKA_ACL_EDIT`，ACL UI
  route/menu 可挂载；`e2e-p2c` 仍是真二进制+authorizer broker 的 curl smoke，不是
  Cucumber/browser E2E；quota 断言要求 key 集合恰为 `producer_byte_rate` 且值为 numeric `1024`。
  历史裁定与 2026-07-23 修订见 ADR-0009。
- **（P2d）分析 job 必须使用 service root context**：POST 的 request context 会在 204 返回后结束，
  后台 goroutine 必须从 `NewAnalysisService` 的 signal-root context 派生，并由 DELETE 的 run cancel
  唯一中断；不要把 HTTP request context 直接传入扫描。
- **（P2d）启动 reservation 不能持锁做 broker 查询**：`Analyze` 的初始 `PartitionRanges` 可能慢，
  只能占用 identity reservation，不能持有 keyed lock；`Cancel` 必须能立即移除 reservation，lookup
  返回后用 token 校验避免注册已取消的孤儿 run。
- **（P2d）分析边界和空 topic 语义**：启动时复制 `PartitionRanges` 的 `[Start,End)` 快照，追加到
  快照外的记录不计入统计；全空 topic 返回 `totalStats: {}`、`partitionStats: []`，`totalMsgs` 留空，
  以保持前端 UI 的 empty 判定。
- **（P2d）分析 reader 与 Kafka 空尾**：普通 `Pool.Open` 过滤事务 marker；分析服务若 reader 提供可选
  `OpenAnalysis` 则启用 `KeepControlRecords`，marker 只推进 offset、不进统计。固定快照连续 30 秒无
  进展时落 `ErrAnalysisNoProgress` 的 partial result，避免压缩/删除尾部导致 goroutine 无限空轮询；这是
  分析专用保护，不把普通浏览的 `nil,nil` 空批次改成 EOF。
- **（P2d）ReaderSession 错误顺序**：`Poll` 可能同时返回 records 和 partition error，forward/backward/tailing
  都必须先交付同批 records 再返回错误；backward 在有 partial records 时合并后再返回首个分区错误。
  `PartitionRanges`/`OffsetsForTimestamp` 对分区级错误、负 offset 和 `Start > End` fail closed，未知 topic
  的 `-1` sentinel 只能作为空结果信号。
- **（P2d）total 统计必须合并 sketch**：跨分区 unique 使用 HLL register max，分位使用对数直方图桶
  merge，不能把分区估计值相加；nil key/value 只计 `null*`，不进入 HLL。API 映射必须保留所有
  `SizeStats`/hourly optional 字段的契约语义。
- **（P2d）readOnly 白名单只允许精确分析路径**：POST/DELETE analysis 虽然只读 Kafka，但仅匹配
  `^.../topics/[^/]+/analysis$`；额外路径段仍须 403。该表只绕过只读模式守卫，不替代 host/认证和
  Resolver 的 cluster/topic 校验。
- **（P2d）真实 broker 验证前置条件**：`internal/infra/kafka/analysis_integration_test.go` 使用
  `confluentinc/confluent-local:7.8.0`；运行前须设置 Colima 的 `DOCKER_HOST` 与
  `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE`。本轮已用这两个变量在 Colima 上跑通分析灵魂用例与
  `make test-integration`；若后续 socket 不存在，只记录容器启动前阻塞，不把单测/编译结果冒充
  真实 Kafka 通过。

## 环境
新机初始化按 docs/adr/0001-dev-environment.md（colima + corepack/pnpm + 仅 vendoring 需要的 JDK）。
