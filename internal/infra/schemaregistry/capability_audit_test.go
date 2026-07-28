package schemaregistry

import "github.com/twmb/franz-go/pkg/sr"

// P2a Task 2 所需 franz-go pkg/sr(v1.7.0) 能力的编译期审计：任何一行编译失败即说明该能力在
// 当前版本缺失，须记录替代方案（ADR-0003 建立的方法论，本包首次把它应用到 pkg/sr 而非
// kadm/kmsg）。核查过程：直接通读
// $(go env GOMODCACHE)/github.com/twmb/franz-go/pkg/sr@v1.7.0/{api.go,client.go,clientopt.go,
// enums.go,params.go} 源码（非猜测、非文档）。
//
// 本文件相对 task-2-brief.md 插图性清单的改动：
//   - 清单里的方法名全部命中真实导出符号，无需改名（Subjects/SchemaByID/SchemaByVersion/
//     SubjectVersions/CreateSchema/DeleteSubject/DeleteSchema/Compatibility/SetCompatibility/
//     CheckCompatibility/sr.NewClient/sr.URLs/sr.BasicAuth/sr.DialTLSConfig 逐一确认）。
//   - 唯一的签名级发现（非改名，是返回值形状）：(*sr.Client).DeleteSchema(ctx, subject string,
//     version int, how DeleteHow) error 只返回 error，不像 brief 插图暗示的那样顺带告诉调用方
//     "删的是哪个具体版本号"。domain 的 SchemaRegistryPort.DeleteVersion 签名是
//     (subject, version string, permanent bool) (int, error) —— 这个 int 由本包的
//     resolveDeleteVersion 在调用 DeleteSchema 之前，用 (*sr.Client).SubjectVersions 自己解析
//     "latest" 落到的具体版本号，再原样返回；DeleteSchema 本身收到的 version 参数就用这个已解析
//     的具体整数（sr 的 -1 latest 语义仍然存在，只是本包不依赖它去反推"删的是第几版"）。
//   - Compatibility(ctx, subjects...) 的"读某 subject 的有效兼容级别"语义需要
//     sr.WithParams(ctx, sr.DefaultToGlobal)：不传这个 Param，无 subject 级覆盖的 subject 会在
//     结果里带一个非 nil CompatibilityResult.Err（对应服务端 404 "Subject not found"）而不是
//     回落到全局默认值——SubjectCompat 与 SchemaByVersion 内部取 CompatLevel 都必须走这条路径，
//     GlobalCompat/SetGlobalCompat 因为本来就是空 subject 调用，不受影响。
//   - SchemaVersion.CompatLevel（domain 类型）没有直接对应的 sr.SubjectSchema 字段：
//     SchemaByVersion 拿到 sr.SubjectSchema 后，额外发一次 Compatibility 请求（同上一条的
//     DefaultToGlobal 用法）取 subject 的有效兼容级别一并塞进去——这是 Task 2 自己决定在 infra
//     层做的组装（brief 的 domain SchemaVersion 结构体本就把 CompatLevel 列为该类型的字段，
//     没有把这次额外拉取推给 Task 3 的 SchemaService）。
//   - 額外用到但 brief 清单未列出的符号：sr.Schema/sr.SubjectSchema/sr.SchemaReference（构造/
//     解构请求体）、sr.SetCompatibility（SetGlobalCompat/SetSubjectCompat 的请求体类型）、
//     sr.DeleteHow/SoftDelete/HardDelete（soft/permanent 开关）、(*sr.SchemaType).UnmarshalText、
//     (*sr.CompatibilityLevel).UnmarshalText（复用 sr 自带的契约同形枚举解析，而非本包重新写一份
//     字符串映射表）、sr.CompatibilityResult（Compatibility/SetCompatibility 的返回元素类型）。
var (
	// --- 客户端构造与连接选项 ---
	_ = sr.NewClient
	_ = sr.URLs
	_ = sr.BasicAuth
	_ = sr.DialTLSConfig

	// --- 读 ---
	_ = (*sr.Client).Subjects
	_ = (*sr.Client).SchemaByID
	_ = (*sr.Client).SchemaByVersion
	_ = (*sr.Client).SubjectVersions

	// --- 写 ---
	_ = (*sr.Client).CreateSchema
	_ = (*sr.Client).DeleteSubject
	_ = (*sr.Client).DeleteSchema

	// --- 兼容性 ---
	_ = (*sr.Client).Compatibility
	_ = (*sr.Client).SetCompatibility
	_ = (*sr.Client).CheckCompatibility

	// --- 请求/响应体类型 与 枚举解析（本包 client.go 里用于构造/解构，非猜测补记）---
	_ sr.Schema              = sr.Schema{}
	_ sr.SubjectSchema       = sr.SubjectSchema{}
	_ sr.SchemaReference     = sr.SchemaReference{}
	_ sr.SetCompatibility    = sr.SetCompatibility{}
	_ sr.CompatibilityResult = sr.CompatibilityResult{}
	_ sr.DeleteHow           = sr.SoftDelete
	_ sr.DeleteHow           = sr.HardDelete
	_                        = (*sr.SchemaType).UnmarshalText
	_                        = (*sr.CompatibilityLevel).UnmarshalText
	_                        = sr.WithParams
	_                        = sr.DefaultToGlobal
)
