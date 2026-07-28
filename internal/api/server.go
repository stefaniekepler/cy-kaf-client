package api

import (
	"context"
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/app/mcpsettings"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/version"
)

// KsqlServicer is the narrow API-facing boundary for KSQL command
// registration, one-shot response pipes, and metadata lists.  The API maps
// the returned domain values to generated OpenAPI models; it deliberately
// does not know anything about the ksqlDB wire format.
type KsqlServicer interface {
	Register(context.Context, string, string, map[string]string) (string, error)
	Open(context.Context, string, string, func(domaincluster.KsqlTable) error) error
	ListStreams(context.Context, string) ([]domaincluster.KsqlStreamDescription, error)
	ListTables(context.Context, string) ([]domaincluster.KsqlTableDescription, error)
}

// MessageServicer is the narrow interface Task 9's SSE handler (not built
// by this task -- see message.go's package doc comment) will consume;
// *appcluster.MessageService satisfies it (same "app service, api depends
// on its own narrow interface" shape as SerdesServicer/TopicServicer/
// GroupServicer). Declared here, ahead of that handler, purely so
// Deps.Messages has a concrete field type to wire through cmd/main.go now.
//
// Send/Delete (P1c Task 11) are handlers_message.go's SendTopicMessages/
// DeleteTopicMessages handlers' backing calls -- genuine cluster-scoped
// writes, unlike Browse (a read).
type MessageServicer interface {
	Browse(ctx context.Context, name, topic string, spec appcluster.BrowseSpec, emit func(appcluster.BrowseEvent) error) error
	Send(ctx context.Context, name, topic string, spec appcluster.SendSpec) error
	Delete(ctx context.Context, name, topic string, partitions []int32) error
}

// SmartFilterServicer is the narrow interface handlers_smartfilter.go's
// RegisterFilter/ExecuteSmartFilterTest consume (P1c Task 12);
// *appcluster.SmartFilterService satisfies it. Both operations are read-only
// in effect -- Register only compiles a filter and caches it under a
// deterministic id, Test only evaluates one against a sample record; neither
// touches cluster state. That is why registerFilter, though its path is
// cluster-scoped (.../topics/{topicName}/smartfilters, a POST), is whitelisted
// past readOnlyGuard rather than 403'd on a read-only cluster (see
// middleware.go's readOnlyWhitelistPatterns).
type SmartFilterServicer interface {
	Register(filterCode string) (id string, err error)
	Test(exec appcluster.SmartFilterTest) (matched bool, evalErr string, compileErr error)
}

// AnalysisServicer is the narrow interface for the asynchronous whole-topic
// analysis handlers. Get returns an error separately from found so the API can
// distinguish an unknown cluster from a known cluster with no analysis run;
// both cases are contract-level 404 responses with different messages.
type AnalysisServicer interface {
	Analyze(ctx context.Context, name, topic string) error
	Get(name, topic string) (appcluster.AnalysisView, bool, error)
	Cancel(ctx context.Context, name, topic string) error
}

type DesktopMCPServicer interface {
	Get(context.Context) (mcpsettings.Policy, error)
	Update(context.Context, mcpsettings.UpdateRequest) (mcpsettings.Policy, error)
	Integrations(context.Context) ([]mcpsettings.Integration, error)
	Configure(context.Context, mcpsettings.Client, bool) (mcpsettings.Integration, error)
}

type Deps struct {
	States  ClusterStater  // 生产环境注入 *appcluster.StateCache
	LogDirs LogDirser      // 生产环境注入 *appcluster.BrokerService
	Brokers BrokerAdmin    // 生产环境注入 *appcluster.BrokerService（与 LogDirs 同一实例）
	Topics  TopicServicer  // 生产环境注入 *appcluster.TopicService（P1b Task 4）
	Groups  GroupServicer  // 生产环境注入 *appcluster.GroupService（P1b Task 6）
	Serdes  SerdesServicer // 生产环境注入 *appcluster.SerdeService（P1c Task 3）
	Schemas SchemaServicer // 生产环境注入 *appcluster.SchemaService（P2a Task 3）
	// Connects 供 getConnects/getConnectsCsv/getConnectorPlugins/
	// validateConnectorPluginConfig（P2b Task 3；Task 4/5 增补同接口的连接器
	// 读/写/动作端点）。生产环境注入 *appcluster.ConnectService（包裹
	// infra/connect.Pool）。
	Connects ConnectServicer
	// Acls 供 listAcls/getAclAsCsv/createAcl/deleteAcl/createConsumerAcl/
	// createProducerAcl/createStreamAppAcl/syncAclsCsv（P2c Task 6）。
	// 生产环境注入 *appcluster.AclService（包裹 infra/kafka.Pool 的 AclAdminPort）。
	Acls AclServicer
	// Quotas 供 listQuotas/upsertClientQuotas（P2c Task 7）。生产环境注入
	// *appcluster.QuotaService（包裹 infra/kafka.Pool 的 QuotaPort）。
	Quotas   QuotaServicer
	Messages MessageServicer // 生产环境注入 *appcluster.MessageService（P1c Task 9 handler 消费；本任务仅装配，尚无 handler 解引用）
	// SmartFilters 供 registerFilter/executeSmartFilterTest（P1c Task 12）。
	// 生产环境注入 *appcluster.SmartFilterService（与 Messages 共用同一
	// infra/filter.Engine，注册的 id 才能在 Browse 的 smartFilterId 路径解析）。
	SmartFilters SmartFilterServicer
	// Config 供 getCurrentConfig/validateConfig（P1c Task 13 读+校验）
	// 与 uploadConfigRelatedFile（Task 14）。生产环境注入
	// *appcluster.ConfigService（包裹 *infra/config.Store）。
	Config ConfigServicer
	// Ksql 供 KSQL 四端点（P3）：登记懒执行 pipe、打开 SSE 响应以及
	// 查询 streams/tables 元数据。生产环境注入 *appcluster.KsqlService。
	Ksql KsqlServicer
	// Analysis 供 analyzeTopic/getTopicAnalysis/cancelTopicAnalysis（P2d）。
	// 生产环境注入 *appcluster.AnalysisService。
	Analysis AnalysisServicer
	// Reloader 供 restartWithConfig（P1c Task 14 进程内平滑重载）。
	// 生产环境注入 *appcluster.Reloader。
	Reloader ReloaderServicer
	// IsReadOnly 供 readOnlyGuard 判定集群是否只读。生产环境注入
	// *appcluster.Resolver.IsReadOnly；调用方必须提供非 nil 值（测试助手默认注入
	// 恒 false 桩，见 handlers_test.go newTestServer/newStatesTestServer）——
	// readOnlyGuard 本身不做 nil 兜底，未设置会在首个写请求触达时 panic
	// （由已挂载的 middleware.Recoverer 兜底为 500，而非直接崩溃进程）。
	IsReadOnly func(string) bool
	Build      version.BuildInfo
	Static     fs.FS
	BasePath   string
	Desktop    *DesktopOptions
	DesktopMCP DesktopMCPServicer
}

// apiServer 实现 generated.ServerInterface：嵌入 501 桩基座，
// 已实现的端点在本包 handlers_*.go 中以同名方法覆写。
type apiServer struct {
	generated.UnimplementedServer
	deps Deps
}

// 编译期证明：apiServer 始终满足全量契约接口
var _ generated.ServerInterface = (*apiServer)(nil)

func NewServer(d Deps) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(hostAllowlist)
	if d.Desktop != nil {
		r.Use(func(next http.Handler) http.Handler {
			return desktopSessionGuard(d.Desktop, next)
		})
	}
	r.Use(func(next http.Handler) http.Handler { return readOnlyGuard(d.IsReadOnly, next) })
	s := &apiServer{deps: d}
	r.Get("/actuator/health", s.health) // 契约外，手动挂载
	if d.Desktop != nil {
		r.Post("/__desktop/shutdown", func(w http.ResponseWriter, _ *http.Request) {
			if d.Desktop.Shutdown == nil {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{
					"message": "desktop shutdown unavailable",
				})
				return
			}
			w.WriteHeader(http.StatusNoContent)
			d.Desktop.Shutdown()
		})
	}
	if d.Desktop != nil && d.DesktopMCP != nil {
		mountDesktopMCP(r, d.DesktopMCP)
	}
	generated.HandlerWithOptions(s, generated.ChiServerOptions{BaseRouter: r})
	mountDesktopNotFound(r, d.Desktop != nil)
	mountStatic(r, d.Static, d.BasePath)
	return r
}
