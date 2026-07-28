package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHostAllowlist(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	h := hostAllowlist(inner)
	for _, host := range []string{"127.0.0.1:8080", "localhost:8080", "localhost", "[::1]:9999"} {
		req := httptest.NewRequest(http.MethodGet, "/api/clusters", nil)
		req.Host = host
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, 200, rec.Code, host)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/clusters", nil)
	req.Host = "evil.example.com:8080" // DNS rebinding 载荷
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestReadOnlyGuard(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	ro := func(name string) bool { return name == "prod" }
	h := readOnlyGuard(ro, inner)
	cases := []struct {
		method, path string
		want         int
	}{
		{"GET", "/api/clusters/prod/topics", 200},  // 读操作放行
		{"POST", "/api/clusters/prod/topics", 403}, // readOnly 集群写被拒
		{"DELETE", "/api/clusters/prod/topics/x", 403},
		{"PATCH", "/api/clusters/prod/topics/x", 403},
		{"POST", "/api/clusters/prod/cache", 200}, // 白名单：只读语义的 POST
		{"POST", "/api/clusters/dev/topics", 200}, // 非 readOnly 集群放行
		{"POST", "/api/other", 200},               // 非集群路径不适用
		// 模式白名单（P1c Task 12）：registerFilter 注册=试运行语义，路径含活 topic
		// 变量段，精确 map 匹配不到，靠 readOnlyWhitelistPatterns 放行只读集群。
		{"POST", "/api/clusters/prod/topics/my-topic/smartfilters", 200},
		// 对照：同集群同 topic 的真写（发消息）不在任何白名单，仍拒。
		{"POST", "/api/clusters/prod/topics/my-topic/messages", 403},
		// 边界：smartfilters 前缀但多一段子路径，不得被模式误放行（真写兜底 403）。
		{"POST", "/api/clusters/prod/topics/my-topic/smartfilters/extra", 403},
		// 模式白名单（P2a Task 5）：checkSchemaCompatibility 是只读语义的 POST（干跑，
		// 不改状态），路径含活 subject 变量段，靠 readOnlyWhitelistPatterns 放行。
		{"POST", "/api/clusters/prod/schemas/orders-value/check", 200},
		// 对照：改 subject 兼容级别是真写，仍拒。
		{"PUT", "/api/clusters/prod/schemas/orders-value/compatibility", 403},
		// 对照：注册新 schema 是真写，仍拒。
		{"POST", "/api/clusters/prod/schemas", 403},
		// 边界：check 前缀但多一段子路径，不得被模式误放行（真写兜底 403）。
		{"POST", "/api/clusters/prod/schemas/orders-value/check/extra", 403},
		// 模式白名单（P2b Task 3，P2b-D6）：validateConnectorPluginConfig 是只读语义
		// 的 PUT（干跑校验插件配置，不创建/修改 connector），路径含 connectName+
		// pluginName 两段活变量，靠 readOnlyWhitelistPatterns 放行只读集群。
		{"PUT", "/api/clusters/prod/connects/connect-1/plugins/JdbcSinkConnector/config/validate", 200},
		// 对照：改一个 connector 的 config 是真写，仍拒。
		{"PUT", "/api/clusters/prod/connects/connect-1/connectors/my-connector/config", 403},
		// 边界：validate 前缀但多一段子路径，不得被模式误放行（真写兜底 403）。
		{"PUT", "/api/clusters/prod/connects/connect-1/plugins/JdbcSinkConnector/config/validate/extra", 403},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, tc.want, rec.Code, "%s %s", tc.method, tc.path)
	}
	// 403 信封
	req := httptest.NewRequest("POST", "/api/clusters/prod/topics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Contains(t, body["message"], "read-only")
	require.NotZero(t, body["code"])
}
