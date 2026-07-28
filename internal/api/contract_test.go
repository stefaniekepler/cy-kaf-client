package api_test

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/stretchr/testify/require"
)

// contractPath 定位仓库根下的契约（测试从包目录运行）。
func contractPath(t *testing.T) string {
	t.Helper()
	_, self, _, _ := runtime.Caller(0)
	p := filepath.Join(filepath.Dir(self), "..", "..", "contract", "openapi.yaml")
	_, err := os.Stat(p)
	require.NoError(t, err, "contract/openapi.yaml 必须已 vendored（Task 3）")
	return p
}

var (
	contractOnce   sync.Once
	contractRouter routers.Router
	contractErr    error
)

// contractRouterFor 加载契约并构建路由。加载 5547 行契约一次约百毫秒级，
// 用 sync.Once 缓存，多个校验用例共享同一份 doc/router。
func contractRouterFor(t *testing.T) routers.Router {
	t.Helper()
	p := contractPath(t)
	contractOnce.Do(func() {
		doc, err := openapi3.NewLoader().LoadFromFile(p)
		if err != nil {
			contractErr = err
			return
		}
		// 契约 servers 声明 http://localhost:8080，而 httptest 监听
		// 127.0.0.1:<随机端口>；gorillamux 对非空 servers 会按 host 匹配，
		// 全部请求将失配。契约校验只关心 path/method/schema，这里清空
		// servers 走纯路径匹配。
		doc.Servers = nil
		contractRouter, contractErr = gorillamux.NewRouter(doc)
	})
	require.NoError(t, contractErr)
	return contractRouter
}

// TestContractValidatorRejectsBadBody 是负向对照：证明校验器确实按 schema
// 拒绝坏响应（缺 required 字段 rbacEnabled），排除"校验全部空转假绿"的可能。
func TestContractValidatorRejectsBadBody(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://any-host/api/authorization", nil)
	require.NoError(t, err)
	route, params, err := contractRouterFor(t).FindRoute(req)
	require.NoError(t, err)
	in := &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route}
	err = openapi3filter.ValidateResponse(req.Context(), &openapi3filter.ResponseValidationInput{
		RequestValidationInput: in, Status: 200,
		Header: http.Header{"Content-Type": []string{"application/json"}},
		Body:   io.NopCloser(bytes.NewReader([]byte(`{}`))),
	})
	require.ErrorContains(t, err, "rbacEnabled")
}

// validateAgainstContract 校验一次真实的 req/resp 是否符合 openapi 契约。
func validateAgainstContract(t *testing.T, req *http.Request, status int, hdr http.Header, body []byte) {
	t.Helper()
	router := contractRouterFor(t)
	route, params, err := router.FindRoute(req)
	require.NoError(t, err, "端点必须存在于契约中: %s %s", req.Method, req.URL.Path)
	in := &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route}
	require.NoError(t, openapi3filter.ValidateRequest(req.Context(), in))
	require.NoError(t, openapi3filter.ValidateResponse(req.Context(), &openapi3filter.ResponseValidationInput{
		RequestValidationInput: in, Status: status, Header: hdr,
		Body: io.NopCloser(bytes.NewReader(body)),
	}))
}
