package api_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
)

func staticServer(t *testing.T) *httptest.Server {
	t.Helper()
	h := api.NewServer(api.Deps{
		States: fakeStater{},
		Static: fstest.MapFS{
			// <head> is a bare tag (no attributes), matching the real vite build
			// product (confirmed against webui/static/index.html) — it's
			// loadIndex's <base href> injection anchor (static.go).
			"index.html":    {Data: []byte(`<html><head><script>window.basePath="PUBLIC-PATH-VARIABLE"</script></head></html>`)},
			"assets/a.js":   {Data: []byte("console.log(1)")},
			"manifest.json": {Data: []byte(`{"name":"cy-kaf-client"}`)},
		},
	})
	return httptest.NewServer(h)
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func getHeader(t *testing.T, url string) (int, http.Header) {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header
}

func TestIndexPlaceholderReplacedWithBasePath(t *testing.T) {
	srv := staticServer(t)
	defer srv.Close()
	code, body := get(t, srv.URL+"/")
	require.Equal(t, 200, code)
	require.NotContains(t, body, "PUBLIC-PATH-VARIABLE") // 默认 basePath="" → 替换为空串
	require.Contains(t, body, `window.basePath=""`)
	code, body = get(t, srv.URL+"/index.html") // 直接请求 index.html 也不得泄露原始占位符
	require.Equal(t, 200, code)
	require.NotContains(t, body, "PUBLIC-PATH-VARIABLE")
}

func TestSPAFallbackAndAssets(t *testing.T) {
	srv := staticServer(t)
	defer srv.Close()
	code, body := get(t, srv.URL+"/ui/clusters/local/all-topics") // SPA 路由 → index.html
	require.Equal(t, 200, code)
	require.Contains(t, body, "window.basePath")
	code, body = get(t, srv.URL+"/assets/a.js")
	require.Equal(t, 200, code)
	require.Equal(t, "console.log(1)", body)
	code, _ = get(t, srv.URL+"/api/definitely-not-a-route") // /api/* 不允许回退到 SPA
	require.Equal(t, 404, code)
	code, _ = get(t, srv.URL+"/actuator/definitely-not-a-route")
	require.Equal(t, 404, code)
}

// TestIndexInjectsBaseHrefForDeepRoutes locks in the deep-route white-screen
// fix (loadIndex, static.go): every response carrying index.html — root,
// a deep SPA route, or a direct /index.html request — must contain a <base
// href> so the upstream build's un-templated relative asset references
// (script/modulepreload/stylesheet assets/... tags, which don't carry
// PUBLIC-PATH-VARIABLE) resolve against the site root instead of whatever
// deep path triggered the request.
func TestIndexInjectsBaseHrefForDeepRoutes(t *testing.T) {
	srv := staticServer(t) // 既有助手：fstest.MapFS 含占位符 index.html
	defer srv.Close()
	for _, p := range []string{"/", "/ui/clusters/local/all-topics", "/index.html"} {
		code, body := get(t, srv.URL+p)
		require.Equal(t, 200, code, p)
		require.Contains(t, body, `<base href="/">`, p) // 相对资源引用自深路径可绝对解析
	}
}

// TestCacheControlHeaders 校验规格 §5.3「静态资源缓存头对齐」义务：index.html
// （根路径/直接请求/SPA fallback）与非哈希命名静态文件必须 no-cache（每次revalidate，
// 避免品牌/路由变更后浏览器长期缓存旧壳）；assets/ 前缀的 vite 内容哈希文件可安全
// 长期不可变缓存。
func TestCacheControlHeaders(t *testing.T) {
	srv := staticServer(t)
	defer srv.Close()

	code, hdr := getHeader(t, srv.URL+"/")
	require.Equal(t, 200, code)
	require.Equal(t, "no-cache", hdr.Get("Cache-Control"))

	code, hdr = getHeader(t, srv.URL+"/index.html")
	require.Equal(t, 200, code)
	require.Equal(t, "no-cache", hdr.Get("Cache-Control"))

	code, hdr = getHeader(t, srv.URL+"/manifest.json")
	require.Equal(t, 200, code)
	require.Equal(t, "no-cache", hdr.Get("Cache-Control"))

	code, hdr = getHeader(t, srv.URL+"/assets/a.js")
	require.Equal(t, 200, code)
	require.Equal(t, "public, max-age=31536000, immutable", hdr.Get("Cache-Control"))
}
