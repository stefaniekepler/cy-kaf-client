package api

import (
	"io"
	"io/fs"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

// mountStatic serves the embedded SPA, replacing the upstream Spring
// placeholder PUBLIC-PATH-VARIABLE in index.html with the configured base
// path, and falling back to index.html for client-side routes (never /api/*).
func mountStatic(r chi.Router, static fs.FS, basePath string) {
	index := loadIndex(static, basePath)
	fileServer := http.FileServer(http.FS(static))
	r.NotFound(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, "/api/") ||
			strings.HasPrefix(req.URL.Path, "/actuator/") ||
			strings.HasPrefix(req.URL.Path, "/__desktop/") {
			writeJSON(w, http.StatusNotFound, map[string]string{"message": "not found"})
			return
		}
		p := strings.TrimPrefix(req.URL.Path, "/")
		if p != "" && p != "index.html" {
			if f, err := static.Open(p); err == nil {
				_ = f.Close()
				if strings.HasPrefix(p, "assets/") {
					// vite content-hashed filenames: safe to cache forever.
					w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				} else {
					// non-hashed static files (manifest.json, favicon, ...): must revalidate.
					w.Header().Set("Cache-Control", "no-cache")
				}
				fileServer.ServeHTTP(w, req)
				return
			}
		}
		// index.html itself (direct request or SPA fallback): must revalidate.
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(index)
	})
}

func loadIndex(static fs.FS, basePath string) []byte {
	f, err := static.Open("index.html")
	if err != nil {
		return []byte("cy-kaf-client: frontend assets not embedded (run `make build-fe`)")
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(f)
	if err != nil {
		return []byte("cy-kaf-client: failed reading embedded index.html")
	}
	html := strings.ReplaceAll(string(raw), "PUBLIC-PATH-VARIABLE", basePath)

	// 深路由白屏修复：上游构建产物的三个 vite 入口标签（script/modulepreload/
	// stylesheet 的 assets/...）是裸相对路径，不含 PUBLIC-PATH-VARIABLE 占位符
	// （已用 webui/static/index.html 真实产物核实）。硬刷新一个深路径（如
	// /ui/clusters/local/all-topics）时，浏览器按当前路径而非站点根解析这些相对
	// 引用，请求落回 SPA fallback 拿到 HTML 而非 JS/CSS，白屏。注入 <base href>
	// 让所有相对引用（包括这三个不含占位符的标签）都相对于站点根解析，无论
	// 触发请求的路径深度。
	base := basePath
	if base == "" || !strings.HasSuffix(base, "/") {
		base += "/"
	}
	html = strings.Replace(html, "<head>",
		"<head><base href=\""+base+"\">", 1)

	return []byte(html)
}
