// Package webui embeds the built frontend (populated by `make build-fe`).
package webui

import (
	"embed"
	"io/fs"
)

//go:embed all:static
var static embed.FS

func FS() fs.FS {
	sub, err := fs.Sub(static, "static")
	if err != nil {
		panic(err) // embed 布局由构建保证，不可恢复
	}
	return sub
}
