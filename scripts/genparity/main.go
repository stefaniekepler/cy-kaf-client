// 从 contract/openapi.yaml 生成 parity matrix 骨架（一次性脚手架工具）。
// 用法: go run ./scripts/genparity contract/openapi.yaml > docs/parity/feature-matrix.md
package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type operation struct {
	OperationID string   `yaml:"operationId"`
	Tags        []string `yaml:"tags"`
}

type doc struct {
	Paths map[string]map[string]operation `yaml:"paths"`
}

var methods = map[string]bool{"get": true, "post": true, "put": true, "delete": true, "patch": true}

type row struct{ tag, method, path, opID string }

func main() {
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var d doc
	if err := yaml.Unmarshal(raw, &d); err != nil {
		panic(err)
	}
	rows := []row{}
	for p, item := range d.Paths {
		for m, op := range item {
			if !methods[strings.ToLower(m)] {
				continue
			}
			tag := "untagged"
			if len(op.Tags) > 0 {
				tag = op.Tags[0]
			}
			rows = append(rows, row{tag, strings.ToUpper(m), p, op.OperationID})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.tag != b.tag {
			return a.tag < b.tag
		}
		if a.path != b.path {
			return a.path < b.path
		}
		return a.method < b.method
	})
	fmt.Println("# 功能对等矩阵（kafbat/kafka-ui v1.5.0 → cy-kaf-client）")
	fmt.Println()
	fmt.Println("状态: `todo` 未实现 · `done` 已实现+已测 · `exempt` 已确认豁免(spec §6.3)")
	fmt.Println()
	fmt.Println("| Tag | Method | Path | operationId | 阶段 | 状态 | 测试引用 |")
	fmt.Println("|---|---|---|---|---|---|---|")
	for _, r := range rows {
		fmt.Printf("| %s | %s | `%s` | %s | - | todo | - |\n", r.tag, r.method, r.path, r.opID)
	}
	fmt.Fprintf(os.Stderr, "total endpoints: %d\n", len(rows))
}
