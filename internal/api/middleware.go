package api

import (
	"net"
	"net/http"
	"regexp"
)

// hostAllowlist 拒绝非本机 Host 头（DNS rebinding 防护，D2：服务只面向 localhost）。
func hostAllowlist(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		switch host {
		case "127.0.0.1", "localhost", "::1":
			next.ServeHTTP(w, r)
		default:
			writeJSON(w, http.StatusForbidden, map[string]string{"message": "forbidden host"})
		}
	})
}

// clusterPathRe 提取 /api/clusters/{name}/... 的集群名（一段式，不含斜杠）。
// 边界决策：无尾段的 /api/clusters（集群列表）不匹配——该路径本就只挂 GET，且
// 契约里没有 POST /api/clusters（集群通过配置文件而非 API 创建），万一未来出现
// 这种形态的写请求，本正则按"非集群子路径"放行，404/桩兜底，不由本守卫越权判定。
var clusterPathRe = regexp.MustCompile(`^/api/clusters/([^/]+)(/.*)?$`)

// readOnlyWhitelist 放行只读语义的写方法端点（上游 ReadOnlyModeFilter 同思路）。
// key: "METHOD 子路径后缀"（P1c 增补 smartfilters/testexecutions 等时改此表）。
var readOnlyWhitelist = map[string]bool{
	"POST /cache": true, // 强制刷新缓存：不改变集群本身状态
}

// readOnlyWhitelistPatterns 是 readOnlyWhitelist 的正则版：当只读语义的写端点
// 路径含活变量段（如 registerFilter 的 .../topics/{topicName}/smartfilters）时，
// 精确字符串 map 匹配不到，改由这里对同一 "METHOD 子路径后缀" 串做模式匹配。
// P1c Task 12 引入：registerFilter 语义上=编译+试运行一段 CEL（不改集群状态），
// 故只读集群应放行；正则用 [^/]+ 精确锁住恰好一段活 topic 名并以 smartfilters
// 收尾（$ 锚定），任何多余子路径段（.../smartfilters/xxx）落回守卫的 403 兜底。
var readOnlyWhitelistPatterns = []*regexp.Regexp{
	regexp.MustCompile(`^POST /topics/[^/]+/smartfilters$`),
	// checkSchemaCompatibility（P2a Task 5）：POST .../schemas/{subject}/check 是
	// 只读语义（干跑校验，不注册、不改兼容级别），路径含活 subject 变量段，精确
	// map 匹配不到，故只读集群应放行；[^/]+ 精确锁住恰好一段 subject 并以 /check
	// 收尾（$ 锚定），多余子路径段落回守卫的 403 兜底。
	regexp.MustCompile(`^POST /schemas/[^/]+/check$`),
	// validateConnectorPluginConfig（P2b Task 3，P2b-D6）：PUT .../connects/
	// {connectName}/plugins/{pluginName}/config/validate 是只读语义（干跑校验插件
	// 配置，不创建/修改任何 connector），路径含两段活变量（connectName、
	// pluginName），精确 map 匹配不到，故只读集群应放行；两个 [^/]+ 精确锁住恰好
	// 各一段并以 /config/validate 收尾（$ 锚定），多余子路径段（或真写端点，如
	// PUT .../connectors/{c}/config）落回守卫的 403 兜底。
	regexp.MustCompile(`^PUT /connects/[^/]+/plugins/[^/]+/config/validate$`),
	// analyzeTopic/cancelTopicAnalysis（P2d）：扫描消息或取消内存中的分析
	// job，不创建/修改 Kafka 状态，故只读集群也应允许；活 topic 段必须精确
	// 锚定，额外路径段继续落回 403 兜底。
	regexp.MustCompile(`^POST /topics/[^/]+/analysis$`),
	regexp.MustCompile(`^DELETE /topics/[^/]+/analysis$`),
}

// readOnlyGuard 拒绝对 readOnly 集群的写请求（POST/PUT/PATCH/DELETE），白名单内
// 只读语义的写方法除外。未知集群交给 isReadOnly 返回 false（Resolver.IsReadOnly
// 语义），本守卫不越权判定 404——那是 handler 层通过 Lookup/ErrUnknownCluster 的职责。
func readOnlyGuard(isReadOnly func(string) bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		default:
			next.ServeHTTP(w, r)
			return
		}
		m := clusterPathRe.FindStringSubmatch(r.URL.Path)
		if m == nil || !isReadOnly(m[1]) {
			next.ServeHTTP(w, r)
			return
		}
		suffix := m[2]
		key := r.Method + " " + suffix
		if readOnlyWhitelist[key] {
			next.ServeHTTP(w, r)
			return
		}
		for _, re := range readOnlyWhitelistPatterns {
			if re.MatchString(key) {
				next.ServeHTTP(w, r)
				return
			}
		}
		writeJSON(w, http.StatusForbidden,
			errorResponse(http.StatusForbidden, "cluster is in read-only mode"))
	})
}
