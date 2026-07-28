package cluster

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// StateCache 持有每集群运行时快照：后台按 refreshEvery 刷新，读路径零 IO。
// 上游对应 ClustersStatisticsScheduler + StatisticsCache 的 P1a 等价物。
//
// 每集群一个后台抓取 goroutine，可被 Reload 单独取消/新建（配置向导平滑重载，
// P1c Task 14）：baseCtx 是 Start 收到的进程级 ctx，per-cluster 子 ctx 都从它派生
// （故新起的 goroutine 活到进程退出、而非某个请求结束）；cancels 记每集群的取消函数，
// 与 states 同受 mu 保护。
type StateCache struct {
	res          *Resolver
	scraper      cluster.StateScraper
	lifecycle    cluster.ClientLifecycle
	refreshEvery time.Duration

	mu      sync.RWMutex
	states  map[string]cluster.RuntimeState
	baseCtx context.Context
	cancels map[string]context.CancelFunc
}

func NewStateCache(res *Resolver, scraper cluster.StateScraper, lifecycle cluster.ClientLifecycle, refreshEvery time.Duration) *StateCache {
	return &StateCache{res: res, scraper: scraper, lifecycle: lifecycle, refreshEvery: refreshEvery,
		states: map[string]cluster.RuntimeState{}, cancels: map[string]context.CancelFunc{}}
}

// Start 启动后台刷新（每集群并行、集群间独立），ctx 取消即停。记录 ctx 为 baseCtx，
// 供后续 Reload 新起的集群 goroutine 派生子 ctx。
func (c *StateCache) Start(ctx context.Context) {
	c.mu.Lock()
	c.baseCtx = ctx
	c.mu.Unlock()
	for _, d := range c.res.Definitions() {
		c.launch(d.Name)
	}
}

// launch 为 name 起一个后台抓取 goroutine，子 ctx 从 baseCtx 派生，取消函数存入
// cancels（供 Reload 单独停掉该集群）。调用方不得持 mu。幂等：若该 name 已有在跑的
// goroutine（cancels 里已有），直接返回、不重复启动——否则并发 Reload 各自 launch 同一
// 新集群会双启动 goroutine（旧 cancel 被覆写、无法取消而泄漏，且双重抓取）。
func (c *StateCache) launch(name string) {
	c.mu.Lock()
	if c.baseCtx == nil { // Reload/launch 早于 Start：无 base ctx 可派生，跳过
		c.mu.Unlock()
		return
	}
	if _, running := c.cancels[name]; running {
		c.mu.Unlock()
		return
	}
	cctx, cancel := context.WithCancel(c.baseCtx)
	c.cancels[name] = cancel
	c.mu.Unlock()
	go c.scrapeLoop(cctx, name)
}

// scrapeLoop 是单集群后台循环：首轮立即抓一次，之后每 refreshEvery 一抓，ctx 取消即退。
func (c *StateCache) scrapeLoop(ctx context.Context, name string) {
	c.scrapeNow(ctx, name)
	t := time.NewTicker(c.refreshEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.scrapeNow(ctx, name)
		}
	}
}

// scrapeNow 按名取当前 Definition 再抓一次——按名（而非闭包捕获 def）是为了让"配置
// 变更但未删除"的集群下一 tick 自动用新 def 抓取；集群已从 Resolver 移除时 Lookup 失败，
// 本轮直接放弃（配合 Reload 删除其缓存条目，移除的集群不再出现）。
func (c *StateCache) scrapeNow(ctx context.Context, name string) {
	if ctx.Err() != nil {
		return
	}
	d, err := c.res.Lookup(name)
	if err != nil {
		return
	}
	c.refreshOne(ctx, d)
}

// refreshOne 抓取单个集群的最新状态并写入缓存，两条新鲜度/取消守卫：
//   - ctx 取消（调用方已放弃，如进程退出/请求中断）：FetchState 的报错不可信，
//     直接返回、不碰缓存——避免用取消噪音污染已有的（可能仍然有效的）状态。
//   - 旧数据永不覆盖新数据：若缓存里已有 RefreshedAt 更新的条目（后台 tick 与
//     显式 Refresh 竞态时，慢的旧一轮可能晚到），拒绝这次写，保留已有的新状态。
//
// 真正的抓取失败（非取消）仍然入缓存——Status/Err 已表达失败态，这是
// TestStateCacheKeepsServingWhenRefreshFails 锁定的既有行为。
func (c *StateCache) refreshOne(ctx context.Context, d cluster.Definition) cluster.RuntimeState {
	fctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, err := c.scraper.FetchState(fctx, d)
	if err != nil && ctx.Err() != nil {
		// 调用方取消（进程退出/请求中断）：结果不可信，不污染缓存
		return st
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil {
		// This scrape's context was cancelled (process shutdown, or a Reload
		// that removed this cluster) while FetchState ran: don't write, so a
		// removed cluster can't be resurrected between Reload's delete and this
		// goroutine noticing it's done. No-op for the live-ctx callers (the
		// background tick and forced Refresh).
		return st
	}
	if cur, ok := c.states[d.Name]; ok && cur.RefreshedAt.After(st.RefreshedAt) {
		return cur // 已有更新的数据：拒绝旧写（Refresh/后台竞态）
	}
	if st.Err != "" {
		slog.Warn("cluster state refresh failed", "cluster", d.Name, "err", st.Err)
	}
	c.states[d.Name] = st
	return st
}

func (c *StateCache) List(context.Context) []cluster.Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	defs := c.res.Definitions()
	out := make([]cluster.Snapshot, 0, len(defs))
	for _, d := range defs { // 保持配置顺序
		if st, ok := c.states[d.Name]; ok {
			out = append(out, st.Snapshot())
		} else { // 首轮未完成：先按未知=OFFLINE 呈现
			out = append(out, cluster.Snapshot{Definition: d,
				Status: cluster.StatusOffline,
				// TopicDeletionEnabled 显式置 true：首轮扫描前尚无运行时信号，
				// 与 FetchState "读不到默认 true" 的约定保持一致，而不是
				// RuntimeState{} 零值 false 造成的误报"已关闭"。
				Features: cluster.RuntimeState{Definition: d, TopicDeletionEnabled: true}.Features()})
		}
	}
	return out
}

func (c *StateCache) Get(_ context.Context, name string) (cluster.RuntimeState, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	st, ok := c.states[name]
	return st, ok
}

// Refresh 强制刷新单个集群（POST /clusters/{c}/cache 端点语义）。
func (c *StateCache) Refresh(ctx context.Context, name string) (cluster.RuntimeState, error) {
	d, err := c.res.Lookup(name)
	if err != nil {
		return cluster.RuntimeState{}, err
	}
	c.lifecycle.Invalidate(name) // 连接可能已腐坏：先失效再抓取
	return c.refreshOne(ctx, d), nil
}

// RefreshWithoutInvalidate re-scrapes name using the EXISTING pooled connection
// -- unlike Refresh, it does NOT call lifecycle.Invalidate. This is the light
// read-your-writes refresh wired after a message produce/delete (P1c Task 16):
// the connection is known-good (a write just succeeded over it), only the
// cached topic counts (messagesCount) are stale, so paying Refresh's
// Invalidate+reconnect+scrape cost per message would be wasteful -- exactly the
// refresh-without-invalidate optimization ADR-0005 §3 anticipated for
// high-frequency writes.
func (c *StateCache) RefreshWithoutInvalidate(ctx context.Context, name string) (cluster.RuntimeState, error) {
	d, err := c.res.Lookup(name)
	if err != nil {
		return cluster.RuntimeState{}, err
	}
	return c.refreshOne(ctx, d), nil
}

// Reload realigns the per-cluster scrape goroutines to the Resolver's current
// defs (call after Resolver.Replace, config-wizard reload P1c Task 14): removed
// clusters' goroutines are cancelled and their cache entries dropped; added
// clusters get a fresh goroutine (which scrapes immediately). Clusters present
// in both keep their goroutine untouched -- it picks up any changed Definition
// on its next tick (scrapeNow re-Looks-up by name), and the Reloader's
// Invalidate handles dropping their stale connection. No-op if called before
// Start (no baseCtx to derive goroutines from).
func (c *StateCache) Reload(ctx context.Context) {
	desired := map[string]bool{}
	definitions := map[string]cluster.Definition{}
	for _, d := range c.res.Definitions() {
		desired[d.Name] = true
		definitions[d.Name] = d
	}

	var toAdd []string
	c.mu.Lock()
	for name, cancel := range c.cancels {
		if !desired[name] {
			cancel()
			delete(c.cancels, name)
			delete(c.states, name)
		}
	}
	for name := range desired {
		if _, running := c.cancels[name]; !running {
			toAdd = append(toAdd, name)
		}
		if current, ok := c.states[name]; ok {
			current.Definition = definitions[name]
			c.states[name] = current
		}
	}
	c.mu.Unlock()

	for _, name := range toAdd {
		c.launch(name)
	}
}
