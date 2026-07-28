package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// ClusterStater is the single data source api consumes for everything cluster
// state related: cached list view, single-cluster read, and forced refresh.
// app.StateCache satisfies this (List/Get/Refresh).
type ClusterStater interface {
	List(ctx context.Context) []cluster.Snapshot
	Get(ctx context.Context, name string) (cluster.RuntimeState, bool)
	Refresh(ctx context.Context, name string) (cluster.RuntimeState, error)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// serverError is every handler's single path to a 500 response: it logs the
// underlying err (op names the failing operation, e.g. "GetBrokerConfig")
// through the default slog logger, then writes a contract-shaped 500 whose
// body carries only the fixed, safe message — never err's own text (the
// existing "500 must not leak the underlying error" rule every *BackendFailureIs500
// test in this package already asserts via assertErrorEnvelope's wantAbsent
// param). Centralizes that split in one place instead of repeated ad hoc
// slog+writeJSON pairs at every 500 call site.
func serverError(w http.ResponseWriter, op, message string, err error) {
	slog.Error("request failed", "op", op, "err", err)
	writeJSON(w, http.StatusInternalServerError, errorResponse(http.StatusInternalServerError, message))
}

func (s *apiServer) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
}

// GetApplicationInfo 返回构建元数据。ApplicationInfo.Build 在生成代码中是匿名内联
// struct（契约里 build 是未具名的嵌套 object），字段名/tag 必须与
// generated/models.gen.go 完全一致（ADR-0002 对照表）。
func (s *apiServer) GetApplicationInfo(w http.ResponseWriter, _ *http.Request) {
	latest := true // D11: 版本检查默认关闭 → 恒报最新
	b := s.deps.Build
	writeJSON(w, http.StatusOK, generated.ApplicationInfo{
		EnabledFeatures: &[]generated.ApplicationInfoEnabledFeatures{
			generated.DYNAMICCONFIG,
		},
		Build: &struct {
			BuildTime       *string `json:"buildTime,omitempty"`
			CommitId        *string `json:"commitId,omitempty"`
			IsLatestRelease *bool   `json:"isLatestRelease,omitempty"`
			Version         *string `json:"version,omitempty"`
		}{
			BuildTime: &b.BuildTime, CommitId: &b.Commit,
			IsLatestRelease: &latest, Version: &b.Version,
		},
	})
}

func (s *apiServer) GetAuthenticationSettings(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, generated.AppAuthenticationSettings{
		AuthType:       ptr(generated.DISABLED),
		OAuthProviders: &[]generated.OAuthProvider{},
	})
}

func (s *apiServer) GetUserAuthInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, generated.AuthenticationInfo{
		RbacEnabled: false,
		UserInfo:    &generated.UserInfo{Username: "", Permissions: []generated.UserPermission{}},
	})
}

func (s *apiServer) GetClusters(w http.ResponseWriter, r *http.Request) {
	snaps := s.deps.States.List(r.Context())
	out := make([]generated.Cluster, 0, len(snaps))
	for _, sn := range snaps {
		out = append(out, snapshotToGenerated(sn))
	}
	writeJSON(w, http.StatusOK, out)
}

// snapshotToGenerated maps a domain Snapshot onto the contract's Cluster
// shape. Extracted from GetClusters (P0) so UpdateClusterInfo — which returns
// the same Cluster representation after a forced refresh — can reuse it.
func snapshotToGenerated(sn cluster.Snapshot) generated.Cluster {
	feats := make([]generated.ClusterFeatures, 0, len(sn.Features))
	for _, f := range sn.Features {
		feats = append(feats, generated.ClusterFeatures(f))
	}
	c := generated.Cluster{
		Name:     sn.Definition.Name,
		ReadOnly: ptr(sn.Definition.ReadOnly),
		Features: &feats,
	}
	if sn.Status == cluster.StatusOnline {
		c.Status = generated.ONLINE
		c.BrokerCount = ptr(int32(sn.BrokerCount))
	} else {
		c.Status = generated.OFFLINE
	}
	return c
}

// GetClusterStats serves /api/clusters/{clusterName}/stats from the cached
// runtime snapshot (no live probe on the request path).
func (s *apiServer) GetClusterStats(w http.ResponseWriter, r *http.Request, clusterName string) {
	st, ok := s.deps.States.Get(r.Context(), clusterName)
	if !ok {
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
		return
	}
	stats := generated.ClusterStats{
		BrokerCount:                   ptr(int32(len(st.Brokers))),
		ActiveControllers:             ptr(st.Controller), // 契约语义是 controller broker 的 ID，不是数量
		OnlinePartitionCount:          ptr(int32(st.Partitions.Online)),
		OfflinePartitionCount:         ptr(int32(st.Partitions.Offline)),
		InSyncReplicasCount:           ptr(int32(st.Partitions.InSync)),
		OutOfSyncReplicasCount:        ptr(int32(st.Partitions.OutOfSync)),
		UnderReplicatedPartitionCount: ptr(int32(st.Partitions.UnderReplicated)),
	}
	var du []generated.BrokerDiskUsage
	for _, d := range st.Disk {
		du = append(du, generated.BrokerDiskUsage{BrokerId: d.Broker,
			SegmentSize: ptr(d.SegmentSize), SegmentCount: ptr(int32(d.SegmentCount))})
	}
	if len(du) > 0 { // 契约 diskUsage 非 nullable：没有数据时整字段省略，而不是发 null
		stats.DiskUsage = &du
	}
	if st.Version != "" {
		stats.Version = ptr(st.Version)
	}
	writeJSON(w, http.StatusOK, stats)
}

// UpdateClusterInfo serves POST /api/clusters/{clusterName}/cache: force a
// refresh and return the resulting cluster row (contract's Cluster shape).
func (s *apiServer) UpdateClusterInfo(w http.ResponseWriter, r *http.Request, clusterName string) {
	st, err := s.deps.States.Refresh(r.Context(), clusterName)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "UpdateClusterInfo", "failed to refresh cluster", err)
		return
	}
	writeJSON(w, http.StatusOK, snapshotToGenerated(st.Snapshot()))
}

// GetClusterMetrics serves /api/clusters/{clusterName}/metrics: inferred
// Prometheus-style gauges derived from the cached snapshot (spec §7.5's
// "inferred metrics" tier, P1a subset — no external metrics backend).
func (s *apiServer) GetClusterMetrics(w http.ResponseWriter, r *http.Request, clusterName string) {
	st, ok := s.deps.States.Get(r.Context(), clusterName)
	if !ok {
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
		return
	}
	writeJSON(w, http.StatusOK, generated.ClusterMetrics{Items: inferredMetrics(st)})
}

// inferredMetrics assembles the P1a subset of inferred metrics: broker_count,
// topic_count, kafka_topic_partitions{status=online|offline} and, per broker
// with log dir data, broker_bytes_disk{broker=<id>}. Always returns a non-nil
// slice: ClusterMetrics.items is a required, non-nullable contract field.
func inferredMetrics(st cluster.RuntimeState) []generated.Metric {
	items := []generated.Metric{
		{Name: ptr("broker_count"), Value: ptr(float32(len(st.Brokers)))},
		{Name: ptr("topic_count"), Value: ptr(float32(st.TopicCount))},
		{Name: ptr("kafka_topic_partitions"), Value: ptr(float32(st.Partitions.Online)),
			Labels: ptr(map[string]string{"status": "online"})},
		{Name: ptr("kafka_topic_partitions"), Value: ptr(float32(st.Partitions.Offline)),
			Labels: ptr(map[string]string{"status": "offline"})},
	}
	for _, d := range st.Disk {
		items = append(items, generated.Metric{
			Name:   ptr("broker_bytes_disk"),
			Value:  ptr(float32(d.SegmentSize)),
			Labels: ptr(map[string]string{"broker": strconv.Itoa(int(d.Broker))}),
		})
	}
	return items
}

// errorResponse builds a contract-shaped ErrorResponse: code/message/
// requestId/timestamp are all required (non-pointer) fields in the schema,
// so every response — even ad hoc 404s — must populate all four.
func errorResponse(status int, message string) generated.ErrorResponse {
	return generated.ErrorResponse{
		Code:      int32(status),
		Message:   message,
		RequestId: fmt.Sprintf("%x", time.Now().UnixNano()),
		Timestamp: float32(time.Now().UnixMilli()),
	}
}

func ptr[T any](v T) *T { return &v }
