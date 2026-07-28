package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// LogDirser is the single data source GetAllBrokersLogdirs consumes:
// app.BrokerService satisfies this by resolving clusterName to its
// configured Definition (via the same Resolver.Lookup app.StateCache's
// Refresh uses) and delegating to the underlying kadm client pool.
type LogDirser interface {
	LogDirs(ctx context.Context, name string, brokers []int32) ([]cluster.BrokerLogDirs, error)
}

// BrokerAdmin is the data source GetBrokerConfig/UpdateBrokerConfigByName/
// UpdateBrokerTopicPartitionLogDir consume: broker-scoped admin operations
// that go through the live kadm client on every call (unlike
// ClusterStater/LogDirser, which serve a periodically-refreshed cache or a
// per-request describe). app.BrokerService satisfies this via the same
// name->Definition lookup (Resolver.Lookup) LogDirs and app.StateCache's
// Refresh use.
type BrokerAdmin interface {
	BrokerConfigs(ctx context.Context, name string, broker int32) ([]cluster.ConfigEntry, error)
	AlterBrokerConfig(ctx context.Context, name string, broker int32, cfgName, value string) error
	MoveReplicaLogDir(ctx context.Context, name string, broker int32, topic string, partition int32, dir string) error
}

// GetBrokers serves /api/clusters/{clusterName}/brokers: every broker's
// cached identity plus the per-broker partition tallies FetchState computed.
func (s *apiServer) GetBrokers(w http.ResponseWriter, r *http.Request, clusterName string) {
	st, ok := s.deps.States.Get(r.Context(), clusterName)
	if !ok {
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
		return
	}
	writeJSON(w, http.StatusOK, brokersFor(st))
}

// GetBrokersCsv serves /api/clusters/{clusterName}/brokers/csv: the same data
// as GetBrokers, rendered as CSV via rowsToCsv (csv.go; upstream's "export as
// CSV" affordance).
func (s *apiServer) GetBrokersCsv(w http.ResponseWriter, r *http.Request, clusterName string) {
	st, ok := s.deps.States.Get(r.Context(), clusterName)
	if !ok {
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
		return
	}
	body, err := rowsToCsv(brokersFor(st))
	if err != nil {
		// Unreachable today (brokersFor always returns a []generated.Broker,
		// which rowsToCsv always accepts) — guarded anyway since rowsToCsv is a
		// shared, general-purpose reflector Task 4/6's topics/groups CSV
		// endpoints reuse against their own row types.
		serverError(w, "GetBrokersCsv", "failed to render brokers csv", err)
		return
	}
	w.Header().Set("Content-Type", "text/csv")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body))
}

// brokersFor maps a cluster's cached broker list onto the contract's Broker
// shape; shared by GetBrokers and GetBrokersCsv so the two stay in lockstep.
func brokersFor(st cluster.RuntimeState) []generated.Broker {
	out := make([]generated.Broker, 0, len(st.Brokers))
	for _, b := range st.Brokers {
		out = append(out, generated.Broker{
			Id:               b.ID,
			Host:             ptr(b.Host),
			Port:             ptr(b.Port),
			PartitionsLeader: ptr(int32(b.PartitionsLeader)),
			Partitions:       ptr(int32(b.Partitions)),
			InSyncPartitions: ptr(int32(b.InSyncPartitions)),
		})
	}
	return out
}

// GetAllBrokersLogdirs serves /api/clusters/{clusterName}/brokers/logdirs:
// per-(broker,dir) partition disk usage, optionally filtered to a set of
// broker IDs via the repeated/CSV ?broker= query parameter. Filtering itself
// happens inside LogDirser.LogDirs (BrokerService/Pool); this handler only
// parses the params and maps the result onto the contract shape.
func (s *apiServer) GetAllBrokersLogdirs(w http.ResponseWriter, r *http.Request, clusterName string, params generated.GetAllBrokersLogdirsParams) {
	var brokers []int32
	if params.Broker != nil {
		brokers = *params.Broker
	}
	dirs, err := s.deps.LogDirs.LogDirs(r.Context(), clusterName, brokers)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetAllBrokersLogdirs", "failed to describe log dirs", err)
		return
	}
	out := make([]generated.BrokersLogdirs, 0, len(dirs))
	for _, d := range dirs {
		out = append(out, brokerLogDirsToGenerated(d))
	}
	writeJSON(w, http.StatusOK, out)
}

// brokerLogDirsToGenerated maps one domain (broker,dir) entry onto the
// contract's BrokersLogdirs shape. Name-vs-Dir is not a typo: per the
// contract (and upstream's BrokersLogdirsDTO, confirmed against the vendored
// frontend fixtures in frontend/src/lib/fixtures/brokers.ts), "name" holds
// the log directory path, e.g. "/opt/kafka/data-0/logs" — not a broker
// identifier. The broker ID instead travels on every leaf partition entry
// (BrokerTopicPartitionLogdir.Broker), since a single BrokersLogdirs array
// can mix entries from different brokers.
func brokerLogDirsToGenerated(d cluster.BrokerLogDirs) generated.BrokersLogdirs {
	g := generated.BrokersLogdirs{Name: ptr(d.Dir)}
	if d.Error != "" {
		g.Error = ptr(d.Error)
	}
	topics := make([]generated.BrokerTopicLogdirs, 0, len(d.Topics))
	for _, t := range d.Topics {
		parts := make([]generated.BrokerTopicPartitionLogdir, 0, len(t.Partitions))
		for _, p := range t.Partitions {
			parts = append(parts, generated.BrokerTopicPartitionLogdir{
				Broker:    ptr(d.Broker),
				Partition: ptr(p.Partition),
				Size:      ptr(p.Size),
				OffsetLag: ptr(p.OffsetLag),
			})
		}
		topics = append(topics, generated.BrokerTopicLogdirs{Name: ptr(t.Topic), Partitions: &parts})
	}
	g.Topics = &topics
	return g
}

// GetBrokerConfig serves /api/clusters/{clusterName}/brokers/{id}/configs: one
// broker's full configuration entry list (dynamic/static/default, with
// synonyms), fetched live from the cluster on every call (no caching — a
// config edit made through this same client pool must be immediately
// visible).
func (s *apiServer) GetBrokerConfig(w http.ResponseWriter, r *http.Request, clusterName string, id int32) {
	cfgs, err := s.deps.Brokers.BrokerConfigs(r.Context(), clusterName, id)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetBrokerConfig", "failed to describe broker config", err)
		return
	}
	out := make([]generated.BrokerConfig, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, configEntryToGenerated(c))
	}
	writeJSON(w, http.StatusOK, out)
}

// configEntryToGenerated maps one domain ConfigEntry onto the contract's
// BrokerConfig shape, translating its raw driver-string Source (and each
// synonym's) through sourceToGenerated.
func configEntryToGenerated(c cluster.ConfigEntry) generated.BrokerConfig {
	bc := generated.BrokerConfig{
		Name: c.Name, Value: c.Value, Source: sourceToGenerated(c.Source),
		IsSensitive: c.IsSensitive, IsReadOnly: c.IsReadOnly,
	}
	if len(c.Synonyms) > 0 {
		syns := make([]generated.ConfigSynonym, 0, len(c.Synonyms))
		for _, syn := range c.Synonyms {
			syns = append(syns, generated.ConfigSynonym{
				Name: ptr(syn.Name), Value: ptr(syn.Value), Source: ptr(sourceToGenerated(syn.Source)),
			})
		}
		bc.Synonyms = &syns
	}
	return bc
}

// sourceToGenerated maps a kadm/kmsg ConfigSource.String() output (as stored
// verbatim in domain ConfigEntry.Source/ConfigSynonym.Source, see
// Pool.BrokerConfigs) onto the contract's ConfigSource enum. The two
// vocabularies are *not* the same strings end to end — confirmed by reading
// both sides' source rather than assuming a 1:1 match:
//   - kmsg@v1.13.1 generated.go's ConfigSource.String() (case 7) returns
//     "CLIENT_METRICS_CONFIG" (KIP-714), while the contract's enum
//     (contract/openapi.yaml ConfigSource) spells the equivalent value
//     "DYNAMIC_CLIENT_METRICS_CONFIG" — same concept, different literal
//     string, mapped explicitly below.
//   - kmsg case 8, "GROUP_CONFIG" (KAFKA-14511 group configs), has no
//     contract equivalent at all — falls to UNKNOWN.
//
// Every other value is an exact string match. Anything unrecognized
// (including kmsg's own "UNKNOWN" default) also falls to UNKNOWN, never
// propagating a raw/unmapped string into the HTTP response.
func sourceToGenerated(source string) generated.ConfigSource {
	switch source {
	case "DYNAMIC_TOPIC_CONFIG":
		return generated.ConfigSourceDYNAMICTOPICCONFIG
	case "DYNAMIC_BROKER_CONFIG":
		return generated.ConfigSourceDYNAMICBROKERCONFIG
	case "DYNAMIC_DEFAULT_BROKER_CONFIG":
		return generated.ConfigSourceDYNAMICDEFAULTBROKERCONFIG
	case "STATIC_BROKER_CONFIG":
		return generated.ConfigSourceSTATICBROKERCONFIG
	case "DEFAULT_CONFIG":
		return generated.ConfigSourceDEFAULTCONFIG
	case "DYNAMIC_BROKER_LOGGER_CONFIG":
		return generated.ConfigSourceDYNAMICBROKERLOGGERCONFIG
	case "CLIENT_METRICS_CONFIG":
		return generated.ConfigSourceDYNAMICCLIENTMETRICSCONFIG
	default:
		return generated.ConfigSourceUNKNOWN
	}
}

// UpdateBrokerConfigByName serves PUT
// /api/clusters/{clusterName}/brokers/{id}/configs/{name}: sets a single
// broker config key. The contract declares 204 (success, no body) and 400
// (malformed body) — no 200 and no declared 404, but unknown-cluster still
// reports 404 to stay consistent with every other cluster-scoped endpoint in
// this package.
func (s *apiServer) UpdateBrokerConfigByName(w http.ResponseWriter, r *http.Request, clusterName string, id int32, name string) {
	var body generated.BrokerConfigItem
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	var value string
	if body.Value != nil {
		value = *body.Value
	}
	if err := s.deps.Brokers.AlterBrokerConfig(r.Context(), clusterName, id, name, value); err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "UpdateBrokerConfigByName", "failed to alter broker config", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// UpdateBrokerTopicPartitionLogDir serves PATCH
// /api/clusters/{clusterName}/brokers/{id}/logdirs: moves one topic-partition
// replica to a different log directory on broker id. Same 204/400/(404)
// response shape as UpdateBrokerConfigByName above.
func (s *apiServer) UpdateBrokerTopicPartitionLogDir(w http.ResponseWriter, r *http.Request, clusterName string, id int32) {
	var body generated.BrokerLogdirUpdate
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	var topic, dir string
	var partition int32
	if body.Topic != nil {
		topic = *body.Topic
	}
	if body.Partition != nil {
		partition = *body.Partition
	}
	if body.LogDir != nil {
		dir = *body.LogDir
	}
	if err := s.deps.Brokers.MoveReplicaLogDir(r.Context(), clusterName, id, topic, partition, dir); err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "UpdateBrokerTopicPartitionLogDir", "failed to move replica log dir", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetBrokersMetrics serves /api/clusters/{clusterName}/brokers/{id}/metrics.
// Unlike the three handlers above, this one reads the periodically-refreshed
// cache (ClusterStater.Get) rather than issuing a live kadm call — it only
// ever reports what the last background scrape already collected: the
// broker's disk usage (segmentSize/segmentCount) and a small set of inferred
// per-broker metrics built from its cached partition tallies, mirroring
// GetClusterMetrics' "inferred metrics" approach at cluster scope.
func (s *apiServer) GetBrokersMetrics(w http.ResponseWriter, r *http.Request, clusterName string, id int32) {
	st, ok := s.deps.States.Get(r.Context(), clusterName)
	if !ok {
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
		return
	}
	writeJSON(w, http.StatusOK, brokerMetricsFor(st, id))
}

// brokerMetricsFor builds one broker's metrics from a cached RuntimeState. An
// id with no matching BrokerInfo/DiskUsage entry in the cache yields an
// all-fields-absent generated.BrokerMetrics{} rather than an error: this
// endpoint only ever reads the cache (no live per-broker kadm call), so
// "broker not in the last scrape" isn't distinguishable from "broker doesn't
// exist" — both degrade to an empty-but-valid response, the same way
// GetClusterStats tolerates a stale/incomplete cache.
func brokerMetricsFor(st cluster.RuntimeState, id int32) generated.BrokerMetrics {
	var bm generated.BrokerMetrics
	for _, d := range st.Disk {
		if d.Broker == id {
			bm.SegmentSize = ptr(d.SegmentSize)
			bm.SegmentCount = ptr(int32(d.SegmentCount))
			break
		}
	}
	for _, b := range st.Brokers {
		if b.ID == id {
			metrics := []generated.Metric{
				{Name: ptr("partitions_leader"), Value: ptr(float32(b.PartitionsLeader))},
				{Name: ptr("partitions"), Value: ptr(float32(b.Partitions))},
				{Name: ptr("in_sync_partitions"), Value: ptr(float32(b.InSyncPartitions))},
			}
			bm.Metrics = &metrics
			break
		}
	}
	return bm
}
