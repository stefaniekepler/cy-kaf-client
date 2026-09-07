package config

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	infrakafka "github.com/cy-kaf/cy-kaf-client/internal/infra/kafka"
)

// nilProbe is the "every cluster reachable" probe injected where the test
// cares about parsing/mapping, not real connectivity.
func nilProbe(context.Context, cluster.Definition) error { return nil }

// fakeProbe returns a preset error keyed by cluster name (nil = reachable) and
// records the order it was asked to probe, so per-cluster mapping can be
// asserted without touching a real broker.
type fakeProbe struct {
	errByName map[string]error
	seen      []string
}

func (f *fakeProbe) probe(_ context.Context, def cluster.Definition) error {
	f.seen = append(f.seen, def.Name)
	return f.errByName[def.Name]
}

// TestStoreCurrentParsesRawTreeAndClusters proves Current keeps the full
// config.yaml as a generic Raw tree (so Save can round-trip unmodeled fields)
// and also parses kafka.clusters[] to domain Definitions.
func TestStoreCurrentParsesRawTreeAndClusters(t *testing.T) {
	p := write(t, `
server: {port: 9090}
kafka:
  clusters:
    - name: prod
      bootstrapServers: k1:9092,k2:9092
      readOnly: true
      properties: {security.protocol: SSL}
`)
	store := NewStore(p, nilProbe)

	snap, err := store.Current()
	require.NoError(t, err)

	kafka, ok := snap.Raw["kafka"].(map[string]any)
	require.True(t, ok, "Raw must keep the kafka subtree verbatim")
	require.Contains(t, kafka, "clusters")
	require.Contains(t, snap.Raw, "server", "Raw must preserve fields this codebase doesn't remodel")

	require.Len(t, snap.Clusters, 1)
	require.Equal(t, "prod", snap.Clusters[0].Name)
	require.Equal(t, []string{"k1:9092", "k2:9092"}, snap.Clusters[0].Conn.BootstrapServers)
	require.True(t, snap.Clusters[0].ReadOnly)
}

// TestStoreCurrentMissingDefaultFileReturnsEmptyConfig locks the desktop
// first-run path: Load accepts an absent default config, so the config wizard's
// subsequent GET /api/config must expose an editable empty tree rather than
// failing with 500.
func TestStoreCurrentMissingDefaultFileReturnsEmptyConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-created", "config.yaml")
	store := NewStore(path, nilProbe)

	snap, err := store.Current()
	require.NoError(t, err)
	require.Empty(t, snap.Clusters)
	require.Equal(t, map[string]any{
		"kafka": map[string]any{
			"clusters": []any{},
		},
	}, snap.Raw)
}

// TestStoreValidateReachableClusterLeavesKafkaOkAndP2Nil proves a passing probe
// yields Kafka.Error=false and leaves every P2 subsystem nil (not probed).
func TestStoreValidateReachableClusterLeavesKafkaOkAndP2Nil(t *testing.T) {
	p := write(t, "kafka:\n  clusters:\n    - {name: prod, bootstrapServers: k:9092}\n")
	store := NewStore(p, nilProbe)

	snap, err := store.Current()
	require.NoError(t, err)
	val, err := store.Validate(context.Background(), snap)
	require.NoError(t, err)

	require.Contains(t, val.Clusters, "prod", "every input cluster must appear in the verdict map")
	cv := val.Clusters["prod"]
	require.False(t, cv.Kafka.Error)
	require.Empty(t, cv.Kafka.ErrorMessage)
	require.Nil(t, cv.SchemaRegistry, "P2 subsystems stay nil in P1c")
	require.Nil(t, cv.Ksqldb)
	require.Nil(t, cv.PrometheusStorage)
	require.Nil(t, cv.KafkaConnects)
}

// TestStoreValidateMapsPerClusterProbeError proves each cluster's verdict comes
// from its own probe result -- one failing cluster doesn't taint the others.
func TestStoreValidateMapsPerClusterProbeError(t *testing.T) {
	p := write(t, "kafka:\n  clusters:\n    - {name: good, bootstrapServers: g:9092}\n    - {name: bad, bootstrapServers: b:9092}\n")
	fp := &fakeProbe{errByName: map[string]error{"bad": errors.New("dial tcp: connection refused")}}
	store := NewStore(p, fp.probe)

	snap, err := store.Current()
	require.NoError(t, err)
	val, err := store.Validate(context.Background(), snap)
	require.NoError(t, err)

	require.False(t, val.Clusters["good"].Kafka.Error)
	require.True(t, val.Clusters["bad"].Kafka.Error)
	require.Contains(t, val.Clusters["bad"].Kafka.ErrorMessage, "connection refused")
	require.ElementsMatch(t, []string{"good", "bad"}, fp.seen)
}

// TestStoreValidateWithRealProbeReportsUnreachableKafka drives the actual
// infra/kafka.ProbeConnectivity against an address nothing listens on
// (127.0.0.1:1, connection-refused-fast) and proves it surfaces as a
// Kafka.Error verdict -- the plan's "unreachable bootstrap -> Kafka.Error"
// soul case. No testcontainers needed: this exercises the failure path.
func TestStoreValidateWithRealProbeReportsUnreachableKafka(t *testing.T) {
	p := write(t, "kafka:\n  clusters:\n    - {name: dead, bootstrapServers: 127.0.0.1:1}\n")
	store := NewStore(p, infrakafka.ProbeConnectivity)

	snap, err := store.Current()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	val, err := store.Validate(ctx, snap)
	require.NoError(t, err)

	require.True(t, val.Clusters["dead"].Kafka.Error, "an unreachable bootstrap must fail the kafka probe")
	require.Contains(t, val.Clusters["dead"].Kafka.ErrorMessage, "现象：目标端口 127.0.0.1:1 当前不接受 TCP 连接")
	require.Contains(t, val.Clusters["dead"].Kafka.ErrorMessage, "常见原因：Kafka 未启动")
	require.Contains(t, val.Clusters["dead"].Kafka.ErrorMessage, "建议检查：")
}

// TestStoreSaveRoundTripsEditsAndPreservesUnmodeledFields proves Save writes
// the (edited) Raw tree back verbatim: a re-read sees the new cluster bootstrap,
// and a field this codebase never models (auth) survives untouched -- the whole
// reason Save persists Raw rather than a re-serialized typed struct.
func TestStoreSaveRoundTripsEditsAndPreservesUnmodeledFields(t *testing.T) {
	p := write(t, `
auth:
  type: OAUTH2
kafka:
  clusters:
    - name: prod
      bootstrapServers: old:9092
`)
	store := NewStore(p, nilProbe)

	snap, err := store.Current()
	require.NoError(t, err)

	// Edit the cluster's bootstrap directly in the Raw tree.
	kafka := snap.Raw["kafka"].(map[string]any)
	clusters := kafka["clusters"].([]any)
	clusters[0].(map[string]any)["bootstrapServers"] = "new:9092"
	require.NoError(t, store.Save(context.Background(), snap))

	snap2, err := store.Current()
	require.NoError(t, err)
	require.Equal(t, []string{"new:9092"}, snap2.Clusters[0].Conn.BootstrapServers)
	require.Contains(t, snap2.Raw, "auth", "an unmodeled auth section must survive Save")
	require.Equal(t, "OAUTH2", snap2.Raw["auth"].(map[string]any)["type"])
}

// TestStoreSaveRejectsEmptySnapshot proves Save refuses a nil Raw rather than
// truncating config.yaml to an empty document.
func TestStoreSaveRejectsEmptySnapshot(t *testing.T) {
	store := NewStore(write(t, "kafka: {clusters: []}\n"), nilProbe)
	require.Error(t, store.Save(context.Background(), cluster.ConfigSnapshot{}))
}

// TestStoreSaveCreatesFirstConfigSecurely proves the first cluster can be
// submitted before the platform config directory or config.yaml exists. The
// file may contain credentials, so it must be user-only.
func TestStoreSaveCreatesFirstConfigSecurely(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app-support", "config.yaml")
	store := NewStore(path, nilProbe)
	snap := cluster.ConfigSnapshot{Raw: map[string]any{
		"kafka": map[string]any{
			"clusters": []any{
				map[string]any{"name": "local", "bootstrapServers": "127.0.0.1:9092"},
			},
		},
	}}

	require.NoError(t, store.Save(context.Background(), snap))
	require.FileExists(t, path)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	reloaded, err := store.Current()
	require.NoError(t, err)
	require.Len(t, reloaded.Clusters, 1)
	require.Equal(t, "local", reloaded.Clusters[0].Name)
}

// TestStoreSaveRelatedFileWritesUnderUploadsAndReturnsLocation proves an
// uploaded file lands under an uploads/ dir next to config.yaml with its bytes
// intact, and its location is returned.
func TestStoreSaveRelatedFileWritesUnderUploadsAndReturnsLocation(t *testing.T) {
	p := write(t, "kafka: {clusters: []}\n")
	store := NewStore(p, nilProbe)

	loc, err := store.SaveRelatedFile(context.Background(), "truststore.pem", []byte("PEMDATA"))
	require.NoError(t, err)
	require.FileExists(t, loc)
	got, err := os.ReadFile(loc)
	require.NoError(t, err)
	require.Equal(t, "PEMDATA", string(got))
	require.Equal(t, filepath.Join(filepath.Dir(p), "uploads", "truststore.pem"), loc)
}

// TestStoreSaveRelatedFileRejectsPathTraversal proves every attempt to escape
// the uploads dir (parent refs, absolute paths, nested paths, empty) is
// rejected before anything is written.
func TestStoreSaveRelatedFileRejectsPathTraversal(t *testing.T) {
	p := write(t, "kafka: {clusters: []}\n")
	store := NewStore(p, nilProbe)

	for _, name := range []string{"../evil", "../../etc/passwd", "/abs/evil", "a/b", "", ".", ".."} {
		_, err := store.SaveRelatedFile(context.Background(), name, []byte("x"))
		require.Error(t, err, "must reject %q", name)
	}
}
