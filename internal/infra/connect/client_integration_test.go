//go:build integration

package connect

import (
	"context"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// kafkaAlias is both this test's chosen Docker network alias AND the Kafka
// container's own --hostname (same reasoning as P2a schemaregistry's
// client_integration_test.go): the confluent-local image advertises its
// inter-container "BROKER" listener as "<container hostname>:9092", so the
// alias and hostname must coincide for the sibling Connect container to be
// able to resolve it on the shared user-defined network.
const kafkaAlias = "kafka-broker"

// connectImage pins the same Confluent Platform version already used for
// the Kafka broker fixture elsewhere in this repo's integration tests.
const connectImage = "confluentinc/cp-kafka-connect:7.8.0"

// filestreamPluginPath is confluentinc/cp-kafka-connect:7.8.0's default
// CONNECT_PLUGIN_PATH (/usr/share/java,/usr/share/confluent-hub-components)
// plus /usr/share/filestream-connectors -- verified by manually running this
// exact image and inspecting it live while implementing this test: the
// image ships connect-file-7.8.0-ccs.jar (which provides
// FileStreamSourceConnector/FileStreamSinkConnector, needed below because
// they require no external system) at that path, but the default plugin
// path excludes the directory, so FileStreamSourceConnector is otherwise
// unavailable from GET /connector-plugins.
const filestreamPluginPath = "/usr/share/java,/usr/share/confluent-hub-components,/usr/share/filestream-connectors"

// fileStreamSourceClass is the connector class this soul test registers:
// dependency-free (reads a local file inside the container), unlike every
// other bundled connector class (JDBC/S3/etc need an external system) --
// see task-2-brief.md's own suggestion of "MockSourceConnector/
// FileStreamSource" for exactly this reason.
const fileStreamSourceClass = "org.apache.kafka.connect.file.FileStreamSourceConnector"

// TestPoolRealRoundTripAgainstRealConnect is P2b Task 2's soul test: a real
// confluentinc/cp-kafka-connect worker, backed by a real tckafka-provisioned
// Kafka broker (both on a shared Docker network), driven through the full
// KafkaConnectPort connector lifecycle against FileStreamSourceConnector --
// the one bundled connector class that needs no external system, so this
// test has no dependency beyond the two containers it starts itself.
func TestPoolRealRoundTripAgainstRealConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	nw, err := network.New(ctx)
	testcontainers.CleanupNetwork(t, nw)
	require.NoError(t, err)

	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0",
		network.WithNetwork([]string{kafkaAlias}, nw),
		testcontainers.WithConfigModifier(func(cfg *container.Config) { cfg.Hostname = kafkaAlias }),
	)
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)

	connectCtr, err := testcontainers.Run(ctx, connectImage,
		network.WithNetwork([]string{"kafka-connect"}, nw),
		testcontainers.WithExposedPorts("8083/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CONNECT_BOOTSTRAP_SERVERS":                 kafkaAlias + ":9092",
			"CONNECT_REST_ADVERTISED_HOST_NAME":         "kafka-connect",
			"CONNECT_REST_PORT":                         "8083",
			"CONNECT_GROUP_ID":                          "it-connect-group",
			"CONNECT_CONFIG_STORAGE_TOPIC":              "_connect-configs",
			"CONNECT_OFFSET_STORAGE_TOPIC":              "_connect-offsets",
			"CONNECT_STATUS_STORAGE_TOPIC":              "_connect-status",
			"CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR": "1",
			"CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR": "1",
			"CONNECT_STATUS_STORAGE_REPLICATION_FACTOR": "1",
			"CONNECT_KEY_CONVERTER":                     "org.apache.kafka.connect.json.JsonConverter",
			"CONNECT_VALUE_CONVERTER":                   "org.apache.kafka.connect.json.JsonConverter",
			"CONNECT_INTERNAL_KEY_CONVERTER":            "org.apache.kafka.connect.json.JsonConverter",
			"CONNECT_INTERNAL_VALUE_CONVERTER":          "org.apache.kafka.connect.json.JsonConverter",
			"CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE":      "false",
			"CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE":    "false",
			"CONNECT_PLUGIN_PATH":                       filestreamPluginPath,
			"CONNECT_PLUGIN_DISCOVERY":                  "service_load",
		}),
		testcontainers.WithWaitStrategyAndDeadline(5*time.Minute,
			wait.ForHTTP("/connector-plugins").WithPort("8083/tcp").WithStartupTimeout(5*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, connectCtr)
	require.NoError(t, err)

	connectURL, err := connectCtr.PortEndpoint(ctx, "8083/tcp", "http")
	require.NoError(t, err)

	pool := NewPool()
	const connectName = "it-connect"
	def := cluster.Definition{
		Name:     "it-connect-cluster",
		Connects: []cluster.ConnectSpec{{Name: connectName, Address: connectURL}},
	}

	plugins, err := pool.Plugins(ctx, def, connectName)
	require.NoError(t, err)
	require.NotEmpty(t, plugins)
	var haveFileStream bool
	for _, p := range plugins {
		if p.Class == fileStreamSourceClass {
			haveFileStream = true
			break
		}
	}
	require.True(t, haveFileStream, "expected %s among %v (CONNECT_PLUGIN_PATH=%s)", fileStreamSourceClass, plugins, filestreamPluginPath)

	// A dry-run validation against an under-specified config (missing the
	// connector-specific required keys "file"/"topic") must surface a
	// non-zero error_count from Connect's own validation engine -- srfake
	// has no Connect equivalent, so only a real worker can honestly answer
	// this (mirrors P2a's CheckCompat soul-test reasoning).
	validation, err := pool.ValidatePlugin(ctx, def, connectName, fileStreamSourceClass, map[string]any{
		"connector.class": fileStreamSourceClass,
	})
	require.NoError(t, err)
	require.NotZero(t, validation.ErrorCount)

	const connectorName = "it-conn"
	cfg := map[string]any{
		"connector.class": fileStreamSourceClass,
		"tasks.max":       "1",
		"file":            "/tmp/it-conn-source.txt",
		"topic":           "it-conn-topic",
	}
	created, err := pool.CreateConnector(ctx, def, connectName, connectorName, cfg)
	require.NoError(t, err)
	require.Equal(t, connectorName, created.Name)
	require.Equal(t, connectName, created.ConnectName)

	names, err := pool.Connectors(ctx, def, connectName)
	require.NoError(t, err)
	require.Contains(t, names, connectorName)

	require.Eventually(t, func() bool {
		c, err := pool.Connector(ctx, def, connectName, connectorName)
		return err == nil && c.State == "RUNNING"
	}, 30*time.Second, 500*time.Millisecond, "connector should reach RUNNING")

	// AllConnectors (P2b Task 4 review Fix 2) now hits Kafka Connect's bulk
	// KIP-465 expand=status&expand=info endpoint instead of returning a
	// ConnectName/Name-only stub, so this soul-tests that the real status
	// comes through end to end against a live worker: State must be an
	// actual observed state (RUNNING, matching the single-connector
	// Connector() call above, not the UNASSIGNED placeholder the old
	// AllConnectors always produced), plus a non-empty WorkerID and correct
	// TasksCount. Wrapped in Eventually because AllConnectors' bulk endpoint
	// and the single-connector status endpoint are two independent REST
	// calls against the same eventually-consistent status store.
	var allRef cluster.ConnectorRef
	require.Eventually(t, func() bool {
		refs, err := pool.AllConnectors(ctx, def)
		if err != nil {
			return false
		}
		for _, r := range refs {
			if r.ConnectName == connectName && r.Name == connectorName {
				allRef = r
				return r.State == "RUNNING"
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "AllConnectors should report real RUNNING state, not UNASSIGNED")
	require.Equal(t, connectorName, allRef.Name)
	require.Equal(t, connectName, allRef.ConnectName)
	require.Equal(t, "RUNNING", allRef.State)
	require.NotEmpty(t, allRef.WorkerID)
	require.Equal(t, 1, allRef.TasksCount)

	// P2b-D3 multi-Connect aggregation skip-bad, end to end against real
	// infra: a Definition with TWO Connects -- the real, reachable worker
	// above plus a dead one nothing listens on (127.0.0.1:1, so the dial
	// fails fast with "connection refused" rather than hanging). Connects
	// and AllConnectors must silently skip the dead entry -- no error, and
	// the result reflects only the reachable worker's data -- rather than
	// failing the whole call or surfacing the dead Connect as some
	// "offline" placeholder. No extra container needed: this reuses the
	// already-running connectCtr via connectURL.
	multiDef := cluster.Definition{
		Name: def.Name,
		Connects: []cluster.ConnectSpec{
			{Name: connectName, Address: connectURL},
			{Name: "dead", Address: "http://127.0.0.1:1"},
		},
	}

	connects, err := pool.Connects(ctx, multiDef)
	require.NoError(t, err)
	require.Len(t, connects, 1, "dead Connect must be skipped, not error out")
	require.Equal(t, connectName, connects[0].Name)

	multiRefs, err := pool.AllConnectors(ctx, multiDef)
	require.NoError(t, err)
	require.NotEmpty(t, multiRefs, "reachable Connect's connectors should still be aggregated")
	for _, r := range multiRefs {
		require.Equal(t, connectName, r.ConnectName, "AllConnectors must not surface entries from the dead Connect")
	}

	got, err := pool.Connector(ctx, def, connectName, connectorName)
	require.NoError(t, err)
	require.Equal(t, connectorName, got.Name)
	require.Equal(t, "RUNNING", got.State)
	require.Equal(t, fileStreamSourceClass, got.Config["connector.class"])

	gotCfg, err := pool.ConnectorConfig(ctx, def, connectName, connectorName)
	require.NoError(t, err)
	require.Equal(t, fileStreamSourceClass, gotCfg["connector.class"])

	require.Eventually(t, func() bool {
		tasks, err := pool.ConnectorTasks(ctx, def, connectName, connectorName)
		return err == nil && len(tasks) > 0
	}, 30*time.Second, 500*time.Millisecond, "connector should have at least one task")

	require.NoError(t, pool.UpdateConnectorState(ctx, def, connectName, connectorName, "PAUSE"))
	require.Eventually(t, func() bool {
		c, err := pool.Connector(ctx, def, connectName, connectorName)
		return err == nil && c.State == "PAUSED"
	}, 30*time.Second, 500*time.Millisecond, "connector should reach PAUSED")

	require.NoError(t, pool.UpdateConnectorState(ctx, def, connectName, connectorName, "RESUME"))
	require.Eventually(t, func() bool {
		c, err := pool.Connector(ctx, def, connectName, connectorName)
		return err == nil && c.State == "RUNNING"
	}, 30*time.Second, 500*time.Millisecond, "connector should reach RUNNING again")

	updated, err := pool.SetConnectorConfig(ctx, def, connectName, connectorName, map[string]any{
		"connector.class": fileStreamSourceClass,
		"tasks.max":       "1",
		"file":            "/tmp/it-conn-source.txt",
		"topic":           "it-conn-topic-changed",
	})
	require.NoError(t, err)
	require.Equal(t, "it-conn-topic-changed", updated.Config["topic"])

	reread, err := pool.ConnectorConfig(ctx, def, connectName, connectorName)
	require.NoError(t, err)
	require.Equal(t, "it-conn-topic-changed", reread["topic"])

	// Strengthen RestartConnectorTask (Task 2 review: only lightly asserted)
	// -- assert it succeeds AND that task 0 actually returns to RUNNING.
	// Connect's restart endpoint is async (it schedules the restart), so the
	// task can transition through a transient non-RUNNING state before
	// settling back on its own, hence Eventually rather than a single
	// immediate read.
	require.NoError(t, pool.RestartConnectorTask(ctx, def, connectName, connectorName, 0))
	require.Eventually(t, func() bool {
		tasks, err := pool.ConnectorTasks(ctx, def, connectName, connectorName)
		if err != nil {
			return false
		}
		for _, tk := range tasks {
			if tk.ID == 0 {
				return tk.State == "RUNNING"
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "task 0 should return to RUNNING after restart")

	// Strengthen ResetConnectorOffsets (Task 2 review: only lightly
	// asserted). P2b-D7: Connect only accepts DELETE .../offsets while the
	// connector is STOPPED, and only on Connect 3.6+ (this client does not
	// itself gate on either precondition -- an unmet one must surface as
	// whatever error Connect returns, not be swallowed). cp-kafka-connect:
	// 7.8.0 bundles Apache Kafka 3.8.x, which supports both the STOP action
	// (KIP-875) and the offsets-reset endpoint, so this test drives BOTH
	// paths against real infra rather than picking just one:
	//   1. success path: STOP the connector, then reset its offsets, and
	//      assert no error -- proves the happy path Connect actually
	//      supports on this image/version.
	//   2. passthrough-error path: RESUME back to RUNNING, then reset
	//      offsets again -- Connect rejects this (no longer STOPPED), and
	//      the client must surface that rejection as a non-nil error rather
	//      than swallowing it.
	require.NoError(t, pool.UpdateConnectorState(ctx, def, connectName, connectorName, "STOP"))
	require.Eventually(t, func() bool {
		c, err := pool.Connector(ctx, def, connectName, connectorName)
		return err == nil && c.State == "STOPPED"
	}, 30*time.Second, 500*time.Millisecond, "connector should reach STOPPED")

	require.NoError(t, pool.ResetConnectorOffsets(ctx, def, connectName, connectorName),
		"resetting offsets on a STOPPED connector should succeed on this Connect version")

	require.NoError(t, pool.UpdateConnectorState(ctx, def, connectName, connectorName, "RESUME"))
	require.Eventually(t, func() bool {
		c, err := pool.Connector(ctx, def, connectName, connectorName)
		return err == nil && c.State == "RUNNING"
	}, 30*time.Second, 500*time.Millisecond, "connector should reach RUNNING again after resume")

	err = pool.ResetConnectorOffsets(ctx, def, connectName, connectorName)
	require.Error(t, err, "resetting offsets on a non-STOPPED connector must surface Connect's rejection, not be swallowed")

	require.NoError(t, pool.DeleteConnector(ctx, def, connectName, connectorName))
	require.Eventually(t, func() bool {
		names, err := pool.Connectors(ctx, def, connectName)
		if err != nil {
			return false
		}
		for _, n := range names {
			if n == connectorName {
				return false
			}
		}
		return true
	}, 30*time.Second, 500*time.Millisecond, "connector should be gone after delete")
}
