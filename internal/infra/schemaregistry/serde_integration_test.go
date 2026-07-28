//go:build integration

package schemaregistry

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

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	infrafilter "github.com/cy-kaf/cy-kaf-client/internal/infra/filter"
	infrakafka "github.com/cy-kaf/cy-kaf-client/internal/infra/kafka"
	infraserde "github.com/cy-kaf/cy-kaf-client/internal/infra/serde"
)

// startKafkaAndSR provisions a Kafka broker and a cp-schema-registry backed by
// it on a shared user-defined Docker network (the alias/hostname coincidence
// rationale is documented on kafkaAlias in client_integration_test.go), and
// returns the broker seed list plus the externally-reachable SR URL. All
// containers/networks are torn down via t.Cleanup.
func startKafkaAndSR(ctx context.Context, t *testing.T) (brokers []string, srURL string) {
	t.Helper()
	nw, err := network.New(ctx)
	testcontainers.CleanupNetwork(t, nw)
	require.NoError(t, err)

	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0",
		network.WithNetwork([]string{kafkaAlias}, nw),
		testcontainers.WithConfigModifier(func(cfg *container.Config) { cfg.Hostname = kafkaAlias }),
	)
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)

	srCtr, err := testcontainers.Run(ctx, srImage,
		network.WithNetwork([]string{"schema-registry"}, nw),
		testcontainers.WithExposedPorts("8081/tcp"),
		testcontainers.WithEnv(map[string]string{
			"SCHEMA_REGISTRY_HOST_NAME":                    "schema-registry",
			"SCHEMA_REGISTRY_LISTENERS":                    "http://0.0.0.0:8081",
			"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": "PLAINTEXT://" + kafkaAlias + ":9092",
		}),
		testcontainers.WithWaitStrategyAndDeadline(3*time.Minute,
			wait.ForHTTP("/subjects").WithPort("8081/tcp").WithStartupTimeout(3*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, srCtr)
	require.NoError(t, err)

	srURL, err = srCtr.PortEndpoint(ctx, "8081/tcp", "http")
	require.NoError(t, err)
	brokers, err = kc.Brokers(ctx)
	require.NoError(t, err)
	return brokers, srURL
}

// browseValue runs one earliest-to-now Browse and returns the decoded messages.
func browseValue(t *testing.T, ctx context.Context, svc *appcluster.MessageService, name, topic string) []appcluster.DecodedMessage {
	t.Helper()
	var msgs []appcluster.DecodedMessage
	err := svc.Browse(ctx, name, topic, appcluster.BrowseSpec{Mode: appcluster.ModeEarliest, Limit: 100, ValueSerde: "SchemaRegistry"},
		func(ev appcluster.BrowseEvent) error {
			if ev.Kind == appcluster.EventMessage {
				msgs = append(msgs, *ev.Message)
			}
			return nil
		})
	require.NoError(t, err)
	return msgs
}

// strptr returns a pointer to s (SendSpec.Key/Value are *string: nil means
// "don't send this side").
func strptr(s string) *string { return &s }

// TestSchemaRegistrySerdeProduceBrowseAgainstRealInfra is Task 7's serde soul
// test: the real app-layer MessageService (real serde Provider with a live SR
// candidate, real Kafka Pool as reader+writer) produces a record encoded by
// the SchemaRegistry serde and browses it back decoded to JSON — once for an
// Avro schema, once for a JSON Schema — against a real broker + real
// cp-schema-registry. Unlike the unit round-trip (fake port, self-produced
// bytes), this proves the wire id we embed on Serialize is the same id
// SchemaByID resolves on Deserialize through a real registry and a real
// broker's byte storage.
func TestSchemaRegistrySerdeProduceBrowseAgainstRealInfra(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	brokers, srURL := startKafkaAndSR(ctx, t)

	def := cluster.Definition{
		Name:           "it-sr-serde",
		Conn:           cluster.ConnectionSpec{BootstrapServers: brokers},
		SchemaRegistry: cluster.SchemaRegistrySpec{URL: srURL},
	}

	kafkaPool := infrakafka.NewPool()
	defer kafkaPool.Close()
	srPool := NewPool()
	res := appcluster.NewResolver([]cluster.Definition{def})
	serdeProv := infraserde.NewProviderWithSchemaRegistry(infraserde.NewRegistry(), srPool)
	svc := appcluster.NewMessageService(res, kafkaPool, kafkaPool, serdeProv, infrafilter.NewEngine(), appcluster.NewCursorCache(10*time.Minute, 1000), nil)

	t.Run("avro produce/browse round-trip", func(t *testing.T) {
		const topic = "sr-avro"
		require.NoError(t, kafkaPool.CreateTopic(ctx, def, cluster.TopicSpec{Name: topic, Partitions: 1, ReplicationFactor: 1}))
		_, err := srPool.Register(ctx, def, topic+"-value", cluster.NewSchema{
			Schema:     `{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"city","type":"string"}]}`,
			SchemaType: "AVRO",
		})
		require.NoError(t, err)

		require.NoError(t, svc.Send(ctx, def.Name, topic, appcluster.SendSpec{
			Partition:  0,
			Value:      strptr(`{"name":"alice","city":"NYC"}`),
			ValueSerde: "SchemaRegistry",
		}))

		msgs := browseValue(t, ctx, svc, def.Name, topic)
		require.Len(t, msgs, 1)
		require.JSONEq(t, `{"name":"alice","city":"NYC"}`, msgs[0].Value)
		require.Equal(t, "SchemaRegistry", msgs[0].ValueSerde)
	})

	t.Run("json schema produce/browse round-trip", func(t *testing.T) {
		const topic = "sr-json"
		require.NoError(t, kafkaPool.CreateTopic(ctx, def, cluster.TopicSpec{Name: topic, Partitions: 1, ReplicationFactor: 1}))
		_, err := srPool.Register(ctx, def, topic+"-value", cluster.NewSchema{
			Schema:     `{"type":"object","properties":{"hello":{"type":"string"}}}`,
			SchemaType: "JSON",
		})
		require.NoError(t, err)

		require.NoError(t, svc.Send(ctx, def.Name, topic, appcluster.SendSpec{
			Partition:  0,
			Value:      strptr(`{"hello":"world"}`),
			ValueSerde: "SchemaRegistry",
		}))

		msgs := browseValue(t, ctx, svc, def.Name, topic)
		require.Len(t, msgs, 1)
		require.JSONEq(t, `{"hello":"world"}`, msgs[0].Value)
		require.Equal(t, "SchemaRegistry", msgs[0].ValueSerde)
	})
}
