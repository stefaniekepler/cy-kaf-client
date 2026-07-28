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

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// kafkaAlias is both this test's chosen Docker network alias AND the
// Kafka container's own --hostname: the confluent-local image's starter
// script (see the tckafka module's starterScriptContent) advertises its
// inter-container "BROKER" listener as "<container hostname>:9092" --
// setting the container's hostname to the same string we register as its
// network alias means that advertised address is directly resolvable by
// the sibling Schema Registry container on the shared user-defined network
// (Docker's embedded DNS only resolves container names/aliases on such
// networks, not arbitrary --hostname values, so the two must be forced to
// coincide rather than relying on the image's default random hostname).
const kafkaAlias = "kafka-broker"

// srImage pins the same Confluent Platform version already used for the
// Kafka broker fixture in internal/infra/kafka's own integration tests
// (confluentinc/confluent-local:7.8.0), for version consistency.
const srImage = "confluentinc/cp-schema-registry:7.8.0"

// TestPoolRealRoundTripAgainstRealSchemaRegistry is P2a Task 2's soul test:
// a real confluentinc/cp-schema-registry container, backed by a real
// tckafka-provisioned Kafka broker (both on a shared Docker network), driven
// through every SchemaRegistryPort method in one lifecycle:
//
//   - Register(subject, AVRO schema) -> registry-global ID.
//   - Subjects contains the new subject.
//   - SchemaByVersion(subject, "latest") -> ID/Schema/Version=1 match, plus
//     the subject's effective (global-fallback) compatibility level.
//   - Versions -> [1].
//   - SchemaByID(id) -> same schema text/type.
//   - SetSubjectCompat(subject, "FULL") + SubjectCompat readback == "FULL".
//   - CheckCompat(subject, a backward-compatible schema addition) == true
//     against a *real* registry's compatibility engine (srfake's equivalent
//     is a hardcoded stub -- see client_test.go's package doc comment --
//     this is the assertion that only a real container can make honest).
//   - DeleteVersion(subject, "1", false) resolves and deletes version 1,
//     leaving Versions empty for the subject.
func TestPoolRealRoundTripAgainstRealSchemaRegistry(t *testing.T) {
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

	srURL, err := srCtr.PortEndpoint(ctx, "8081/tcp", "http")
	require.NoError(t, err)

	pool := NewPool()
	def := cluster.Definition{Name: "it-sr", SchemaRegistry: cluster.SchemaRegistrySpec{URL: srURL}}

	const subject = "t-value"
	id, err := pool.Register(ctx, def, subject, cluster.NewSchema{
		Schema:     `{"type":"record","name":"T","fields":[{"name":"a","type":"string"}]}`,
		SchemaType: "AVRO",
	})
	require.NoError(t, err)
	require.NotZero(t, id)

	subjects, err := pool.Subjects(ctx, def)
	require.NoError(t, err)
	require.Contains(t, subjects, subject)

	sv, err := pool.SchemaByVersion(ctx, def, subject, "latest")
	require.NoError(t, err)
	require.Equal(t, id, sv.ID)
	require.Equal(t, 1, sv.Version)
	require.Equal(t, "AVRO", sv.SchemaType)
	require.NotEmpty(t, sv.CompatLevel, "SchemaByVersion must populate the subject's effective compatibility level")

	versions, err := pool.Versions(ctx, def, subject)
	require.NoError(t, err)
	require.Equal(t, []int{1}, versions)

	raw, err := pool.SchemaByID(ctx, def, id)
	require.NoError(t, err)
	require.Equal(t, sv.Schema, raw.Schema)
	require.Equal(t, "AVRO", raw.SchemaType)

	require.NoError(t, pool.SetSubjectCompat(ctx, def, subject, "FULL"))
	compat, err := pool.SubjectCompat(ctx, def, subject)
	require.NoError(t, err)
	require.Equal(t, "FULL", compat)

	// Adding a field with a default is FULL-compatible (both backward and
	// forward): a real compatibility engine must say yes here, unlike
	// srfake's hardcoded-true stub (see client_test.go), so this line is
	// the one assertion in the whole soul test that specifically requires
	// a real container rather than the fake.
	compatible, err := pool.CheckCompat(ctx, def, subject, cluster.NewSchema{
		Schema:     `{"type":"record","name":"T","fields":[{"name":"a","type":"string"},{"name":"b","type":"string","default":"x"}]}`,
		SchemaType: "AVRO",
	})
	require.NoError(t, err)
	require.True(t, compatible)

	deleted, err := pool.DeleteVersion(ctx, def, subject, "1", false)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)

	remaining, err := pool.Versions(ctx, def, subject)
	require.Error(t, err, "no versions remain for the subject; the real registry 404s rather than returning an empty list")
	require.Empty(t, remaining)

	// --- Task 7: multi-version schema evolution on a second subject ---
	// Exercises getAllVersions/getSchemaByVersion precision across two
	// versions, an incompatible-change rejection only a real compatibility
	// engine can make honestly, and "latest" moving back after a version
	// delete -- all against the same running containers.
	const evoSubject = "evo-value"
	v1 := cluster.NewSchema{Schema: `{"type":"record","name":"Evo","fields":[{"name":"a","type":"string"}]}`, SchemaType: "AVRO"}
	v2 := cluster.NewSchema{Schema: `{"type":"record","name":"Evo","fields":[{"name":"a","type":"string"},{"name":"b","type":"string","default":"x"}]}`, SchemaType: "AVRO"}
	id1, err := pool.Register(ctx, def, evoSubject, v1)
	require.NoError(t, err)
	id2, err := pool.Register(ctx, def, evoSubject, v2)
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)

	evoVersions, err := pool.Versions(ctx, def, evoSubject)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, evoVersions)

	at1, err := pool.SchemaByVersion(ctx, def, evoSubject, "1")
	require.NoError(t, err)
	require.Equal(t, 1, at1.Version)
	require.Equal(t, id1, at1.ID)
	at2, err := pool.SchemaByVersion(ctx, def, evoSubject, "2")
	require.NoError(t, err)
	require.Equal(t, 2, at2.Version)
	latest, err := pool.SchemaByVersion(ctx, def, evoSubject, "latest")
	require.NoError(t, err)
	require.Equal(t, 2, latest.Version)

	// An incompatible change under BACKWARD (a new required field with no
	// default) must be rejected by the real compatibility engine -- srfake's
	// hardcoded-true stub could never surface this.
	incompatible, err := pool.CheckCompat(ctx, def, evoSubject, cluster.NewSchema{
		Schema:     `{"type":"record","name":"Evo","fields":[{"name":"a","type":"string"},{"name":"c","type":"string"}]}`,
		SchemaType: "AVRO",
	})
	require.NoError(t, err)
	require.False(t, incompatible)

	// Soft-deleting the latest version moves "latest" back to the prior one.
	deletedEvo, err := pool.DeleteVersion(ctx, def, evoSubject, "latest", false)
	require.NoError(t, err)
	require.Equal(t, 2, deletedEvo)
	afterDelete, err := pool.SchemaByVersion(ctx, def, evoSubject, "latest")
	require.NoError(t, err)
	require.Equal(t, 1, afterDelete.Version)
}
