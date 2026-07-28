package api

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// Whitebox unit tests for the pure mapping/rendering functions behind the
// brokers handlers (package api, not api_test) — mirrors
// handlers_cluster_test.go's approach of exercising unexported functions
// directly rather than only indirectly through an HTTP round trip.

// TestBrokerLogDirsToGeneratedMapsDirAndPropagatesBrokerID locks in the
// name=dir-path (not broker id) mapping documented on
// brokerLogDirsToGenerated, and that the domain-level Broker ID travels down
// onto every leaf partition entry rather than being dropped.
func TestBrokerLogDirsToGeneratedMapsDirAndPropagatesBrokerID(t *testing.T) {
	g := brokerLogDirsToGenerated(cluster.BrokerLogDirs{
		Broker: 7, Dir: "/kafka/data-0",
		Topics: []cluster.TopicLogDirs{
			{Topic: "t1", Partitions: []cluster.PartitionLogDir{{Partition: 0, Size: 100, OffsetLag: 5}}},
		},
	})
	require.Equal(t, "/kafka/data-0", *g.Name)
	require.Nil(t, g.Error)
	require.Len(t, *g.Topics, 1)
	topic := (*g.Topics)[0]
	require.Equal(t, "t1", *topic.Name)
	require.Len(t, *topic.Partitions, 1)
	part := (*topic.Partitions)[0]
	require.Equal(t, int32(7), *part.Broker)
	require.Equal(t, int32(0), *part.Partition)
	require.Equal(t, int64(100), *part.Size)
	require.Equal(t, int64(5), *part.OffsetLag)
}

func TestBrokerLogDirsToGeneratedSetsErrorWhenPresent(t *testing.T) {
	g := brokerLogDirsToGenerated(cluster.BrokerLogDirs{Broker: 1, Dir: "/d", Error: "permission denied"})
	require.Equal(t, "permission denied", *g.Error)
	require.NotNil(t, g.Topics)
	require.Empty(t, *g.Topics)
}

// TestSourceToGenerated is a table test covering every kmsg@v1.13.1
// ConfigSource.String() output (generated.go's switch: default->"UNKNOWN"
// plus cases 1-8) against the contract's ConfigSource enum, written down
// item-by-item from both sides' source rather than assumed to line up
// 1:1 — two of the nine kmsg strings are deliberately exercised as
// non-identity mappings:
//   - "CLIENT_METRICS_CONFIG" (kmsg case 7, KIP-714) maps to the contract's
//     differently-spelled "DYNAMIC_CLIENT_METRICS_CONFIG" (same concept).
//   - "GROUP_CONFIG" (kmsg case 8, KAFKA-14511) has no contract equivalent
//     at all and falls to UNKNOWN.
//
// Plus two defensive cases (empty string, an unrecognized string) proving
// the default branch never propagates a raw/foreign value.
func TestSourceToGenerated(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want generated.ConfigSource
	}{
		{"dynamic topic", "DYNAMIC_TOPIC_CONFIG", generated.ConfigSourceDYNAMICTOPICCONFIG},
		{"dynamic broker", "DYNAMIC_BROKER_CONFIG", generated.ConfigSourceDYNAMICBROKERCONFIG},
		{"dynamic default broker", "DYNAMIC_DEFAULT_BROKER_CONFIG", generated.ConfigSourceDYNAMICDEFAULTBROKERCONFIG},
		{"static broker", "STATIC_BROKER_CONFIG", generated.ConfigSourceSTATICBROKERCONFIG},
		{"default config", "DEFAULT_CONFIG", generated.ConfigSourceDEFAULTCONFIG},
		{"dynamic broker logger", "DYNAMIC_BROKER_LOGGER_CONFIG", generated.ConfigSourceDYNAMICBROKERLOGGERCONFIG},
		{"client metrics (kmsg name != contract name)", "CLIENT_METRICS_CONFIG", generated.ConfigSourceDYNAMICCLIENTMETRICSCONFIG},
		{"kmsg UNKNOWN", "UNKNOWN", generated.ConfigSourceUNKNOWN},
		{"group config has no contract equivalent", "GROUP_CONFIG", generated.ConfigSourceUNKNOWN},
		{"empty string", "", generated.ConfigSourceUNKNOWN},
		{"unrecognized string", "totally-made-up-value", generated.ConfigSourceUNKNOWN},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sourceToGenerated(tc.in))
		})
	}
}

// TestConfigEntryToGeneratedMapsAllFieldsAndSynonyms covers the full-field
// case (sensitive, read-only, source mapped through sourceToGenerated,
// synonyms each independently mapped).
func TestConfigEntryToGeneratedMapsAllFieldsAndSynonyms(t *testing.T) {
	g := configEntryToGenerated(cluster.ConfigEntry{
		Name: "log.retention.ms", Value: "604800000", Source: "DYNAMIC_BROKER_CONFIG",
		IsSensitive: true, IsReadOnly: true,
		Synonyms: []cluster.ConfigSynonym{
			{Name: "log.retention.ms", Value: "604800000", Source: "DEFAULT_CONFIG"},
			{Name: "log.retention.minutes", Value: "", Source: "CLIENT_METRICS_CONFIG"},
		},
	})
	require.Equal(t, "log.retention.ms", g.Name)
	require.Equal(t, "604800000", g.Value)
	require.Equal(t, generated.ConfigSourceDYNAMICBROKERCONFIG, g.Source)
	require.True(t, g.IsSensitive)
	require.True(t, g.IsReadOnly)
	require.NotNil(t, g.Synonyms)
	require.Len(t, *g.Synonyms, 2)
	require.Equal(t, "log.retention.ms", *(*g.Synonyms)[0].Name)
	require.Equal(t, generated.ConfigSourceDEFAULTCONFIG, *(*g.Synonyms)[0].Source)
	require.Equal(t, generated.ConfigSourceDYNAMICCLIENTMETRICSCONFIG, *(*g.Synonyms)[1].Source)
}

// TestConfigEntryToGeneratedOmitsSynonymsWhenEmpty covers the zero-synonyms
// branch: the contract's synonyms field is optional/omittable, and a nil
// slice (rather than an empty-but-non-nil one) is the correct "absent" value.
func TestConfigEntryToGeneratedOmitsSynonymsWhenEmpty(t *testing.T) {
	g := configEntryToGenerated(cluster.ConfigEntry{Name: "x", Value: "y", Source: "UNKNOWN"})
	require.Nil(t, g.Synonyms)
	require.Equal(t, generated.ConfigSourceUNKNOWN, g.Source)
}
