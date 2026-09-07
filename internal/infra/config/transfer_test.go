package config_test

import (
	"context"
	"github.com/cy-kaf/cy-kaf-client/internal/infra/config"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
	"testing"
)

func TestTransferCodecRoundTripPreservesClusterFields(t *testing.T) {
	codec := config.TransferCodec{}
	input := []byte(`server: {port: 9999}
kafka:
  clusters:
    - name: team
      bootstrapServers: broker:9092
      properties: {sasl.jaas.config: 'test-password', retries: 3}
      schemaRegistrySsl: {truststoreLocation: /local/ca.pem}
      customField: {enabled: true}
`)
	snap, err := codec.Parse(input)
	require.NoError(t, err)
	require.Len(t, snap.Clusters, 1)
	require.Equal(t, "3", snap.Clusters[0].Conn.Security["retries"])
	output, err := codec.Export(snap)
	require.NoError(t, err)
	require.NotContains(t, string(output), "server:")
	require.Contains(t, string(output), "test-password")
	again, err := codec.Parse(output)
	require.NoError(t, err)
	require.Equal(t, snap, again)
}

func TestTransferCodecRejectsWholeMalformedFileWithoutLeakingValues(t *testing.T) {
	for _, input := range []string{
		"", "[]", "kafka: {}", "kafka: {clusters: null}",
		"kafka: {clusters: secret}",
		"kafka: {clusters: [{name: x}]}",
		"kafka: {clusters: [{Name: x, BootstrapServers: 'a:9092'}]}",
		"kafka: {clusters: [{name: x, bootstrapServers: 'a:9092', readonly: true}]}",
		"kafka: {clusters: [{name: 12, bootstrapServers: 'a:9092'}]}",
		"kafka: {clusters: [{name: ' ', bootstrapServers: 'a:9092'}]}",
		"kafka: {clusters: [{name: x, bootstrapServers: a}]}",
		"kafka: {clusters: [{name: x, bootstrapServers: 'a:70000'}]}",
		"kafka: {clusters: [{name: x, bootstrapServers: 'a:9092', readOnly: secret}]}",
		"kafka: {clusters: [{name: x, name: y, bootstrapServers: 'a:9092'}]}",
		"kafka: {clusters: [{name: x, bootstrapServers: 'a:9092'}, {name: x, bootstrapServers: 'b:9092'}]}",
		"kafka: {clusters: [{name: x, bootstrapServers: 'a:9092', properties: {password: [secret]}}]}",
		"kafka: {clusters: []}\n---\nkafka: {clusters: []}",
		strings.Repeat(" ", (10<<20)+1),
	} {
		t.Run(input[:min(len(input), 90)], func(t *testing.T) {
			_, err := (config.TransferCodec{}).Parse([]byte(input))
			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestTransferCodecAllowsEmptyExportAndIPv6(t *testing.T) {
	for _, input := range []string{"kafka: {clusters: []}", "kafka: {clusters: [{name: ipv6, bootstrapServers: '[::1]:9092, host:9093'}]}"} {
		_, err := (config.TransferCodec{}).Parse([]byte(input))
		require.NoError(t, err)
	}
}

func TestTransferSSLMatchesReloadedConfiguration(t *testing.T) {
	input := []byte(`kafka:
  clusters:
    - name: tls
      bootstrapServers: broker:9092
      ssl:
        truststoreLocation: /local/ca.pem
        truststorePassword: fixture-password
        verify: false
`)
	codec := config.TransferCodec{}
	imported, err := codec.Parse(input)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "config.yaml")
	store := config.NewStore(path, nil)
	require.NoError(t, store.Save(context.Background(), imported))
	current, err := store.Current()
	require.NoError(t, err)
	require.Equal(t, imported.Clusters, current.Clusters)
	loaded, err := config.Load(path, true)
	require.NoError(t, err)
	def, err := loaded.Kafka.Clusters[0].ToDomain()
	require.NoError(t, err)
	require.Equal(t, "/local/ca.pem", def.Conn.Security["ssl.truststore.location"])
	require.Equal(t, "fixture-password", def.Conn.Security["ssl.truststore.password"])
	require.Contains(t, def.Conn.Security, "ssl.endpoint.identification.algorithm")
}

func TestTransferNumericPropertiesMatchStartupWithoutPrecisionLoss(t *testing.T) {
	input := []byte("kafka: {clusters: [{name: numeric, bootstrapServers: 'broker:9092', properties: {timeout: 1000000, identifier: 9007199254740993}, serde: [{name: custom, properties: {limit: 123}}]}]}")
	snap, err := (config.TransferCodec{}).Parse(input)
	require.NoError(t, err)
	require.Equal(t, "1000000", snap.Clusters[0].Conn.Security["timeout"])
	require.Equal(t, "9007199254740993", snap.Clusters[0].Conn.Security["identifier"])
	path := filepath.Join(t.TempDir(), "config.yaml")
	store := config.NewStore(path, nil)
	require.NoError(t, store.Save(context.Background(), snap))
	current, err := store.Current()
	require.NoError(t, err)
	require.Equal(t, snap.Clusters, current.Clusters)
}
