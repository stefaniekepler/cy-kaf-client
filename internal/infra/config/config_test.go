package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func write(t *testing.T, s string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(p, []byte(s), 0o644))
	return p
}

func TestLoadHappyPath(t *testing.T) {
	p := write(t, `
server: {port: 9090}
kafka:
  clusters:
    - name: prod
      bootstrapServers: k1:9092,k2:9092
      readOnly: true
      properties: {security.protocol: SSL, request.timeout.ms: 30000}
      schemaRegistry: http://sr:8085
      kafkaConnect: [{name: main, address: http://c:8083}]
      ksqldbServer: http://ksql:8088
`)
	cfg, err := Load(p, false)
	require.NoError(t, err)
	require.Equal(t, 9090, cfg.Server.Port)
	require.Len(t, cfg.Kafka.Clusters, 1)
	c := cfg.Kafka.Clusters[0]
	require.Equal(t, "prod", c.Name)
	require.True(t, c.ReadOnly)
	require.Equal(t, "30000", c.Properties["request.timeout.ms"]) // 数值统一字符串化
}

func TestLoadDefaultsWhenFileMissing(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "nope.yaml"), false)
	require.NoError(t, err)
	require.Equal(t, 8080, cfg.Server.Port)
	require.Empty(t, cfg.Kafka.Clusters)
}

func TestLoadRejectsDuplicateNamesAndMissingBootstrap(t *testing.T) {
	_, err := Load(write(t, "kafka:\n  clusters:\n    - {name: a, bootstrapServers: x:1}\n    - {name: a, bootstrapServers: y:1}\n"), false)
	require.ErrorContains(t, err, "duplicate cluster name")
	_, err = Load(write(t, "kafka:\n  clusters:\n    - {name: b}\n"), false)
	require.ErrorContains(t, err, "bootstrapServers")
}

func TestLoadExplicitMissingPathErrors(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.yaml"), true)
	require.ErrorContains(t, err, "指定的配置文件不存在")
	_, err = Load(filepath.Join(t.TempDir(), "nope.yaml"), false)
	require.NoError(t, err) // 默认路径缺失仍是合法首启
}

func TestLoadRejectsMissingName(t *testing.T) {
	_, err := Load(write(t, "kafka:\n  clusters:\n    - bootstrapServers: x:1\n"), false)
	require.ErrorContains(t, err, "name is required")
}

func TestToDomain(t *testing.T) {
	c := ClusterCfg{Name: "x", BootstrapServers: "a:1, b:2", ReadOnly: true,
		Properties:     map[string]any{"security.protocol": "SSL"},
		SchemaRegistry: "http://sr", KafkaConnect: []ConnectCfg{{Name: "m", Address: "http://c"}}}
	d, err := c.ToDomain()
	require.NoError(t, err)
	require.Equal(t, []string{"a:1", "b:2"}, d.Conn.BootstrapServers)
	require.Equal(t, "SSL", d.Conn.Security["security.protocol"])
	require.Len(t, d.Connects, 1)
	require.Equal(t, "m", d.Connects[0].Name)
	require.Equal(t, "http://c", d.Connects[0].Address)
}

// KSQL auth and TLS are optional pointer blocks.  The pointers preserve the
// distinction between an omitted block and a configured block whose values
// happen to be empty, while the existing ksqldbServer URL remains unchanged.
func TestToDomainMapsKsqlAuthAndSSL(t *testing.T) {
	user, pass := os.Getenv("KSQL_TEST_USER"), os.Getenv("KSQL_TEST_PASSWORD")
	c := ClusterCfg{
		Name:             "local",
		BootstrapServers: "localhost:9092",
		KsqldbServer:     "http://ksql:8088",
		KsqldbServerAuth: &KsqlAuthCfg{Username: user, Password: pass},
		KsqldbServerSsl: &KsqlSSLCfg{
			TruststoreLocation: "/tmp/truststore.jks",
			TruststorePassword: pass,
			KeystoreLocation:   "/tmp/keystore.jks",
			KeystorePassword:   pass,
		},
	}
	d, err := c.ToDomain()
	require.NoError(t, err)
	require.Equal(t, "http://ksql:8088", d.KsqlURL)
	require.NotNil(t, d.KsqlAuth)
	require.Equal(t, user, d.KsqlAuth.Username)
	require.Equal(t, pass, d.KsqlAuth.Password)
	require.NotNil(t, d.KsqlSSL)
	require.Equal(t, "/tmp/truststore.jks", d.KsqlSSL.TruststoreLocation)
	require.Equal(t, pass, d.KsqlSSL.TruststorePassword)
	require.Equal(t, "/tmp/keystore.jks", d.KsqlSSL.KeystoreLocation)
	require.Equal(t, pass, d.KsqlSSL.KeystorePassword)
}

func TestLoadMapsKsqlAuthAndSSLFromYAML(t *testing.T) {
	p := write(t, `
kafka:
  clusters:
    - name: local
      bootstrapServers: localhost:9092
      ksqldbServer: http://ksql:8088
      ksqldbServerAuth:
        username: yaml-user
        password: yaml-password
      ksqldbServerSsl:
        truststoreLocation: /tmp/truststore.jks
        truststorePassword: yaml-truststore-password
        keystoreLocation: /tmp/keystore.jks
        keystorePassword: yaml-keystore-password
`)
	cfg, err := Load(p, false)
	require.NoError(t, err)
	d, err := cfg.Kafka.Clusters[0].ToDomain()
	require.NoError(t, err)
	require.Equal(t, "http://ksql:8088", d.KsqlURL)
	require.NotNil(t, d.KsqlAuth)
	require.Equal(t, "yaml-user", d.KsqlAuth.Username)
	require.Equal(t, "yaml-password", d.KsqlAuth.Password)
	require.NotNil(t, d.KsqlSSL)
	require.Equal(t, "/tmp/truststore.jks", d.KsqlSSL.TruststoreLocation)
	require.Equal(t, "yaml-truststore-password", d.KsqlSSL.TruststorePassword)
	require.Equal(t, "/tmp/keystore.jks", d.KsqlSSL.KeystoreLocation)
	require.Equal(t, "yaml-keystore-password", d.KsqlSSL.KeystorePassword)
}

func TestKsqlConfigRemainsBackwardCompatibleWhenOptionalBlocksAbsent(t *testing.T) {
	p := write(t, `
kafka:
  clusters:
    - name: local
      bootstrapServers: localhost:9092
      ksqldbServer: http://ksql:8088
`)
	cfg, err := Load(p, false)
	require.NoError(t, err)
	c := cfg.Kafka.Clusters[0]
	require.Equal(t, "http://ksql:8088", c.KsqldbServer)
	require.Nil(t, c.KsqldbServerAuth)
	require.Nil(t, c.KsqldbServerSsl)
	d, err := c.ToDomain()
	require.NoError(t, err)
	require.Equal(t, "http://ksql:8088", d.KsqlURL)
	require.Nil(t, d.KsqlAuth)
	require.Nil(t, d.KsqlSSL)
}

// --- P1c Task 1: serde/masking/defaults/throttle YAML parsing (brief cases 1-5) ---

// Case 1: serde[] + defaultValueSerde map onto Definition.SerdeConfigs/
// DefaultValueSerde. Also locks the brief's resolved ambiguity: a serde item
// carrying the contract's className/filePath fields (unused by this stage's
// built-in serdes) must parse without error, not just be silently ignored.
func TestToDomainMapsSerdeConfigsAndDefaultValueSerde(t *testing.T) {
	p := write(t, `
kafka:
  clusters:
    - name: a
      bootstrapServers: x:1
      serde:
        - name: MySerde
          topicValuesPattern: ".*-value"
          className: com.example.Ignored
          filePath: /ignored.jar
      defaultValueSerde: String
`)
	cfg, err := Load(p, false)
	require.NoError(t, err)
	d, err := cfg.Kafka.Clusters[0].ToDomain()
	require.NoError(t, err)
	require.Len(t, d.SerdeConfigs, 1)
	require.Equal(t, "MySerde", d.SerdeConfigs[0].Name)
	require.Equal(t, ".*-value", d.SerdeConfigs[0].TopicValuesPattern)
	require.Equal(t, "String", d.DefaultValueSerde)
}

// Case 2: masking[] with type MASK maps onto a MaskingRule with the matching
// enum plus its FieldsNamePattern/MaskingCharsReplacement.
func TestToDomainMapsMaskingRule(t *testing.T) {
	p := write(t, `
kafka:
  clusters:
    - name: a
      bootstrapServers: x:1
      masking:
        - type: MASK
          fieldsNamePattern: "pass.*"
          maskingCharsReplacement: ["X"]
`)
	cfg, err := Load(p, false)
	require.NoError(t, err)
	d, err := cfg.Kafka.Clusters[0].ToDomain()
	require.NoError(t, err)
	require.Len(t, d.Maskings, 1)
	require.Equal(t, cluster.MaskMask, d.Maskings[0].Type)
	require.Equal(t, "pass.*", d.Maskings[0].FieldsNamePattern)
	require.Equal(t, []string{"X"}, d.Maskings[0].MaskingCharsReplacement)
}

// Case 3: pollingThrottleRate maps straight onto Definition.PollingThrottleRate (int64).
func TestToDomainMapsPollingThrottleRate(t *testing.T) {
	p := write(t, `
kafka:
  clusters:
    - name: a
      bootstrapServers: x:1
      pollingThrottleRate: 1024
`)
	cfg, err := Load(p, false)
	require.NoError(t, err)
	d, err := cfg.Kafka.Clusters[0].ToDomain()
	require.NoError(t, err)
	require.Equal(t, int64(1024), d.PollingThrottleRate)
}

// Case 4: when serde/masking/pollingThrottleRate are all absent, the
// Definition must default to empty (non-nil) slices/zero values, never a nil
// slice or a lookup panic.
func TestToDomainDefaultsSerdeMaskingThrottleWhenAbsent(t *testing.T) {
	p := write(t, `
kafka:
  clusters:
    - name: a
      bootstrapServers: x:1
`)
	cfg, err := Load(p, false)
	require.NoError(t, err)
	d, err := cfg.Kafka.Clusters[0].ToDomain()
	require.NoError(t, err)
	require.NotNil(t, d.SerdeConfigs)
	require.Len(t, d.SerdeConfigs, 0)
	require.NotNil(t, d.Maskings)
	require.Len(t, d.Maskings, 0)
	require.Equal(t, "", d.DefaultKeySerde)
	require.Equal(t, "", d.DefaultValueSerde)
	require.Equal(t, int64(0), d.PollingThrottleRate)
}

// Case 5: masking[].type REMOVE/MASK/REPLACE each map onto the matching
// enum value; an unrecognized type string fails config loading with a clear
// error naming the offending value (never a silent zero-value enum).
func TestToDomainMasksTypeEnumMapping(t *testing.T) {
	cases := []struct {
		in   string
		want cluster.MaskingType
	}{
		{"REMOVE", cluster.MaskRemove},
		{"MASK", cluster.MaskMask},
		{"REPLACE", cluster.MaskReplace},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			p := write(t, fmt.Sprintf(`
kafka:
  clusters:
    - name: a
      bootstrapServers: x:1
      masking:
        - type: %s
`, tc.in))
			cfg, err := Load(p, false)
			require.NoError(t, err)
			d, err := cfg.Kafka.Clusters[0].ToDomain()
			require.NoError(t, err)
			require.Len(t, d.Maskings, 1)
			require.Equal(t, tc.want, d.Maskings[0].Type)
		})
	}
}

// --- P2a Task 1: schemaRegistryAuth/schemaRegistrySsl YAML parsing ---

// schemaRegistryAuth/schemaRegistrySsl map onto
// Definition.SchemaRegistry.Auth/SSL when present, and stay nil (not
// zero-value structs) when absent — the URL itself keeps mapping onto
// SchemaRegistry.URL exactly as before the Task 1 rename.
func TestToDomainMapsSchemaRegistryAuthAndSsl(t *testing.T) {
	p := write(t, `
kafka:
  clusters:
    - name: a
      bootstrapServers: x:1
      schemaRegistry: http://sr:8085
      schemaRegistryAuth: {username: u, password: p}
      schemaRegistrySsl: {truststoreLocation: /t.jks, truststorePassword: tp}
`)
	cfg, err := Load(p, false)
	require.NoError(t, err)
	d, err := cfg.Kafka.Clusters[0].ToDomain()
	require.NoError(t, err)
	require.Equal(t, "http://sr:8085", d.SchemaRegistry.URL)
	require.NotNil(t, d.SchemaRegistry.Auth)
	require.Equal(t, "u", d.SchemaRegistry.Auth.Username)
	require.Equal(t, "p", d.SchemaRegistry.Auth.Password)
	require.NotNil(t, d.SchemaRegistry.SSL)
	require.Equal(t, "/t.jks", d.SchemaRegistry.SSL.TruststoreLocation)
	require.Equal(t, "tp", d.SchemaRegistry.SSL.TruststorePassword)

	p2 := write(t, `
kafka:
  clusters:
    - name: a
      bootstrapServers: x:1
      schemaRegistry: http://sr:8085
`)
	cfg2, err := Load(p2, false)
	require.NoError(t, err)
	d2, err := cfg2.Kafka.Clusters[0].ToDomain()
	require.NoError(t, err)
	require.Equal(t, "http://sr:8085", d2.SchemaRegistry.URL)
	require.Nil(t, d2.SchemaRegistry.Auth)
	require.Nil(t, d2.SchemaRegistry.SSL)
}

// --- P2b Task 1: per-connect address/auth/ssl YAML parsing ---

// kafkaConnect[] items map onto Definition.Connects, carrying not just Name
// (as before this task) but also Address, plus optional Auth/SSL that stay
// nil (not zero-value structs) when the corresponding fields are absent —
// same "nil means not configured" convention as P2a's SchemaRegistry.Auth/
// SSL mapping.
func TestToDomainMapsConnectAddressAuthAndSsl(t *testing.T) {
	p := write(t, `
kafka:
  clusters:
    - name: a
      bootstrapServers: x:1
      kafkaConnect:
        - name: main
          address: http://connect:8083
          username: u
          password: p
          truststoreLocation: /t.jks
          truststorePassword: tp
`)
	cfg, err := Load(p, false)
	require.NoError(t, err)
	d, err := cfg.Kafka.Clusters[0].ToDomain()
	require.NoError(t, err)
	require.Len(t, d.Connects, 1)
	require.Equal(t, "main", d.Connects[0].Name)
	require.Equal(t, "http://connect:8083", d.Connects[0].Address)
	require.NotNil(t, d.Connects[0].Auth)
	require.Equal(t, "u", d.Connects[0].Auth.Username)
	require.Equal(t, "p", d.Connects[0].Auth.Password)
	require.NotNil(t, d.Connects[0].SSL)
	require.Equal(t, "/t.jks", d.Connects[0].SSL.TruststoreLocation)
	require.Equal(t, "tp", d.Connects[0].SSL.TruststorePassword)

	p2 := write(t, `
kafka:
  clusters:
    - name: a
      bootstrapServers: x:1
      kafkaConnect:
        - name: main
          address: http://connect:8083
`)
	cfg2, err := Load(p2, false)
	require.NoError(t, err)
	d2, err := cfg2.Kafka.Clusters[0].ToDomain()
	require.NoError(t, err)
	require.Len(t, d2.Connects, 1)
	require.Equal(t, "main", d2.Connects[0].Name)
	require.Equal(t, "http://connect:8083", d2.Connects[0].Address)
	require.Nil(t, d2.Connects[0].Auth)
	require.Nil(t, d2.Connects[0].SSL)
}

func TestLoadRejectsUnknownMaskingType(t *testing.T) {
	_, err := Load(write(t, `
kafka:
  clusters:
    - name: a
      bootstrapServers: x:1
      masking:
        - type: BOGUS
`), false)
	require.ErrorContains(t, err, "BOGUS")
}

func TestParseConfigValid(t *testing.T) {
	snap, err := parseConfig([]byte(`
kafka:
  clusters:
    - name: local
      bootstrapServers: localhost:9092
`))
	require.NoError(t, err)
	require.Len(t, snap.Clusters, 1)
	require.Equal(t, "local", snap.Clusters[0].Name)
	require.NotNil(t, snap.Raw)
}

func TestParseConfigRejectsInvalid(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{"missing name", "kafka:\n  clusters:\n    - bootstrapServers: localhost:9092\n"},
		{"missing bootstrap", "kafka:\n  clusters:\n    - name: local\n"},
		{"duplicate name", "kafka:\n  clusters:\n    - name: a\n      bootstrapServers: x:9092\n    - name: a\n      bootstrapServers: y:9092\n"},
		{"unknown masking", "kafka:\n  clusters:\n    - name: a\n      bootstrapServers: x:9092\n      masking:\n        - type: NOPE\n"},
		{"not yaml", "kafka: [unclosed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfig([]byte(tc.data))
			require.Error(t, err)
			require.ErrorIs(t, err, cluster.ErrInvalidConfig)
		})
	}
}
