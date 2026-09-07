// Package config loads the upstream-shaped (kafka.clusters[]) YAML config.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

type App struct {
	Server ServerCfg `yaml:"server"`
	Kafka  KafkaCfg  `yaml:"kafka"`
}

type ServerCfg struct {
	Port int `yaml:"port"`
}

type KafkaCfg struct {
	Clusters []ClusterCfg `yaml:"clusters"`
}

// ConnectCfg is one entry of ClusterCfg.KafkaConnect: a Kafka Connect
// worker's name/address plus optional basic auth and/or custom TLS
// material. Unlike ClusterCfg.SchemaRegistryAuth/Ssl (single pointer
// sub-structs, since a cluster has exactly one Schema Registry), these
// fields are flat directly on ConnectCfg — the contract's kafkaConnect[]
// array items are themselves flat objects (name/address/username/password/
// keystoreLocation/keystorePassword), not nested auth/ssl sub-objects, so
// there's no YAML shape to mirror with a pointer field here. Truststore*
// fields extend beyond what the contract's kafkaConnect[] schema documents,
// same "config-sourced SSL material" convention SRSSLCfg already
// established for schemaRegistrySsl.
type ConnectCfg struct {
	Name    string `yaml:"name"`
	Address string `yaml:"address"`

	Username           string `yaml:"username"`
	Password           string `yaml:"password"`
	KeystoreLocation   string `yaml:"keystoreLocation"`
	KeystorePassword   string `yaml:"keystorePassword"`
	TruststoreLocation string `yaml:"truststoreLocation"`
	TruststorePassword string `yaml:"truststorePassword"`
}

// SRAuthCfg is the YAML shape of ClusterCfg.SchemaRegistryAuth (contract's
// schemaRegistryAuth.username/password — the oauth sub-object isn't modeled
// here, same P2a franz-go pkg/sr scope limit as domain's SRAuth).
type SRAuthCfg struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// SRSSLCfg is the YAML shape of ClusterCfg.SchemaRegistrySsl: the contract's
// keystore fields plus truststore fields (this codebase's existing
// convention for config-sourced SSL material verifying a server's own
// certificate).
type SRSSLCfg struct {
	KeystoreLocation   string `yaml:"keystoreLocation"`
	KeystorePassword   string `yaml:"keystorePassword"`
	TruststoreLocation string `yaml:"truststoreLocation"`
	TruststorePassword string `yaml:"truststorePassword"`
}

// KsqlAuthCfg is the optional basic-auth block for ClusterCfg.KsqldbServer.
// It remains a pointer on ClusterCfg so an omitted block stays distinct from
// a configured block whose values are empty.
type KsqlAuthCfg struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// KsqlSSLCfg is the optional TLS material block for ClusterCfg.KsqldbServer.
// The keystore fields are decoded and preserved for the future mTLS adapter;
// Task 3 deliberately performs no certificate loading.
type KsqlSSLCfg struct {
	TruststoreLocation string `yaml:"truststoreLocation"`
	TruststorePassword string `yaml:"truststorePassword"`
	KeystoreLocation   string `yaml:"keystoreLocation"`
	KeystorePassword   string `yaml:"keystorePassword"`
}

type KafkaSSLCfg struct {
	TruststoreLocation string `yaml:"truststoreLocation"`
	TruststorePassword string `yaml:"truststorePassword"`
	Verify             *bool  `yaml:"verify"`
}

type ClusterCfg struct {
	SSL              *KafkaSSLCfg   `yaml:"ssl"`
	Name             string         `yaml:"name"`
	BootstrapServers string         `yaml:"bootstrapServers"`
	ReadOnly         bool           `yaml:"readOnly"`
	Properties       map[string]any `yaml:"properties"`
	SchemaRegistry   string         `yaml:"schemaRegistry"`
	KafkaConnect     []ConnectCfg   `yaml:"kafkaConnect"`
	KsqldbServer     string         `yaml:"ksqldbServer"`
	KsqldbServerAuth *KsqlAuthCfg   `yaml:"ksqldbServerAuth"`
	KsqldbServerSsl  *KsqlSSLCfg    `yaml:"ksqldbServerSsl"`

	// P2a Task 1: optional basic auth / custom TLS material for the
	// SchemaRegistry connection above (contract-shaped: schemaRegistryAuth.
	// username/password, schemaRegistrySsl.{keystore,truststore}{Location,
	// Password}). Pointers so "key absent" maps onto a nil
	// SchemaRegistrySpec.Auth/SSL, distinct from a configured-but-empty one.
	SchemaRegistryAuth *SRAuthCfg `yaml:"schemaRegistryAuth"`
	SchemaRegistrySsl  *SRSSLCfg  `yaml:"schemaRegistrySsl"`

	// P1c Task 1: serde/masking/defaults/throttle, contract-shaped (see
	// ApplicationConfig.properties.kafka.clusters[] in contract/openapi.yaml).
	// SerdeCfg deliberately doesn't declare the contract's className/filePath
	// fields — this stage's built-in serdes don't need them, and yaml.v3's
	// default (non-strict) Unmarshal silently drops unknown mapping keys, so
	// their presence in a config file never fails parsing.
	Serde               []SerdeCfg   `yaml:"serde"`
	DefaultKeySerde     string       `yaml:"defaultKeySerde"`
	DefaultValueSerde   string       `yaml:"defaultValueSerde"`
	Masking             []MaskingCfg `yaml:"masking"`
	PollingThrottleRate int64        `yaml:"pollingThrottleRate"`
}

type SerdeCfg struct {
	Name               string         `yaml:"name"`
	TopicKeysPattern   string         `yaml:"topicKeysPattern"`
	TopicValuesPattern string         `yaml:"topicValuesPattern"`
	Properties         map[string]any `yaml:"properties"`
}

type MaskingCfg struct {
	Type                    string   `yaml:"type"`
	Fields                  []string `yaml:"fields"`
	FieldsNamePattern       string   `yaml:"fieldsNamePattern"`
	MaskingCharsReplacement []string `yaml:"maskingCharsReplacement"`
	Replacement             string   `yaml:"replacement"`
	TopicKeysPattern        string   `yaml:"topicKeysPattern"`
	TopicValuesPattern      string   `yaml:"topicValuesPattern"`
}

// maskingTypesByName looks up the contract's masking[].type enum strings.
// Used exclusively through ToDomain (and, via ToDomain, App.validate — see
// its doc comment) so an unrecognized value always surfaces as an explicit
// error rather than silently defaulting to the zero enum value (MaskRemove).
var maskingTypesByName = map[string]cluster.MaskingType{
	"REMOVE":  cluster.MaskRemove,
	"MASK":    cluster.MaskMask,
	"REPLACE": cluster.MaskReplace,
}

// Load reads the YAML config at path. explicit distinguishes a user-supplied
// --config value from the computed default: a missing default path is a
// legitimate first-run state (silently falls back to zero-value defaults),
// but a missing path the user explicitly named is a fail-fast error.
func Load(path string, explicit bool) (*App, error) {
	cfg := &App{Server: ServerCfg{Port: 8080}}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if explicit {
			return nil, fmt.Errorf("指定的配置文件不存在: %s", path)
		}
		return cfg, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if cfg.Server.Port == 0 {
		cfg.Server.Port = 8080
	}
	// Stringify all property values in place (Properties stays map[string]any;
	// stringifyProps is the sole fmt.Sprint implementation point).
	for i := range cfg.Kafka.Clusters {
		props := cfg.Kafka.Clusters[i].Properties
		for k, v := range stringifyProps(props) {
			props[k] = v
		}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// stringifyProps converts a raw YAML-decoded properties map (values may be
// strings, numbers, bools, …) into an all-string map, matching upstream's
// string-typed properties. Sole fmt.Sprint loop: Load and ToDomain both call
// this instead of keeping their own duplicate stringify loops.
func stringifyProps(in map[string]any) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = fmt.Sprint(v)
	}
	return out
}

// validate also invokes ToDomain per cluster (discarding the resulting
// Definition) purely so an unrecognized masking[].type string fails config
// Load itself with a clear error — not just a later, easy-to-miss ToDomain
// call by whichever caller happens to convert the cluster next.
func (a *App) validate() error {
	seen := map[string]bool{}
	for i, c := range a.Kafka.Clusters {
		if c.Name == "" {
			return fmt.Errorf("kafka.clusters[%d]: name is required", i)
		}
		if seen[c.Name] {
			return fmt.Errorf("duplicate cluster name %q", c.Name)
		}
		seen[c.Name] = true
		if strings.TrimSpace(c.BootstrapServers) == "" {
			return fmt.Errorf("cluster %q: bootstrapServers is required", c.Name)
		}
		if _, err := c.ToDomain(); err != nil {
			return err
		}
	}
	return nil
}

// ToDomain converts c to its domain Definition. The only failure mode is an
// unrecognized masking[].type string (checked against maskingTypesByName);
// every other field maps unconditionally, defaulting absent serde/masking
// lists to empty (non-nil) slices and absent defaults/throttle to their zero
// values.
func (c ClusterCfg) ToDomain() (cluster.Definition, error) {
	servers := []string{}
	for _, s := range strings.Split(c.BootstrapServers, ",") {
		if s = strings.TrimSpace(s); s != "" {
			servers = append(servers, s)
		}
	}
	sec := stringifyProps(c.Properties)
	if c.SSL != nil {
		if c.SSL.TruststoreLocation != "" {
			sec["ssl.truststore.location"] = c.SSL.TruststoreLocation
		}
		if c.SSL.TruststorePassword != "" {
			sec["ssl.truststore.password"] = c.SSL.TruststorePassword
		}
		if c.SSL.Verify != nil && !*c.SSL.Verify {
			sec["ssl.endpoint.identification.algorithm"] = ""
		}
	}
	connects := make([]cluster.ConnectSpec, 0, len(c.KafkaConnect))
	for _, kc := range c.KafkaConnect {
		cs := cluster.ConnectSpec{Name: kc.Name, Address: kc.Address}
		if kc.Username != "" || kc.Password != "" {
			cs.Auth = &cluster.ConnectAuth{Username: kc.Username, Password: kc.Password}
		}
		if kc.KeystoreLocation != "" || kc.KeystorePassword != "" || kc.TruststoreLocation != "" || kc.TruststorePassword != "" {
			cs.SSL = &cluster.ConnectSSL{
				KeystoreLocation:   kc.KeystoreLocation,
				KeystorePassword:   kc.KeystorePassword,
				TruststoreLocation: kc.TruststoreLocation,
				TruststorePassword: kc.TruststorePassword,
			}
		}
		connects = append(connects, cs)
	}

	serdes := []cluster.SerdeConfig{}
	for _, s := range c.Serde {
		serdes = append(serdes, cluster.SerdeConfig{
			Name:               s.Name,
			TopicKeysPattern:   s.TopicKeysPattern,
			TopicValuesPattern: s.TopicValuesPattern,
			Properties:         s.Properties,
		})
	}

	maskings := []cluster.MaskingRule{}
	for _, m := range c.Masking {
		t, ok := maskingTypesByName[m.Type]
		if !ok {
			return cluster.Definition{}, fmt.Errorf("cluster %q: unknown masking type %q", c.Name, m.Type)
		}
		maskings = append(maskings, cluster.MaskingRule{
			Type:                    t,
			Fields:                  m.Fields,
			FieldsNamePattern:       m.FieldsNamePattern,
			MaskingCharsReplacement: m.MaskingCharsReplacement,
			Replacement:             m.Replacement,
			TopicKeysPattern:        m.TopicKeysPattern,
			TopicValuesPattern:      m.TopicValuesPattern,
		})
	}

	sr := cluster.SchemaRegistrySpec{URL: c.SchemaRegistry}
	if c.SchemaRegistryAuth != nil {
		sr.Auth = &cluster.SRAuth{
			Username: c.SchemaRegistryAuth.Username,
			Password: c.SchemaRegistryAuth.Password,
		}
	}
	if c.SchemaRegistrySsl != nil {
		sr.SSL = &cluster.SRSSL{
			KeystoreLocation:   c.SchemaRegistrySsl.KeystoreLocation,
			KeystorePassword:   c.SchemaRegistrySsl.KeystorePassword,
			TruststoreLocation: c.SchemaRegistrySsl.TruststoreLocation,
			TruststorePassword: c.SchemaRegistrySsl.TruststorePassword,
		}
	}

	var ksqlAuth *cluster.KsqlAuth
	if c.KsqldbServerAuth != nil {
		ksqlAuth = &cluster.KsqlAuth{
			Username: c.KsqldbServerAuth.Username,
			Password: c.KsqldbServerAuth.Password,
		}
	}
	var ksqlSSL *cluster.KsqlSSL
	if c.KsqldbServerSsl != nil {
		ksqlSSL = &cluster.KsqlSSL{
			TruststoreLocation: c.KsqldbServerSsl.TruststoreLocation,
			TruststorePassword: c.KsqldbServerSsl.TruststorePassword,
			KeystoreLocation:   c.KsqldbServerSsl.KeystoreLocation,
			KeystorePassword:   c.KsqldbServerSsl.KeystorePassword,
		}
	}

	return cluster.Definition{
		Name:                c.Name,
		ReadOnly:            c.ReadOnly,
		Conn:                cluster.ConnectionSpec{BootstrapServers: servers, Security: sec},
		SchemaRegistry:      sr,
		Connects:            connects,
		KsqlURL:             c.KsqldbServer,
		KsqlAuth:            ksqlAuth,
		KsqlSSL:             ksqlSSL,
		SerdeConfigs:        serdes,
		DefaultKeySerde:     c.DefaultKeySerde,
		DefaultValueSerde:   c.DefaultValueSerde,
		Maskings:            maskings,
		PollingThrottleRate: c.PollingThrottleRate,
	}, nil
}
