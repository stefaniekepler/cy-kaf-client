package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"gopkg.in/yaml.v3"
)

// TransferCodec shares only kafka.clusters, never machine-wide settings.
type TransferCodec struct{}

func importError(message string) error { return &cluster.ConfigImportError{Message: message} }

func (TransferCodec) Parse(content []byte) (cluster.ConfigSnapshot, error) {
	empty := cluster.ConfigSnapshot{}
	if len(content) > 10<<20 {
		return empty, importError("配置文件不能超过 10 MiB")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	var raw map[string]any
	if err := decoder.Decode(&raw); err != nil {
		return empty, importError("YAML 格式错误，请检查语法和重复字段")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return empty, importError("配置文件只能包含一个 YAML 文档")
	}
	kafka, ok := raw["kafka"].(map[string]any)
	if !ok {
		return empty, importError("kafka 必须是对象")
	}
	entries, ok := kafka["clusters"].([]any)
	if !ok {
		return empty, importError("kafka.clusters 必须是数组")
	}
	defs := make([]cluster.Definition, 0, len(entries))
	seen := map[string]bool{}
	for i, entry := range entries {
		def, err := parseImportCluster(entry)
		if err != nil {
			return empty, importError(fmt.Sprintf("kafka.clusters[%d]: %s", i, err.Error()))
		}
		if seen[def.Name] {
			return empty, importError(fmt.Sprintf("kafka.clusters[%d]: 文件内环境名称重复", i))
		}
		seen[def.Name] = true
		defs = append(defs, def)
	}
	return cluster.ConfigSnapshot{Raw: map[string]any{"kafka": map[string]any{"clusters": entries}}, Clusters: defs}, nil
}

func parseImportCluster(entry any) (cluster.Definition, error) {
	empty := cluster.Definition{}
	if _, ok := entry.(map[string]any); !ok {
		return empty, errors.New("环境配置必须是对象")
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return empty, errors.New("字段包含不支持的值")
	}
	var cfg ClusterCfg
	if err := validateImportKeys(entry, reflect.TypeOf(cfg)); err != nil {
		return empty, err
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			return empty, fmt.Errorf("字段 %s 类型错误", typeErr.Field)
		}
		return empty, errors.New("字段格式错误")
	}
	if strings.TrimSpace(cfg.Name) == "" {
		return empty, errors.New("name 必须是非空字符串")
	}
	if strings.TrimSpace(cfg.BootstrapServers) == "" {
		return empty, errors.New("bootstrapServers 必须是非空字符串")
	}
	for _, endpoint := range strings.Split(cfg.BootstrapServers, ",") {
		host, port, err := net.SplitHostPort(strings.TrimSpace(endpoint))
		n, portErr := strconv.Atoi(port)
		if err != nil || host == "" || strings.ContainsAny(host, " /@\t\r\n") || portErr != nil || n < 1 || n > 65535 {
			return empty, errors.New("bootstrapServers 必须包含完整主机地址和有效端口（多个地址用逗号分隔）")
		}
	}
	for _, value := range cfg.Properties {
		switch value.(type) {
		case string, float64, bool:
		default:
			return empty, errors.New("properties 的值必须是字符串、数字或布尔值")
		}
	}
	// JSON is used only for strict field-type checks. Decode the actual model
	// through YAML, just like startup, preserving integer property values.
	encoded, err := yaml.Marshal(entry)
	if err != nil {
		return empty, errors.New("字段包含不支持的值")
	}
	cfg = ClusterCfg{}
	if err := yaml.Unmarshal(encoded, &cfg); err != nil {
		return empty, errors.New("字段格式错误")
	}
	def, err := cfg.ToDomain()
	if err != nil {
		return empty, errors.New("masking.type 不受支持")
	}
	return def, nil
}

func (TransferCodec) Export(snap cluster.ConfigSnapshot) ([]byte, error) {
	entries := []any{}
	if kafka, ok := snap.Raw["kafka"].(map[string]any); ok {
		if configured, ok := kafka["clusters"].([]any); ok {
			entries = configured
		}
	}
	return yaml.Marshal(map[string]any{"kafka": map[string]any{"clusters": entries}})
}

// JSON's case-insensitive struct matching must not admit keys the YAML loader
// ignores. Unknown extension fields remain lossless; modeled keys are exact.
func validateImportKeys(value any, typ reflect.Type) error {
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Slice {
		if values, ok := value.([]any); ok {
			for _, item := range values {
				if err := validateImportKeys(item, typ.Elem()); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if typ.Kind() != reflect.Struct {
		return nil
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name := strings.Split(field.Tag.Get("yaml"), ",")[0]
		for key, nested := range fields {
			if strings.EqualFold(key, name) || strings.EqualFold(key, field.Name) {
				if key != name {
					return fmt.Errorf("字段名大小写错误，应为 %s", name)
				}
				if err := validateImportKeys(nested, field.Type); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
