package mcpserver

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"unicode"
)

const redactedValue = "[REDACTED]"

// redactCredentials is intentionally opt-in for configuration-bearing results.
// Message handlers pass Kafka envelopes directly to safeResult, so payload keys,
// values, and headers never enter this credential-key traversal.
func redactCredentials(value any) (redacted any) {
	defer func() {
		if recover() != nil {
			redacted = nil
		}
	}()

	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		return nil
	}
	return redactJSONCredentials(normalized)
}

func redactJSONCredentials(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, nested := range typed {
			if isCredentialKey(key) {
				out[key] = redactedValue
				continue
			}
			out[key] = redactJSONCredentials(nested)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, nested := range typed {
			out[index] = redactJSONCredentials(nested)
		}
		return out
	default:
		return value
	}
}

func isCredentialKey(key string) bool {
	normalized := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, key)

	return strings.Contains(normalized, "password") ||
		strings.Contains(normalized, "secret") ||
		strings.Contains(normalized, "token") ||
		strings.Contains(normalized, "jaas") ||
		strings.Contains(normalized, "basicauthuserinfo") ||
		strings.Contains(normalized, "apikey") ||
		strings.Contains(normalized, "authorization") ||
		strings.Contains(normalized, "privatekey") ||
		strings.Contains(normalized, "clientkey") ||
		strings.Contains(normalized, "accesskey")
}

func normalizeResult(output any, maxItems int) (any, error) {
	value, err := resultShape(output)
	if err != nil {
		return nil, err
	}
	if value.IsValid() && (value.Kind() == reflect.Slice || value.Kind() == reflect.Array) {
		if maxItems > 0 && value.Len() > maxItems {
			if value.Kind() == reflect.Array {
				truncated := reflect.MakeSlice(reflect.SliceOf(value.Type().Elem()), maxItems, maxItems)
				reflect.Copy(truncated, value)
				value = truncated
			} else {
				value = value.Slice(0, maxItems)
			}
		}
		return map[string]any{"result": value.Interface()}, nil
	}
	if value.IsValid() && (value.Kind() == reflect.Map || value.Kind() == reflect.Struct) {
		return output, nil
	}
	return map[string]any{"result": output}, nil
}

func resultShape(output any) (reflect.Value, error) {
	type pointerVisit struct {
		typ     reflect.Type
		pointer uintptr
	}

	value := reflect.ValueOf(output)
	visited := make(map[pointerVisit]struct{})
	for value.IsValid() && (value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer) {
		if value.IsNil() {
			return reflect.Value{}, nil
		}
		if value.Kind() == reflect.Pointer {
			visit := pointerVisit{typ: value.Type(), pointer: value.Pointer()}
			if _, exists := visited[visit]; exists {
				return reflect.Value{}, errOperationFailed
			}
			visited[visit] = struct{}{}
		}
		value = value.Elem()
	}
	return value, nil
}

func marshalBoundedResult(meta ToolMeta, output any) ([]byte, error) {
	raw, err := json.Marshal(output)
	if err != nil {
		return nil, errOperationFailed
	}

	limit := meta.MaxBytes
	if limit <= 0 {
		limit = maxResultBytes
	}
	if isCSVTool(meta.Name) && limit > maxCSVBytes {
		limit = maxCSVBytes
	}
	if len(raw) > limit {
		return nil, errResultTooLarge
	}
	return raw, nil
}

func isCSVTool(name string) bool {
	return strings.HasSuffix(strings.ToLower(name), "csv")
}
