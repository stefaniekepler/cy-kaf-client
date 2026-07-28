package mcpserver

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

const cyclicResultShapeTestMode = "CY_KAF_TEST_CYCLIC_RESULT_SHAPE"

type countingJSONResult struct {
	marshalCount *atomic.Int32
}

type typedCredentialConfig struct {
	Password  string               `json:"db.password"`
	ClientKey string               `json:"client-key-material"`
	Visible   string               `json:"visible"`
	Ignored   string               `json:"-"`
	Nested    map[string]string    `json:"nested"`
	Message   typedMessageEnvelope `json:"message"`
	Interface any                  `json:"interface"`
	Items     []map[string]string  `json:"items"`
	Array     [1]map[string]string `json:"array"`
	Pointer   *map[string]string   `json:"pointer"`
	Named     typedStringMap       `json:"named"`
	Structs   []typedNestedConfig  `json:"structs"`
	StructPtr *typedNestedConfig   `json:"structPointer"`
	Wrapped   any                  `json:"wrapped"`
}

type typedNestedConfig struct {
	Token   string `json:"AUTH_TOKEN"`
	Visible string `json:"visible"`
}

type typedMessageEnvelope struct {
	Key     string                `json:"key"`
	Value   string                `json:"value"`
	Headers [1]typedMessageHeader `json:"headers"`
}

type typedMessageHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type typedStringMap map[string]string

type cyclicCredentialConfig struct {
	Password string                  `json:"password"`
	Next     *cyclicCredentialConfig `json:"next"`
}

type pointerResult struct {
	Status string `json:"status"`
}

func (r countingJSONResult) MarshalJSON() ([]byte, error) {
	r.marshalCount.Add(1)
	return []byte(`{"status":"ok"}`), nil
}

func TestRedactCredentialsRecursivelyPreservesMessageEnvelope(t *testing.T) {
	input := map[string]any{
		"password": "p",
		"nested": []any{map[string]any{
			"sasl.jaas.config":   "secret-jaas",
			"truststorePassword": "trust",
		}},
		"message": map[string]any{
			"key":   "order-42",
			"value": "visible",
			"headers": []any{
				map[string]any{"key": "trace", "value": "trace-42"},
			},
		},
	}
	want := map[string]any{
		"password": "[REDACTED]",
		"nested": []any{map[string]any{
			"sasl.jaas.config":   "[REDACTED]",
			"truststorePassword": "[REDACTED]",
		}},
		"message": map[string]any{
			"key":   "order-42",
			"value": "visible",
			"headers": []any{
				map[string]any{"key": "trace", "value": "trace-42"},
			},
		},
	}

	require.Equal(t, want, redactCredentials(input))
	require.Equal(t, input["message"], redactCredentials(input).(map[string]any)["message"])
}

func TestRedactCredentialsRecognizesFrozenCredentialKeyVariants(t *testing.T) {
	input := map[string]any{
		"secret":                "a",
		"AUTH_TOKEN":            "b",
		"Private-Key":           "c",
		"client.key":            "d",
		"Access Key":            "e",
		"private_key_pem":       "c2",
		"client-key-material":   "d2",
		"aws_access_key_id":     "e2",
		"basic.auth.user.info":  "e3",
		"api.key":               "e4",
		"api_key":               "e5",
		"apikey":                "e6",
		"authorization":         "e7",
		"SSL_KEY_PASSWORD":      "f",
		"KeyStore.Password":     "g",
		"TRUST-STORE_password":  "h",
		"SaSl_JaAs-CoNfIg":      "i",
		"nestedArrays":          []any{[]any{map[string]any{"db_password": "j"}}},
		"monkey":                "ordinary",
		"messageKey":            "order-42",
		"header":                "visible-header",
		"nonCredentialProperty": "visible",
	}

	got := redactCredentials(input).(map[string]any)

	for _, key := range []string{
		"secret",
		"AUTH_TOKEN",
		"Private-Key",
		"client.key",
		"Access Key",
		"private_key_pem",
		"client-key-material",
		"aws_access_key_id",
		"basic.auth.user.info",
		"api.key",
		"api_key",
		"apikey",
		"authorization",
		"SSL_KEY_PASSWORD",
		"KeyStore.Password",
		"TRUST-STORE_password",
		"SaSl_JaAs-CoNfIg",
	} {
		require.Equal(t, "[REDACTED]", got[key], key)
	}
	nested := got["nestedArrays"].([]any)[0].([]any)[0].(map[string]any)
	require.Equal(t, "[REDACTED]", nested["db_password"])
	require.Equal(t, "ordinary", got["monkey"])
	require.Equal(t, "order-42", got["messageKey"])
	require.Equal(t, "visible-header", got["header"])
	require.Equal(t, "visible", got["nonCredentialProperty"])
}

func TestRedactCredentialsHandlesTypedJSONValuesWithoutMutation(t *testing.T) {
	pointerMap := map[string]string{
		"truststore.password": "pointer-trust",
		"visible":             "pointer-visible",
	}
	structPointer := &typedNestedConfig{
		Token:   "pointer-token",
		Visible: "pointer-visible",
	}
	input := typedCredentialConfig{
		Password:  "db-password",
		ClientKey: "client-key",
		Visible:   "visible",
		Ignored:   "not-json-visible",
		Nested: map[string]string{
			"sasl.jaas.config": "nested-jaas",
			"visible":          "nested-visible",
		},
		Message: typedMessageEnvelope{
			Key:   "order-42",
			Value: "visible-message",
			Headers: [1]typedMessageHeader{{
				Key:   "trace",
				Value: "trace-42",
			}},
		},
		Interface: typedStringMap{
			"secret":  "interface-secret",
			"visible": "interface-visible",
		},
		Items: []map[string]string{{
			"access_key": "slice-access",
			"visible":    "slice-visible",
		}},
		Array: [1]map[string]string{{
			"private-key": "array-private",
			"visible":     "array-visible",
		}},
		Pointer: &pointerMap,
		Named: typedStringMap{
			"keystorePassword": "named-keystore",
			"visible":          "named-visible",
		},
		Structs: []typedNestedConfig{{
			Token:   "slice-token",
			Visible: "slice-visible",
		}},
		StructPtr: structPointer,
		Wrapped: any(typedStringMap{
			"client.key": "wrapped-client-key",
			"visible":    "wrapped-visible",
		}),
	}

	got := redactCredentials(any(&input))

	require.Equal(t, "db-password", input.Password)
	require.Equal(t, "client-key", input.ClientKey)
	require.Equal(t, "nested-jaas", input.Nested["sasl.jaas.config"])
	require.Equal(t, "interface-secret", input.Interface.(typedStringMap)["secret"])
	require.Equal(t, "slice-access", input.Items[0]["access_key"])
	require.Equal(t, "array-private", input.Array[0]["private-key"])
	require.Equal(t, "pointer-trust", (*input.Pointer)["truststore.password"])
	require.Equal(t, "named-keystore", input.Named["keystorePassword"])
	require.Equal(t, "slice-token", input.Structs[0].Token)
	require.Equal(t, "pointer-token", input.StructPtr.Token)
	require.Equal(t, "wrapped-client-key", input.Wrapped.(typedStringMap)["client.key"])
	require.Equal(t, map[string]any{
		"db.password":         "[REDACTED]",
		"client-key-material": "[REDACTED]",
		"visible":             "visible",
		"nested": map[string]any{
			"sasl.jaas.config": "[REDACTED]",
			"visible":          "nested-visible",
		},
		"message": map[string]any{
			"key":   "order-42",
			"value": "visible-message",
			"headers": []any{map[string]any{
				"key":   "trace",
				"value": "trace-42",
			}},
		},
		"interface": map[string]any{
			"secret":  "[REDACTED]",
			"visible": "interface-visible",
		},
		"items": []any{map[string]any{
			"access_key": "[REDACTED]",
			"visible":    "slice-visible",
		}},
		"array": []any{map[string]any{
			"private-key": "[REDACTED]",
			"visible":     "array-visible",
		}},
		"pointer": map[string]any{
			"truststore.password": "[REDACTED]",
			"visible":             "pointer-visible",
		},
		"named": map[string]any{
			"keystorePassword": "[REDACTED]",
			"visible":          "named-visible",
		},
		"structs": []any{map[string]any{
			"AUTH_TOKEN": "[REDACTED]",
			"visible":    "slice-visible",
		}},
		"structPointer": map[string]any{
			"AUTH_TOKEN": "[REDACTED]",
			"visible":    "pointer-visible",
		},
		"wrapped": map[string]any{
			"client.key": "[REDACTED]",
			"visible":    "wrapped-visible",
		},
	}, got)
}

func TestRedactCredentialsFailsClosedOnCyclesAndUnsupportedValues(t *testing.T) {
	cyclic := &cyclicCredentialConfig{Password: "cycle-secret"}
	cyclic.Next = cyclic

	require.Nil(t, redactCredentials(cyclic))
	require.Equal(t, "cycle-secret", cyclic.Password)
	require.Same(t, cyclic, cyclic.Next)

	unsupported := struct {
		Password string   `json:"password"`
		Channel  chan int `json:"channel"`
	}{
		Password: "unsupported-secret",
		Channel:  make(chan int),
	}
	require.Nil(t, redactCredentials(unsupported))
	require.Equal(t, "unsupported-secret", unsupported.Password)
}

func TestResultLimitTruncatesSlicesAtMaxItems(t *testing.T) {
	executor := &Executor{}
	result, output, err := executor.safeResult(resultMeta("getItems", 3, 1024), []int{1, 2, 3, 4, 5})

	require.NoError(t, err)
	require.Nil(t, output)
	require.JSONEq(t, `{"result":[1,2,3]}`, callToolText(t, result))
	require.JSONEq(t, `{"result":[1,2,3]}`, string(result.StructuredContent.(json.RawMessage)))
}

func TestResultLimitTruncatesArraysAtMaxItems(t *testing.T) {
	executor := &Executor{}
	result, output, err := executor.safeResult(resultMeta("getItems", 2, 1024), [4]int{1, 2, 3, 4})

	require.NoError(t, err)
	require.Nil(t, output)
	require.JSONEq(t, `{"result":[1,2]}`, callToolText(t, result))
}

func TestResultLimitNormalizesScalarsButPreservesObjects(t *testing.T) {
	executor := &Executor{}
	meta := resultMeta("getValue", 10, 1024)

	scalar, output, err := executor.safeResult(meta, "ok")
	require.NoError(t, err)
	require.Nil(t, output)
	require.JSONEq(t, `{"result":"ok"}`, callToolText(t, scalar))

	object := map[string]any{"status": "ok"}
	structured, output, err := executor.safeResult(meta, object)
	require.NoError(t, err)
	require.Nil(t, output)
	require.JSONEq(t, `{"status":"ok"}`, callToolText(t, structured))
}

func TestResultLimitPreservesPointerObjectsWithoutScalarWrapper(t *testing.T) {
	executor := &Executor{}
	meta := resultMeta("getValue", 10, 1024)

	structured, output, err := executor.safeResult(meta, &pointerResult{Status: "ok"})
	require.NoError(t, err)
	require.Nil(t, output)
	require.JSONEq(t, `{"status":"ok"}`, callToolText(t, structured))

	object := map[string]any{"status": "ok"}
	structured, output, err = executor.safeResult(meta, &object)
	require.NoError(t, err)
	require.Nil(t, output)
	require.JSONEq(t, `{"status":"ok"}`, callToolText(t, structured))
}

func TestResultLimitTruncatesPointerSlicesAtMaxItems(t *testing.T) {
	executor := &Executor{}
	items := []int{1, 2, 3}

	result, output, err := executor.safeResult(resultMeta("getItems", 2, 1024), &items)

	require.NoError(t, err)
	require.Nil(t, output)
	require.JSONEq(t, `{"result":[1,2]}`, callToolText(t, result))
}

func TestResultLimitRejectsCyclicPointerShapes(t *testing.T) {
	cycleKind := os.Getenv(cyclicResultShapeTestMode)
	if cycleKind != "" {
		var output any
		switch cycleKind {
		case "self":
			output = &output
		case "multi":
			var first, second any
			first = &second
			second = &first
			output = first
		default:
			t.Fatalf("unknown cycle kind %q", cycleKind)
		}

		result, rawOutput, err := (&Executor{}).safeResult(resultMeta("getValue", 10, 1024), output)
		require.EqualError(t, err, "operation_failed")
		require.Nil(t, result)
		require.Nil(t, rawOutput)
		return
	}

	for _, cycleKind := range []string{"self", "multi"} {
		t.Run(cycleKind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			command := exec.CommandContext(
				ctx,
				os.Args[0],
				"-test.run=^TestResultLimitRejectsCyclicPointerShapes$",
				"-test.count=1",
			)
			command.Env = append(os.Environ(), cyclicResultShapeTestMode+"="+cycleKind)
			childOutput, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("cyclic result shaping hung until the child-process guard expired: %v\n%s", ctx.Err(), childOutput)
			}
			require.NoError(t, err, string(childOutput))
		})
	}
}

func TestResultLimitRejectsNonPositiveMaxItems(t *testing.T) {
	executor := &Executor{}
	for _, maxItems := range []int{0, -1} {
		_, _, err := executor.safeResult(resultMeta("getItems", maxItems, 1024), []int{1})
		require.EqualError(t, err, "operation_failed")
	}
}

func TestResultLimitRejectsSerializedOutputOverMaxBytes(t *testing.T) {
	executor := &Executor{}
	_, _, err := executor.safeResult(resultMeta("getValue", 10, 32), strings.Repeat("x", 64))

	require.EqualError(t, err, "result_too_large")
}

func TestResultLimitCapsCSVAtOneMiB(t *testing.T) {
	executor := &Executor{}
	meta := resultMeta("getBrokersCsv", 10, 2*maxCSVBytes)
	_, _, err := executor.safeResult(meta, strings.Repeat("x", maxCSVBytes))

	require.EqualError(t, err, "result_too_large")
}

func TestResultLimitRejectsValuesThatCannotBeSafelySerialized(t *testing.T) {
	executor := &Executor{}
	_, _, err := executor.safeResult(resultMeta("getValue", 10, 1024), make(chan int))

	require.EqualError(t, err, "operation_failed")
}

func TestResultLimitMeasuresTheExactStructuredObject(t *testing.T) {
	executor := &Executor{}
	raw, err := json.Marshal(map[string]any{"result": "ok"})
	require.NoError(t, err)

	_, _, err = executor.safeResult(resultMeta("getValue", 10, len(raw)), "ok")
	require.NoError(t, err)

	_, _, err = executor.safeResult(resultMeta("getValue", 10, len(raw)-1), "ok")
	require.EqualError(t, err, "result_too_large")
}

func TestResultLimitMarshalsAndReturnsTheSameBytesOnce(t *testing.T) {
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, false)))
	executor, err := NewExecutor(Dependencies{
		Policy:     store,
		IsReadOnly: func(string) bool { return false },
	})
	require.NoError(t, err)

	var marshalCount atomic.Int32
	meta := resultMeta("marshalOnce", 10, 1024)
	spec := newTool(
		meta,
		nil,
		func(context.Context, *Executor, noInput) (AccessClass, error) {
			return AccessReadOnly, nil
		},
		func(context.Context, *Executor, noInput) (any, error) {
			return countingJSONResult{marshalCount: &marshalCount}, nil
		},
	)
	session := newSDKSession(t, spec, executor)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      meta.Name,
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	require.False(t, result.IsError)
	require.Equal(t, int32(1), marshalCount.Load())
	require.JSONEq(t, `{"status":"ok"}`, callToolText(t, result))
	require.Equal(t, map[string]any{"status": "ok"}, result.StructuredContent)
}

func resultMeta(name string, maxItems, maxBytes int) ToolMeta {
	return ToolMeta{
		Name:     name,
		Timeout:  time.Second,
		MaxItems: maxItems,
		MaxBytes: maxBytes,
	}
}
