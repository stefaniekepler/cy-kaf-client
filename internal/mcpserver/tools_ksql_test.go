package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	infraksql "github.com/cy-kaf/cy-kaf-client/internal/infra/ksql"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

type ksqlCall struct {
	name       string
	cluster    string
	sql        string
	properties map[string]string
	pipeID     string
}

type recordingKSQLApp struct {
	mu sync.Mutex

	registerKind domaincluster.KsqlStatementKind
	pipeKinds    map[string]domaincluster.KsqlStatementKind
	streams      []domaincluster.KsqlStreamDescription
	tables       []domaincluster.KsqlTableDescription
	frames       []domaincluster.KsqlTable
	errs         map[string]error
	calls        []ksqlCall
}

func newRecordingKSQLApp() *recordingKSQLApp {
	streamA, streamB := "stream-a", "stream-b"
	topicA, topicB := "topic-a", "topic-b"
	keyJSON, keyString := "JSON", "KAFKA"
	valueJSON := "JSON"
	tableA, tableB := "table-a", "table-b"
	windowed := true
	return &recordingKSQLApp{
		registerKind: domaincluster.KsqlQuery,
		pipeKinds:    make(map[string]domaincluster.KsqlStatementKind),
		streams: []domaincluster.KsqlStreamDescription{
			{Name: &streamB, Topic: &topicB, KeyFormat: &keyString, ValueFormat: &valueJSON},
			{Name: &streamA, Topic: &topicA, KeyFormat: &keyJSON, ValueFormat: &valueJSON},
		},
		tables: []domaincluster.KsqlTableDescription{
			{Name: &tableB, Topic: &topicB, KeyFormat: &keyString, ValueFormat: &valueJSON},
			{Name: &tableA, Topic: &topicA, KeyFormat: &keyJSON, ValueFormat: &valueJSON, IsWindowed: &windowed},
		},
		frames: []domaincluster.KsqlTable{
			{Header: "Query Result", ColumnNames: []string{"ID", "NAME"}},
			{Values: [][]any{{int64(7), "alice"}}},
		},
		errs: make(map[string]error),
	}
}

func (f *recordingKSQLApp) Register(
	_ context.Context,
	cluster,
	sql string,
	properties map[string]string,
) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ksqlCall{
		name: "register", cluster: cluster, sql: sql,
		properties: cloneKSQLProperties(properties),
	})
	if err := f.errs["register"]; err != nil {
		return "", err
	}
	const pipeID = "pipe-123"
	f.pipeKinds[pipeID] = f.registerKind
	return pipeID, nil
}

func (f *recordingKSQLApp) Open(
	ctx context.Context,
	cluster,
	pipeID string,
	emit func(domaincluster.KsqlTable) error,
) error {
	return f.OpenAuthorized(ctx, cluster, pipeID, nil, emit)
}

func (f *recordingKSQLApp) OpenAuthorized(
	_ context.Context,
	cluster,
	pipeID string,
	authorize appcluster.KsqlAuthorize,
	emit func(domaincluster.KsqlTable) error,
) error {
	f.mu.Lock()
	f.calls = append(f.calls, ksqlCall{name: "open", cluster: cluster, pipeID: pipeID})
	kind, exists := f.pipeKinds[pipeID]
	if exists {
		delete(f.pipeKinds, pipeID)
	}
	frames := cloneKSQLFrames(f.frames)
	openErr := f.errs["open"]
	f.mu.Unlock()

	if !exists {
		return appcluster.ErrKsqlPipeNotFound
	}
	if authorize != nil {
		if err := authorize(kind); err != nil {
			return err
		}
	}
	for _, frame := range frames {
		if err := emit(frame); err != nil {
			if openErr != nil {
				return errors.Join(err, openErr)
			}
			return err
		}
	}
	return openErr
}

func (f *recordingKSQLApp) ListStreams(
	_ context.Context,
	cluster string,
) ([]domaincluster.KsqlStreamDescription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ksqlCall{name: "listStreams", cluster: cluster})
	return append([]domaincluster.KsqlStreamDescription(nil), f.streams...), f.errs["listStreams"]
}

func (f *recordingKSQLApp) ListTables(
	_ context.Context,
	cluster string,
) ([]domaincluster.KsqlTableDescription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ksqlCall{name: "listTables", cluster: cluster})
	return append([]domaincluster.KsqlTableDescription(nil), f.tables...), f.errs["listTables"]
}

func (f *recordingKSQLApp) snapshot() []ksqlCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]ksqlCall(nil), f.calls...)
	for index := range out {
		out[index].properties = cloneKSQLProperties(out[index].properties)
	}
	return out
}

func cloneKSQLProperties(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneKSQLFrames(input []domaincluster.KsqlTable) []domaincluster.KsqlTable {
	output := make([]domaincluster.KsqlTable, len(input))
	for index, frame := range input {
		output[index] = frame
		output[index].ColumnNames = append([]string(nil), frame.ColumnNames...)
		output[index].Values = append([][]any(nil), frame.Values...)
	}
	return output
}

func TestKSQLToolsDelegateAllFourOperationsWithExactSDKResults(t *testing.T) {
	tests := []struct {
		name      string
		input     map[string]any
		wantJSON  string
		wantCalls []ksqlCall
	}{
		{
			name: "executeKsql",
			input: map[string]any{
				"clusterName": "prod",
				"body": map[string]any{
					"ksql": "SELECT * FROM orders;",
					"streamsProperties": map[string]any{
						"auto.offset.reset": "earliest",
					},
				},
			},
			wantJSON: `{"pipeId":"pipe-123"}`,
			wantCalls: []ksqlCall{{
				name: "register", cluster: "prod", sql: "SELECT * FROM orders;",
				properties: map[string]string{"auto.offset.reset": "earliest"},
			}},
		},
		{
			name: "openKsqlResponsePipe",
			input: map[string]any{
				"clusterName": "prod",
				"query":       map[string]any{"pipeId": "pipe-ready"},
			},
			wantJSON: `{"result":[
				{"table":{"columnNames":["ID","NAME"],"header":"Query Result","values":[]}},
				{"table":{"columnNames":[],"header":"","values":[[7,"alice"]]}}
			]}`,
			wantCalls: []ksqlCall{{name: "open", cluster: "prod", pipeID: "pipe-ready"}},
		},
		{
			name:      "listStreams",
			input:     map[string]any{"clusterName": "prod"},
			wantJSON:  `{"result":[{"keyFormat":"JSON","name":"stream-a","topic":"topic-a","valueFormat":"JSON"},{"keyFormat":"KAFKA","name":"stream-b","topic":"topic-b","valueFormat":"JSON"}]}`,
			wantCalls: []ksqlCall{{name: "listStreams", cluster: "prod"}},
		},
		{
			name:      "listTables",
			input:     map[string]any{"clusterName": "prod"},
			wantJSON:  `{"result":[{"isWindowed":true,"keyFormat":"JSON","name":"table-a","topic":"topic-a","valueFormat":"JSON"},{"keyFormat":"KAFKA","name":"table-b","topic":"topic-b","valueFormat":"JSON"}]}`,
			wantCalls: []ksqlCall{{name: "listTables", cluster: "prod"}},
		},
	}
	require.Len(t, tests, 4)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingKSQLApp()
			if test.name == "openKsqlResponsePipe" {
				app.pipeKinds["pipe-ready"] = domaincluster.KsqlQuery
			}
			executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, true)
			result := callKSQLTool(t, executor, test.name, test.input)
			require.False(t, result.IsError, callToolText(t, result))
			require.JSONEq(t, test.wantJSON, callToolText(t, result))
			requireStructuredJSONEq(t, test.wantJSON, result.StructuredContent)
			require.Equal(t, test.wantCalls, app.snapshot())
		})
	}
}

func TestKSQLToolsConditionalPolicyMatrixAndFreshOpenAuthorization(t *testing.T) {
	t.Run("read-only policy allows SELECT execute and open", func(t *testing.T) {
		app := newRecordingKSQLApp()
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
		execute := callKSQLTool(t, executor, "executeKsql", ksqlExecuteInput("SELECT * FROM orders EMIT CHANGES;", nil))
		require.False(t, execute.IsError, callToolText(t, execute))
		open := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-123"))
		require.False(t, open.IsError, callToolText(t, open))
	})

	t.Run("read-only policy denies CREATE before register", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.registerKind = domaincluster.KsqlStatement
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
		result := callKSQLTool(t, executor, "executeKsql", ksqlExecuteInput("CREATE STREAM orders (id BIGINT) WITH (kafka_topic='orders', value_format='JSON');", nil))
		require.True(t, result.IsError)
		require.Equal(t, "writes_disabled", callToolText(t, result))
		require.Empty(t, app.snapshot())
	})

	t.Run("write policy and writable cluster allow CREATE execute and open", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.registerKind = domaincluster.KsqlStatement
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, true)
		execute := callKSQLTool(t, executor, "executeKsql", ksqlExecuteInput("CREATE STREAM orders (id BIGINT) WITH (kafka_topic='orders', value_format='JSON');", nil))
		require.False(t, execute.IsError, callToolText(t, execute))
		open := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-123"))
		require.False(t, open.IsError, callToolText(t, open))
	})

	t.Run("cluster read-only denies CREATE before register", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.registerKind = domaincluster.KsqlStatement
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, true, true)
		result := callKSQLTool(t, executor, "executeKsql", ksqlExecuteInput("CREATE STREAM orders (id BIGINT) WITH (kafka_topic='orders', value_format='JSON');", nil))
		require.True(t, result.IsError)
		require.Equal(t, "cluster_read_only", callToolText(t, result))
		require.Empty(t, app.snapshot())
	})

	t.Run("pipe registered as CREATE cannot execute after writes disabled", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.registerKind = domaincluster.KsqlStatement
		executor, store := newKSQLExecutor(t, app, infraksql.Classifier{}, false, true)
		execute := callKSQLTool(t, executor, "executeKsql", ksqlExecuteInput("CREATE STREAM orders (id BIGINT) WITH (kafka_topic='orders', value_format='JSON');", nil))
		require.False(t, execute.IsError, callToolText(t, execute))

		require.NoError(t, store.Save(context.Background(), *policy(true, false)))
		open := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-123"))
		require.True(t, open.IsError)
		require.Equal(t, "writes_disabled", callToolText(t, open))

		retry := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-123"))
		require.True(t, retry.IsError)
		require.Equal(t, "not_found", callToolText(t, retry))
	})

	t.Run("pipe registered as CREATE cannot execute after cluster becomes read-only", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.registerKind = domaincluster.KsqlStatement
		var readOnly atomic.Bool
		executor, _ := newKSQLExecutorWithReadOnly(
			t,
			app,
			infraksql.Classifier{},
			func(string) bool { return readOnly.Load() },
			true,
		)
		execute := callKSQLTool(t, executor, "executeKsql", ksqlExecuteInput("CREATE STREAM orders (id BIGINT) WITH (kafka_topic='orders', value_format='JSON');", nil))
		require.False(t, execute.IsError, callToolText(t, execute))

		readOnly.Store(true)
		open := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-123"))
		require.True(t, open.IsError)
		require.Equal(t, "cluster_read_only", callToolText(t, open))

		retry := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-123"))
		require.True(t, retry.IsError)
		require.Equal(t, "not_found", callToolText(t, retry))
	})

	t.Run("query pipe remains allowed after writes disabled and on read-only cluster", func(t *testing.T) {
		app := newRecordingKSQLApp()
		executor, store := newKSQLExecutor(t, app, infraksql.Classifier{}, true, true)
		execute := callKSQLTool(t, executor, "executeKsql", ksqlExecuteInput("SELECT * FROM orders EMIT CHANGES;", nil))
		require.False(t, execute.IsError, callToolText(t, execute))
		require.NoError(t, store.Save(context.Background(), *policy(true, false)))
		open := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-123"))
		require.False(t, open.IsError, callToolText(t, open))
	})
}

func TestKSQLToolsRejectInvalidSQLWithoutEchoOrDelegation(t *testing.T) {
	tests := []string{
		"SELECT secret_one; SELECT secret_two;",
		"PRINT 'secret_topic';",
		"THIS IS secret_invalid;",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			app := newRecordingKSQLApp()
			executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, true)
			result := callKSQLTool(t, executor, "executeKsql", ksqlExecuteInput(sql, nil))
			require.True(t, result.IsError)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.NotContains(t, callToolText(t, result), "secret")
			require.Empty(t, app.snapshot())
		})
	}
}

func TestKSQLToolsBoundInputsBeforeDelegation(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
	}{
		{name: "oversized SQL", input: ksqlExecuteInput(strings.Repeat("x", 64<<10+1), nil)},
		{name: "unsafe network property", input: ksqlExecuteInput("SELECT * FROM orders EMIT CHANGES;", map[string]any{"bootstrap.servers": "attacker.example.test:9092"})},
		{name: "unsafe security property", input: ksqlExecuteInput("SELECT * FROM orders EMIT CHANGES;", map[string]any{"sasl.jaas.config": "credential-marker"})},
		{name: "unsafe file property", input: ksqlExecuteInput("SELECT * FROM orders EMIT CHANGES;", map[string]any{"ssl.truststore.location": "/tmp/attacker"})},
		{name: "unsupported property", input: ksqlExecuteInput("SELECT * FROM orders EMIT CHANGES;", map[string]any{"arbitrary.property": "value"})},
		{name: "oversized property value", input: ksqlExecuteInput("SELECT * FROM orders EMIT CHANGES;", map[string]any{"auto.offset.reset": strings.Repeat("x", 4097)})},
		{name: "invalid offset value", input: ksqlExecuteInput("SELECT * FROM orders EMIT CHANGES;", map[string]any{"auto.offset.reset": "attacker"})},
		{name: "empty pipe", input: ksqlOpenInput("")},
		{name: "oversized pipe", input: ksqlOpenInput(strings.Repeat("x", 1025))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingKSQLApp()
			executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, true)
			name := "executeKsql"
			if strings.Contains(test.name, "pipe") {
				name = "openKsqlResponsePipe"
			}
			result := callKSQLTool(t, executor, name, test.input)
			require.True(t, result.IsError)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Empty(t, app.snapshot())
		})
	}
}

func TestKSQLToolsBoundRawListsBeforeCopyAndSort(t *testing.T) {
	t.Run("raw streams over bound fail", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.streams = make([]domaincluster.KsqlStreamDescription, 2001)
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
		result := callKSQLTool(t, executor, "listStreams", map[string]any{"clusterName": "prod"})
		require.True(t, result.IsError)
		require.Equal(t, "result_too_large", callToolText(t, result))
	})

	t.Run("raw tables over bound fail", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.tables = make([]domaincluster.KsqlTableDescription, 2001)
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
		result := callKSQLTool(t, executor, "listTables", map[string]any{"clusterName": "prod"})
		require.True(t, result.IsError)
		require.Equal(t, "result_too_large", callToolText(t, result))
	})

	t.Run("lists truncate deterministically at tool item bound", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.streams = make([]domaincluster.KsqlStreamDescription, 501)
		for index := range app.streams {
			name := fmt.Sprintf("stream-%03d", 500-index)
			app.streams[index].Name = &name
		}
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
		result := callKSQLTool(t, executor, "listStreams", map[string]any{"clusterName": "prod"})
		require.False(t, result.IsError, callToolText(t, result))
		var envelope struct {
			Result []generated.KsqlStreamDescription `json:"result"`
		}
		require.NoError(t, remarshalKSQLResult(result, &envelope))
		require.Len(t, envelope.Result, maxListItems)
		require.Equal(t, "stream-000", *envelope.Result[0].Name)
		require.Equal(t, "stream-499", *envelope.Result[499].Name)
	})
}

func TestKSQLToolsBoundOpenRowsAndBytesWithoutSuppressingPortErrors(t *testing.T) {
	t.Run("501st row fails closed", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.pipeKinds["pipe-ready"] = domaincluster.KsqlQuery
		app.frames = []domaincluster.KsqlTable{{Values: make([][]any, maxKsqlRows+1)}}
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
		result := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-ready"))
		require.True(t, result.IsError)
		require.Equal(t, "result_too_large", callToolText(t, result))
	})

	t.Run("exactly 500 rows are returned", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.pipeKinds["pipe-ready"] = domaincluster.KsqlQuery
		app.frames = []domaincluster.KsqlTable{{Values: make([][]any, maxKsqlRows)}}
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
		result := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-ready"))
		require.False(t, result.IsError, callToolText(t, result))
		var envelope struct {
			Result []generated.KsqlResponse `json:"result"`
		}
		require.NoError(t, remarshalKSQLResult(result, &envelope))
		require.Len(t, envelope.Result, 1)
		require.Len(t, *envelope.Result[0].Table.Values, maxKsqlRows)
	})

	t.Run("one MiB result fails closed without returning rows", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.pipeKinds["pipe-ready"] = domaincluster.KsqlQuery
		app.frames = []domaincluster.KsqlTable{{
			ColumnNames: []string{"value"},
			Values:      [][]any{{strings.Repeat("x", maxResultBytes)}},
		}}
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
		result := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-ready"))
		require.True(t, result.IsError)
		require.Equal(t, "result_too_large", callToolText(t, result))
		require.NotContains(t, callToolText(t, result), "xxxx")
	})

	t.Run("real port error joined with local limit is not suppressed", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.pipeKinds["pipe-ready"] = domaincluster.KsqlQuery
		app.frames = []domaincluster.KsqlTable{{Values: make([][]any, maxKsqlRows+1)}}
		app.errs["open"] = errors.New("credential-marker")
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
		result := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-ready"))
		require.True(t, result.IsError)
		require.Equal(t, "operation_failed", callToolText(t, result))
		require.NotContains(t, callToolText(t, result), "credential-marker")
	})

	t.Run("KSQL error frames are never returned", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.pipeKinds["pipe-ready"] = domaincluster.KsqlQuery
		app.frames = []domaincluster.KsqlTable{{
			Header:  "Execution error",
			Values:  [][]any{{"credential-marker", "SELECT secret"}},
			IsError: true,
		}}
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
		result := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-ready"))
		require.True(t, result.IsError)
		require.Equal(t, "operation_failed", callToolText(t, result))
		require.NotContains(t, callToolText(t, result), "credential-marker")
		require.NotContains(t, callToolText(t, result), "SELECT")
	})

	t.Run("credential columns and nested credential fields are redacted", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.pipeKinds["pipe-ready"] = domaincluster.KsqlQuery
		app.frames = []domaincluster.KsqlTable{{
			ColumnNames: []string{"ID", "api_token", "DETAILS"},
			Values: [][]any{{
				"order-1",
				"credential-marker",
				map[string]any{"password": "nested-marker", "visible": "kept"},
			}},
		}}
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
		result := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-ready"))
		require.False(t, result.IsError, callToolText(t, result))
		require.JSONEq(t, `{"result":[{"table":{
			"columnNames":["ID","api_token","DETAILS"],
			"header":"",
			"values":[["order-1","[REDACTED]",{"password":"[REDACTED]","visible":"kept"}]]
		}}]}`, callToolText(t, result))
		require.NotContains(t, callToolText(t, result), "credential-marker")
		require.NotContains(t, callToolText(t, result), "nested-marker")
	})

	t.Run("unknown stored kind fails closed and consumes pipe", func(t *testing.T) {
		app := newRecordingKSQLApp()
		app.pipeKinds["pipe-ready"] = domaincluster.KsqlStatementKind("future")
		executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, true)
		result := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-ready"))
		require.True(t, result.IsError)
		require.Equal(t, "invalid_request", callToolText(t, result))
		retry := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-ready"))
		require.True(t, retry.IsError)
		require.Equal(t, "not_found", callToolText(t, retry))
	})
}

func TestKSQLToolsInheritAcceptedSchemaForCredentialRedactionAcrossFrames(t *testing.T) {
	app := newRecordingKSQLApp()
	app.pipeKinds["pipe-ready"] = domaincluster.KsqlQuery
	app.frames = []domaincluster.KsqlTable{
		{
			Header:      "Schema",
			ColumnNames: []string{"id", "api_token", "details"},
		},
		{
			Header: "Row",
			Values: [][]any{{
				"order-1",
				"credential-marker",
				map[string]any{"password": "nested-marker", "visible": "kept"},
			}},
		},
	}
	executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
	result := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-ready"))
	require.False(t, result.IsError, callToolText(t, result))

	want := `{"result":[
		{"table":{"columnNames":["id","api_token","details"],"header":"Schema","values":[]}},
		{"table":{"columnNames":[],"header":"Row","values":[
			["order-1","[REDACTED]",{"password":"[REDACTED]","visible":"kept"}]
		]}}
	]}`
	require.JSONEq(t, want, callToolText(t, result))
	requireStructuredJSONEq(t, want, result.StructuredContent)
	require.NotContains(t, callToolText(t, result), "credential-marker")
	require.NotContains(t, callToolText(t, result), "nested-marker")
	structured, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	require.NotContains(t, string(structured), "credential-marker")
	require.NotContains(t, string(structured), "nested-marker")
	require.Contains(t, callToolText(t, result), "order-1")
	require.Contains(t, callToolText(t, result), "kept")
}

func TestKSQLToolsReplaceAcceptedSchemaForLaterRowsWithoutChangingFrameShape(t *testing.T) {
	app := newRecordingKSQLApp()
	app.pipeKinds["pipe-ready"] = domaincluster.KsqlQuery
	app.frames = []domaincluster.KsqlTable{
		{Header: "Schema", ColumnNames: []string{"id", "api_token"}},
		{Header: "Row", Values: [][]any{{"order-1", "first-credential-marker"}}},
		{Header: "Schema", ColumnNames: []string{"id", "description"}},
		{Header: "Row", Values: [][]any{{"order-2", "visible-after-replacement"}}},
	}
	executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
	result := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-ready"))
	require.False(t, result.IsError, callToolText(t, result))
	require.NotContains(t, callToolText(t, result), "first-credential-marker")
	require.Contains(t, callToolText(t, result), "[REDACTED]")
	require.Contains(t, callToolText(t, result), "visible-after-replacement")

	var envelope struct {
		Result []generated.KsqlResponse `json:"result"`
	}
	require.NoError(t, remarshalKSQLResult(result, &envelope))
	require.Len(t, envelope.Result, 4)
	require.Equal(t, []string{"id", "api_token"}, *envelope.Result[0].Table.ColumnNames)
	require.Empty(t, *envelope.Result[1].Table.ColumnNames)
	require.Equal(t, []string{"id", "description"}, *envelope.Result[2].Table.ColumnNames)
	require.Empty(t, *envelope.Result[3].Table.ColumnNames)
	require.Equal(t, "[REDACTED]", (*envelope.Result[1].Table.Values)[0][1])
	require.Equal(t, "visible-after-replacement", (*envelope.Result[3].Table.Values)[0][1])
}

func TestKSQLCollectorRejectedSchemaDoesNotReplaceLastAcceptedSchema(t *testing.T) {
	collector := ksqlResultCollector{
		frames:   make([]generated.KsqlResponse, 0, maxMCPKsqlFrames),
		maxRows:  maxKsqlRows,
		maxBytes: 256,
		budget: ksqlValueBudget{
			bytes:    ksqlResultEnvelopeReserve,
			visiting: make(map[ksqlValueVisit]struct{}),
		},
	}
	require.NoError(t, collector.collect(domaincluster.KsqlTable{
		Header:      "Schema",
		ColumnNames: []string{"id", "api_token"},
	}))

	err := collector.collect(domaincluster.KsqlTable{
		Header:      "Schema",
		ColumnNames: []string{"id", strings.Repeat("description", 64)},
	})
	require.ErrorIs(t, err, errKsqlCollectionLimit)

	// Continue only to observe which accepted schema is active. A real port
	// stops on the rejected frame, so widening this local test budget does not
	// model a supported retry path.
	collector.maxBytes = maxResultBytes
	require.NoError(t, collector.collect(domaincluster.KsqlTable{
		Header: "Row",
		Values: [][]any{{"order-1", "credential-marker"}},
	}))
	require.Equal(t, "[REDACTED]", (*collector.frames[1].Table.Values)[0][1])
	require.Empty(t, *collector.frames[1].Table.ColumnNames)
}

func TestKSQLToolsRejectMalformedSchemaRowAlignmentWithoutLeakingValues(t *testing.T) {
	app := newRecordingKSQLApp()
	app.pipeKinds["pipe-ready"] = domaincluster.KsqlQuery
	app.frames = []domaincluster.KsqlTable{
		{Header: "Schema", ColumnNames: []string{"id", "api_token"}},
		{Header: "Row", Values: [][]any{{"credential-marker"}}},
	}
	executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
	result := callKSQLTool(t, executor, "openKsqlResponsePipe", ksqlOpenInput("pipe-ready"))
	require.True(t, result.IsError)
	require.Equal(t, "operation_failed", callToolText(t, result))
	require.NotContains(t, callToolText(t, result), "credential-marker")
}

func TestKSQLToolsMapApplicationErrorsWithoutLeakingSQLOrCredentials(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		err       error
		want      string
		input     map[string]any
	}{
		{name: "cluster", operation: "listStreams", err: appcluster.ErrKsqlClusterNotFound, want: "cluster_not_found", input: map[string]any{"clusterName": "prod"}},
		{name: "not configured", operation: "listTables", err: appcluster.ErrKsqlNotConfigured, want: "feature_not_configured", input: map[string]any{"clusterName": "prod"}},
		{name: "pipe", operation: "open", err: appcluster.ErrKsqlPipeNotFound, want: "not_found", input: ksqlOpenInput("pipe-ready")},
		{name: "port", operation: "listStreams", err: errors.New("password=credential-marker SELECT secret"), want: "operation_failed", input: map[string]any{"clusterName": "prod"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingKSQLApp()
			app.pipeKinds["pipe-ready"] = domaincluster.KsqlQuery
			app.errs[test.operation] = test.err
			executor, _ := newKSQLExecutor(t, app, infraksql.Classifier{}, false, false)
			tool := test.operation
			if tool == "open" {
				tool = "openKsqlResponsePipe"
			}
			result := callKSQLTool(t, executor, tool, test.input)
			require.True(t, result.IsError)
			require.Equal(t, test.want, callToolText(t, result))
			require.NotContains(t, callToolText(t, result), "credential-marker")
			require.NotContains(t, callToolText(t, result), "SELECT")
		})
	}
}

func TestKSQLToolsExposeExactStaticAccessClasses(t *testing.T) {
	require.Equal(t, AccessConditionalKSQL, requireCatalogSpec(t, "executeKsql").Meta.Access)
	for _, name := range []string{"openKsqlResponsePipe", "listStreams", "listTables"} {
		require.Equal(t, AccessReadOnly, requireCatalogSpec(t, name).Meta.Access)
	}
}

func newKSQLExecutor(
	t *testing.T,
	app KSQLServicer,
	classifier appcluster.KsqlClassifier,
	readOnly,
	allowWrites bool,
) (*Executor, *mcppolicy.Store) {
	t.Helper()
	return newKSQLExecutorWithReadOnly(
		t,
		app,
		classifier,
		func(string) bool { return readOnly },
		allowWrites,
	)
}

func newKSQLExecutorWithReadOnly(
	t *testing.T,
	app KSQLServicer,
	classifier appcluster.KsqlClassifier,
	isReadOnly func(string) bool,
	allowWrites bool,
) (*Executor, *mcppolicy.Store) {
	t.Helper()
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, allowWrites)))
	executor, err := NewExecutor(Dependencies{
		KSQL: app, KSQLClassifier: classifier, Policy: store,
		IsReadOnly: isReadOnly,
	})
	require.NoError(t, err)
	return executor, store
}

func callKSQLTool(
	t *testing.T,
	executor *Executor,
	name string,
	input map[string]any,
) *mcp.CallToolResult {
	t.Helper()
	result, err := newSDKSession(t, requireCatalogSpec(t, name), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{Name: name, Arguments: input},
	)
	require.NoError(t, err)
	return result
}

func ksqlExecuteInput(sql string, properties map[string]any) map[string]any {
	body := map[string]any{"ksql": sql}
	if properties != nil {
		body["streamsProperties"] = properties
	}
	return map[string]any{"clusterName": "prod", "body": body}
}

func ksqlOpenInput(pipeID string) map[string]any {
	return map[string]any{
		"clusterName": "prod",
		"query":       map[string]any{"pipeId": pipeID},
	}
}

func remarshalKSQLResult(result *mcp.CallToolResult, target any) error {
	raw, err := json.Marshal(result.StructuredContent)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}
