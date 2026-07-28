package ksql

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func TestDecodeQueryResponseFramesAndSchemaColumns(t *testing.T) {
	wire := strings.NewReader(`{"heartbeat":true}
{"header":{"schema":"ID INT, PAYLOAD STRUCT<LEFT STRING, RIGHT ARRAY<STRING>>, ` + "`quoted,name`" + ` STRING"}}
{"row":{"columns":[1,{"left":"a","right":["b"]},"x"]}}
{"row":{"columns":[2,{"left":"c","right":["d"]},"y"]}}
{"finalMessage":""}
`)
	var got []cluster.KsqlTable
	err := decodeQueryResponse(wire, func(v cluster.KsqlTable) error {
		got = append(got, v)
		return nil
	})
	if err != nil {
		t.Fatalf("decodeQueryResponse() error = %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("frames = %#v", got)
	}
	if got[0].Header != "Schema" || !reflect.DeepEqual(got[0].ColumnNames, []string{"ID", "PAYLOAD", "`quoted,name`"}) {
		t.Fatalf("schema frame = %#v", got[0])
	}
	if got[1].Header != "Row" || got[2].Header != "Row" || got[3].Header != "Query Result" {
		t.Fatalf("headers = %#v", got)
	}
	if got[3].Values[0][0] != "Success" {
		t.Fatalf("empty final message = %#v", got[3])
	}
}

func TestDecodeQueryResponseAcceptsOuterArrayAndTruncatedAfterFrame(t *testing.T) {
	var got []cluster.KsqlTable
	err := decodeQueryResponse(strings.NewReader(`[{"header":{"schema":"ID STRING"}},{"row":{"columns":["x"]}}`), func(v cluster.KsqlTable) error {
		got = append(got, v)
		return nil
	})
	if err != nil || len(got) != 2 {
		t.Fatalf("outer array = %#v, %v", got, err)
	}
	got = nil
	err = decodeQueryResponse(strings.NewReader(`{"row":{"columns":[1]}`), func(v cluster.KsqlTable) error {
		got = append(got, v)
		return nil
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("truncated after frame = %#v, %v", got, err)
	}
	got = nil
	err = decodeQueryResponse(strings.NewReader(`[{"row":{"columns":[1]}}`), func(v cluster.KsqlTable) error {
		got = append(got, v)
		return nil
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("truncated outer array after frame = %#v, %v", got, err)
	}
	got = nil
	if err = decodeQueryResponse(strings.NewReader(`{"row":{"columns":[1]}`), func(v cluster.KsqlTable) error {
		got = append(got, v)
		return nil
	}); err != nil || len(got) != 1 || got[0].Header != "Row" {
		t.Fatalf("partial object recovery = %#v, %v", got, err)
	}
	if err = decodeQueryResponse(strings.NewReader(`{"row":`), func(cluster.KsqlTable) error { return nil }); err == nil {
		t.Fatal("empty/truncated response returned nil")
	}
}

func TestDecodeQueryResponseBoundsEachFrameNotTheWholeStream(t *testing.T) {
	const frameLimit = 48
	wire := `[{"row":{"columns":[1]}},{"row":{"columns":[2]}},{"row":{"columns":[3]}}]`
	if len(wire) <= frameLimit {
		t.Fatalf("test stream must exceed one frame limit: len=%d", len(wire))
	}
	var got []cluster.KsqlTable
	err := decodeQueryResponseWithLimit(strings.NewReader(wire), func(v cluster.KsqlTable) error {
		got = append(got, v)
		return nil
	}, frameLimit)
	if err != nil || len(got) != 3 {
		t.Fatalf("many small frames = %#v, %v", got, err)
	}
}

func TestQueryFramerAcceptsExactFrameLimit(t *testing.T) {
	const limit = 32
	prefix, suffix := `{"row":{"columns":["`, `"]}}`
	wire := prefix + strings.Repeat("x", limit-len(prefix)-len(suffix)) + suffix
	framer := newQueryFramer(strings.NewReader(wire), limit)
	raw, done, err := framer.next()
	if err != nil || done || len(raw) != limit {
		t.Fatalf("exact frame = len:%d done:%v err:%v, want len:%d", len(raw), done, err, limit)
	}
	if _, done, err := framer.next(); !done || err != nil {
		t.Fatalf("exact frame trailing state = done:%v err:%v", done, err)
	}
}

func TestQueryFramerReadsTopLevelStringWithEscapes(t *testing.T) {
	framer := newQueryFramer(strings.NewReader(`"left\\\"right"`), 64)

	raw, done, err := framer.next()
	if err != nil || done || string(raw) != `"left\\\"right"` {
		t.Fatalf("top-level string = %q, done:%v err:%v", raw, done, err)
	}
	if _, done, err := framer.next(); !done || err != nil {
		t.Fatalf("top-level string trailing state = done:%v err:%v", done, err)
	}
}

func TestQueryFramerNonPositiveLimitReturnsBoundedError(t *testing.T) {
	for _, limit := range []int{0, -1} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			framer := newQueryFramer(strings.NewReader(`{"row":{"columns":[1]}}`), limit)
			_, _, err := framer.next()
			if !errors.Is(err, errKsqlFrameLimit) {
				t.Fatalf("limit %d error = %v, want %v", limit, err, errKsqlFrameLimit)
			}
		})
	}
}

func TestIsDecimalNumber(t *testing.T) {
	tests := map[string]bool{
		"":      false,
		"+":     false,
		"-":     false,
		"0":     true,
		"+42":   true,
		"-42":   true,
		"4.2":   false,
		"12abc": false,
	}
	for value, want := range tests {
		if got := isDecimalNumber(value); got != want {
			t.Errorf("isDecimalNumber(%q) = %v, want %v", value, got, want)
		}
	}
}

func TestDecodeQueryResponseRejectsNestedOrStringPartialEvents(t *testing.T) {
	tests := []struct {
		name string
		wire string
	}{
		{
			name: "nested event",
			wire: `{"junk":{"row":{"columns":[1]}`,
		},
		{
			name: "malformed outer array",
			wire: `[,{"row":{"columns":[1]}}]`,
		},
		{
			name: "event-looking string",
			wire: `{"junk":"row":{"columns":[1]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []cluster.KsqlTable
			err := decodeQueryResponse(strings.NewReader(tt.wire), func(v cluster.KsqlTable) error {
				got = append(got, v)
				return nil
			})
			if err == nil {
				t.Fatalf("decodeQueryResponse(%q) returned nil, frames=%#v", tt.wire, got)
			}
			if len(got) != 0 {
				t.Fatalf("decodeQueryResponse(%q) emitted false frames: %#v", tt.wire, got)
			}
		})
	}
}

func TestDecodeQueryResponseRejectsNDJSONGarbageAfterFrame(t *testing.T) {
	for _, tail := range []string{"]", "garbage", `{"row":`} {
		t.Run(strings.ReplaceAll(tail, "{", "open-"), func(t *testing.T) {
			wire := `{"row":{"columns":[1]}}` + "\n" + tail
			var got []cluster.KsqlTable
			err := decodeQueryResponse(strings.NewReader(wire), func(v cluster.KsqlTable) error {
				got = append(got, v)
				return nil
			})
			if err == nil {
				t.Fatalf("NDJSON tail %q returned nil, frames=%#v", tail, got)
			}
		})
	}
}

func TestDecodeQueryResponseRejectsOuterArrayTrailingGarbage(t *testing.T) {
	for _, tail := range []string{"garbage", `{"row":{"columns":[2]}}`} {
		t.Run(strings.ReplaceAll(tail, "{", "open-"), func(t *testing.T) {
			wire := `[{"row":{"columns":[1]}}]` + tail
			var got []cluster.KsqlTable
			err := decodeQueryResponse(strings.NewReader(wire), func(v cluster.KsqlTable) error {
				got = append(got, v)
				return nil
			})
			if err == nil {
				t.Fatalf("outer-array tail %q returned nil, frames=%#v", tail, got)
			}
		})
	}
}

func TestDecodeQueryResponseErrorAndEmitterFailure(t *testing.T) {
	var got []cluster.KsqlTable
	err := decodeQueryResponse(strings.NewReader(`{"errorMessage":"bad SQL"}`), func(v cluster.KsqlTable) error {
		got = append(got, v)
		return nil
	})
	if err != nil || len(got) != 1 || !got[0].IsError || got[0].Header != "Execution error" {
		t.Fatalf("error frame = %#v, %v", got, err)
	}
	want := errors.New("stop")
	err = decodeQueryResponse(strings.NewReader(`{"row":{"columns":[1]}}`), func(cluster.KsqlTable) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("emitter error = %v, want %v", err, want)
	}
}

func TestDecodeQueryResponseBoundsErrorMessage(t *testing.T) {
	message := strings.Repeat("sensitive-error-", 80)
	var got []cluster.KsqlTable
	err := decodeQueryResponse(strings.NewReader(`{"errorMessage":"`+message+`"}`), func(v cluster.KsqlTable) error {
		got = append(got, v)
		return nil
	})
	if err != nil {
		t.Fatalf("decodeQueryResponse() error = %v", err)
	}
	if len(got) != 1 || len(got[0].Values) != 1 || len(got[0].Values[0]) != 1 {
		t.Fatalf("error frame = %#v", got)
	}
	text, ok := got[0].Values[0][0].(string)
	if !ok || len(text) > maxRemoteErrorBytes {
		t.Fatalf("bounded error message = %#v (ok=%v)", got[0].Values[0][0], ok)
	}
}

func TestDecodeKsqlResponseKindsAndEmptyBody(t *testing.T) {
	wire := []byte(`[
{"@type":"currentStatus","commandStatus":"SUCCESS","message":"done"},
{"@type":"streams","streams":[{"name":"S","topic":"t","keyFormat":"K","valueFormat":"V"}]},
{"@type":"tables","tables":[{"name":"T","topic":"t","isWindowed":false}]},
{"@type":"properties","properties":{"auto.offset.reset":"earliest"}},
{"@type":"queries","queries":[{"id":"Q1","queryString":"SELECT 1"}]},
{"@type":"sourceDescription","name":"S","fields":[{"name":"ID","type":"INT"}]},
{"@type":"futureType","hello":"world"}
]`)
	var got []cluster.KsqlTable
	if err := decodeKsqlResponse(wire, func(v cluster.KsqlTable) error { got = append(got, v); return nil }); err != nil {
		t.Fatalf("decodeKsqlResponse() error = %v", err)
	}
	wantHeaders := []string{"Status", "Streams", "Tables", "properties", "Queries", "Source Description", "Ksql Response"}
	if len(got) != len(wantHeaders) {
		t.Fatalf("frames = %#v", got)
	}
	for i, want := range wantHeaders {
		if got[i].Header != want {
			t.Errorf("frame %d header = %q, want %q", i, got[i].Header, want)
		}
	}
	if got[0].ColumnNames[0] != "status" || got[0].ColumnNames[1] != "message" {
		t.Fatalf("status columns = %#v", got[0].ColumnNames)
	}
	var empty []cluster.KsqlTable
	if err := decodeKsqlResponse(nil, func(v cluster.KsqlTable) error { empty = append(empty, v); return nil }); err != nil {
		t.Fatalf("empty ksql body = %v", err)
	}
	if len(empty) != 1 || empty[0].Header != "Query Result" || empty[0].Values[0][0] != "Success" {
		t.Fatalf("empty ksql frames = %#v", empty)
	}
}

func TestDecodeKsqlResponseRemoteError(t *testing.T) {
	var got []cluster.KsqlTable
	err := decodeKsqlResponse([]byte(`{"error_code":40001,"message":"bad"}`), func(v cluster.KsqlTable) error { got = append(got, v); return nil })
	if err != nil || len(got) != 1 || !got[0].IsError || got[0].Header != "Execution error" {
		t.Fatalf("remote error = %#v, %v", got, err)
	}
}

func TestDecodeKsqlResponseRemoteErrorWhitelistsAndBoundsFields(t *testing.T) {
	secret := strings.Repeat("sensitive-sql-", 80)
	wire := []byte(`{"error_code":40001,"message":"` + secret + `","status":{"password":"nested-secret"},"statementText":"SELECT password FROM users","Authorization":"Basic dXNlcjpwYXNz","password":"top-secret","url":"file:///tmp/private"}`)
	var got []cluster.KsqlTable
	if err := decodeKsqlResponse(wire, func(v cluster.KsqlTable) error { got = append(got, v); return nil }); err != nil {
		t.Fatalf("decodeKsqlResponse() error = %v", err)
	}
	if len(got) != 1 || !got[0].IsError {
		t.Fatalf("error frames = %#v", got)
	}
	if !reflect.DeepEqual(got[0].ColumnNames, []string{"error_code", "message"}) {
		t.Fatalf("safe columns = %#v", got[0].ColumnNames)
	}
	for _, value := range got[0].Values[0] {
		if text, ok := value.(string); ok && len(text) > maxRemoteErrorBytes {
			t.Fatalf("unbounded safe value length = %d", len(text))
		}
	}
	joined := strings.Join(got[0].ColumnNames, "|") + "|" + fmt.Sprint(got[0].Values)
	for _, forbidden := range []string{"statementText", "SELECT password", "Authorization", "top-secret", "file:///tmp/private"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("sensitive value %q leaked in %#v", forbidden, got[0])
		}
	}
}

func TestRemoteErrorMessagesRejectCredentialsURLsPathsAndSQL(t *testing.T) {
	sensitive := "Authorization: Basic dXNlcjpwYXNz password=top-secret https://alice:secret@example.test/private file:///var/lib/ksql SELECT secret FROM users"
	for _, tt := range []struct {
		name     string
		body     []byte
		wantCode bool
	}{
		{name: "query errorMessage", body: []byte(`{"errorMessage":"` + sensitive + `"}`)},
		{name: "ksql error object", body: []byte(`{"error_code":40001,"message":"` + sensitive + `"}`), wantCode: true},
		{name: "message only", body: []byte(`{"message":"` + sensitive + `"}`)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var got []cluster.KsqlTable
			var err error
			if strings.HasPrefix(tt.name, "query") {
				err = decodeQueryResponse(strings.NewReader(string(tt.body)), func(v cluster.KsqlTable) error {
					got = append(got, v)
					return nil
				})
			} else {
				err = decodeKsqlResponse(tt.body, func(v cluster.KsqlTable) error {
					got = append(got, v)
					return nil
				})
			}
			if err != nil {
				t.Fatalf("decode error = %v", err)
			}
			if len(got) != 1 || !got[0].IsError || got[0].Header != "Execution error" {
				t.Fatalf("error frame = %#v", got)
			}
			joined := got[0].Header + "|" + strings.Join(got[0].ColumnNames, "|") + "|" + fmt.Sprint(got[0].Values)
			for _, forbidden := range []string{"Authorization", "dXNlcjpwYXNz", "password=top-secret", "https://", "file://", "/var/lib/ksql", "SELECT secret", "statementText"} {
				if strings.Contains(strings.ToLower(joined), strings.ToLower(forbidden)) {
					t.Fatalf("sensitive value %q leaked in %#v", forbidden, got[0])
				}
			}
			if tt.wantCode && (len(got[0].ColumnNames) == 0 || got[0].ColumnNames[0] != "error_code" || !strings.Contains(fmt.Sprint(got[0].Values), "40001")) {
				t.Fatalf("numeric error code was not preserved: %#v", got[0])
			}
			if strings.Contains(got[0].Header, "Ksql Response") {
				t.Fatal("message-only error was rendered as a dynamic Ksql Response")
			}
		})
	}
}

func TestRemoteErrorSanitizerHandlesCaseVariantsAndErrorStatuses(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "upper message key", body: `{"MESSAGE":"PASSWORD: top-secret"}`},
		{name: "current status error", body: `{"@type":"currentStatus","commandStatus":"ERROR","message":"SELECT secret FROM S"}`},
		{name: "typed error", body: `{"@type":"error","message":"file:///var/lib/ksql"}`},
		{name: "scalar body", body: `"Authorization: Bearer abc"`},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var got []cluster.KsqlTable
			if err := decodeKsqlResponse([]byte(tt.body), func(v cluster.KsqlTable) error {
				got = append(got, v)
				return nil
			}); err != nil {
				t.Fatalf("decodeKsqlResponse() error = %v", err)
			}
			if len(got) != 1 || !got[0].IsError || got[0].Header != "Execution error" {
				t.Fatalf("unsafe response = %#v", got)
			}
			joined := strings.Join(got[0].ColumnNames, "|") + "|" + fmt.Sprint(got[0].Values)
			for _, forbidden := range []string{"PASSWORD", "top-secret", "SELECT secret", "file:///", "/var/lib/ksql", "Authorization", "Bearer abc", "Ksql Response"} {
				if strings.Contains(strings.ToLower(joined), strings.ToLower(forbidden)) {
					t.Fatalf("sensitive value %q leaked in %#v", forbidden, got[0])
				}
			}
		})
	}
}

func TestRemoteErrorSanitizerRejectsCredentialAndStatementIdentifierPrefixes(t *testing.T) {
	cases := []string{
		"passwordHash=top-secret",
		"password123: top-secret",
		"secretValue -> top-secret",
		"tokenValue=abc",
		"authorizationHeader: Bearer abc",
		"PaSsWoRd-HaSh: top-secret",
		"SECRET_value: top-secret",
		"Token.Value: abc",
		"authorization-header: Bearer abc",
		"statementTextPreview: payload",
		"statement-text-preview: payload",
		"sqlStatementPreview: payload",
		"sql.statement-preview: payload",
		"ksqlStatementPreview: payload",
		"selectQueryText: payload",
	}
	for _, text := range cases {
		t.Run(text, func(t *testing.T) {
			if safe, ok := sanitizeRemoteErrorText(text); ok {
				t.Fatalf("sensitive identifier was accepted: safe=%q", safe)
			}
		})
	}
}

func TestCurrentStatusKeepsOnlySafeScalarStatusAndMessage(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		wantStatus  any
		wantMessage any
	}{
		{
			name:        "structured command status and credential message",
			body:        `{"@type":"currentStatus","commandStatus":{"password":"nested-secret"},"message":"passwordHash=top-secret"}`,
			wantStatus:  ksqlExecutionErrorFallback,
			wantMessage: ksqlExecutionErrorFallback,
		},
		{
			name:        "structured status fallback",
			body:        `{"@type":"currentStatus","status":["SUCCESS"],"message":{"token":"nested-secret"}}`,
			wantStatus:  ksqlExecutionErrorFallback,
			wantMessage: ksqlExecutionErrorFallback,
		},
		{
			name:        "numeric command status remains scalar",
			body:        `{"@type":"currentStatus","commandStatus":7,"message":"done"}`,
			wantStatus:  int64(7),
			wantMessage: "done",
		},
		{
			name:        "nested status keeps safe status and rejects sensitive message",
			body:        `{"@type":"currentStatus","commandStatus":{"status":"SUCCESS","message":"passwordHash=top-secret"}}`,
			wantStatus:  "SUCCESS",
			wantMessage: ksqlExecutionErrorFallback,
		},
		{
			name:        "nested status and message reject structured values",
			body:        `{"@type":"currentStatus","commandStatus":{"status":{"token":"nested-secret"},"message":["Stream created"]}}`,
			wantStatus:  ksqlExecutionErrorFallback,
			wantMessage: ksqlExecutionErrorFallback,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var got []cluster.KsqlTable
			if err := decodeKsqlResponse([]byte(tt.body), func(v cluster.KsqlTable) error {
				got = append(got, v)
				return nil
			}); err != nil {
				t.Fatalf("decodeKsqlResponse() error = %v", err)
			}
			if len(got) != 1 || got[0].Header != "Status" || !reflect.DeepEqual(got[0].ColumnNames, []string{"status", "message"}) {
				t.Fatalf("status frame = %#v", got)
			}
			if len(got[0].Values) != 1 || len(got[0].Values[0]) != 2 {
				t.Fatalf("status values = %#v", got[0].Values)
			}
			if !reflect.DeepEqual(got[0].Values[0][0], tt.wantStatus) {
				t.Errorf("status value = %#v, want %#v", got[0].Values[0][0], tt.wantStatus)
			}
			if !reflect.DeepEqual(got[0].Values[0][1], tt.wantMessage) {
				t.Errorf("message value = %#v, want %#v", got[0].Values[0][1], tt.wantMessage)
			}
		})
	}
}

func TestCurrentStatusMapsNestedCommandStatus(t *testing.T) {
	var got []cluster.KsqlTable
	body := []byte(`{"@type":"currentStatus","commandStatus":{"status":"SUCCESS","message":"Stream created","queryId":null}}`)
	if err := decodeKsqlResponse(body, func(v cluster.KsqlTable) error {
		got = append(got, v)
		return nil
	}); err != nil {
		t.Fatalf("decodeKsqlResponse() error = %v", err)
	}
	if len(got) != 1 || got[0].Header != "Status" {
		t.Fatalf("status frame = %#v", got)
	}
	if !reflect.DeepEqual(got[0].Values, [][]any{{"SUCCESS", "Stream created"}}) {
		t.Fatalf("nested command status values = %#v, want [[SUCCESS Stream created]]", got[0].Values)
	}
}

func TestNestedCurrentStatusErrorIsSafeExecutionError(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		wantValues [][]any
	}{
		{
			name:       "safe nested message",
			body:       `{"@type":"currentStatus","commandStatus":{"status":"ERROR","message":"execution failed"}}`,
			wantValues: [][]any{{"ERROR", "execution failed"}},
		},
		{
			name:       "sensitive nested message falls back",
			body:       `{"@type":"currentStatus","commandStatus":{"status":"ERROR","message":"SELECT password FROM S"}}`,
			wantValues: [][]any{{"ERROR", ksqlExecutionErrorFallback}},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var got []cluster.KsqlTable
			if err := decodeKsqlResponse([]byte(tt.body), func(v cluster.KsqlTable) error {
				got = append(got, v)
				return nil
			}); err != nil {
				t.Fatalf("decodeKsqlResponse() error = %v", err)
			}
			if len(got) != 1 || !got[0].IsError || got[0].Header != "Execution error" {
				t.Fatalf("nested error frame = %#v", got)
			}
			if !reflect.DeepEqual(got[0].Values, tt.wantValues) {
				t.Fatalf("nested error values = %#v, want %#v", got[0].Values, tt.wantValues)
			}
		})
	}
}

func TestRemoteErrorSanitizerAllowsKsqlStatusSuccessMessages(t *testing.T) {
	for _, text := range []string{"Stream created", "Table created", "Statement executed"} {
		t.Run(text, func(t *testing.T) {
			if safe, ok := sanitizeRemoteErrorText(text); !ok || safe != text {
				t.Fatalf("status message = (%q, %v), want safe original text", safe, ok)
			}
		})
	}
}

func TestUnknownKsqlTypeWithMessageRemainsDynamicResponse(t *testing.T) {
	var got []cluster.KsqlTable
	if err := decodeKsqlResponse([]byte(`{"@type":"futureType","message":"passwordHash=top-secret"}`), func(v cluster.KsqlTable) error {
		got = append(got, v)
		return nil
	}); err != nil {
		t.Fatalf("decodeKsqlResponse() error = %v", err)
	}
	if len(got) != 1 || got[0].Header != "Ksql Response" || got[0].IsError {
		t.Fatalf("unknown typed response was classified as error: %#v", got)
	}
}

func TestDecodeListResponseRequiresExpectedTypeAndPreservesOrder(t *testing.T) {
	streams, tables, err := decodeListResponse([]byte(`[{"@type":"streams","streams":[{"name":"B","topic":"tb"},{"name":"A","topic":"ta"}]}]`), true)
	if err != nil || len(streams) != 2 || len(tables) != 0 {
		t.Fatalf("streams list = %#v %#v %v", streams, tables, err)
	}
	if *streams[0].Name != "B" || *streams[1].Name != "A" {
		t.Fatalf("order = %#v", streams)
	}
	if _, _, err := decodeListResponse([]byte(`[{"@type":"tables","tables":[]}]`), true); err == nil {
		t.Fatal("wrong list type returned nil")
	}
	if _, _, err := decodeListResponse([]byte(`[{"@type":"streams","streams":[{"name":4}]}]`), true); err == nil {
		t.Fatal("wrong field type returned nil")
	}
}

func TestDecodeListResponseRejectsWrongListFieldAndElementTypes(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{name: "streams scalar", body: `[{"@type":"streams","streams":"not-an-array"}]`, want: true},
		{name: "streams object", body: `[{"@type":"streams","streams":{}}]`, want: true},
		{name: "streams null", body: `[{"@type":"streams","streams":null}]`, want: true},
		{name: "streams scalar element", body: `[{"@type":"streams","streams":[1]}]`, want: true},
		{name: "streams null element", body: `[{"@type":"streams","streams":[null]}]`, want: true},
		{name: "tables scalar", body: `[{"@type":"tables","tables":"not-an-array"}]`, want: false},
		{name: "tables scalar element", body: `[{"@type":"tables","tables":[1]}]`, want: false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := decodeListResponse([]byte(tt.body), tt.want)
			if err == nil {
				t.Fatalf("decodeListResponse(%s) returned nil", tt.body)
			}
		})
	}
}

func TestSplitSchemaColumns(t *testing.T) {
	got := splitSchemaColumns("A STRING, B STRUCT<X STRING, Y ARRAY<STRING>>, `C,D` STRING, E MAP<STRING, STRUCT<F INT, G STRING>>")
	want := []string{"A", "B", "`C,D`", "E"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitSchemaColumns() = %#v, want %#v", got, want)
	}
}
