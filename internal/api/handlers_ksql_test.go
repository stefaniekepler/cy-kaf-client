package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/version"
)

type fakeKsqlService struct {
	pipeID           string
	frames           []cluster.KsqlTable
	err              error
	errAfterFrames   bool
	registerCalls    int
	openCalls        int
	lastCluster      string
	lastSQL          string
	lastStreamsProps map[string]string
	streams          []cluster.KsqlStreamDescription
	tables           []cluster.KsqlTableDescription
	listStreamsCalls int
	listTablesCalls  int
}

func (f *fakeKsqlService) Register(_ context.Context, clusterName, sql string, props map[string]string) (string, error) {
	f.registerCalls++
	f.lastCluster, f.lastSQL, f.lastStreamsProps = clusterName, sql, props
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)
	if trimmed == "" || strings.Count(trimmed, ";") > 1 ||
		strings.HasPrefix(upper, "PRINT") || strings.HasPrefix(upper, "DEFINE") || strings.HasPrefix(upper, "UNDEFINE") {
		return "", appcluster.ErrKsqlInvalidCommand
	}
	if f.err != nil {
		return "", f.err
	}
	return f.pipeID, nil
}

func (f *fakeKsqlService) Open(_ context.Context, clusterName, _ string, emit func(cluster.KsqlTable) error) error {
	f.openCalls++
	f.lastCluster = clusterName
	if f.err != nil && !f.errAfterFrames {
		return f.err
	}
	for _, frame := range f.frames {
		if err := emit(frame); err != nil {
			return err
		}
	}
	if f.err != nil {
		return f.err
	}
	return nil
}

func (f *fakeKsqlService) ListStreams(context.Context, string) ([]cluster.KsqlStreamDescription, error) {
	f.listStreamsCalls++
	if f.err != nil {
		return nil, f.err
	}
	return f.streams, nil
}

func (f *fakeKsqlService) ListTables(context.Context, string) ([]cluster.KsqlTableDescription, error) {
	f.listTablesCalls++
	if f.err != nil {
		return nil, f.err
	}
	return f.tables, nil
}

func ksqlServer(svc api.KsqlServicer, readOnly func(string) bool) http.Handler {
	if readOnly == nil {
		readOnly = func(string) bool { return false }
	}
	return api.NewServer(api.Deps{
		Ksql:       svc,
		IsReadOnly: readOnly,
		Build:      version.BuildInfo{Version: "test"},
		Static:     fstest.MapFS{"index.html": {Data: []byte("ok")}},
	})
}

func serveKsql(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Host = "localhost"
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestExecuteKsqlReturnsPipeIDWithoutOpening(t *testing.T) {
	svc := &fakeKsqlService{pipeID: "7f0e5d4d-5de8-4c7e-8d57-54c9d5b2d62d"}
	rec := serveKsql(t, ksqlServer(svc, nil), http.MethodPost,
		"/api/clusters/local/ksql/v2", `{"ksql":"SELECT 1;"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.JSONEq(t, `{"pipeId":"7f0e5d4d-5de8-4c7e-8d57-54c9d5b2d62d"}`, rec.Body.String())
	require.Equal(t, 1, svc.registerCalls)
	require.Equal(t, 0, svc.openCalls, "POST only registers a lazy pipe")
	require.Equal(t, "local", svc.lastCluster)
	require.Equal(t, "SELECT 1;", svc.lastSQL)
	require.NotNil(t, svc.lastStreamsProps, "omitted streamsProperties is normalized to an empty map")
}

func TestExecuteKsqlRejectsInvalidBodyAndSQLWithoutEcho(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		sql  string
	}{
		{name: "malformed json", body: `{"ksql":`, sql: "SELECT secret_malformed"},
		{name: "empty ksql", body: `{"ksql":""}`, sql: ""},
		{name: "multiple statements", body: `{"ksql":"SELECT secret_a; SELECT secret_b;"}`, sql: "secret_a"},
		{name: "forbidden print", body: `{"ksql":"PRINT 'secret_topic';"}`, sql: "secret_topic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &fakeKsqlService{pipeID: "unexpected"}
			rec := serveKsql(t, ksqlServer(svc, nil), http.MethodPost,
				"/api/clusters/local/ksql/v2", tc.body)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			if tc.sql != "" {
				require.NotContains(t, rec.Body.String(), tc.sql)
			}
			if tc.name == "malformed json" {
				require.Equal(t, 0, svc.registerCalls)
			} else {
				require.Equal(t, 1, svc.registerCalls, "classification is owned by the application service")
			}
		})
	}
}

func TestKsqlErrorsMapToSafeHTTPStatuses(t *testing.T) {
	t.Run("unknown pipe", func(t *testing.T) {
		svc := &fakeKsqlService{err: appcluster.ErrKsqlPipeNotFound}
		rec := serveKsql(t, ksqlServer(svc, nil), http.MethodGet,
			"/api/clusters/local/ksql/response?pipeId=missing", "")
		require.Equal(t, http.StatusNotFound, rec.Code)
	})
	t.Run("unknown cluster", func(t *testing.T) {
		svc := &fakeKsqlService{err: appcluster.ErrKsqlClusterNotFound}
		rec := serveKsql(t, ksqlServer(svc, nil), http.MethodGet,
			"/api/clusters/nope/ksql/streams", "")
		require.Equal(t, http.StatusNotFound, rec.Code)
	})
	t.Run("service failure", func(t *testing.T) {
		svc := &fakeKsqlService{err: errors.New("remote sql=secret credential=secret")}
		rec := serveKsql(t, ksqlServer(svc, nil), http.MethodGet,
			"/api/clusters/local/ksql/tables", "")
		require.Equal(t, http.StatusInternalServerError, rec.Code)
		require.NotContains(t, rec.Body.String(), "secret")
	})
}

func TestKsqlNotConfiguredMapsEveryOperationTo404WithoutDetails(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "streams", method: http.MethodGet, path: "/api/clusters/local/ksql/streams"},
		{name: "tables", method: http.MethodGet, path: "/api/clusters/local/ksql/tables"},
		{name: "register", method: http.MethodPost, path: "/api/clusters/local/ksql/v2", body: `{"ksql":"SELECT 1"}`},
		{name: "open", method: http.MethodGet, path: "/api/clusters/local/ksql/response?pipeId=pipe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &fakeKsqlService{
				err: fmt.Errorf("%w: internal configuration details", appcluster.ErrKsqlNotConfigured),
			}
			rec := serveKsql(t, ksqlServer(svc, nil), tt.method, tt.path, tt.body)

			require.Equal(t, http.StatusNotFound, rec.Code)
			assertErrorEnvelope(t, rec.Body.Bytes(), "ksql not configured", "internal configuration details")
		})
	}
}

func TestOpenKsqlResponsePipeRequiresPipeIDBeforeService(t *testing.T) {
	svc := &fakeKsqlService{}
	rec := serveKsql(t, ksqlServer(svc, nil), http.MethodGet,
		"/api/clusters/local/ksql/response", "")
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, 0, svc.openCalls)
}

func TestOpenKsqlResponsePipeStreamsGeneratedFrames(t *testing.T) {
	svc := &fakeKsqlService{frames: []cluster.KsqlTable{{
		Header:      "Row",
		ColumnNames: []string{"id", "name"},
		Values:      [][]any{{float64(1), "one"}},
	}}}
	rec := serveKsql(t, ksqlServer(svc, nil), http.MethodGet,
		"/api/clusters/local/ksql/response?pipeId=pipe", "")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	require.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
	require.Contains(t, rec.Body.String(), `"table":{"columnNames":["id","name"],"header":"Row","values":[[1,"one"]]}`)
}

func TestOpenKsqlResponsePipeErrorBeforeAndAfterFirstFrame(t *testing.T) {
	t.Run("before first frame is ordinary 500", func(t *testing.T) {
		svc := &fakeKsqlService{err: errors.New("backend secret")}
		rec := serveKsql(t, ksqlServer(svc, nil), http.MethodGet,
			"/api/clusters/local/ksql/response?pipeId=pipe", "")
		require.Equal(t, http.StatusInternalServerError, rec.Code)
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		require.NotContains(t, rec.Body.String(), "backend secret")
	})
	t.Run("after first frame closes stream", func(t *testing.T) {
		svc := &fakeKsqlService{frames: []cluster.KsqlTable{{Header: "Row"}}, err: errors.New("backend secret"), errAfterFrames: true}
		rec := serveKsql(t, ksqlServer(svc, nil), http.MethodGet,
			"/api/clusters/local/ksql/response?pipeId=pipe", "")
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
		require.NotContains(t, rec.Body.String(), "backend secret")
	})
}

func TestKsqlListsPreserveOrderAndOptionalPointers(t *testing.T) {
	name1, topic1, key1, value1 := "s1", "topic-1", "KAFKA", "JSON"
	name2 := "s2"
	isWindowed := true
	svc := &fakeKsqlService{
		streams: []cluster.KsqlStreamDescription{{Name: &name1, Topic: &topic1, KeyFormat: &key1, ValueFormat: &value1}, {Name: &name2}},
		tables:  []cluster.KsqlTableDescription{{Name: &name1, IsWindowed: &isWindowed}, {Name: &name2}},
	}
	h := ksqlServer(svc, nil)
	streams := serveKsql(t, h, http.MethodGet, "/api/clusters/local/ksql/streams", "")
	require.Equal(t, http.StatusOK, streams.Code)
	var gotStreams []map[string]any
	require.NoError(t, json.Unmarshal(streams.Body.Bytes(), &gotStreams))
	require.Len(t, gotStreams, 2)
	require.Equal(t, "s1", gotStreams[0]["name"])
	require.Equal(t, "s2", gotStreams[1]["name"])
	require.NotContains(t, gotStreams[1], "topic")
	tables := serveKsql(t, h, http.MethodGet, "/api/clusters/local/ksql/tables", "")
	require.Equal(t, http.StatusOK, tables.Code)
	var gotTables []map[string]any
	require.NoError(t, json.Unmarshal(tables.Body.Bytes(), &gotTables))
	require.Len(t, gotTables, 2)
	require.Equal(t, true, gotTables[0]["isWindowed"])
	require.Equal(t, "s2", gotTables[1]["name"])
}

func TestKsqlReadOnlyPOSTIncludesSelectButGETsPass(t *testing.T) {
	svc := &fakeKsqlService{pipeID: "pipe", streams: []cluster.KsqlStreamDescription{}}
	h := ksqlServer(svc, func(string) bool { return true })
	post := serveKsql(t, h, http.MethodPost, "/api/clusters/prod/ksql/v2", `{"ksql":"SELECT 1"}`)
	require.Equal(t, http.StatusForbidden, post.Code)
	require.Equal(t, 0, svc.registerCalls)
	streams := serveKsql(t, h, http.MethodGet, "/api/clusters/prod/ksql/streams", "")
	require.Equal(t, http.StatusOK, streams.Code)
	// GET pipe is also outside the write guard; the fake may return a 404, but
	// it must be reached rather than rejected as read-only.
	pipe := serveKsql(t, h, http.MethodGet, "/api/clusters/prod/ksql/response?pipeId=pipe", "")
	require.NotEqual(t, http.StatusForbidden, pipe.Code)
}

func TestKsqlServicerCompileContract(t *testing.T) {
	var _ api.KsqlServicer = (*fakeKsqlService)(nil)
}
