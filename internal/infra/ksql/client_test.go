package ksql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func TestPoolRoutesSelectAndKsql(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var bodies []map[string]any
	var auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		var wire map[string]any
		if err := json.Unmarshal(body, &wire); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		mu.Lock()
		paths = append(paths, r.URL.Path)
		bodies = append(bodies, wire)
		auths = append(auths, r.Header.Get("Authorization"))
		mu.Unlock()
		switch r.URL.Path {
		case "/query":
			if got := r.Header.Get("Accept"); got != ksqlMediaType {
				t.Errorf("query Accept = %q", got)
			}
			if got := r.Header.Get("Content-Type"); got != ksqlMediaType {
				t.Errorf("query Content-Type = %q", got)
			}
			_, _ = io.WriteString(w, "{\"header\":{\"schema\":\"ID STRING\"}}\n{\"row\":{\"columns\":[1]}}\n{\"finalMessage\":\"OK\"}\n")
		case "/ksql":
			if got := r.Header.Get("Accept"); got != ksqlMediaType {
				t.Errorf("ksql Accept = %q", got)
			}
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Errorf("ksql Content-Type = %q", got)
			}
			_, _ = io.WriteString(w, "[]")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	p := NewPool()
	def := cluster.Definition{
		Name:     "local",
		KsqlURL:  srv.URL + "/",
		KsqlAuth: &cluster.KsqlAuth{Username: "alice", Password: "secret"},
	}
	props := map[string]string{"auto.offset.reset": "earliest"}
	var queryFrames []cluster.KsqlTable
	if err := p.Execute(context.Background(), def, cluster.KsqlCommand{
		SQL: "SELECT 1;", Kind: cluster.KsqlQuery, StreamsProperties: props,
	}, func(v cluster.KsqlTable) error {
		queryFrames = append(queryFrames, v)
		return nil
	}); err != nil {
		t.Fatalf("query Execute() error = %v", err)
	}
	var ksqlFrames []cluster.KsqlTable
	if err := p.Execute(context.Background(), def, cluster.KsqlCommand{
		SQL: "CREATE STREAM S (ID INT);", Kind: cluster.KsqlStatement, StreamsProperties: props,
	}, func(v cluster.KsqlTable) error {
		ksqlFrames = append(ksqlFrames, v)
		return nil
	}); err != nil {
		t.Fatalf("ksql Execute() error = %v", err)
	}
	if len(queryFrames) != 3 || queryFrames[0].Header != "Schema" || queryFrames[2].Header != "Query Result" {
		t.Fatalf("unexpected query frames: %+v", queryFrames)
	}
	if len(ksqlFrames) != 1 || ksqlFrames[0].Header != "Query Result" {
		t.Fatalf("unexpected ksql frames: %+v", ksqlFrames)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("alice:secret"))
	mu.Lock()
	defer mu.Unlock()
	if got, want := paths, []string{"/query", "/ksql"}; !equalStrings(got, want) {
		t.Fatalf("paths = %#v, want %#v", got, want)
	}
	for _, got := range auths {
		if got != wantAuth {
			t.Fatalf("Authorization = %q, want %q", got, wantAuth)
		}
	}
	for _, body := range bodies {
		if body["ksql"] == nil || body["streamsProperties"] == nil {
			t.Fatalf("request body missing fields: %#v", body)
		}
		if got := body["streamsProperties"].(map[string]any)["auto.offset.reset"]; got != "earliest" {
			t.Fatalf("streamsProperties = %#v", body["streamsProperties"])
		}
	}
}

func TestPoolErrorsAreBoundedAndSafe(t *testing.T) {
	p := NewPool()
	if err := p.Execute(context.Background(), cluster.Definition{Name: "empty"}, cluster.KsqlCommand{SQL: "SELECT 'password'", Kind: cluster.KsqlQuery}, func(cluster.KsqlTable) error { return nil }); err == nil || strings.Contains(err.Error(), "password") {
		t.Fatalf("empty URL error = %v, want safe infrastructure error", err)
	}

	secret := strings.Repeat("x", 700)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, `{"error_code":50001,"message":"`+secret+`"}`)
	}))
	defer srv.Close()
	var frames []cluster.KsqlTable
	err := p.Execute(context.Background(), cluster.Definition{Name: "bad", KsqlURL: srv.URL}, cluster.KsqlCommand{SQL: "SELECT 'password'", Kind: cluster.KsqlQuery}, func(v cluster.KsqlTable) error {
		frames = append(frames, v)
		return nil
	})
	if err != nil {
		t.Fatalf("non-2xx Execute() error = %v", err)
	}
	if len(frames) != 1 || !frames[0].IsError || frames[0].Header != "Execution error" {
		t.Fatalf("non-2xx frames = %#v", frames)
	}
	if len(frames[0].Values) != 1 || len(frames[0].Values[0]) != 1 {
		t.Fatalf("non-2xx values = %#v", frames[0].Values)
	}
	if got, ok := frames[0].Values[0][0].(string); !ok || !strings.Contains(got, "50001") || len(got) > 500 {
		t.Fatalf("bounded remote error = %#v (len=%d)", frames[0].Values[0][0], len(got))
	}

	transportErr := p.Execute(context.Background(), cluster.Definition{Name: "transport", KsqlURL: "http://127.0.0.1:1"}, cluster.KsqlCommand{SQL: "SELECT 1", Kind: cluster.KsqlQuery}, func(cluster.KsqlTable) error { return nil })
	if transportErr == nil {
		t.Fatal("transport failure returned nil")
	}
}

func TestPoolRejectsOversizedKsqlResponse(t *testing.T) {
	large := strings.Repeat("x", maxKsqlResponseBytes+1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"@type":"future","payload":"`+large+`"}`)
	}))
	defer srv.Close()

	err := NewPool().Execute(context.Background(), cluster.Definition{Name: "oversized", KsqlURL: srv.URL}, cluster.KsqlCommand{
		SQL: "CREATE STREAM S;", Kind: cluster.KsqlStatement,
	}, func(cluster.KsqlTable) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "KSQL response exceeds size limit") {
		t.Fatalf("oversized /ksql response error = %v, want bounded-response error", err)
	}
}

func TestReadBoundedKSQLBodyAcceptsExactLimitAndRejectsNextByte(t *testing.T) {
	const limit = 8
	got, err := readBoundedKSQLBodyWithLimit(strings.NewReader("12345678"), limit)
	if err != nil || string(got) != "12345678" {
		t.Fatalf("exact response = %q, %v", got, err)
	}
	if _, err := readBoundedKSQLBodyWithLimit(strings.NewReader("123456789"), limit); !errors.Is(err, errKsqlResponseLimit) {
		t.Fatalf("oversized response error = %v, want %v", err, errKsqlResponseLimit)
	}
}

func TestPoolRejectsOversizedQueryFrame(t *testing.T) {
	large := strings.Repeat("x", maxKsqlResponseFrameBytes+1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"row":{"columns":["`+large+`"]}}`)
	}))
	defer srv.Close()

	err := NewPool().Execute(context.Background(), cluster.Definition{Name: "oversized-query", KsqlURL: srv.URL}, cluster.KsqlCommand{
		SQL: "SELECT 1;", Kind: cluster.KsqlQuery,
	}, func(cluster.KsqlTable) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "KSQL response frame exceeds size limit") {
		t.Fatalf("oversized /query frame error = %v, want bounded-frame error", err)
	}
}

func TestPoolNon2xxPreservesRemoteErrorFields(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error_code":40001,"message":"statement rejected"}`)
	}))
	defer srv.Close()
	var frames []cluster.KsqlTable
	if err := NewPool().Execute(context.Background(), cluster.Definition{Name: "remote", KsqlURL: srv.URL}, cluster.KsqlCommand{SQL: "CREATE STREAM S;", Kind: cluster.KsqlStatement}, func(v cluster.KsqlTable) error {
		frames = append(frames, v)
		return nil
	}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(frames) != 1 || !frames[0].IsError || frames[0].Header != "Execution error" {
		t.Fatalf("frames = %#v", frames)
	}
	if !equalStrings(frames[0].ColumnNames, []string{"error_code", "message"}) || len(frames[0].Values) != 1 || frames[0].Values[0][1] != "statement rejected" {
		t.Fatalf("remote error frame = %#v", frames[0])
	}
}

func TestPoolNon2xxFiltersSensitiveRemoteErrorFields(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"errorCode":40001,"errorMessage":"statement rejected","statementText":"SELECT password FROM users","SQL":"SELECT secret","Authorization":"Basic dXNlcjpwYXNz","password":"top-secret","path":"/tmp/private"}`)
	}))
	defer srv.Close()
	var frames []cluster.KsqlTable
	if err := NewPool().Execute(context.Background(), cluster.Definition{Name: "remote-safe", KsqlURL: srv.URL}, cluster.KsqlCommand{SQL: "CREATE STREAM S;", Kind: cluster.KsqlStatement}, func(v cluster.KsqlTable) error {
		frames = append(frames, v)
		return nil
	}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(frames) != 1 || !frames[0].IsError {
		t.Fatalf("frames = %#v", frames)
	}
	if !equalStrings(frames[0].ColumnNames, []string{"errorCode", "errorMessage"}) {
		t.Fatalf("safe columns = %#v", frames[0].ColumnNames)
	}
	joined := strings.Join(frames[0].ColumnNames, "|") + "|" + fmt.Sprint(frames[0].Values)
	for _, forbidden := range []string{"statementText", "SELECT password", "Authorization", "top-secret", "/tmp/private"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("sensitive value %q leaked in %#v", forbidden, frames[0])
		}
	}
}

func TestPoolRemoteErrorsSanitizeSensitiveMessages(t *testing.T) {
	sensitive := "Authorization: Bearer abc password=top-secret https://alice:secret@example.test/private file:///tmp/ksql SELECT secret FROM users"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/query" {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, `{"error_code":50201,"message":"`+sensitive+`"}`)
			return
		}
		if r.URL.Path == "/ksql" {
			_, _ = io.WriteString(w, `{"message":"`+sensitive+`"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	p := NewPool()
	def := cluster.Definition{Name: "remote-sanitize", KsqlURL: srv.URL}
	for _, command := range []cluster.KsqlCommand{
		{SQL: "SELECT 1", Kind: cluster.KsqlQuery},
		{SQL: "CREATE STREAM S;", Kind: cluster.KsqlStatement},
	} {
		var frames []cluster.KsqlTable
		if err := p.Execute(context.Background(), def, command, func(v cluster.KsqlTable) error {
			frames = append(frames, v)
			return nil
		}); err != nil {
			t.Fatalf("Execute(%s) error = %v", command.Kind, err)
		}
		if len(frames) != 1 || !frames[0].IsError || frames[0].Header != "Execution error" {
			t.Fatalf("Execute(%s) frames = %#v", command.Kind, frames)
		}
		joined := strings.Join(frames[0].ColumnNames, "|") + "|" + fmt.Sprint(frames[0].Values)
		for _, forbidden := range []string{"Authorization", "Bearer abc", "password=top-secret", "https://", "file://", "/tmp/ksql", "SELECT secret", "statementText"} {
			if strings.Contains(strings.ToLower(joined), strings.ToLower(forbidden)) {
				t.Fatalf("Execute(%s) leaked %q: %#v", command.Kind, forbidden, frames[0])
			}
		}
	}
}

func TestPoolRejectsURLUserinfoAndUsesContext(t *testing.T) {
	p := NewPool()
	err := p.Execute(context.Background(), cluster.Definition{Name: "userinfo", KsqlURL: "http://user:pass@example.test"}, cluster.KsqlCommand{SQL: "SELECT 1", Kind: cluster.KsqlQuery}, func(cluster.KsqlTable) error { return nil })
	if err == nil || strings.Contains(err.Error(), "user") || strings.Contains(err.Error(), "pass") {
		t.Fatalf("userinfo error = %v", err)
	}

	canceled := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// net/http's server keeps the request context alive while an
		// unread request body is still owned by the handler. Drain the
		// small command body before waiting so this test observes the
		// cancellation boundary rather than an unrelated body-read state.
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
		close(canceled)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err = p.Execute(ctx, cluster.Definition{Name: "cancel", KsqlURL: srv.URL}, cluster.KsqlCommand{SQL: "SELECT 1", Kind: cluster.KsqlQuery}, func(cluster.KsqlTable) error { return nil })
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancel error = %v, want deadline exceeded", err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("server did not observe context cancellation")
	}
}

func TestPoolClosesResponseBodyWhenContextIsCanceled(t *testing.T) {
	started := make(chan struct{})
	closed := make(chan struct{})
	p := NewPool()
	p.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(started)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: &blockingResponseBody{
				ctx:    req.Context(),
				closed: closed,
			},
			Request: req,
		}, nil
	})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- p.Execute(ctx, cluster.Definition{Name: "cancel-body", KsqlURL: "http://ksql.test"}, cluster.KsqlCommand{SQL: "SELECT 1", Kind: cluster.KsqlQuery}, func(cluster.KsqlTable) error { return nil })
	}()
	<-started
	cancel()
	if err := <-done; err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() after cancel = %v, want context.Canceled", err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("response body was not closed after context cancellation")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type blockingResponseBody struct {
	ctx    context.Context
	closed chan struct{}
}

func (b *blockingResponseBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b *blockingResponseBody) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}

func TestPoolListStreamsAndTablesPreserveOrder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode list body: %v", err)
		}
		statement := body["ksql"]
		switch statement {
		case "LIST STREAMS;":
			_, _ = io.WriteString(w, `[{"@type":"streams","streams":[{"name":"S2","topic":"t2","keyFormat":"K","format":"V2"},{"name":"S1","topic":"t1","keyFormat":"K","valueFormat":"V1"}]}]`)
		case "LIST TABLES;":
			_, _ = io.WriteString(w, `[{"@type":"tables","tables":[{"name":"T2","topic":"t2","keyFormat":"K","valueFormat":"V2","isWindowed":true},{"name":"T1","topic":"t1","keyFormat":"K","valueFormat":"V1","isWindowed":false}]}]`)
		default:
			t.Errorf("unexpected list statement %#v", statement)
		}
	}))
	defer srv.Close()
	p := NewPool()
	def := cluster.Definition{Name: "local", KsqlURL: srv.URL}
	streams, err := p.ListStreams(context.Background(), def)
	if err != nil || len(streams) != 2 {
		t.Fatalf("ListStreams() = %#v, %v", streams, err)
	}
	if *streams[0].Name != "S2" || *streams[1].Name != "S1" || *streams[0].ValueFormat != "V2" {
		t.Fatalf("streams = %#v", streams)
	}
	tables, err := p.ListTables(context.Background(), def)
	if err != nil || len(tables) != 2 {
		t.Fatalf("ListTables() = %#v, %v", tables, err)
	}
	if *tables[0].Name != "T2" || !*tables[0].IsWindowed || *tables[1].IsWindowed {
		t.Fatalf("tables = %#v", tables)
	}
}

func TestPoolCacheKeyChangesWithEndpointAuthAndTLS(t *testing.T) {
	p := NewPool()
	base := cluster.Definition{Name: "local", KsqlURL: "http://ksql-a:8088/"}
	a, err := p.clientFor(base)
	if err != nil {
		t.Fatalf("clientFor(base) error = %v", err)
	}
	if got, err := p.clientFor(cluster.Definition{Name: "local", KsqlURL: "http://ksql-a:8088"}); err != nil || got != a {
		t.Fatalf("normalized URL did not reuse client: got=%p want=%p err=%v", got, a, err)
	}
	variants := []cluster.Definition{
		{Name: "local", KsqlURL: "http://ksql-b:8088"},
		{Name: "local", KsqlURL: base.KsqlURL, KsqlAuth: &cluster.KsqlAuth{Username: "u", Password: "p"}},
		{Name: "local", KsqlURL: base.KsqlURL, KsqlSSL: &cluster.KsqlSSL{KeystoreLocation: "/does/not/load"}},
		{Name: "other", KsqlURL: base.KsqlURL},
	}
	for i, def := range variants {
		got, err := p.clientFor(def)
		if err != nil {
			t.Fatalf("variant %d clientFor() error = %v", i, err)
		}
		if got == a {
			t.Fatalf("variant %d reused stale client", i)
		}
	}
	if len(p.clients) != 2 {
		t.Fatalf("cached clients = %d, want one current client per cluster", len(p.clients))
	}
	localClients := 0
	for key := range p.clients {
		if key.cluster == "local" {
			localClients++
		}
	}
	if localClients != 1 {
		t.Fatalf("local cached clients = %d, want one current client", localClients)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
