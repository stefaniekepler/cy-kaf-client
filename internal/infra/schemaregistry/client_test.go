package schemaregistry

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// These are fast, in-process unit tests against srfake (an httptest-backed
// fake Confluent-compatible SR server bundled with pkg/sr) -- they prove
// Pool's HTTP-shape wiring, request/response mapping, and error plumbing.
// srfake's handleCheckCompatibility is a hardcoded stub that always returns
// true (see its doc comment), so genuine compatibility-rule semantics are
// covered separately by client_integration_test.go against a real
// confluentinc/cp-schema-registry container.

func newTestDef(url string) cluster.Definition {
	return cluster.Definition{Name: "t", SchemaRegistry: cluster.SchemaRegistrySpec{URL: url}}
}

func avroSchema(s string) cluster.NewSchema {
	return cluster.NewSchema{Schema: s, SchemaType: "AVRO"}
}

const boolAvro = `{"type":"boolean"}`

func TestPoolSubjectsRegisterSchemaByVersionAndByID(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	p := NewPool()
	def := newTestDef(reg.URL())
	ctx := context.Background()

	id, err := p.Register(ctx, def, "t-value", avroSchema(boolAvro))
	require.NoError(t, err)
	require.NotZero(t, id)

	subjects, err := p.Subjects(ctx, def)
	require.NoError(t, err)
	require.Contains(t, subjects, "t-value")

	sv, err := p.SchemaByVersion(ctx, def, "t-value", "latest")
	require.NoError(t, err)
	require.Equal(t, 1, sv.Version)
	require.Equal(t, id, sv.ID)
	require.Equal(t, boolAvro, sv.Schema)
	require.Equal(t, "AVRO", sv.SchemaType)
	// srfake's default global compat is BACKWARD (matches sr.CompatBackward's
	// zero-configuration default); no subject-level override has been set,
	// so SchemaByVersion's effectiveCompat lookup should fall back to it.
	require.Equal(t, "BACKWARD", sv.CompatLevel)

	svNumeric, err := p.SchemaByVersion(ctx, def, "t-value", "1")
	require.NoError(t, err)
	require.Equal(t, sv, svNumeric)

	versions, err := p.Versions(ctx, def, "t-value")
	require.NoError(t, err)
	require.Equal(t, []int{1}, versions)

	raw, err := p.SchemaByID(ctx, def, id)
	require.NoError(t, err)
	require.Equal(t, boolAvro, raw.Schema)
	require.Equal(t, "AVRO", raw.SchemaType)
}

func TestPoolSchemaByIDCacheReusesSuccessfulLookup(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/schemas/ids/7" {
			t.Errorf("unexpected request path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"schema":"{\"type\":\"boolean\"}","schemaType":"AVRO"}`)); err != nil {
			t.Errorf("write schema response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	p := NewPool()
	def := newTestDef(server.URL)
	first, err := p.SchemaByID(context.Background(), def, 7)
	require.NoError(t, err)
	second, err := p.SchemaByID(context.Background(), def, 7)
	require.NoError(t, err)

	require.Equal(t, first, second)
	require.Equal(t, boolAvro, second.Schema)
	require.Equal(t, int32(1), requests.Load())
}

func TestPoolSchemaByIDCacheDoesNotStoreFailures(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "temporary failure", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	p := NewPool()
	def := newTestDef(server.URL)
	for range 2 {
		_, err := p.SchemaByID(context.Background(), def, 7)
		require.Error(t, err)
	}

	require.Equal(t, int32(2), requests.Load())
}

func TestPoolSchemaByIDCacheRotatesWithClientIdentity(t *testing.T) {
	var firstRequests atomic.Int32
	firstServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstRequests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"schema":"{\"type\":\"boolean\"}","schemaType":"AVRO"}`)); err != nil {
			t.Errorf("write first schema response: %v", err)
		}
	}))
	t.Cleanup(firstServer.Close)

	var secondRequests atomic.Int32
	secondServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondRequests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"schema":"{\"type\":\"string\"}","schemaType":"AVRO"}`)); err != nil {
			t.Errorf("write second schema response: %v", err)
		}
	}))
	t.Cleanup(secondServer.Close)

	p := NewPool()
	def := newTestDef(firstServer.URL)
	first, err := p.SchemaByID(context.Background(), def, 7)
	require.NoError(t, err)

	def.SchemaRegistry.URL = secondServer.URL
	second, err := p.SchemaByID(context.Background(), def, 7)
	require.NoError(t, err)

	require.Equal(t, boolAvro, first.Schema)
	require.Equal(t, `{"type":"string"}`, second.Schema)
	require.Equal(t, int32(1), firstRequests.Load())
	require.Equal(t, int32(1), secondRequests.Load())
}

func TestPoolSchemaByIDCacheEvictsOldestAtCapacity(t *testing.T) {
	p := NewPool()
	def := newTestDef("http://localhost")
	_, err := p.clientFor(def)
	require.NoError(t, err)

	for id := 0; id <= schemaCacheCapacity; id++ {
		p.storeSchema(def, id, cluster.RawSchema{Schema: boolAvro, SchemaType: "AVRO"})
	}

	_, ok := p.cachedSchema(def, 0)
	require.False(t, ok)
	latest, ok := p.cachedSchema(def, schemaCacheCapacity)
	require.True(t, ok)
	require.Equal(t, boolAvro, latest.Schema)
}

func TestPoolSchemaByVersionInvalidVersionErrors(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	p := NewPool()
	def := newTestDef(reg.URL())
	ctx := context.Background()

	_, err := p.Register(ctx, def, "t-value", avroSchema(boolAvro))
	require.NoError(t, err)

	_, err = p.SchemaByVersion(ctx, def, "t-value", "not-a-number")
	require.Error(t, err)
}

func TestPoolSchemaByVersionUnknownSubjectErrors(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	p := NewPool()
	def := newTestDef(reg.URL())

	_, err := p.SchemaByVersion(context.Background(), def, "does-not-exist", "latest")
	require.Error(t, err)
}

func TestPoolDeleteVersionResolvesLatestAndReturnsConcreteNumber(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	p := NewPool()
	def := newTestDef(reg.URL())
	ctx := context.Background()

	_, err := p.Register(ctx, def, "t-value", avroSchema(boolAvro))
	require.NoError(t, err)
	_, err = p.Register(ctx, def, "t-value", avroSchema(`{"type":"string"}`))
	require.NoError(t, err)

	versions, err := p.Versions(ctx, def, "t-value")
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, versions)

	deleted, err := p.DeleteVersion(ctx, def, "t-value", "latest", false)
	require.NoError(t, err)
	require.Equal(t, 2, deleted, "DeleteVersion must resolve \"latest\" to the concrete version number it deleted, never echo back a sentinel")

	remaining, err := p.Versions(ctx, def, "t-value")
	require.NoError(t, err)
	require.Equal(t, []int{1}, remaining)
}

func TestPoolDeleteSubjectRemovesAllVersions(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	p := NewPool()
	def := newTestDef(reg.URL())
	ctx := context.Background()

	_, err := p.Register(ctx, def, "t-value", avroSchema(boolAvro))
	require.NoError(t, err)

	deleted, err := p.DeleteSubject(ctx, def, "t-value", false)
	require.NoError(t, err)
	require.Equal(t, []int{1}, deleted)

	subjects, err := p.Subjects(ctx, def)
	require.NoError(t, err)
	require.NotContains(t, subjects, "t-value")
}

func TestPoolGlobalCompatRoundTrip(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	p := NewPool()
	def := newTestDef(reg.URL())
	ctx := context.Background()

	got, err := p.GlobalCompat(ctx, def)
	require.NoError(t, err)
	require.Equal(t, "BACKWARD", got, "srfake's zero-configuration default global compat")

	require.NoError(t, p.SetGlobalCompat(ctx, def, "FULL"))

	got, err = p.GlobalCompat(ctx, def)
	require.NoError(t, err)
	require.Equal(t, "FULL", got)
}

func TestPoolSubjectCompatRoundTripAndFallback(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	p := NewPool()
	def := newTestDef(reg.URL())
	ctx := context.Background()

	_, err := p.Register(ctx, def, "t-value", avroSchema(boolAvro))
	require.NoError(t, err)

	// No subject-level override yet: falls back to the global default.
	got, err := p.SubjectCompat(ctx, def, "t-value")
	require.NoError(t, err)
	require.Equal(t, "BACKWARD", got)

	require.NoError(t, p.SetSubjectCompat(ctx, def, "t-value", "NONE"))

	got, err = p.SubjectCompat(ctx, def, "t-value")
	require.NoError(t, err)
	require.Equal(t, "NONE", got)

	// The global default is untouched by a subject-level override.
	global, err := p.GlobalCompat(ctx, def)
	require.NoError(t, err)
	require.Equal(t, "BACKWARD", global)
}

func TestPoolSetCompatInvalidLevelErrors(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	p := NewPool()
	def := newTestDef(reg.URL())

	err := p.SetGlobalCompat(context.Background(), def, "NOT_A_LEVEL")
	require.Error(t, err)
}

func TestPoolRegisterInvalidSchemaTypeErrors(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	p := NewPool()
	def := newTestDef(reg.URL())

	_, err := p.Register(context.Background(), def, "t-value", cluster.NewSchema{Schema: boolAvro, SchemaType: "NOT_A_TYPE"})
	require.Error(t, err)
}

func TestPoolCheckCompat(t *testing.T) {
	// srfake's compatibility-check handler is a hardcoded stub that always
	// reports true (see package doc comment above) -- this only proves the
	// Pool wires the request/response correctly, not real compatibility
	// semantics (covered by the integration test).
	reg := srfake.New()
	t.Cleanup(reg.Close)
	p := NewPool()
	def := newTestDef(reg.URL())
	ctx := context.Background()

	_, err := p.Register(ctx, def, "t-value", avroSchema(boolAvro))
	require.NoError(t, err)

	ok, err := p.CheckCompat(ctx, def, "t-value", avroSchema(boolAvro))
	require.NoError(t, err)
	require.True(t, ok)
}

func TestPoolBasicAuthWiring(t *testing.T) {
	const user, pass = "alice", "s3cret"
	expected := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
	reg := srfake.New(srfake.WithAuth(expected))
	t.Cleanup(reg.Close)
	p := NewPool()
	ctx := context.Background()

	noAuthDef := newTestDef(reg.URL())
	_, err := p.Subjects(ctx, noAuthDef)
	require.Error(t, err, "server requires basic auth; a client with none configured must fail, not silently succeed")

	authedDef := cluster.Definition{
		Name: "authed",
		SchemaRegistry: cluster.SchemaRegistrySpec{
			URL:  reg.URL(),
			Auth: &cluster.SRAuth{Username: user, Password: pass},
		},
	}
	_, err = p.Subjects(ctx, authedDef)
	require.NoError(t, err)
}

func TestPoolClientIsCachedPerClusterName(t *testing.T) {
	reg := srfake.New()
	t.Cleanup(reg.Close)
	p := NewPool()
	def := newTestDef(reg.URL())

	cl1, err := p.clientFor(def)
	require.NoError(t, err)
	cl2, err := p.clientFor(def)
	require.NoError(t, err)
	require.Same(t, cl1, cl2, "clientFor must cache and reuse the *sr.Client per cluster name")
}

func TestPoolClientRotatesWhenConnectionSettingsChange(t *testing.T) {
	p := NewPool()
	base := cluster.Definition{
		Name: "local",
		SchemaRegistry: cluster.SchemaRegistrySpec{
			URL: "http://sr-a:8081",
		},
	}
	first, err := p.clientFor(base)
	require.NoError(t, err)

	changed := base
	changed.SchemaRegistry.URL = "http://sr-b:8081"
	changed.SchemaRegistry.Auth = &cluster.SRAuth{Username: "u", Password: "p"}
	second, err := p.clientFor(changed)
	require.NoError(t, err)

	require.NotSame(t, first, second, "a dynamic-config edit must not reuse the stale SR client")
	require.Len(t, p.clients, 1, "retain only the current client for a cluster")
}
