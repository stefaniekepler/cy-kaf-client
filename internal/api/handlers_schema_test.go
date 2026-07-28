package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// fakeSchemaServicer implements api.SchemaServicer for the schema handler
// tests — same "error map wins, else result-map presence = known cluster,
// else ErrUnknownCluster" convention (keyed by cluster name) as
// handlers_group_test.go's fakeGroupServicer. Carries only the four read
// methods Task 3 wires; Task 4/5 grow it alongside the interface.
type fakeSchemaServicer struct {
	listPages map[string]appcluster.SchemaPage
	listErr   map[string]error
	lastQuery map[string]appcluster.SchemaListQuery

	latest            map[string]cluster.SchemaVersion
	latestErr         map[string]error
	lastLatestSubject map[string]string

	byVersion    map[string]cluster.SchemaVersion
	byVersionErr map[string]error
	lastVersion  map[string]string

	allVersions    map[string][]cluster.SchemaVersion
	allVersionsErr map[string]error

	registerResult      map[string]cluster.SchemaVersion
	registerErr         map[string]error
	lastRegisterSubject map[string]string
	lastRegister        map[string]cluster.NewSchema

	deleteSubjectResult map[string][]int
	deleteSubjectErr    map[string]error
	deleteSubjectKnown  map[string]bool
	lastDeleteSubject   map[string]string

	deleteVersionResult map[string]int
	deleteVersionErr    map[string]error
	deleteVersionKnown  map[string]bool
	lastDeleteVersion   map[string]string // subject@version

	globalCompat    map[string]string
	globalCompatErr map[string]error

	setGlobalErr   map[string]error
	setGlobalKnown map[string]bool
	lastSetGlobal  map[string]string

	setSubjectErr   map[string]error
	setSubjectKnown map[string]bool
	lastSetSubject  map[string]string // subject@level

	checkResult      map[string]bool
	checkErr         map[string]error
	checkKnown       map[string]bool
	lastCheckSubject map[string]string
	lastCheck        map[string]cluster.NewSchema
}

func newFakeSchemaServicer() *fakeSchemaServicer {
	return &fakeSchemaServicer{
		listPages: map[string]appcluster.SchemaPage{}, listErr: map[string]error{}, lastQuery: map[string]appcluster.SchemaListQuery{},
		latest: map[string]cluster.SchemaVersion{}, latestErr: map[string]error{}, lastLatestSubject: map[string]string{},
		byVersion: map[string]cluster.SchemaVersion{}, byVersionErr: map[string]error{}, lastVersion: map[string]string{},
		allVersions: map[string][]cluster.SchemaVersion{}, allVersionsErr: map[string]error{},
		registerResult: map[string]cluster.SchemaVersion{}, registerErr: map[string]error{}, lastRegisterSubject: map[string]string{}, lastRegister: map[string]cluster.NewSchema{},
		deleteSubjectResult: map[string][]int{}, deleteSubjectErr: map[string]error{}, deleteSubjectKnown: map[string]bool{}, lastDeleteSubject: map[string]string{},
		deleteVersionResult: map[string]int{}, deleteVersionErr: map[string]error{}, deleteVersionKnown: map[string]bool{}, lastDeleteVersion: map[string]string{},
		globalCompat: map[string]string{}, globalCompatErr: map[string]error{},
		setGlobalErr: map[string]error{}, setGlobalKnown: map[string]bool{}, lastSetGlobal: map[string]string{},
		setSubjectErr: map[string]error{}, setSubjectKnown: map[string]bool{}, lastSetSubject: map[string]string{},
		checkResult: map[string]bool{}, checkErr: map[string]error{}, checkKnown: map[string]bool{}, lastCheckSubject: map[string]string{}, lastCheck: map[string]cluster.NewSchema{},
	}
}

func (f *fakeSchemaServicer) ListSchemas(_ context.Context, name string, q appcluster.SchemaListQuery) (appcluster.SchemaPage, error) {
	f.lastQuery[name] = q
	if err, ok := f.listErr[name]; ok {
		return appcluster.SchemaPage{}, err
	}
	p, ok := f.listPages[name]
	if !ok {
		return appcluster.SchemaPage{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return p, nil
}

func (f *fakeSchemaServicer) LatestSchema(_ context.Context, name, subject string) (cluster.SchemaVersion, error) {
	f.lastLatestSubject[name] = subject
	if err, ok := f.latestErr[name]; ok {
		return cluster.SchemaVersion{}, err
	}
	sv, ok := f.latest[name]
	if !ok {
		return cluster.SchemaVersion{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return sv, nil
}

func (f *fakeSchemaServicer) SchemaByVersion(_ context.Context, name, _, version string) (cluster.SchemaVersion, error) {
	f.lastVersion[name] = version
	if err, ok := f.byVersionErr[name]; ok {
		return cluster.SchemaVersion{}, err
	}
	sv, ok := f.byVersion[name]
	if !ok {
		return cluster.SchemaVersion{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return sv, nil
}

func (f *fakeSchemaServicer) AllVersions(_ context.Context, name, _ string) ([]cluster.SchemaVersion, error) {
	if err, ok := f.allVersionsErr[name]; ok {
		return nil, err
	}
	svs, ok := f.allVersions[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return svs, nil
}

func (f *fakeSchemaServicer) Register(_ context.Context, name, subject string, ns cluster.NewSchema) (cluster.SchemaVersion, error) {
	f.lastRegisterSubject[name] = subject
	f.lastRegister[name] = ns
	if err, ok := f.registerErr[name]; ok {
		return cluster.SchemaVersion{}, err
	}
	sv, ok := f.registerResult[name]
	if !ok {
		return cluster.SchemaVersion{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return sv, nil
}

func (f *fakeSchemaServicer) DeleteSubject(_ context.Context, name, subject string) ([]int, error) {
	f.lastDeleteSubject[name] = subject
	if err, ok := f.deleteSubjectErr[name]; ok {
		return nil, err
	}
	if !f.deleteSubjectKnown[name] {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return f.deleteSubjectResult[name], nil
}

func (f *fakeSchemaServicer) DeleteVersion(_ context.Context, name, subject, version string) (int, error) {
	f.lastDeleteVersion[name] = subject + "@" + version
	if err, ok := f.deleteVersionErr[name]; ok {
		return 0, err
	}
	if !f.deleteVersionKnown[name] {
		return 0, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return f.deleteVersionResult[name], nil
}

func (f *fakeSchemaServicer) GlobalCompat(_ context.Context, name string) (string, error) {
	if err, ok := f.globalCompatErr[name]; ok {
		return "", err
	}
	lvl, ok := f.globalCompat[name]
	if !ok {
		return "", fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return lvl, nil
}

func (f *fakeSchemaServicer) SetGlobalCompat(_ context.Context, name, level string) error {
	f.lastSetGlobal[name] = level
	if err, ok := f.setGlobalErr[name]; ok {
		return err
	}
	if !f.setGlobalKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return nil
}

func (f *fakeSchemaServicer) SetSubjectCompat(_ context.Context, name, subject, level string) error {
	f.lastSetSubject[name] = subject + "@" + level
	if err, ok := f.setSubjectErr[name]; ok {
		return err
	}
	if !f.setSubjectKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return nil
}

func (f *fakeSchemaServicer) CheckCompat(_ context.Context, name, subject string, ns cluster.NewSchema) (bool, error) {
	f.lastCheckSubject[name] = subject
	f.lastCheck[name] = ns
	if err, ok := f.checkErr[name]; ok {
		return false, err
	}
	if !f.checkKnown[name] {
		return false, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return f.checkResult[name], nil
}

var _ api.SchemaServicer = (*fakeSchemaServicer)(nil)

func withSchemas(fs *fakeSchemaServicer) testServerOption {
	return func(d *api.Deps) { d.Schemas = fs }
}

func sampleSchemaVersion(subject string, version int) cluster.SchemaVersion {
	return cluster.SchemaVersion{
		ID: 42, Subject: subject, Version: version,
		Schema: `{"type":"string"}`, SchemaType: "AVRO", CompatLevel: "BACKWARD",
	}
}

func TestGetSchemas(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.listPages["prod"] = appcluster.SchemaPage{
		Schemas:   []cluster.SchemaVersion{sampleSchemaVersion("orders-value", 1)},
		PageCount: 2,
	}
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/schemas?page=2&perPage=10&search=ord&sortOrder=DESC", &got)
	require.Equal(t, 200, code)
	require.Equal(t, float64(2), got["pageCount"])
	schemas, ok := got["schemas"].([]any)
	require.True(t, ok)
	require.Len(t, schemas, 1)
	s0 := schemas[0].(map[string]any)
	require.Equal(t, "orders-value", s0["subject"])
	require.Equal(t, "1", s0["version"]) // SchemaSubject.version is a string in the contract
	require.Equal(t, float64(42), s0["id"])
	require.Equal(t, "AVRO", s0["schemaType"])
	require.Equal(t, "BACKWARD", s0["compatibilityLevel"])
	validateAgainstContract(t, req, code, hdr, body)

	require.Equal(t, appcluster.SchemaListQuery{Page: 2, PerPage: 10, Search: "ord", SortOrder: "DESC"}, fs.lastQuery["prod"])
}

func TestGetSchemasUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/schemas", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetSchemasUnconfiguredRegistryIs404(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.listErr["prod"] = fmt.Errorf("%w: internal configuration details", appcluster.ErrSchemaRegistryNotConfigured)
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/schemas", nil)
	require.Equal(t, http.StatusNotFound, code)
	assertErrorEnvelope(t, body, "schema registry not configured", "internal configuration details")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetSchemasBackendFailureIs500(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.listErr["prod"] = fmt.Errorf("sr boom")
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/schemas", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to list schemas", "sr boom") // underlying error never leaked
}

func TestGetLatestSchema(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.latest["prod"] = sampleSchemaVersion("orders-value", 3)
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/schemas/orders-value/latest", &got)
	require.Equal(t, 200, code)
	require.Equal(t, "orders-value", got["subject"])
	require.Equal(t, "3", got["version"])
	require.Equal(t, "AVRO", got["schemaType"])
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, "orders-value", fs.lastLatestSubject["prod"])
}

func TestGetLatestSchemaUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/schemas/s/latest", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetSchemaByVersion(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.byVersion["prod"] = sampleSchemaVersion("orders-value", 2)
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/schemas/orders-value/versions/2", &got)
	require.Equal(t, 200, code)
	require.Equal(t, "2", got["version"])
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, "2", fs.lastVersion["prod"]) // int32 path param forwarded as a numeric string
}

func TestGetSchemaByVersionUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/schemas/s/versions/1", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetAllVersionsBySubject(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.allVersions["prod"] = []cluster.SchemaVersion{
		sampleSchemaVersion("orders-value", 1),
		sampleSchemaVersion("orders-value", 2),
	}
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/schemas/orders-value/versions", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 2)
	require.Equal(t, "1", got[0]["version"])
	require.Equal(t, "2", got[1]["version"])
	require.Equal(t, "orders-value", got[0]["subject"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetAllVersionsBySubjectUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/schemas/s/versions", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

// --- Task 4: write endpoints + readOnly-403 ---

func TestCreateNewSchema(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.registerResult["prod"] = sampleSchemaVersion("orders-value", 1)
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	reqBody := `{"subject":"orders-value","schema":"{\"type\":\"string\"}","schemaType":"AVRO"}`
	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/schemas", reqBody)
	require.Equal(t, 200, code)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "orders-value", got["subject"])
	require.Equal(t, "1", got["version"])
	validateAgainstContract(t, req, code, hdr, body)

	require.Equal(t, "orders-value", fs.lastRegisterSubject["prod"])
	require.Equal(t, cluster.NewSchema{Schema: `{"type":"string"}`, SchemaType: "AVRO"}, fs.lastRegister["prod"])
}

func TestCreateNewSchemaBadBodyIs400(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.registerResult["prod"] = sampleSchemaVersion("orders-value", 1)
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	// No validateAgainstContract: kin-openapi's own request validation would
	// reject this malformed body itself (same reason the broker-config
	// invalid-body test skips it).
	_, code, _, _ := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/schemas", `not json`)
	require.Equal(t, 400, code)
	require.Empty(t, fs.lastRegisterSubject["prod"]) // decode failed before Register
}

func TestCreateNewSchemaReadOnlyIs403(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.registerResult["prod"] = sampleSchemaVersion("orders-value", 1)
	srv := newTestServer(withSchemas(fs), withReadOnly("prod"))
	defer srv.Close()

	reqBody := `{"subject":"orders-value","schema":"{\"type\":\"string\"}","schemaType":"AVRO"}`
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/schemas", reqBody)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fs.lastRegisterSubject["prod"]) // guard rejects before the handler
}

func TestCreateNewSchemaUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	_, code, _, _ := bodyJSON(t, http.MethodPost, srv, "/api/clusters/nope/schemas", `{"subject":"s","schema":"{}","schemaType":"AVRO"}`)
	require.Equal(t, 404, code)
}

func TestDeleteSchema(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.deleteSubjectKnown["prod"] = true
	fs.deleteSubjectResult["prod"] = []int{1, 2}
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/schemas/orders-value", nil)
	require.Equal(t, 204, code)
	require.Empty(t, body)
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, "orders-value", fs.lastDeleteSubject["prod"])
}

func TestDeleteSchemaReadOnlyIs403(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.deleteSubjectKnown["prod"] = true
	srv := newTestServer(withSchemas(fs), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/schemas/orders-value", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fs.lastDeleteSubject["prod"])
}

func TestDeleteSchemaUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/nope/schemas/s", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestDeleteLatestSchema(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.deleteVersionKnown["prod"] = true
	fs.deleteVersionResult["prod"] = 3
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/schemas/orders-value/latest", nil)
	require.Equal(t, 204, code)
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, "orders-value@latest", fs.lastDeleteVersion["prod"]) // deleteLatest forwards the "latest" sentinel
}

func TestDeleteLatestSchemaReadOnlyIs403(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.deleteVersionKnown["prod"] = true
	srv := newTestServer(withSchemas(fs), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, _ := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/schemas/orders-value/latest", nil)
	require.Equal(t, 403, code)
	require.Empty(t, fs.lastDeleteVersion["prod"])
}

func TestDeleteSchemaByVersion(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.deleteVersionKnown["prod"] = true
	fs.deleteVersionResult["prod"] = 2
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/schemas/orders-value/versions/2", nil)
	require.Equal(t, 204, code)
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, "orders-value@2", fs.lastDeleteVersion["prod"]) // int32 path param forwarded as a numeric string
}

func TestDeleteSchemaByVersionReadOnlyIs403(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.deleteVersionKnown["prod"] = true
	srv := newTestServer(withSchemas(fs), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, _ := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/schemas/orders-value/versions/2", nil)
	require.Equal(t, 403, code)
	require.Empty(t, fs.lastDeleteVersion["prod"])
}

// --- Task 5: compatibility endpoints ---

func TestGetGlobalSchemaCompatibilityLevel(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.globalCompat["prod"] = "FULL"
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/schemas/compatibility", &got)
	require.Equal(t, 200, code)
	require.Equal(t, "FULL", got["compatibility"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetGlobalSchemaCompatibilityLevelUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/schemas/compatibility", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestUpdateGlobalSchemaCompatibilityLevel(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.setGlobalKnown["prod"] = true
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/schemas/compatibility", `{"compatibility":"BACKWARD"}`)
	require.Equal(t, 204, code)
	require.Empty(t, body)
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, "BACKWARD", fs.lastSetGlobal["prod"])
}

func TestUpdateGlobalSchemaCompatibilityLevelReadOnlyIs403(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.setGlobalKnown["prod"] = true
	srv := newTestServer(withSchemas(fs), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/schemas/compatibility", `{"compatibility":"BACKWARD"}`)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fs.lastSetGlobal["prod"])
}

func TestUpdateGlobalSchemaCompatibilityLevelBadBodyIs400(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.setGlobalKnown["prod"] = true
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	_, code, _, _ := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/schemas/compatibility", `not json`)
	require.Equal(t, 400, code)
	require.Empty(t, fs.lastSetGlobal["prod"])
}

func TestUpdateGlobalSchemaCompatibilityLevelUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	_, code, _, _ := bodyJSON(t, http.MethodPut, srv, "/api/clusters/nope/schemas/compatibility", `{"compatibility":"BACKWARD"}`)
	require.Equal(t, 404, code)
}

func TestUpdateSchemaCompatibilityLevel(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.setSubjectKnown["prod"] = true
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/schemas/orders-value/compatibility", `{"compatibility":"FORWARD"}`)
	require.Equal(t, 204, code)
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, "orders-value@FORWARD", fs.lastSetSubject["prod"])
}

func TestUpdateSchemaCompatibilityLevelReadOnlyIs403(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.setSubjectKnown["prod"] = true
	srv := newTestServer(withSchemas(fs), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, _ := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/schemas/orders-value/compatibility", `{"compatibility":"FORWARD"}`)
	require.Equal(t, 403, code)
	require.Empty(t, fs.lastSetSubject["prod"])
}

func TestCheckSchemaCompatibility(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.checkKnown["prod"] = true
	fs.checkResult["prod"] = true
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	reqBody := `{"subject":"orders-value","schema":"{\"type\":\"string\"}","schemaType":"AVRO"}`
	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/schemas/orders-value/check", reqBody)
	require.Equal(t, 200, code)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, true, got["isCompatible"])
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, "orders-value", fs.lastCheckSubject["prod"])
}

// TestCheckSchemaCompatibilityReadOnlyIsAllowed locks in that
// checkSchemaCompatibility — a POST, but a read-only-in-effect dry run — is
// whitelisted past readOnlyGuard: a read-only cluster still gets a 200 result,
// not a 403 (contrast every other schema write endpoint's readOnly-403 test).
func TestCheckSchemaCompatibilityReadOnlyIsAllowed(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.checkKnown["prod"] = true
	fs.checkResult["prod"] = false
	srv := newTestServer(withSchemas(fs), withReadOnly("prod"))
	defer srv.Close()

	reqBody := `{"subject":"orders-value","schema":"{\"type\":\"string\"}","schemaType":"AVRO"}`
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/schemas/orders-value/check", reqBody)
	require.Equal(t, 200, code) // whitelisted → not 403
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, false, got["isCompatible"])
	require.Equal(t, "orders-value", fs.lastCheckSubject["prod"]) // reached the handler
}

func TestCheckSchemaCompatibilityUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	_, code, _, _ := bodyJSON(t, http.MethodPost, srv, "/api/clusters/nope/schemas/s/check", `{"subject":"s","schema":"{}","schemaType":"AVRO"}`)
	require.Equal(t, 404, code)
}

// --- edge branches: references round-trip, delete/update 404s, backend 500s ---

func TestCreateNewSchemaWithReferences(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.registerResult["prod"] = sampleSchemaVersion("orders-value", 1)
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	reqBody := `{"subject":"orders-value","schema":"{\"type\":\"string\"}","schemaType":"AVRO",` +
		`"references":[{"name":"com.example.Other","subject":"other-value","version":2}]}`
	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/schemas", reqBody)
	require.Equal(t, 200, code)
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, []cluster.SchemaReference{{Name: "com.example.Other", Subject: "other-value", Version: 2}}, fs.lastRegister["prod"].References)
}

func TestGetLatestSchemaRendersReferences(t *testing.T) {
	fs := newFakeSchemaServicer()
	sv := sampleSchemaVersion("orders-value", 3)
	sv.References = []cluster.SchemaReference{{Name: "com.example.Other", Subject: "other-value", Version: 2}}
	fs.latest["prod"] = sv
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/schemas/orders-value/latest", &got)
	require.Equal(t, 200, code)
	refs, ok := got["references"].([]any)
	require.True(t, ok)
	require.Len(t, refs, 1)
	require.Equal(t, "other-value", refs[0].(map[string]any)["subject"])
	require.Equal(t, float64(2), refs[0].(map[string]any)["version"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetLatestSchemaBackendFailureIs500(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.latestErr["prod"] = fmt.Errorf("sr boom")
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/schemas/s/latest", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to get latest schema", "sr boom")
}

func TestDeleteLatestSchemaUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/nope/schemas/s/latest", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestDeleteSchemaByVersionUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/nope/schemas/s/versions/1", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestDeleteSchemaByVersionBackendFailureIs500(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.deleteVersionErr["prod"] = fmt.Errorf("sr boom")
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/schemas/s/versions/1", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to delete schema version", "sr boom")
}

func TestGetGlobalSchemaCompatibilityLevelBackendFailureIs500(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.globalCompatErr["prod"] = fmt.Errorf("sr boom")
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/schemas/compatibility", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to get global compatibility level", "sr boom")
}

func TestUpdateSchemaCompatibilityLevelUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	_, code, _, _ := bodyJSON(t, http.MethodPut, srv, "/api/clusters/nope/schemas/s/compatibility", `{"compatibility":"FORWARD"}`)
	require.Equal(t, 404, code)
}

func TestUpdateSchemaCompatibilityLevelBadBodyIs400(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.setSubjectKnown["prod"] = true
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()
	_, code, _, _ := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/schemas/s/compatibility", `not json`)
	require.Equal(t, 400, code)
	require.Empty(t, fs.lastSetSubject["prod"])
}

func TestCheckSchemaCompatibilityBadBodyIs400(t *testing.T) {
	fs := newFakeSchemaServicer()
	fs.checkKnown["prod"] = true
	srv := newTestServer(withSchemas(fs))
	defer srv.Close()
	_, code, _, _ := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/schemas/s/check", `not json`)
	require.Equal(t, 400, code)
	require.Empty(t, fs.lastCheckSubject["prod"])
}
