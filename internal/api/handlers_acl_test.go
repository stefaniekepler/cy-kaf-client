package api_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

type fakeAclServicer struct {
	known        map[string]bool
	list         []cluster.AclBinding
	filter       cluster.AclFilter
	deleteCount  int
	createCalled bool
	syncCalled   bool
	writeCalled  bool
	formatCalled bool
	writeErr     error
	syncErr      error // 覆写 SyncCSV 返回值：注入 ErrBadAclCSV（→400）或后端错误（→500）
	formatErr    error // 覆写 FormatAclCSV 返回错误（getAclAsCsv 500 路径，可选）
	formatted    []cluster.AclBinding
	consumerSpec appcluster.ConsumerAclSpec
	producerSpec appcluster.ProducerAclSpec
	streamSpec   appcluster.StreamAppAclSpec
}

func (f *fakeAclServicer) resolve(name string) error {
	if f.known[name] {
		return nil
	}
	return appcluster.ErrUnknownCluster
}
func (f *fakeAclServicer) List(_ context.Context, name string, filter cluster.AclFilter) ([]cluster.AclBinding, error) {
	if err := f.resolve(name); err != nil {
		return nil, err
	}
	f.filter = filter
	return f.list, nil
}
func (f *fakeAclServicer) FormatAclCSV(bindings []cluster.AclBinding) (string, error) {
	f.formatCalled = true
	f.formatted = bindings
	if f.formatErr != nil {
		return "", f.formatErr
	}
	return new(appcluster.AclService).FormatAclCSV(bindings)
}
func (f *fakeAclServicer) CreateAcl(_ context.Context, name string, _ cluster.AclBinding) error {
	f.createCalled = true
	f.writeCalled = true
	if err := f.resolve(name); err != nil {
		return err
	}
	return f.writeErr
}
func (f *fakeAclServicer) DeleteAcl(_ context.Context, name string, _ cluster.AclBinding) (int, error) {
	f.writeCalled = true
	if err := f.resolve(name); err != nil {
		return 0, err
	}
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.deleteCount, nil
}
func (f *fakeAclServicer) CreateConsumerAcl(_ context.Context, name string, spec appcluster.ConsumerAclSpec) error {
	f.createCalled = true
	f.writeCalled = true
	f.consumerSpec = spec
	if err := f.resolve(name); err != nil {
		return err
	}
	return f.writeErr
}
func (f *fakeAclServicer) CreateProducerAcl(_ context.Context, name string, spec appcluster.ProducerAclSpec) error {
	f.createCalled = true
	f.writeCalled = true
	f.producerSpec = spec
	if err := f.resolve(name); err != nil {
		return err
	}
	return f.writeErr
}
func (f *fakeAclServicer) CreateStreamAppAcl(_ context.Context, name string, spec appcluster.StreamAppAclSpec) error {
	f.createCalled = true
	f.writeCalled = true
	f.streamSpec = spec
	if err := f.resolve(name); err != nil {
		return err
	}
	return f.writeErr
}
func (f *fakeAclServicer) SyncCSV(_ context.Context, name string, _ string) error {
	f.syncCalled = true
	f.writeCalled = true
	if err := f.resolve(name); err != nil {
		return err
	}
	return f.syncErr
}

func withAcls(f *fakeAclServicer) func(*api.Deps) {
	return func(d *api.Deps) { d.Acls = f }
}

func bodyText(t *testing.T, method string, srvURL, path, reqBody string) (*http.Request, int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, srvURL+path, strings.NewReader(reqBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	req.Body, err = req.GetBody()
	require.NoError(t, err)
	return req, resp.StatusCode, resp.Header, body
}

func TestListAcls200(t *testing.T) {
	f := &fakeAclServicer{known: map[string]bool{"c1": true}, list: []cluster.AclBinding{
		{Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
	}}
	srv := newTestServer(withAcls(f))
	defer srv.Close()
	var out []map[string]any
	req, code, hdr, body := doJSON(t, http.MethodGet, srv, "/api/clusters/c1/acls", &out)
	require.Equal(t, http.StatusOK, code)
	require.Len(t, out, 1)
	require.Equal(t, map[string]any{
		"resourceType": "TOPIC", "resourceName": "t", "namePatternType": "LITERAL",
		"principal": "User:a", "host": "*", "operation": "READ", "permission": "ALLOW",
	}, out[0])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestListAclsUnknownCluster404(t *testing.T) {
	// newTestServer's default fake has a nil known map; nil and empty maps both
	// resolve to unknown, matching every other default API fake.
	srv := newTestServer()
	defer srv.Close()
	_, code, _, _ := doJSON(t, http.MethodGet, srv, "/api/clusters/nope/acls", nil)
	require.Equal(t, http.StatusNotFound, code)
}

func TestListAclsMapsQueryFilter(t *testing.T) {
	f := &fakeAclServicer{known: map[string]bool{"c1": true}}
	srv := newTestServer(withAcls(f))
	defer srv.Close()
	_, code, _, _ := doJSON(t, http.MethodGet, srv,
		"/api/clusters/c1/acls?resourceType=TOPIC&resourceName=orders&namePatternType=PREFIXED&search=alice&fts=true", nil)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, cluster.AclFilter{
		ResourceType: "TOPIC", ResourceName: "orders", PatternType: "PREFIXED", Search: "alice", Fts: true,
	}, f.filter)
}

func TestGetAclAsCsvColumnOrder(t *testing.T) {
	f := &fakeAclServicer{known: map[string]bool{"c1": true}, list: []cluster.AclBinding{
		{Principal: "User:a", Host: "*", ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
	}}
	srv := newTestServer(withAcls(f))
	defer srv.Close()
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/clusters/c1/acls/csv", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(resp.Body)
	lines := strings.SplitN(buf.String(), "\n", 2)
	require.Equal(t, "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host", lines[0])
	require.True(t, f.formatCalled)
	require.Equal(t, f.list, f.formatted)
	validateAgainstContract(t, req, resp.StatusCode, resp.Header, buf.Bytes())
}

func TestCreateAcl204(t *testing.T) {
	srv := newTestServer(withAcls(&fakeAclServicer{known: map[string]bool{"c1": true}}))
	defer srv.Close()
	body := `{"resourceType":"TOPIC","resourceName":"t","namePatternType":"LITERAL","principal":"User:a","host":"*","operation":"READ","permission":"ALLOW"}`
	req, code, hdr, respBody := bodyJSON(t, http.MethodPost, srv, "/api/clusters/c1/acls", body)
	require.Equal(t, http.StatusNoContent, code)
	require.Empty(t, respBody)
	validateAgainstContract(t, req, code, hdr, respBody)
}

func TestCreateAclBadBody400(t *testing.T) {
	srv := newTestServer(withAcls(&fakeAclServicer{known: map[string]bool{"c1": true}}))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/c1/acls", "{not json")
	require.Equal(t, http.StatusBadRequest, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
	// Response 400 is contract-declared, but validateAgainstContract also validates
	// this intentionally malformed request first; that request validation must fail.
}

func TestAclJSONWritesRejectNullOrMultipleValuesWithoutService(t *testing.T) {
	validAcl := `{"resourceType":"TOPIC","resourceName":"t","namePatternType":"LITERAL","principal":"User:a","host":"*","operation":"READ","permission":"ALLOW"}`
	for _, endpoint := range []struct {
		name, method, path, valid string
	}{
		{name: "create", method: http.MethodPost, path: "/api/clusters/c1/acls", valid: validAcl},
		{name: "delete", method: http.MethodDelete, path: "/api/clusters/c1/acls", valid: validAcl},
		{name: "consumer", method: http.MethodPost, path: "/api/clusters/c1/acls/consumer", valid: `{"principal":"User:a","topics":["t"]}`},
		{name: "producer", method: http.MethodPost, path: "/api/clusters/c1/acls/producer", valid: `{"principal":"User:a","topics":["t"]}`},
		{name: "streamapp", method: http.MethodPost, path: "/api/clusters/c1/acls/streamapp", valid: `{"principal":"User:a","inputTopics":["t"]}`},
	} {
		for _, payload := range []struct {
			name, body string
		}{
			{name: "null", body: "null"},
			{name: "multiple", body: endpoint.valid + endpoint.valid},
		} {
			t.Run(endpoint.name+"/"+payload.name, func(t *testing.T) {
				f := &fakeAclServicer{known: map[string]bool{"c1": true}}
				srv := newTestServer(withAcls(f))
				defer srv.Close()

				req, code, hdr, body := bodyJSON(t, endpoint.method, srv, endpoint.path, payload.body)
				require.Equal(t, http.StatusBadRequest, code)
				assertErrorEnvelope(t, body, "invalid request body", "")
				require.False(t, f.writeCalled)
				validateResponseOnlyAgainstContract(t, req, code, hdr, body)
			})
		}
	}
}

func TestAclHelpersRequirePrincipalPointerBeforeService(t *testing.T) {
	for _, path := range []string{
		"/api/clusters/c1/acls/consumer",
		"/api/clusters/c1/acls/producer",
		"/api/clusters/c1/acls/streamapp",
	} {
		t.Run(path, func(t *testing.T) {
			f := &fakeAclServicer{known: map[string]bool{"c1": true}}
			srv := newTestServer(withAcls(f))
			defer srv.Close()
			req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, path, `{}`)
			require.Equal(t, http.StatusBadRequest, code)
			assertErrorEnvelope(t, body, "invalid acl request", "")
			require.False(t, f.writeCalled)
			validateResponseOnlyAgainstContract(t, req, code, hdr, body)
		})
	}
}

func TestAclWriteBadRequestMaps400(t *testing.T) {
	validAcl := `{"resourceType":"TOPIC","resourceName":"t","namePatternType":"LITERAL","principal":"User:a","host":"*","operation":"READ","permission":"ALLOW"}`
	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodPost, "/api/clusters/c1/acls", validAcl},
		{http.MethodDelete, "/api/clusters/c1/acls", validAcl},
		{http.MethodPost, "/api/clusters/c1/acls/consumer", `{"principal":"User:a","topics":["t"]}`},
		{http.MethodPost, "/api/clusters/c1/acls/producer", `{"principal":"User:a","topics":["t"]}`},
		{http.MethodPost, "/api/clusters/c1/acls/streamapp", `{"principal":"User:a","inputTopics":["t"]}`},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			f := &fakeAclServicer{known: map[string]bool{"c1": true}, writeErr: fmt.Errorf("%w: rejected", appcluster.ErrBadAclRequest)}
			srv := newTestServer(withAcls(f))
			defer srv.Close()
			req, code, hdr, body := bodyJSON(t, tc.method, srv, tc.path, tc.body)
			require.Equal(t, http.StatusBadRequest, code)
			assertErrorEnvelope(t, body, "invalid acl request", "")
			validateResponseOnlyAgainstContract(t, req, code, hdr, body)
		})
	}
}

func TestAclWriteBodiesOver10MiBReturn400WithoutService(t *testing.T) {
	huge := strings.Repeat("a", (10<<20)+1)
	for _, tc := range []struct {
		name, ctype, body string
	}{
		{
			name: "json", ctype: "application/json",
			body: `{"resourceType":"TOPIC","resourceName":"t","namePatternType":"LITERAL","principal":"User:` + huge + `","host":"*","operation":"READ","permission":"ALLOW"}`,
		},
		{
			name: "csv", ctype: "text/plain",
			body: "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n" +
				"User:a,TOPIC,LITERAL," + huge + ",READ,ALLOW,*\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeAclServicer{known: map[string]bool{"c1": true}}
			srv := newTestServer(withAcls(f))
			defer srv.Close()
			path := "/api/clusters/c1/acls"
			if tc.name == "csv" {
				path += "/csv"
			}
			req, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(tc.body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", tc.ctype)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assertErrorEnvelope(t, body, "invalid request body", "")
			require.False(t, f.writeCalled)
		})
	}
}

func TestDeleteAclNoMatch404(t *testing.T) {
	f := &fakeAclServicer{known: map[string]bool{"c1": true}, deleteCount: 0}
	srv := newTestServer(withAcls(f))
	defer srv.Close()
	body := `{"resourceType":"TOPIC","resourceName":"t","namePatternType":"LITERAL","principal":"User:a","host":"*","operation":"READ","permission":"ALLOW"}`
	req, code, hdr, respBody := bodyJSON(t, http.MethodDelete, srv, "/api/clusters/c1/acls", body)
	require.Equal(t, http.StatusNotFound, code)
	assertErrorEnvelope(t, respBody, "no matching acl", "")
	validateAgainstContract(t, req, code, hdr, respBody)
}

func TestDeleteAclMatch204(t *testing.T) {
	f := &fakeAclServicer{known: map[string]bool{"c1": true}, deleteCount: 1}
	srv := newTestServer(withAcls(f))
	defer srv.Close()
	body := `{"resourceType":"TOPIC","resourceName":"t","namePatternType":"LITERAL","principal":"User:a","host":"*","operation":"READ","permission":"ALLOW"}`
	req, code, hdr, respBody := bodyJSON(t, http.MethodDelete, srv, "/api/clusters/c1/acls", body)
	require.Equal(t, http.StatusNoContent, code)
	require.Empty(t, respBody)
	validateAgainstContract(t, req, code, hdr, respBody)
}

func TestCreateAclHelpers204(t *testing.T) {
	cases := []struct {
		name, path, body string
		check            func(*fakeAclServicer)
	}{
		{
			name: "consumer", path: "/api/clusters/c1/acls/consumer", body: `{"principal":"User:a","topics":["orders"]}`,
			check: func(f *fakeAclServicer) {
				require.Equal(t, appcluster.ConsumerAclSpec{Principal: "User:a", Host: "*", Topics: []string{"orders"}}, f.consumerSpec)
			},
		},
		{
			name: "producer", path: "/api/clusters/c1/acls/producer",
			body: `{"principal":"User:a","host":"host-a","topics":["orders"],"topicsPrefix":"pre-","transactionalId":"tx","transactionsIdPrefix":"tx-","idempotent":true}`,
			check: func(f *fakeAclServicer) {
				require.Equal(t, appcluster.ProducerAclSpec{Principal: "User:a", Host: "host-a", Topics: []string{"orders"}, TopicsPrefix: "pre-", TransactionalID: "tx", TransactionsIDPrefix: "tx-", Idempotent: true}, f.producerSpec)
			},
		},
		{
			name: "streamapp", path: "/api/clusters/c1/acls/streamapp",
			body: `{"principal":"User:a","host":"host-a","inputTopics":["in"],"outputTopics":["out"],"applicationId":"app"}`,
			check: func(f *fakeAclServicer) {
				require.Equal(t, appcluster.StreamAppAclSpec{Principal: "User:a", Host: "host-a", InputTopics: []string{"in"}, OutputTopics: []string{"out"}, ApplicationID: "app"}, f.streamSpec)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeAclServicer{known: map[string]bool{"c1": true}}
			srv := newTestServer(withAcls(f))
			defer srv.Close()
			req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, tc.path, tc.body)
			require.Equal(t, http.StatusNoContent, code)
			require.Empty(t, body)
			require.True(t, f.createCalled)
			tc.check(f)
			validateAgainstContract(t, req, code, hdr, body)
		})
	}
}

func TestSyncAclsCsv204(t *testing.T) {
	f := &fakeAclServicer{known: map[string]bool{"c1": true}}
	srv := newTestServer(withAcls(f))
	defer srv.Close()
	req, code, hdr, body := bodyText(t, http.MethodPost, srv.URL, "/api/clusters/c1/acls/csv", "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n")
	require.Equal(t, http.StatusNoContent, code)
	require.Empty(t, body)
	require.True(t, f.syncCalled)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestSyncAclsCsvBadCsv400(t *testing.T) {
	// SyncCSV 返回 wrap 了 ErrBadAclCSV 的错误（app 层解析/校验失败）→ api 400。
	// 真实解析逻辑由 app 层单测锁定；此处只验 api 的错误分流。
	f := &fakeAclServicer{known: map[string]bool{"c1": true}, syncErr: fmt.Errorf("%w: row 2 must have 7 columns, got 2", appcluster.ErrBadAclCSV)}
	srv := newTestServer(withAcls(f))
	defer srv.Close()
	req, code, hdr, body := bodyText(t, http.MethodPost, srv.URL, "/api/clusters/c1/acls/csv", "bad,row\n")
	require.Equal(t, http.StatusBadRequest, code)
	assertErrorEnvelope(t, body, "invalid acl csv", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestSyncAclsCsvRejectsInvalidPayloadBeforeService(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "empty", body: ""},
		{name: "only whitespace", body: "\n \t\n"},
		{name: "wrong header", body: "Principal,ResourceType,PatternType,ResourceName,Operation,Permission,Host\n"},
		{name: "blank data field", body: "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\nUser:a,TOPIC,LITERAL, ,READ,ALLOW,*\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeAclServicer{known: map[string]bool{"c1": true}}
			srv := newTestServer(withAcls(f))
			defer srv.Close()

			_, code, _, body := bodyText(t, http.MethodPost, srv.URL, "/api/clusters/c1/acls/csv", tc.body)
			require.Equal(t, http.StatusBadRequest, code)
			assertErrorEnvelope(t, body, "invalid acl csv", "")
			require.False(t, f.syncCalled)
		})
	}
}

func TestSyncAclsCsvBackendError500(t *testing.T) {
	// sync 内部 CreateAcls/DeleteAcls 后端失败（非 ErrBadAclCSV / 非 ErrUnknownCluster）
	// → 500，证不再被吞成 400（修正 2 的核心回归）。
	f := &fakeAclServicer{known: map[string]bool{"c1": true}, syncErr: errors.New("kadm: create acls failed")}
	srv := newTestServer(withAcls(f))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/clusters/c1/acls/csv",
		strings.NewReader("Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\nUser:a,TOPIC,LITERAL,t,READ,ALLOW,*\n"))
	req.Header.Set("Content-Type", "text/plain")
	resp, _ := http.DefaultClient.Do(req)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// 6 个写端点 readOnly-403（断言服务未被调）。表驱动：
func TestAclWriteEndpointsReadOnly403(t *testing.T) {
	cases := []struct {
		method, path, body, ctype string
	}{
		{http.MethodPost, "/api/clusters/ro/acls", `{"resourceType":"TOPIC","resourceName":"t","namePatternType":"LITERAL","principal":"User:a","host":"*","operation":"READ","permission":"ALLOW"}`, "application/json"},
		{http.MethodDelete, "/api/clusters/ro/acls", `{"resourceType":"TOPIC","resourceName":"t","namePatternType":"LITERAL","principal":"User:a","host":"*","operation":"READ","permission":"ALLOW"}`, "application/json"},
		{http.MethodPost, "/api/clusters/ro/acls/consumer", `{"principal":"User:a"}`, "application/json"},
		{http.MethodPost, "/api/clusters/ro/acls/producer", `{"principal":"User:a"}`, "application/json"},
		{http.MethodPost, "/api/clusters/ro/acls/streamapp", `{"principal":"User:a"}`, "application/json"},
		{http.MethodPost, "/api/clusters/ro/acls/csv", "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n", "text/plain"},
	}
	for _, tc := range cases {
		f := &fakeAclServicer{known: map[string]bool{"ro": true}}
		srv := newTestServer(withAcls(f), withReadOnly("ro"))
		req, _ := http.NewRequest(tc.method, srv.URL+tc.path, strings.NewReader(tc.body))
		req.Header.Set("Content-Type", tc.ctype)
		resp, _ := http.DefaultClient.Do(req)
		require.Equal(t, http.StatusForbidden, resp.StatusCode, tc.path)
		_ = resp.Body.Close()
		require.False(t, f.createCalled, tc.path)
		require.False(t, f.syncCalled, tc.path)
		require.False(t, f.writeCalled, tc.path)
		srv.Close()
	}
}
