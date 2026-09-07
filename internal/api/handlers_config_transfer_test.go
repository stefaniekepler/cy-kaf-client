package api_test

import (
	"context"
	"errors"
	"github.com/cy-kaf/cy-kaf-client/internal/api"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"strings"
	"testing"
)

type fakeTransfer struct {
	err      error
	selected []int
	content  []byte
	revision string
}

func (f *fakeTransfer) Export() ([]byte, error) { return []byte("kafka:\n  clusters: []\n"), f.err }
func (f *fakeTransfer) Preview(content []byte) (cluster.ConfigImportPreview, error) {
	f.content = content
	return cluster.ConfigImportPreview{Revision: "revision", Entries: []cluster.ConfigImportEntry{}}, f.err
}
func (f *fakeTransfer) Import(_ context.Context, content []byte, selected []int, revision string) (cluster.ConfigImportResult, error) {
	f.content = content
	f.selected = selected
	f.revision = revision
	return cluster.ConfigImportResult{Added: 1, Replaced: 2, Skipped: 3}, f.err
}
func TestConfigTransferHTTP(t *testing.T) {
	f := &fakeTransfer{}
	srv := newTestServer(func(d *api.Deps) { d.ConfigTransfer = f })
	defer srv.Close()
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/api/config/export", "", 200},
		{"POST", "/api/config/import/preview", `{"content":"kafka: {clusters: []}"}`, 200},
		{"POST", "/api/config/import", `{"content":"kafka: {clusters: []}","selected":[0],"revision":"r"}`, 200},
		{"POST", "/api/config/import", `{"content":"yaml","revision":"r"}`, 400},
		{"POST", "/api/config/import/preview", `{"content":"yaml"} {}`, 400},
		{"POST", "/api/config/import/preview", `{"content":12}`, 400},
	} {
		req, err := http.NewRequest(tc.method, srv.URL+tc.path, strings.NewReader(tc.body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.NoError(t, res.Body.Close())
		require.Equal(t, tc.status, res.StatusCode, string(body))
		if tc.status == 200 {
			if req.GetBody != nil {
				req.Body, err = req.GetBody()
				require.NoError(t, err)
			}
			validateAgainstContract(t, req, res.StatusCode, res.Header, body)
		}
		require.Equal(t, "no-store", res.Header.Get("Cache-Control"))
		if tc.method == "GET" {
			require.Contains(t, res.Header.Get("Content-Disposition"), "attachment")
		}
	}
	require.Equal(t, []int{0}, f.selected)
	require.Equal(t, "r", f.revision)
}
func TestConfigTransferErrorsAreSafe(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
	}{
		{&cluster.ConfigImportError{Message: "invalid format"}, 400},
		{cluster.ErrConfigChanged, 409},
		{errors.New("private-file-secret"), 500},
	} {
		srv := newTestServer(func(d *api.Deps) { d.ConfigTransfer = &fakeTransfer{err: tc.err} })
		res, err := http.Post(srv.URL+"/api/config/import/preview", "application/json", strings.NewReader(`{"content":"yaml"}`))
		require.NoError(t, err)
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.NoError(t, res.Body.Close())
		srv.Close()
		require.Equal(t, tc.status, res.StatusCode)
		require.NotContains(t, string(body), "private-file-secret")
	}
}
