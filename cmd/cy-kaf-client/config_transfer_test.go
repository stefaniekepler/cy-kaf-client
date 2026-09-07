package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/infra/config"
	"github.com/stretchr/testify/require"
)

func TestConfigTransferWiringPersistsReviewedSelectionAndRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := []byte("server: {port: 8080}\nkafka:\n  clusters:\n    - {name: keep, bootstrapServers: 'keep.invalid:9092'}\n    - {name: prod, bootstrapServers: 'old.invalid:9092'}\n")
	require.NoError(t, os.WriteFile(path, initial, 0600))
	cfg, err := config.Load(path, true)
	require.NoError(t, err)
	// Cancel the background state cache before wiring, so this configuration-only
	// round trip never connects to brokers. HTTP request contexts remain live.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	services, err := wireServices(ctx, cfg, path)
	require.NoError(t, err)
	defer services.Cleanup()
	srv := httptest.NewServer(api.NewServer(services.apiDeps(nil)))
	defer srv.Close()
	request := func(method, path string, body any) (int, []byte) {
		t.Helper()
		data, err := json.Marshal(body)
		require.NoError(t, err)
		req, err := http.NewRequest(method, srv.URL+path, bytes.NewReader(data))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		output, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.NoError(t, res.Body.Close())
		return res.StatusCode, output
	}
	content := "kafka:\n  clusters:\n    - name: prod\n      bootstrapServers: new.invalid:9092\n      ssl: {truststoreLocation: /fixture/ca.pem}\n      readOnly: true\n      custom: preserved\n    - {name: added, bootstrapServers: 'added.invalid:9092'}\n"
	code, body := request("POST", "/api/config/import/preview", map[string]any{"content": content})
	require.Equal(t, 200, code, string(body))
	var preview cluster.ConfigImportPreview
	require.NoError(t, json.Unmarshal(body, &preview))
	require.Len(t, preview.Entries[0].Conflicts, 1)
	unchanged, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, initial, unchanged)
	code, body = request("POST", "/api/config/import", map[string]any{"content": content, "revision": preview.Revision, "selected": []int{0, 1}})
	require.Equal(t, 200, code, string(body))
	require.JSONEq(t, `{"added":1,"replaced":1,"skipped":0}`, string(body))
	code, exported := request("GET", "/api/config/export", nil)
	require.Equal(t, 200, code)
	require.NotContains(t, string(exported), "server:")
	require.Contains(t, string(exported), "custom: preserved")
	parsed, err := (config.TransferCodec{}).Parse(exported)
	require.NoError(t, err)
	require.Equal(t, parsed.Clusters, services.Resolver.Definitions())
	reloaded, err := config.Load(path, true)
	require.NoError(t, err)
	require.Len(t, reloaded.Kafka.Clusters, 3)
	require.True(t, reloaded.Kafka.Clusters[1].ReadOnly)
	def, err := reloaded.Kafka.Clusters[1].ToDomain()
	require.NoError(t, err)
	require.Equal(t, "/fixture/ca.pem", def.Conn.Security["ssl.truststore.location"])
	code, body = request("POST", "/api/config/import", map[string]any{"content": content, "revision": preview.Revision, "selected": []int{0, 1}})
	require.Equal(t, 409, code, string(body))
}
