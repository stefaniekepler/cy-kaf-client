package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// fakeConfigServicer is a deterministic api.ConfigServicer: Current/Validate
// return caller-preset values and Validate records the snapshot it received, so
// handler tests can assert both the HTTP response and the request->snapshot
// mapping without a real config store or broker.
type fakeConfigServicer struct {
	current     cluster.ConfigSnapshot
	currentErr  error
	validation  cluster.ConfigValidation
	validateErr error
	lastSnap    cluster.ConfigSnapshot
	validateHit bool

	relatedLoc     string
	relatedErr     error
	lastRelName    string
	lastRelContent []byte
}

func (f *fakeConfigServicer) Current() (cluster.ConfigSnapshot, error) {
	return f.current, f.currentErr
}

func (f *fakeConfigServicer) Validate(_ context.Context, snap cluster.ConfigSnapshot) (cluster.ConfigValidation, error) {
	f.validateHit = true
	f.lastSnap = snap
	return f.validation, f.validateErr
}

func (f *fakeConfigServicer) SaveRelatedFile(_ context.Context, name string, content []byte) (string, error) {
	f.lastRelName, f.lastRelContent = name, content
	return f.relatedLoc, f.relatedErr
}

func withConfig(fc *fakeConfigServicer) testServerOption {
	return func(d *api.Deps) { d.Config = fc }
}

// fakeReloader is a deterministic api.ReloaderServicer recording the snapshot
// Apply received and returning a preset error.
type fakeReloader struct {
	err      error
	applied  bool
	lastSnap cluster.ConfigSnapshot

	importErr       error
	imported        bool
	importedContent []byte
}

func (f *fakeReloader) Apply(_ context.Context, snap cluster.ConfigSnapshot) error {
	f.applied = true
	f.lastSnap = snap
	return f.err
}

func (f *fakeReloader) Import(_ context.Context, content []byte) error {
	f.imported = true
	f.importedContent = content
	return f.importErr
}

func withReloader(fr *fakeReloader) testServerOption {
	return func(d *api.Deps) { d.Reloader = fr }
}

// --- getCurrentConfig (GET /api/config) ---

// TestGetCurrentConfig_WrapsRawUnderProperties proves the running config's Raw
// tree is returned verbatim under `properties`, and the whole body validates
// against the ApplicationConfig contract schema.
func TestGetCurrentConfig_WrapsRawUnderProperties(t *testing.T) {
	fc := &fakeConfigServicer{current: cluster.ConfigSnapshot{
		Raw: map[string]any{
			"kafka": map[string]any{
				"clusters": []any{
					map[string]any{"name": "prod", "bootstrapServers": "k:9092"},
				},
			},
		},
	}}
	srv := newTestServer(withConfig(fc))
	defer srv.Close()

	var out struct {
		Properties struct {
			Kafka struct {
				Clusters []struct {
					Name             string `json:"name"`
					BootstrapServers string `json:"bootstrapServers"`
				} `json:"clusters"`
			} `json:"kafka"`
		} `json:"properties"`
	}
	req, code, hdr, body := getJSON(t, srv, "/api/config", &out)
	require.Equal(t, 200, code)
	require.Len(t, out.Properties.Kafka.Clusters, 1)
	require.Equal(t, "prod", out.Properties.Kafka.Clusters[0].Name)
	require.Equal(t, "k:9092", out.Properties.Kafka.Clusters[0].BootstrapServers)
	validateAgainstContract(t, req, code, hdr, body)
}

// TestGetCurrentConfig_StoreErrorIs500 proves a read failure is a 500 with no
// leaked internal detail (serverError's contract).
func TestGetCurrentConfig_StoreErrorIs500(t *testing.T) {
	fc := &fakeConfigServicer{currentErr: errors.New("read config: /etc/secret.yaml permission denied")}
	srv := newTestServer(withConfig(fc))
	defer srv.Close()

	_, code, _, body := getJSON(t, srv, "/api/config", nil)
	require.Equal(t, 500, code)
	require.NotContains(t, string(body), "secret.yaml")
}

// --- validateConfig (PUT /api/config/validated) ---

// TestValidateConfig_EchoesPerClusterVerdict proves the request's clusters are
// mapped into the snapshot handed to Validate, and the domain verdict is mapped
// back into ApplicationConfigValidation (contract-validated).
func TestValidateConfig_EchoesPerClusterVerdict(t *testing.T) {
	fc := &fakeConfigServicer{validation: cluster.ConfigValidation{
		Clusters: map[string]cluster.ClusterValidation{
			"prod": {Kafka: cluster.PropertyValidation{Error: true, ErrorMessage: "dial tcp: connection refused"}},
		},
	}}
	srv := newTestServer(withConfig(fc))
	defer srv.Close()

	var out struct {
		Clusters map[string]struct {
			Kafka struct {
				Error        bool    `json:"error"`
				ErrorMessage *string `json:"errorMessage"`
			} `json:"kafka"`
		} `json:"clusters"`
	}
	req, code, hdr, body := bodyJSON(t, http.MethodPut, srv, "/api/config/validated",
		`{"properties":{"kafka":{"clusters":[{"name":"prod","bootstrapServers":"k1:9092,k2:9092"}]}}}`)
	require.Equal(t, 200, code)
	require.NoError(t, json.Unmarshal(body, &out))
	require.True(t, out.Clusters["prod"].Kafka.Error)
	require.NotNil(t, out.Clusters["prod"].Kafka.ErrorMessage)
	require.Contains(t, *out.Clusters["prod"].Kafka.ErrorMessage, "connection refused")

	require.True(t, fc.validateHit)
	require.Len(t, fc.lastSnap.Clusters, 1)
	require.Equal(t, "prod", fc.lastSnap.Clusters[0].Name)
	require.Equal(t, []string{"k1:9092", "k2:9092"}, fc.lastSnap.Clusters[0].Conn.BootstrapServers)
	validateAgainstContract(t, req, code, hdr, body)
}

// TestValidateConfig_StoreErrorIs500 covers the unexpected probe-layer error
// path: a Validate error is a 500 with no leaked detail (per-cluster
// reachability failures do NOT take this path -- they are a 200 verdict). Not
// contract-validated: 500 is undeclared for this operation, same convention as
// the other backend-error-500 tests in this package.
func TestValidateConfig_StoreErrorIs500(t *testing.T) {
	fc := &fakeConfigServicer{validateErr: errors.New("probe wiring boom: nil client")}
	srv := newTestServer(withConfig(fc))
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPut, srv, "/api/config/validated",
		`{"properties":{"kafka":{"clusters":[{"name":"prod","bootstrapServers":"k:9092"}]}}}`)
	require.Equal(t, 500, code)
	require.NotContains(t, string(body), "nil client")
}

// TestValidateConfig_MalformedBodyIs400 proves a body that won't parse is a
// pre-call 400 -- Validate must never run.
func TestValidateConfig_MalformedBodyIs400(t *testing.T) {
	fc := &fakeConfigServicer{}
	srv := newTestServer(withConfig(fc))
	defer srv.Close()

	_, code, _, _ := bodyJSON(t, http.MethodPut, srv, "/api/config/validated", `{not json`)
	require.Equal(t, 400, code)
	require.False(t, fc.validateHit, "Validate must never be called on a malformed body")
}

// --- restartWithConfig (PUT /api/config) ---

// TestRestartWithConfig_AppliesSnapshotAnd204 proves a well-formed RestartRequest
// reaches Reloader.Apply as a snapshot carrying both the parsed clusters and the
// raw properties tree (for Save fidelity), and the endpoint reports 204.
func TestRestartWithConfig_AppliesSnapshotAnd204(t *testing.T) {
	fr := &fakeReloader{}
	srv := newTestServer(withReloader(fr))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPut, srv, "/api/config",
		`{"config":{"properties":{"auth":{"type":"OAUTH2"},"kafka":{"clusters":[{"name":"prod","bootstrapServers":"k1:9092,k2:9092"}]}}}}`)
	require.Equal(t, 204, code)
	require.Empty(t, body)

	require.True(t, fr.applied)
	require.Len(t, fr.lastSnap.Clusters, 1)
	require.Equal(t, "prod", fr.lastSnap.Clusters[0].Name)
	require.Equal(t, []string{"k1:9092", "k2:9092"}, fr.lastSnap.Clusters[0].Conn.BootstrapServers)
	// Raw preserves the unmodeled auth section for Save.
	require.Contains(t, fr.lastSnap.Raw, "auth")
	require.Contains(t, fr.lastSnap.Raw, "kafka")
	validateAgainstContract(t, req, code, hdr, body)
}

// TestRestartWithConfig_MapsSupportedWizardSectionsToRuntimeDefinition proves
// that saving through the full cluster wizard changes the in-process
// Definition immediately. Persisting the raw JSON alone is insufficient:
// otherwise SR/Connect/KSQL/serde/masking/read-only settings only start
// working after a manual application restart.
func TestRestartWithConfig_MapsSupportedWizardSectionsToRuntimeDefinition(t *testing.T) {
	fr := &fakeReloader{}
	srv := newTestServer(withReloader(fr))
	defer srv.Close()

	_, code, _, _ := bodyJSON(t, http.MethodPut, srv, "/api/config", `{
	  "config": {
	    "properties": {
	      "kafka": {
	        "clusters": [{
	          "name": "prod",
	          "bootstrapServers": "k1:9092,k2:9092",
	          "readOnly": true,
	          "properties": {
	            "security.protocol": "SASL_SSL",
	            "sasl.mechanism": "PLAIN"
	          },
	          "ssl": {
	            "truststoreLocation": "/cfg/kafka-ca.pem",
	            "truststorePassword": "kafka-secret"
	          },
	          "schemaRegistry": "https://sr:8081",
	          "schemaRegistryAuth": {"username": "sr-user", "password": "sr-pass"},
	          "schemaRegistrySsl": {
	            "keystoreLocation": "/cfg/sr.jks",
	            "keystorePassword": "sr-secret"
	          },
	          "kafkaConnect": [{
	            "name": "main",
	            "address": "https://connect:8083",
	            "username": "connect-user",
	            "password": "connect-pass",
	            "keystoreLocation": "/cfg/connect.jks",
	            "keystorePassword": "connect-secret"
	          }],
	          "ksqldbServer": "https://ksql:8088",
	          "ksqldbServerAuth": {"username": "ksql-user", "password": "ksql-pass"},
	          "ksqldbServerSsl": {
	            "keystoreLocation": "/cfg/ksql.jks",
	            "keystorePassword": "ksql-secret"
	          },
	          "serde": [{
	            "name": "Int64",
	            "topicKeysPattern": "^orders$",
	            "topicValuesPattern": "^events$",
	            "properties": {"mode": "strict"}
	          }],
	          "defaultKeySerde": "Int64",
	          "defaultValueSerde": "String",
	          "masking": [{
	            "type": "REPLACE",
	            "fields": ["password"],
	            "replacement": "***",
	            "topicValuesPattern": "^users$"
	          }],
	          "pollingThrottleRate": 4096
	        }]
	      }
	    }
	  }
	}`)
	require.Equal(t, http.StatusNoContent, code)
	require.Len(t, fr.lastSnap.Clusters, 1)

	def := fr.lastSnap.Clusters[0]
	require.True(t, def.ReadOnly)
	require.Equal(t, []string{"k1:9092", "k2:9092"}, def.Conn.BootstrapServers)
	require.Equal(t, "SASL_SSL", def.Conn.Security["security.protocol"])
	require.Equal(t, "/cfg/kafka-ca.pem", def.Conn.Security["ssl.truststore.location"])
	require.Equal(t, "kafka-secret", def.Conn.Security["ssl.truststore.password"])

	require.Equal(t, "https://sr:8081", def.SchemaRegistry.URL)
	require.Equal(t, "sr-user", def.SchemaRegistry.Auth.Username)
	require.Equal(t, "/cfg/sr.jks", def.SchemaRegistry.SSL.KeystoreLocation)

	require.Len(t, def.Connects, 1)
	require.Equal(t, "main", def.Connects[0].Name)
	require.Equal(t, "connect-user", def.Connects[0].Auth.Username)
	require.Equal(t, "/cfg/connect.jks", def.Connects[0].SSL.KeystoreLocation)

	require.Equal(t, "https://ksql:8088", def.KsqlURL)
	require.Equal(t, "ksql-user", def.KsqlAuth.Username)
	require.Equal(t, "/cfg/ksql.jks", def.KsqlSSL.KeystoreLocation)

	require.Equal(t, "Int64", def.DefaultKeySerde)
	require.Equal(t, "String", def.DefaultValueSerde)
	require.Equal(t, int64(4096), def.PollingThrottleRate)
	require.Len(t, def.SerdeConfigs, 1)
	require.Equal(t, "^events$", def.SerdeConfigs[0].TopicValuesPattern)
	require.Equal(t, "strict", def.SerdeConfigs[0].Properties["mode"])
	require.Len(t, def.Maskings, 1)
	require.Equal(t, cluster.MaskReplace, def.Maskings[0].Type)
	require.Equal(t, "***", def.Maskings[0].Replacement)
}

// TestRestartWithConfig_MalformedBodyIs400 proves a body that won't parse is a
// pre-apply 400 -- Apply must never run.
func TestRestartWithConfig_MalformedBodyIs400(t *testing.T) {
	fr := &fakeReloader{}
	srv := newTestServer(withReloader(fr))
	defer srv.Close()

	_, code, _, _ := bodyJSON(t, http.MethodPut, srv, "/api/config", `{not json`)
	require.Equal(t, 400, code)
	require.False(t, fr.applied, "Apply must never run on a malformed body")
}

// TestRestartWithConfig_MissingConfigIs400 proves a RestartRequest with no
// config object is a 400.
func TestRestartWithConfig_MissingConfigIs400(t *testing.T) {
	fr := &fakeReloader{}
	srv := newTestServer(withReloader(fr))
	defer srv.Close()

	_, code, _, _ := bodyJSON(t, http.MethodPut, srv, "/api/config", `{}`)
	require.Equal(t, 400, code)
	require.False(t, fr.applied)
}

// TestRestartWithConfig_ApplyErrorIs500 proves a reload failure (validation
// rollback or persist failure) is a 500 with no leaked detail. Not
// contract-validated (500 undeclared for this op), same convention as the other
// backend-error-500 tests.
func TestRestartWithConfig_ApplyErrorIs500(t *testing.T) {
	fr := &fakeReloader{err: errors.New("new config rejected: cluster \"prod\" kafka unreachable: dial boom")}
	srv := newTestServer(withReloader(fr))
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPut, srv, "/api/config",
		`{"config":{"properties":{"kafka":{"clusters":[{"name":"prod","bootstrapServers":"k:9092"}]}}}}`)
	require.Equal(t, 500, code)
	require.NotContains(t, string(body), "dial boom")
}

// --- uploadConfigRelatedFile (POST /api/config/relatedfiles) ---

func multipartFile(t *testing.T, field, filename, content string) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile(field, filename)
	require.NoError(t, err)
	_, err = fw.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, mw.Close())
	return &buf, mw.FormDataContentType()
}

// TestUploadConfigRelatedFile_StoresAndReturnsLocation proves a multipart file
// upload reaches SaveRelatedFile with its filename+bytes and the returned
// location is echoed as UploadedFileInfo.
func TestUploadConfigRelatedFile_StoresAndReturnsLocation(t *testing.T) {
	fc := &fakeConfigServicer{relatedLoc: "/cfg/uploads/truststore.jks"}
	srv := newTestServer(withConfig(fc))
	defer srv.Close()

	buf, ctype := multipartFile(t, "file", "truststore.jks", "JKSBYTES")
	resp, err := http.Post(srv.URL+"/api/config/relatedfiles", ctype, buf)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, 200, resp.StatusCode)

	var out struct {
		Location string `json:"location"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Equal(t, "/cfg/uploads/truststore.jks", out.Location)
	require.Equal(t, "truststore.jks", fc.lastRelName)
	require.Equal(t, []byte("JKSBYTES"), fc.lastRelContent)
}

// TestUploadConfigRelatedFile_MissingFileIs400 proves a multipart form without a
// "file" part is a 400 -- SaveRelatedFile must never run.
func TestUploadConfigRelatedFile_MissingFileIs400(t *testing.T) {
	fc := &fakeConfigServicer{}
	srv := newTestServer(withConfig(fc))
	defer srv.Close()

	buf, ctype := multipartFile(t, "notfile", "x.txt", "data")
	resp, err := http.Post(srv.URL+"/api/config/relatedfiles", ctype, buf)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, 400, resp.StatusCode)
	require.Empty(t, fc.lastRelName, "SaveRelatedFile must never run without a file part")
}

// TestUploadConfigRelatedFile_SaveErrorIs400 proves a rejected filename (e.g.
// path traversal caught by the store) is surfaced as a 400.
func TestUploadConfigRelatedFile_SaveErrorIs400(t *testing.T) {
	fc := &fakeConfigServicer{relatedErr: errors.New("invalid related file name \"../evil\"")}
	srv := newTestServer(withConfig(fc))
	defer srv.Close()

	buf, ctype := multipartFile(t, "file", "../evil", "data")
	resp, err := http.Post(srv.URL+"/api/config/relatedfiles", ctype, buf)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, 400, resp.StatusCode)
}

// TestImportConfig_AppliesContentAnd204 proves a multipart config upload reaches
// Reloader.Import with the file bytes and the endpoint reports 204.
func TestImportConfig_AppliesContentAnd204(t *testing.T) {
	fr := &fakeReloader{}
	srv := newTestServer(withReloader(fr))
	defer srv.Close()

	buf, ctype := multipartFile(t, "file", "config.yaml", "kafka:\n  clusters: []\n")
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/config/import", buf)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ctype)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.True(t, fr.imported)
	require.Equal(t, []byte("kafka:\n  clusters: []\n"), fr.importedContent)
}

// TestImportConfig_InvalidConfigIs400 proves a schema-violating import maps
// cluster.ErrInvalidConfig to a 400.
func TestImportConfig_InvalidConfigIs400(t *testing.T) {
	fr := &fakeReloader{importErr: cluster.ErrInvalidConfig}
	srv := newTestServer(withReloader(fr))
	defer srv.Close()

	buf, ctype := multipartFile(t, "file", "config.yaml", "bad")
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/config/import", buf)
	require.NoError(t, err)
	req.Header.Set("Content-Type", ctype)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
