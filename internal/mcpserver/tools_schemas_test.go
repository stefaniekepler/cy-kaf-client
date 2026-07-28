package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

type schemaCall struct {
	name      string
	cluster   string
	subject   string
	version   string
	level     string
	query     appcluster.SchemaListQuery
	newSchema domaincluster.NewSchema
}

type recordingSchemaApp struct {
	mu sync.Mutex

	page           appcluster.SchemaPage
	latest         domaincluster.SchemaVersion
	byVersion      domaincluster.SchemaVersion
	allVersions    []domaincluster.SchemaVersion
	globalCompat   string
	compatible     bool
	registerResult domaincluster.SchemaVersion
	deletedSubject []int
	deletedVersion int
	errs           map[string]error
	calls          []schemaCall
}

func newRecordingSchemaApp() *recordingSchemaApp {
	version := schemaFixture(42, "orders-value", 2)
	return &recordingSchemaApp{
		page: appcluster.SchemaPage{
			Schemas:   []domaincluster.SchemaVersion{version},
			PageCount: 3,
		},
		latest:         version,
		byVersion:      version,
		allVersions:    []domaincluster.SchemaVersion{version, schemaFixture(41, "orders-value", 1)},
		globalCompat:   "FULL",
		compatible:     true,
		registerResult: version,
		deletedSubject: []int{2, 1},
		deletedVersion: 2,
		errs:           make(map[string]error),
	}
}

func (f *recordingSchemaApp) ListSchemas(_ context.Context, cluster string, query appcluster.SchemaListQuery) (appcluster.SchemaPage, error) {
	f.record(schemaCall{name: "list", cluster: cluster, query: query})
	return cloneSchemaPage(f.page), f.operationError("list")
}

func (f *recordingSchemaApp) LatestSchema(_ context.Context, cluster, subject string) (domaincluster.SchemaVersion, error) {
	f.record(schemaCall{name: "latest", cluster: cluster, subject: subject})
	return cloneSchemaVersion(f.latest), f.operationError("latest")
}

func (f *recordingSchemaApp) SchemaByVersion(_ context.Context, cluster, subject, version string) (domaincluster.SchemaVersion, error) {
	f.record(schemaCall{name: "byVersion", cluster: cluster, subject: subject, version: version})
	return cloneSchemaVersion(f.byVersion), f.operationError("byVersion")
}

func (f *recordingSchemaApp) GlobalCompat(_ context.Context, cluster string) (string, error) {
	f.record(schemaCall{name: "globalCompat", cluster: cluster})
	return f.globalCompat, f.operationError("globalCompat")
}

func (f *recordingSchemaApp) SetGlobalCompat(_ context.Context, cluster, level string) error {
	f.record(schemaCall{name: "setGlobalCompat", cluster: cluster, level: level})
	return f.operationError("setGlobalCompat")
}

func (f *recordingSchemaApp) SetSubjectCompat(_ context.Context, cluster, subject, level string) error {
	f.record(schemaCall{name: "setSubjectCompat", cluster: cluster, subject: subject, level: level})
	return f.operationError("setSubjectCompat")
}

func (f *recordingSchemaApp) CheckCompat(_ context.Context, cluster, subject string, schema domaincluster.NewSchema) (bool, error) {
	f.record(schemaCall{
		name: "checkCompat", cluster: cluster, subject: subject, newSchema: cloneNewSchema(schema),
	})
	return f.compatible, f.operationError("checkCompat")
}

func (f *recordingSchemaApp) Register(_ context.Context, cluster, subject string, schema domaincluster.NewSchema) (domaincluster.SchemaVersion, error) {
	f.record(schemaCall{
		name: "register", cluster: cluster, subject: subject, newSchema: cloneNewSchema(schema),
	})
	return cloneSchemaVersion(f.registerResult), f.operationError("register")
}

func (f *recordingSchemaApp) DeleteSubject(_ context.Context, cluster, subject string) ([]int, error) {
	f.record(schemaCall{name: "deleteSubject", cluster: cluster, subject: subject})
	return append([]int(nil), f.deletedSubject...), f.operationError("deleteSubject")
}

func (f *recordingSchemaApp) DeleteVersion(_ context.Context, cluster, subject, version string) (int, error) {
	f.record(schemaCall{name: "deleteVersion", cluster: cluster, subject: subject, version: version})
	return f.deletedVersion, f.operationError("deleteVersion")
}

func (f *recordingSchemaApp) AllVersions(_ context.Context, cluster, subject string) ([]domaincluster.SchemaVersion, error) {
	f.record(schemaCall{name: "allVersions", cluster: cluster, subject: subject})
	output := make([]domaincluster.SchemaVersion, len(f.allVersions))
	for index := range f.allVersions {
		output[index] = cloneSchemaVersion(f.allVersions[index])
	}
	return output, f.operationError("allVersions")
}

func (f *recordingSchemaApp) record(call schemaCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *recordingSchemaApp) operationError(operation string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errs[operation]
}

func (f *recordingSchemaApp) snapshot() []schemaCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]schemaCall(nil), f.calls...)
}

func TestSchemaToolsDelegateAllTwelveOperationsWithExactSDKResults(t *testing.T) {
	schemaJSON := `{"type":"record","name":"Order"}`
	schemaResult := `{"compatibilityLevel":"BACKWARD","id":42,"references":[{"name":"common.proto","subject":"common-value","version":1}],"schema":"{\"type\":\"record\",\"name\":\"Order\"}","schemaType":"AVRO","subject":"orders-value","version":"2"}`
	versionOneResult := `{"compatibilityLevel":"BACKWARD","id":41,"references":[{"name":"common.proto","subject":"common-value","version":1}],"schema":"{\"type\":\"record\",\"name\":\"Order\"}","schemaType":"AVRO","subject":"orders-value","version":"1"}`
	newSchemaBody := map[string]any{
		"subject":    "orders-value",
		"schema":     schemaJSON,
		"schemaType": "AVRO",
		"references": []any{map[string]any{
			"name": "common.proto", "subject": "common-value", "version": float64(1),
		}},
	}
	tests := []struct {
		name       string
		input      map[string]any
		wantAccess AccessClass
		wantCall   schemaCall
		wantText   string
	}{
		{
			name: "checkSchemaCompatibility",
			input: map[string]any{
				"clusterName": "prod", "subject": "orders-value", "body": newSchemaBody,
			},
			wantAccess: AccessReadOnly,
			wantCall: schemaCall{
				name: "checkCompat", cluster: "prod", subject: "orders-value",
				newSchema: schemaNewSchemaFixture(),
			},
			wantText: `{"isCompatible":true}`,
		},
		{
			name:       "getAllVersionsBySubject",
			input:      map[string]any{"clusterName": "prod", "subject": "orders-value"},
			wantAccess: AccessReadOnly,
			wantCall:   schemaCall{name: "allVersions", cluster: "prod", subject: "orders-value"},
			wantText:   `{"result":[` + versionOneResult + `,` + schemaResult + `]}`,
		},
		{
			name:       "getGlobalSchemaCompatibilityLevel",
			input:      map[string]any{"clusterName": "prod"},
			wantAccess: AccessReadOnly,
			wantCall:   schemaCall{name: "globalCompat", cluster: "prod"},
			wantText:   `{"compatibility":"FULL"}`,
		},
		{
			name:       "getLatestSchema",
			input:      map[string]any{"clusterName": "prod", "subject": "orders-value"},
			wantAccess: AccessReadOnly,
			wantCall:   schemaCall{name: "latest", cluster: "prod", subject: "orders-value"},
			wantText:   schemaResult,
		},
		{
			name: "getSchemaByVersion",
			input: map[string]any{
				"clusterName": "prod", "subject": "orders-value", "version": "2",
			},
			wantAccess: AccessReadOnly,
			wantCall: schemaCall{
				name: "byVersion", cluster: "prod", subject: "orders-value", version: "2",
			},
			wantText: schemaResult,
		},
		{
			name:       "getSchemas",
			input:      map[string]any{"clusterName": "prod"},
			wantAccess: AccessReadOnly,
			wantCall: schemaCall{
				name: "list", cluster: "prod",
				query: appcluster.SchemaListQuery{Page: 1, PerPage: 25},
			},
			wantText: `{"pageCount":3,"schemas":[` + schemaResult + `]}`,
		},
		{
			name: "createNewSchema",
			input: map[string]any{
				"clusterName": "prod", "body": newSchemaBody,
			},
			wantAccess: AccessWrite,
			wantCall: schemaCall{
				name: "register", cluster: "prod", subject: "orders-value",
				newSchema: schemaNewSchemaFixture(),
			},
			wantText: schemaResult,
		},
		{
			name:       "deleteLatestSchema",
			input:      map[string]any{"clusterName": "prod", "subject": "orders-value"},
			wantAccess: AccessWrite,
			wantCall: schemaCall{
				name: "deleteVersion", cluster: "prod", subject: "orders-value", version: "latest",
			},
			wantText: `{"status":"deleted","deletedVersion":2}`,
		},
		{
			name:       "deleteSchema",
			input:      map[string]any{"clusterName": "prod", "subject": "orders-value"},
			wantAccess: AccessWrite,
			wantCall:   schemaCall{name: "deleteSubject", cluster: "prod", subject: "orders-value"},
			wantText:   `{"status":"deleted","deletedVersions":[1,2]}`,
		},
		{
			name: "deleteSchemaByVersion",
			input: map[string]any{
				"clusterName": "prod", "subject": "orders-value", "version": "2",
			},
			wantAccess: AccessWrite,
			wantCall: schemaCall{
				name: "deleteVersion", cluster: "prod", subject: "orders-value", version: "2",
			},
			wantText: `{"status":"deleted","deletedVersion":2}`,
		},
		{
			name: "updateGlobalSchemaCompatibilityLevel",
			input: map[string]any{
				"clusterName": "prod",
				"body":        map[string]any{"compatibility": "BACKWARD"},
			},
			wantAccess: AccessWrite,
			wantCall: schemaCall{
				name: "setGlobalCompat", cluster: "prod", level: "BACKWARD",
			},
			wantText: `{"status":"updated","compatibility":"BACKWARD"}`,
		},
		{
			name: "updateSchemaCompatibilityLevel",
			input: map[string]any{
				"clusterName": "prod", "subject": "orders-value",
				"body": map[string]any{"compatibility": "FORWARD"},
			},
			wantAccess: AccessWrite,
			wantCall: schemaCall{
				name: "setSubjectCompat", cluster: "prod", subject: "orders-value", level: "FORWARD",
			},
			wantText: `{"status":"updated","compatibility":"FORWARD"}`,
		},
	}
	require.Len(t, tests, 12)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingSchemaApp()
			executor, _ := newSchemaExecutor(t, app, &recordingReadOnlyResolver{}, true)
			spec := requireCatalogSpec(t, test.name)
			require.Equal(t, test.wantAccess, spec.Meta.Access)

			result, err := newSDKSession(t, spec, executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{Name: test.name, Arguments: test.input},
			)
			require.NoError(t, err)
			require.False(t, result.IsError, callToolText(t, result))
			require.Equal(t, test.wantText, callToolText(t, result))
			requireStructuredJSONEq(t, test.wantText, result.StructuredContent)
			require.Equal(t, []schemaCall{test.wantCall}, app.snapshot())
		})
	}
}

func TestSchemaToolsKeepExactlySixReadsDefaultVisible(t *testing.T) {
	executor, store := newSchemaExecutor(
		t, newRecordingSchemaApp(), &recordingReadOnlyResolver{}, false,
	)
	session := newSDKCatalogSession(t, VisibleCatalog(*mustLoadPolicy(t, store)), executor)
	listed, err := session.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 51)

	for _, name := range []string{
		"checkSchemaCompatibility",
		"getAllVersionsBySubject",
		"getGlobalSchemaCompatibilityLevel",
		"getLatestSchema",
		"getSchemaByVersion",
		"getSchemas",
	} {
		require.NotNil(t, findTool(t, listed.Tools, name))
	}
	for _, name := range []string{
		"createNewSchema",
		"deleteLatestSchema",
		"deleteSchema",
		"deleteSchemaByVersion",
		"updateGlobalSchemaCompatibilityLevel",
		"updateSchemaCompatibilityLevel",
	} {
		require.Nil(t, findToolOrNil(listed.Tools, name))
	}
}

func TestSchemaToolsCompatibilityCheckNeedsNoWritePermissionAndAllowsReadOnlyCluster(t *testing.T) {
	app := newRecordingSchemaApp()
	resolver := &recordingReadOnlyResolver{readOnly: true}
	executor, _ := newSchemaExecutor(t, app, resolver, false)
	result, err := newSDKSession(
		t, requireCatalogSpec(t, "checkSchemaCompatibility"), executor,
	).CallTool(context.Background(), &mcp.CallToolParams{
		Name: "checkSchemaCompatibility",
		Arguments: map[string]any{
			"clusterName": "prod",
			"subject":     "orders-value",
			"body": map[string]any{
				"subject": "orders-value", "schema": `{}`, "schemaType": "JSON",
			},
		},
	})
	require.NoError(t, err)
	require.False(t, result.IsError, callToolText(t, result))
	require.Equal(t, `{"isCompatible":true}`, callToolText(t, result))
	require.Equal(t, []schemaCall{{
		name: "checkCompat", cluster: "prod", subject: "orders-value",
		newSchema: domaincluster.NewSchema{Schema: `{}`, SchemaType: "JSON"},
	}}, app.snapshot())
	require.Empty(t, resolver.recordedClusters())
}

func TestSchemaToolsCompatibilityRequiresMatchingPathAndBodySubjects(t *testing.T) {
	t.Run("equal subjects keep the schema payload meaningful", func(t *testing.T) {
		app := newRecordingSchemaApp()
		result := callSchemaTool(t, app, "checkSchemaCompatibility", map[string]any{
			"clusterName": "prod",
			"subject":     "orders-value",
			"body": map[string]any{
				"subject": "orders-value", "schema": `{"type":"string"}`, "schemaType": "AVRO",
			},
		}, false)
		require.Equal(t, `{"isCompatible":true}`, callToolText(t, result))
		require.Equal(t, []schemaCall{{
			name: "checkCompat", cluster: "prod", subject: "orders-value",
			newSchema: domaincluster.NewSchema{
				Schema: `{"type":"string"}`, SchemaType: "AVRO",
			},
		}}, app.snapshot())
	})

	t.Run("mismatched subjects are rejected before delegation", func(t *testing.T) {
		app := newRecordingSchemaApp()
		result := callSchemaToolError(t, app, "checkSchemaCompatibility", map[string]any{
			"clusterName": "prod",
			"subject":     "orders-value",
			"body": map[string]any{
				"subject": "payments-value", "schema": `{"type":"string"}`, "schemaType": "AVRO",
			},
		}, false)
		require.Equal(t, "invalid_request", callToolText(t, result))
		require.Empty(t, app.snapshot())
	})
}

func TestSchemaToolsBoundPaginationSchemaBodiesAndReferences(t *testing.T) {
	t.Run("pagination and deterministic subject order", func(t *testing.T) {
		app := newRecordingSchemaApp()
		app.page = appcluster.SchemaPage{
			Schemas: []domaincluster.SchemaVersion{
				schemaFixture(9, "zeta-value", 1),
				schemaFixture(3, "alpha-value", 2),
			},
			PageCount: 4,
		}
		result := callSchemaTool(t, app, "getSchemas", map[string]any{
			"clusterName": "prod",
			"query": map[string]any{
				"page": float64(2), "perPage": float64(100), "search": "value",
				"orderBy": "SUBJECT", "sortOrder": "DESC", "fts": false,
			},
		}, false)
		calls := app.snapshot()
		require.Equal(t, []schemaCall{{
			name: "list", cluster: "prod",
			query: appcluster.SchemaListQuery{
				Page: 2, PerPage: 100, Search: "value", SortOrder: "DESC",
			},
		}}, calls)
		var subjects []string
		var decoded generated.SchemaSubjectsResponse
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &decoded))
		require.NotNil(t, decoded.Schemas)
		for _, schema := range *decoded.Schemas {
			subjects = append(subjects, schema.Subject)
		}
		require.Equal(t, []string{"zeta-value", "alpha-value"}, subjects)
		require.Equal(t, int32(4), *decoded.PageCount)
	})

	t.Run("invalid pagination is rejected before delegation", func(t *testing.T) {
		tests := []map[string]any{
			{"page": float64(0)},
			{"perPage": float64(0)},
			{"perPage": float64(101)},
			{"search": strings.Repeat("s", 1025)},
			{"orderBy": "PASSWORD"},
			{"sortOrder": "SIDEWAYS"},
		}
		for _, query := range tests {
			app := newRecordingSchemaApp()
			result := callSchemaToolError(t, app, "getSchemas", map[string]any{
				"clusterName": "prod", "query": query,
			}, false)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Empty(t, app.snapshot())
		}
	})

	t.Run("oversized response schema and nested references are rejected", func(t *testing.T) {
		app := newRecordingSchemaApp()
		app.latest.Schema = strings.Repeat("s", (256<<10)+1)
		result := callSchemaToolError(t, app, "getLatestSchema", map[string]any{
			"clusterName": "prod", "subject": "orders-value",
		}, false)
		require.Equal(t, "result_too_large", callToolText(t, result))

		app = newRecordingSchemaApp()
		app.latest.References = make([]domaincluster.SchemaReference, 101)
		result = callSchemaToolError(t, app, "getLatestSchema", map[string]any{
			"clusterName": "prod", "subject": "orders-value",
		}, false)
		require.Equal(t, "result_too_large", callToolText(t, result))
	})

	t.Run("overflowing page metadata is rejected", func(t *testing.T) {
		app := newRecordingSchemaApp()
		app.page.PageCount = int(int64(math.MaxInt32) + 1)
		result := callSchemaToolError(t, app, "getSchemas", map[string]any{
			"clusterName": "prod",
		}, false)
		require.Equal(t, "result_too_large", callToolText(t, result))
	})
}

func TestSchemaToolsRejectUnsupportedFTSBeforeDelegation(t *testing.T) {
	t.Run("true is meaningful and unsupported", func(t *testing.T) {
		app := newRecordingSchemaApp()
		result := callSchemaToolError(t, app, "getSchemas", map[string]any{
			"clusterName": "prod",
			"query":       map[string]any{"fts": true},
		}, false)
		require.Equal(t, "invalid_request", callToolText(t, result))
		require.Empty(t, app.snapshot())
	})

	t.Run("explicit false remains a no-op", func(t *testing.T) {
		app := newRecordingSchemaApp()
		result := callSchemaTool(t, app, "getSchemas", map[string]any{
			"clusterName": "prod",
			"query":       map[string]any{"fts": false},
		}, false)
		require.Equal(t, int32(3), schemaPageCountFromResult(t, result))
		require.Equal(t, []schemaCall{{
			name: "list", cluster: "prod",
			query: appcluster.SchemaListQuery{Page: 1, PerPage: 25},
		}}, app.snapshot())
	})
}

func TestSchemaToolsValidateEveryWriteBeforeApplicationPort(t *testing.T) {
	largeSchema := strings.Repeat("s", (256<<10)+1)
	manyReferences := make([]any, 101)
	for index := range manyReferences {
		manyReferences[index] = map[string]any{
			"name": fmt.Sprintf("ref-%d", index), "subject": "common", "version": float64(1),
		}
	}
	tests := []struct {
		name  string
		input map[string]any
	}{
		{
			name: "createNewSchema",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"subject": " ", "schema": `{}`, "schemaType": "JSON",
			}},
		},
		{
			name: "createNewSchema",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"subject": "orders", "schema": "", "schemaType": "JSON",
			}},
		},
		{
			name: "createNewSchema",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"subject": "orders", "schema": largeSchema, "schemaType": "JSON",
			}},
		},
		{
			name: "createNewSchema",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"subject": "orders", "schema": `{}`, "schemaType": "XML",
			}},
		},
		{
			name: "createNewSchema",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"subject": "orders", "schema": `{}`, "schemaType": "JSON",
				"references": manyReferences,
			}},
		},
		{
			name: "createNewSchema",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"subject": "orders", "schema": `{}`, "schemaType": "JSON",
				"references": []any{map[string]any{
					"name": " ", "subject": "common", "version": float64(1),
				}},
			}},
		},
		{
			name: "createNewSchema",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"subject": "orders", "schema": `{}`, "schemaType": "JSON",
				"references": []any{map[string]any{
					"name": "common", "subject": " ", "version": float64(1),
				}},
			}},
		},
		{
			name: "createNewSchema",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"subject": "orders", "schema": `{}`, "schemaType": "JSON",
				"references": []any{map[string]any{
					"name": "common", "subject": "common", "version": float64(0),
				}},
			}},
		},
		{
			name:  "deleteLatestSchema",
			input: map[string]any{"clusterName": "prod", "subject": " "},
		},
		{
			name:  "deleteSchema",
			input: map[string]any{"clusterName": "prod", "subject": strings.Repeat("s", 1025)},
		},
		{
			name: "deleteSchemaByVersion",
			input: map[string]any{
				"clusterName": "prod", "subject": "orders", "version": "latest",
			},
		},
		{
			name: "deleteSchemaByVersion",
			input: map[string]any{
				"clusterName": "prod", "subject": "orders",
				"version": fmt.Sprint(int64(math.MaxInt32) + 1),
			},
		},
		{
			name: "updateGlobalSchemaCompatibilityLevel",
			input: map[string]any{
				"clusterName": "prod", "body": map[string]any{"compatibility": ""},
			},
		},
		{
			name: "updateSchemaCompatibilityLevel",
			input: map[string]any{
				"clusterName": "prod", "subject": "orders",
				"body": map[string]any{"compatibility": "SIDEWAYS"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingSchemaApp()
			result := callSchemaToolError(t, app, test.name, test.input, true)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Empty(t, app.snapshot())
		})
	}
}

func TestSchemaToolsApplyWriteAndReadOnlyGatesBeforeDelegation(t *testing.T) {
	writes := validSchemaWriteInputs()
	for name, input := range writes {
		t.Run(name+"/writes-disabled", func(t *testing.T) {
			app := newRecordingSchemaApp()
			result := callSchemaToolError(t, app, name, input, false)
			require.Equal(t, "writes_disabled", callToolText(t, result))
			require.Empty(t, app.snapshot())
		})
		t.Run(name+"/read-only-cluster", func(t *testing.T) {
			app := newRecordingSchemaApp()
			resolver := &recordingReadOnlyResolver{readOnly: true}
			executor, _ := newSchemaExecutor(t, app, resolver, true)
			result, err := newSDKSession(t, requireCatalogSpec(t, name), executor).CallTool(
				context.Background(), &mcp.CallToolParams{Name: name, Arguments: input},
			)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "cluster_read_only", callToolText(t, result))
			require.Equal(t, []string{"prod"}, resolver.recordedClusters())
			require.Empty(t, app.snapshot())
		})
	}

	t.Run("stale session reloads policy", func(t *testing.T) {
		app := newRecordingSchemaApp()
		executor, store := newSchemaExecutor(t, app, &recordingReadOnlyResolver{}, true)
		session := newSDKSession(t, requireCatalogSpec(t, "createNewSchema"), executor)
		params := &mcp.CallToolParams{
			Name: "createNewSchema", Arguments: writes["createNewSchema"],
		}
		first, err := session.CallTool(context.Background(), params)
		require.NoError(t, err)
		require.False(t, first.IsError, callToolText(t, first))
		require.NoError(t, store.Save(context.Background(), *policy(true, false)))
		second, err := session.CallTool(context.Background(), params)
		require.NoError(t, err)
		require.True(t, second.IsError)
		require.Equal(t, "writes_disabled", callToolText(t, second))
		require.Len(t, app.snapshot(), 1)
	})
}

func TestSchemaToolsMapUnconfiguredAndHideRegistryErrors(t *testing.T) {
	t.Run("known unconfigured sentinel", func(t *testing.T) {
		app := newRecordingSchemaApp()
		app.errs["latest"] = fmt.Errorf(
			"%w: https://registry.example.test user=admin",
			appcluster.ErrSchemaRegistryNotConfigured,
		)
		result := callSchemaToolError(t, app, "getLatestSchema", map[string]any{
			"clusterName": "prod", "subject": "orders",
		}, false)
		require.Equal(t, "feature_not_configured", callToolText(t, result))
		require.NotContains(t, callToolText(t, result), "registry.example")
		require.NotContains(t, callToolText(t, result), "admin")
	})

	t.Run("unknown subject and raw auth error are safe", func(t *testing.T) {
		app := newRecordingSchemaApp()
		app.errs["byVersion"] = errors.New(
			`schema registry 404 subject missing: endpoint=https://sr.local Authorization=Basic credential-marker`,
		)
		result := callSchemaToolError(t, app, "getSchemaByVersion", map[string]any{
			"clusterName": "prod", "subject": "missing", "version": "1",
		}, false)
		require.Equal(t, "operation_failed", callToolText(t, result))
		require.NotContains(t, callToolText(t, result), "credential-marker")
		require.NotContains(t, callToolText(t, result), "sr.local")
	})

	t.Run("missing application port is safe", func(t *testing.T) {
		executor, _ := newSchemaExecutor(t, nil, &recordingReadOnlyResolver{}, false)
		result, err := newSDKSession(
			t, requireCatalogSpec(t, "getLatestSchema"), executor,
		).CallTool(context.Background(), &mcp.CallToolParams{
			Name:      "getLatestSchema",
			Arguments: map[string]any{"clusterName": "prod", "subject": "orders"},
		})
		require.NoError(t, err)
		require.True(t, result.IsError)
		require.Equal(t, "operation_failed", callToolText(t, result))
	})

	t.Run("registration read-back race is a stable safe failure", func(t *testing.T) {
		app := newRecordingSchemaApp()
		app.errs["register"] = fmt.Errorf(
			"%w: concurrent-backend-marker",
			appcluster.ErrSchemaRegistrationChanged,
		)
		result := callSchemaToolError(t, app, "createNewSchema", map[string]any{
			"clusterName": "prod",
			"body": map[string]any{
				"subject": "orders", "schema": `{}`, "schemaType": "JSON",
			},
		}, true)
		require.Equal(t, "operation_failed", callToolText(t, result))
		require.NotContains(t, callToolText(t, result), "concurrent-backend-marker")
		require.Equal(t, []schemaCall{{
			name: "register", cluster: "prod", subject: "orders",
			newSchema: domaincluster.NewSchema{Schema: `{}`, SchemaType: "JSON"},
		}}, app.snapshot())
	})
}

func TestSchemaToolsKeepEmptyDeletedVersionListAsAnArray(t *testing.T) {
	app := newRecordingSchemaApp()
	app.deletedSubject = nil
	result := callSchemaTool(t, app, "deleteSchema", map[string]any{
		"clusterName": "prod", "subject": "orders",
	}, true)
	require.Equal(t, `{"status":"deleted","deletedVersions":[]}`, callToolText(t, result))
	requireStructuredJSONEq(
		t,
		`{"status":"deleted","deletedVersions":[]}`,
		result.StructuredContent,
	)
}

func TestSchemaToolsExposeContractInputsWithoutPermanentDeleteFlags(t *testing.T) {
	tools := listSDKCatalogTools(t)
	tests := []struct {
		name       string
		properties []string
		required   []string
	}{
		{
			name:       "getSchemas",
			properties: []string{"clusterName", "query"},
			required:   []string{"clusterName"},
		},
		{
			name:       "getSchemaByVersion",
			properties: []string{"clusterName", "subject", "version"},
			required:   []string{"clusterName", "subject", "version"},
		},
		{
			name:       "createNewSchema",
			properties: []string{"body", "clusterName"},
			required:   []string{"body", "clusterName"},
		},
		{
			name:       "deleteSchema",
			properties: []string{"clusterName", "subject"},
			required:   []string{"clusterName", "subject"},
		},
		{
			name:       "updateSchemaCompatibilityLevel",
			properties: []string{"body", "clusterName", "subject"},
			required:   []string{"body", "clusterName", "subject"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := requireSchemaObject(t, findTool(t, tools, test.name).InputSchema)
			require.ElementsMatch(t, test.properties, schemaPropertyNames(t, schema))
			require.ElementsMatch(t, test.required, schemaRequired(t, schema))
			require.NotContains(t, schemaPropertyNames(t, schema), "permanent")
		})
	}
}

func schemaFixture(id int, subject string, version int) domaincluster.SchemaVersion {
	return domaincluster.SchemaVersion{
		ID:          id,
		Subject:     subject,
		Version:     version,
		Schema:      `{"type":"record","name":"Order"}`,
		SchemaType:  "AVRO",
		CompatLevel: "BACKWARD",
		References: []domaincluster.SchemaReference{{
			Name: "common.proto", Subject: "common-value", Version: 1,
		}},
	}
}

func schemaNewSchemaFixture() domaincluster.NewSchema {
	return domaincluster.NewSchema{
		Schema:     `{"type":"record","name":"Order"}`,
		SchemaType: "AVRO",
		References: []domaincluster.SchemaReference{{
			Name: "common.proto", Subject: "common-value", Version: 1,
		}},
	}
}

func cloneSchemaPage(page appcluster.SchemaPage) appcluster.SchemaPage {
	page.Schemas = append([]domaincluster.SchemaVersion(nil), page.Schemas...)
	for index := range page.Schemas {
		page.Schemas[index] = cloneSchemaVersion(page.Schemas[index])
	}
	return page
}

func cloneSchemaVersion(version domaincluster.SchemaVersion) domaincluster.SchemaVersion {
	version.References = append([]domaincluster.SchemaReference(nil), version.References...)
	return version
}

func cloneNewSchema(schema domaincluster.NewSchema) domaincluster.NewSchema {
	schema.References = append([]domaincluster.SchemaReference(nil), schema.References...)
	return schema
}

func schemaPageCountFromResult(t *testing.T, result *mcp.CallToolResult) int32 {
	t.Helper()
	var decoded generated.SchemaSubjectsResponse
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &decoded))
	require.NotNil(t, decoded.PageCount)
	return *decoded.PageCount
}

func validSchemaWriteInputs() map[string]map[string]any {
	body := map[string]any{
		"subject": "orders", "schema": `{}`, "schemaType": "JSON",
	}
	return map[string]map[string]any{
		"createNewSchema": {
			"clusterName": "prod", "body": body,
		},
		"deleteLatestSchema": {
			"clusterName": "prod", "subject": "orders",
		},
		"deleteSchema": {
			"clusterName": "prod", "subject": "orders",
		},
		"deleteSchemaByVersion": {
			"clusterName": "prod", "subject": "orders", "version": "1",
		},
		"updateGlobalSchemaCompatibilityLevel": {
			"clusterName": "prod", "body": map[string]any{"compatibility": "BACKWARD"},
		},
		"updateSchemaCompatibilityLevel": {
			"clusterName": "prod", "subject": "orders",
			"body": map[string]any{"compatibility": "BACKWARD"},
		},
	}
}

func newSchemaExecutor(
	t *testing.T,
	app SchemaServicer,
	resolver *recordingReadOnlyResolver,
	allowWrites bool,
) (*Executor, *mcppolicy.Store) {
	t.Helper()
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, allowWrites)))
	executor, err := NewExecutor(Dependencies{
		Schemas:    app,
		Policy:     store,
		IsReadOnly: resolver.Resolve,
	})
	require.NoError(t, err)
	return executor, store
}

func callSchemaTool(
	t *testing.T,
	app *recordingSchemaApp,
	name string,
	input map[string]any,
	allowWrites bool,
) *mcp.CallToolResult {
	t.Helper()
	executor, _ := newSchemaExecutor(t, app, &recordingReadOnlyResolver{}, allowWrites)
	result, err := newSDKSession(t, requireCatalogSpec(t, name), executor).CallTool(
		context.Background(), &mcp.CallToolParams{Name: name, Arguments: input},
	)
	require.NoError(t, err)
	require.False(t, result.IsError, callToolText(t, result))
	return result
}

func callSchemaToolError(
	t *testing.T,
	app *recordingSchemaApp,
	name string,
	input map[string]any,
	allowWrites bool,
) *mcp.CallToolResult {
	t.Helper()
	executor, _ := newSchemaExecutor(t, app, &recordingReadOnlyResolver{}, allowWrites)
	result, err := newSDKSession(t, requireCatalogSpec(t, name), executor).CallTool(
		context.Background(), &mcp.CallToolParams{Name: name, Arguments: input},
	)
	require.NoError(t, err)
	require.True(t, result.IsError)
	return result
}

var _ SchemaServicer = (*recordingSchemaApp)(nil)
