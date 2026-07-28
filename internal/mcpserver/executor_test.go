package mcpserver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

type policyProbeInput struct {
	ClusterName string `json:"clusterName"`
	Write       bool   `json:"write"`
}

func TestExecutorAuthorizationMatrixUsesEffectiveAccessOnEverySDKCall(t *testing.T) {
	tests := []struct {
		name         string
		policy       *mcppolicy.Policy
		corrupt      bool
		write        bool
		cluster      string
		readOnly     string
		wantError    string
		wantCallback bool
	}{
		{
			name:      "missing policy denies read",
			wantError: "policy_disabled",
		},
		{
			name:      "disabled policy denies read",
			policy:    policy(false, false),
			wantError: "policy_disabled",
		},
		{
			name:      "corrupt policy denies read",
			corrupt:   true,
			wantError: "policy_disabled",
		},
		{
			name:         "enabled without writes allows read",
			policy:       policy(true, false),
			wantCallback: true,
		},
		{
			name:      "read annotation does not authorize effective write",
			policy:    policy(true, false),
			write:     true,
			cluster:   "prod",
			wantError: "writes_disabled",
		},
		{
			name:         "enabled writes allows write",
			policy:       policy(true, true),
			write:        true,
			cluster:      "prod",
			wantCallback: true,
		},
		{
			name:      "cluster read only is final write gate",
			policy:    policy(true, true),
			write:     true,
			cluster:   "prod",
			readOnly:  "prod",
			wantError: "cluster_read_only",
		},
		{
			name:         "other cluster remains writable",
			policy:       policy(true, true),
			write:        true,
			cluster:      "stage",
			readOnly:     "prod",
			wantCallback: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
			switch {
			case tt.corrupt:
				require.NoError(t, os.WriteFile(store.Path(), []byte("{not-json"), 0o600))
			case tt.policy != nil:
				require.NoError(t, store.Save(context.Background(), *tt.policy))
			}

			var callbackCount atomic.Int32
			session := newPolicyProbeSession(t, Dependencies{
				Policy: store,
				IsReadOnly: func(cluster string) bool {
					return cluster == tt.readOnly
				},
			}, func(context.Context, *Executor, policyProbeInput) (any, error) {
				callbackCount.Add(1)
				return map[string]any{"status": "ok"}, nil
			})

			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
				Name: "policyProbe",
				Arguments: map[string]any{
					"clusterName": tt.cluster,
					"write":       tt.write,
				},
			})
			require.NoError(t, err)
			if tt.wantError != "" {
				require.True(t, result.IsError)
				require.Equal(t, tt.wantError, callToolText(t, result))
			} else {
				require.False(t, result.IsError)
			}
			if tt.wantCallback {
				require.Equal(t, int32(1), callbackCount.Load())
			} else {
				require.Zero(t, callbackCount.Load())
			}
		})
	}
}

func TestExecutorCachedSessionLosesWriteAndReadAccessAfterPolicyDowngrade(t *testing.T) {
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), mcppolicy.Policy{
		Version:     mcppolicy.CurrentVersion,
		Enabled:     true,
		AllowWrites: true,
	}))
	session := newPolicyProbeSession(t, Dependencies{
		Policy:     store,
		IsReadOnly: func(string) bool { return false },
	}, func(context.Context, *Executor, policyProbeInput) (any, error) {
		return map[string]any{"status": "ok"}, nil
	})

	writeParams := &mcp.CallToolParams{
		Name: "policyProbe",
		Arguments: map[string]any{
			"clusterName": "prod",
			"write":       true,
		},
	}
	result, err := session.CallTool(context.Background(), writeParams)
	require.NoError(t, err)
	require.False(t, result.IsError)

	require.NoError(t, store.Save(context.Background(), mcppolicy.Policy{
		Version:     mcppolicy.CurrentVersion,
		Enabled:     true,
		AllowWrites: false,
	}))
	result, err = session.CallTool(context.Background(), writeParams)
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Equal(t, "writes_disabled", callToolText(t, result))

	require.NoError(t, store.Save(context.Background(), mcppolicy.Policy{
		Version: mcppolicy.CurrentVersion,
		Enabled: false,
	}))
	result, err = session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "policyProbe",
		Arguments: map[string]any{
			"clusterName": "prod",
			"write":       false,
		},
	})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Equal(t, "policy_disabled", callToolText(t, result))
}

func TestExecutorDeniedPolicyDoesNotInvokeClusterOrAccessSelectors(t *testing.T) {
	tests := []struct {
		name    string
		policy  *mcppolicy.Policy
		corrupt bool
	}{
		{name: "missing"},
		{name: "disabled", policy: policy(false, false)},
		{name: "corrupt", corrupt: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
			switch {
			case tt.corrupt:
				require.NoError(t, os.WriteFile(store.Path(), []byte("{not-json"), 0o600))
			case tt.policy != nil:
				require.NoError(t, store.Save(context.Background(), *tt.policy))
			}
			executor, err := NewExecutor(Dependencies{
				Policy:     store,
				IsReadOnly: func(string) bool { return false },
			})
			require.NoError(t, err)

			var clusterCount atomic.Int32
			var accessCount atomic.Int32
			var callbackCount atomic.Int32
			meta := policyProbeMeta()
			meta.Name = "conditionalProbe"
			meta.Access = AccessConditionalKSQL
			spec := newTool(
				meta,
				func(policyProbeInput) string {
					clusterCount.Add(1)
					return "inspected-user-cluster"
				},
				func(context.Context, *Executor, policyProbeInput) (AccessClass, error) {
					accessCount.Add(1)
					return AccessReadOnly, errors.New("SELECT credential-marker FROM secrets")
				},
				func(context.Context, *Executor, policyProbeInput) (any, error) {
					callbackCount.Add(1)
					return map[string]any{"status": "unexpected"}, nil
				},
			)
			session := newSDKSession(t, spec, executor)

			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
				Name: meta.Name,
				Arguments: map[string]any{
					"clusterName": "prod",
					"write":       false,
				},
			})
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "policy_disabled", callToolText(t, result))
			require.Zero(t, clusterCount.Load())
			require.Zero(t, accessCount.Load())
			require.Zero(t, callbackCount.Load())
		})
	}
}

func TestExecutorConditionalSelectorPolicyDowngradeDeniesCallback(t *testing.T) {
	tests := []struct {
		name      string
		next      mcppolicy.Policy
		effective AccessClass
		wantError string
	}{
		{
			name: "writes disabled during selection",
			next: mcppolicy.Policy{
				Version: mcppolicy.CurrentVersion,
				Enabled: true,
			},
			effective: AccessWrite,
			wantError: "writes_disabled",
		},
		{
			name: "MCP disabled during selection",
			next: mcppolicy.Policy{
				Version: mcppolicy.CurrentVersion,
				Enabled: false,
			},
			effective: AccessReadOnly,
			wantError: "policy_disabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
			require.NoError(t, store.Save(context.Background(), *policy(true, true)))
			executor, err := NewExecutor(Dependencies{
				Policy:     store,
				IsReadOnly: func(string) bool { return false },
			})
			require.NoError(t, err)

			var callbackCount atomic.Int32
			meta := policyProbeMeta()
			meta.Name = "downgradeProbe"
			meta.Access = AccessConditionalKSQL
			spec := newTool(
				meta,
				func(input policyProbeInput) string { return input.ClusterName },
				func(ctx context.Context, _ *Executor, _ policyProbeInput) (AccessClass, error) {
					require.NoError(t, store.Save(ctx, tt.next))
					return tt.effective, nil
				},
				func(context.Context, *Executor, policyProbeInput) (any, error) {
					callbackCount.Add(1)
					return map[string]any{"status": "unexpected"}, nil
				},
			)
			session := newSDKSession(t, spec, executor)

			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
				Name: meta.Name,
				Arguments: map[string]any{
					"clusterName": "prod",
					"write":       false,
				},
			})
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, tt.wantError, callToolText(t, result))
			require.Zero(t, callbackCount.Load())
		})
	}
}

func TestExecutorRequiresPolicyAndReadOnlyResolver(t *testing.T) {
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))

	_, err := NewExecutor(Dependencies{IsReadOnly: func(string) bool { return false }})
	require.EqualError(t, err, "MCP policy store is required")

	_, err = NewExecutor(Dependencies{Policy: store})
	require.EqualError(t, err, "cluster read-only resolver is required")
}

func TestExecutorRequiresClusterForEveryEffectiveWrite(t *testing.T) {
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, true)))
	session := newPolicyProbeSession(t, Dependencies{
		Policy:     store,
		IsReadOnly: func(string) bool { return false },
	}, func(context.Context, *Executor, policyProbeInput) (any, error) {
		return map[string]any{"status": "unexpected"}, nil
	})

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "policyProbe",
		Arguments: map[string]any{
			"clusterName": "",
			"write":       true,
		},
	})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Equal(t, "invalid_request", callToolText(t, result))
}

func TestExecutorAppliesToolTimeoutAndReturnsStableCode(t *testing.T) {
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, false)))
	executor, err := NewExecutor(Dependencies{
		Policy:     store,
		IsReadOnly: func(string) bool { return false },
	})
	require.NoError(t, err)

	meta := policyProbeMeta()
	meta.Name = "timeoutProbe"
	meta.Timeout = 20 * time.Millisecond
	spec := newTool(
		meta,
		func(input policyProbeInput) string { return input.ClusterName },
		func(context.Context, *Executor, policyProbeInput) (AccessClass, error) {
			return AccessReadOnly, nil
		},
		func(ctx context.Context, _ *Executor, _ policyProbeInput) (any, error) {
			_, hasDeadline := ctx.Deadline()
			require.True(t, hasDeadline)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	)
	session := newSDKSession(t, spec, executor)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: meta.Name,
		Arguments: map[string]any{
			"clusterName": "",
			"write":       false,
		},
	})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Equal(t, "timeout", callToolText(t, result))
}

func TestExecutorRejectsLateSuccessFromNonCooperativeCallback(t *testing.T) {
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, false)))
	executor, err := NewExecutor(Dependencies{
		Policy:     store,
		IsReadOnly: func(string) bool { return false },
	})
	require.NoError(t, err)

	meta := policyProbeMeta()
	meta.Name = "lateSuccessProbe"
	meta.Timeout = 20 * time.Millisecond
	spec := newTool(
		meta,
		nil,
		func(context.Context, *Executor, noInput) (AccessClass, error) {
			return AccessReadOnly, nil
		},
		func(context.Context, *Executor, noInput) (any, error) {
			time.Sleep(4 * meta.Timeout)
			return map[string]any{"status": "late-success"}, nil
		},
	)
	session := newSDKSession(t, spec, executor)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      meta.Name,
		Arguments: map[string]any{},
	})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Equal(t, "timeout", callToolText(t, result))
}

func TestSafeToolErrorMapsKnownSentinelsToFrozenCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "cluster", err: appcluster.ErrUnknownCluster, want: "cluster_not_found"},
		{name: "ksql cluster", err: appcluster.ErrKsqlClusterNotFound, want: "cluster_not_found"},
		{name: "schema feature", err: appcluster.ErrSchemaRegistryNotConfigured, want: "feature_not_configured"},
		{name: "ksql feature", err: appcluster.ErrKsqlNotConfigured, want: "feature_not_configured"},
		{name: "disabled feature", err: appcluster.ErrTopicDeletionDisabled, want: "feature_not_configured"},
		{name: "acl request", err: appcluster.ErrBadAclRequest, want: "invalid_request"},
		{name: "acl csv", err: appcluster.ErrBadAclCSV, want: "invalid_request"},
		{name: "quota request", err: appcluster.ErrBadQuotaRequest, want: "invalid_request"},
		{name: "replication factor", err: appcluster.ErrInvalidReplicationFactor, want: "invalid_request"},
		{name: "group state", err: appcluster.ErrGroupNotInactive, want: "invalid_request"},
		{name: "message serialization", err: appcluster.ErrSerialize, want: "invalid_request"},
		{name: "ksql command", err: appcluster.ErrKsqlInvalidCommand, want: "invalid_request"},
		{name: "connect", err: appcluster.ErrUnknownConnect, want: "not_found"},
		{name: "analysis", err: appcluster.ErrAnalysisTopicNotFound, want: "not_found"},
		{name: "ksql pipe", err: appcluster.ErrKsqlPipeNotFound, want: "not_found"},
		{name: "policy", err: errMCPDisabled, want: "policy_disabled"},
		{name: "writes", err: errMCPWritesDisabled, want: "writes_disabled"},
		{name: "generic invalid request", err: errInvalidRequest, want: "invalid_request"},
		{name: "cluster required", err: errClusterRequired, want: "invalid_request"},
		{name: "cluster read only", err: errClusterReadOnly, want: "cluster_read_only"},
		{name: "deadline", err: context.DeadlineExceeded, want: "timeout"},
		{name: "result", err: errResultTooLarge, want: "result_too_large"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.EqualError(t, safeToolError("syntheticOperation", tt.err), tt.want)
			require.EqualError(t, safeToolError("syntheticOperation", errors.Join(
				errors.New("credential-marker"),
				tt.err,
			)), tt.want)
		})
	}
}

func TestSafeToolErrorNeverReturnsOriginalInfrastructureError(t *testing.T) {
	unsafe := errors.New("password=credential-marker SELECT * FROM secrets\n" +
		"goroutine 24 [running]:\ninternal/infra/kafka.(*Client).Call")

	got := safeToolError("executeKsql SELECT * FROM secrets", unsafe)

	require.EqualError(t, got, "operation_failed")
	require.NotContains(t, got.Error(), "credential-marker")
	require.NotContains(t, got.Error(), "SELECT")
	require.NotContains(t, got.Error(), "goroutine")
	require.NotContains(t, got.Error(), "internal/infra")
}

func policy(enabled, allowWrites bool) *mcppolicy.Policy {
	return &mcppolicy.Policy{
		Version:     mcppolicy.CurrentVersion,
		Enabled:     enabled,
		AllowWrites: allowWrites,
	}
}

func policyProbeMeta() ToolMeta {
	return ToolMeta{
		Name:        "policyProbe",
		Title:       "policyProbe",
		Description: "Policy enforcement probe.",
		Subsystem:   "Test",
		Access:      AccessReadOnly,
		Timeout:     time.Second,
		MaxItems:    10,
		MaxBytes:    1024,
	}
}

func newPolicyProbeSession(
	t *testing.T,
	deps Dependencies,
	call toolCall[policyProbeInput],
) *mcp.ClientSession {
	t.Helper()
	executor, err := NewExecutor(deps)
	require.NoError(t, err)
	spec := newTool(
		policyProbeMeta(),
		func(input policyProbeInput) string { return input.ClusterName },
		func(_ context.Context, _ *Executor, input policyProbeInput) (AccessClass, error) {
			if input.Write {
				return AccessWrite, nil
			}
			return AccessReadOnly, nil
		},
		call,
	)
	return newSDKSession(t, spec, executor)
}

func newSDKSession(t *testing.T, spec ToolSpec, executor *Executor) *mcp.ClientSession {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "executor-test-server", Version: "test"}, nil)
	spec.Register(server, executor)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, serverSession.Close()) })

	client := mcp.NewClient(&mcp.Implementation{Name: "executor-test-client", Version: "test"}, nil)
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, clientSession.Close()) })
	return clientSession
}

func callToolText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	require.NotNil(t, result)
	require.Len(t, result.Content, 1)
	text, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok, "content has type %T", result.Content[0])
	return strings.TrimSpace(text.Text)
}
