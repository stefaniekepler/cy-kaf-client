package mcpserver

import (
	"context"
	"encoding/json"
	"errors"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var (
	errMCPDisabled       = errors.New("policy_disabled")
	errMCPWritesDisabled = errors.New("writes_disabled")
	errClusterRequired   = errors.New("cluster_required")
	errClusterReadOnly   = errors.New("cluster_read_only")
	errResultTooLarge    = errors.New("result_too_large")
	errOperationFailed   = errors.New("operation_failed")
	errInvalidRequest    = errors.New("invalid_request")
	errClusterNotFound   = errors.New("cluster_not_found")
	errFeatureNotSet     = errors.New("feature_not_configured")
	errNotFound          = errors.New("not_found")
	errTimeout           = errors.New("timeout")
)

// Executor applies the policy and result-safety boundary shared by every tool.
type Executor struct {
	deps Dependencies
}

func NewExecutor(deps Dependencies) (*Executor, error) {
	if deps.Policy == nil {
		return nil, errors.New("MCP policy store is required")
	}
	if deps.IsReadOnly == nil {
		return nil, errors.New("cluster read-only resolver is required")
	}
	return &Executor{deps: deps}, nil
}

func (e *Executor) authorize(
	ctx context.Context,
	_ ToolMeta,
	clusterName string,
	effective AccessClass,
) error {
	policy, err := e.deps.Policy.Load()
	if err != nil || !policy.Enabled {
		return errMCPDisabled
	}
	if effective == AccessWrite {
		if !policy.AllowWrites {
			return errMCPWritesDisabled
		}
		if clusterName == "" {
			return errClusterRequired
		}
		if e.deps.IsReadOnly(clusterName) {
			return errClusterReadOnly
		}
	}
	return ctx.Err()
}

func (e *Executor) safeResult(meta ToolMeta, output any) (*mcp.CallToolResult, any, error) {
	if meta.MaxItems <= 0 {
		return nil, nil, errOperationFailed
	}
	normalized, err := normalizeResult(output, meta.MaxItems)
	if err != nil {
		return nil, nil, safeToolError(meta.Name, err)
	}
	raw, err := marshalBoundedResult(meta, normalized)
	if err != nil {
		return nil, nil, safeToolError(meta.Name, err)
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(raw)},
		},
		StructuredContent: json.RawMessage(raw),
	}, nil, nil
}

func safeToolError(_ string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errMCPDisabled):
		return errMCPDisabled
	case errors.Is(err, errMCPWritesDisabled):
		return errMCPWritesDisabled
	case errors.Is(err, errClusterReadOnly):
		return errClusterReadOnly
	case errors.Is(err, errResultTooLarge):
		return errResultTooLarge
	case errors.Is(err, domaincluster.ErrResultTooLarge):
		return errResultTooLarge
	case errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, appcluster.ErrAnalysisNoProgress):
		return errTimeout
	case errors.Is(err, appcluster.ErrUnknownCluster),
		errors.Is(err, appcluster.ErrKsqlClusterNotFound):
		return errClusterNotFound
	case errors.Is(err, appcluster.ErrSchemaRegistryNotConfigured),
		errors.Is(err, appcluster.ErrKsqlNotConfigured),
		errors.Is(err, appcluster.ErrTopicDeletionDisabled):
		return errFeatureNotSet
	case errors.Is(err, errInvalidRequest),
		errors.Is(err, errClusterRequired),
		errors.Is(err, appcluster.ErrBadAclCSV),
		errors.Is(err, appcluster.ErrBadAclRequest),
		errors.Is(err, appcluster.ErrBadQuotaRequest),
		errors.Is(err, appcluster.ErrInvalidReplicationFactor),
		errors.Is(err, appcluster.ErrGroupNotInactive),
		errors.Is(err, appcluster.ErrSerialize),
		errors.Is(err, appcluster.ErrKsqlInvalidCommand):
		return errInvalidRequest
	case errors.Is(err, appcluster.ErrUnknownConnect),
		errors.Is(err, appcluster.ErrAnalysisTopicNotFound),
		errors.Is(err, appcluster.ErrKsqlPipeNotFound):
		return errNotFound
	default:
		return errOperationFailed
	}
}
