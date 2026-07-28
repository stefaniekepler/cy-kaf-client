package mcpserver

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

const (
	maxMCPConnectorConfigKeys  = 200
	maxMCPConnectorConfigBytes = 256 << 10

	// maxMCPConnectRawItems bounds only the local work performed after an
	// application port has already materialized a remote aggregate/list
	// response. MCP emits at most maxListItems from this accepted raw window.
	maxMCPConnectRawItems = 2000
)

type connectStatusResult struct {
	Status string `json:"status"`
}

type connectorActionResult struct {
	Action string `json:"action"`
	Status string `json:"status"`
}

type connectorTaskActionResult struct {
	Status string `json:"status"`
	TaskID int32  `json:"taskId"`
}

var connectCSVHeader = []string{"name", "address"}

var allConnectorsCSVHeader = []string{
	"connect",
	"name",
	"type",
	"state",
	"workerId",
	"tasksCount",
	"failedTasksCount",
}

func connectTool(meta ToolMeta) ToolSpec {
	switch meta.Name {
	case "getConnects":
		return connectReadTool(meta, connectClusterQuerySelector[generated.GetConnectsParams], getConnects)
	case "getConnectsCsv":
		return connectReadTool(
			meta,
			connectClusterQuerySelector[generated.GetConnectsCsvParams],
			func(ctx context.Context, executor *Executor, input clusterOptionalQueryInput[generated.GetConnectsCsvParams]) (any, error) {
				return getConnectsCSV(ctx, executor, input, meta.MaxItems)
			},
		)
	case "getConnectors":
		return connectReadTool(meta, connectInputSelector, func(ctx context.Context, executor *Executor, input connectInput) (any, error) {
			return getConnectors(ctx, executor, input, meta.MaxItems)
		})
	case "getConnector":
		return connectReadTool(meta, connectorInputSelector, getConnector)
	case "getAllConnectors":
		return connectReadTool(meta, connectAllQuerySelector[generated.GetAllConnectorsParams], func(
			ctx context.Context,
			executor *Executor,
			input clusterOptionalQueryInput[generated.GetAllConnectorsParams],
		) (any, error) {
			query, err := connectorListQuery(input.Query)
			if err != nil {
				return nil, err
			}
			return getAllConnectors(ctx, executor, input.ClusterName, query, meta.MaxItems)
		})
	case "getAllConnectorsCsv":
		return connectReadTool(meta, connectAllQuerySelector[generated.GetAllConnectorsCsvParams], func(
			ctx context.Context,
			executor *Executor,
			input clusterOptionalQueryInput[generated.GetAllConnectorsCsvParams],
		) (any, error) {
			query, err := connectorCSVQuery(input.Query)
			if err != nil {
				return nil, err
			}
			return getAllConnectorsCSV(ctx, executor, input.ClusterName, query, meta.MaxItems)
		})
	case "getConnectorConfig":
		return connectReadTool(meta, connectorInputSelector, getConnectorConfig)
	case "getConnectorTasks":
		return connectReadTool(meta, connectorInputSelector, func(ctx context.Context, executor *Executor, input connectorInput) (any, error) {
			return getConnectorTasks(ctx, executor, input, meta.MaxItems)
		})
	case "getConnectorPlugins":
		return connectReadTool(meta, connectInputSelector, func(ctx context.Context, executor *Executor, input connectInput) (any, error) {
			return getConnectorPlugins(ctx, executor, input, meta.MaxItems)
		})
	case "validateConnectorPluginConfig":
		return connectReadTool(meta, connectorPluginInputSelector, validateConnectorPluginConfig)
	case "createConnector":
		return connectWriteTool(meta, connectNewConnectorInputSelector, createConnector)
	case "deleteConnector":
		return connectWriteTool(meta, connectorInputSelector, deleteConnector)
	case "setConnectorConfig":
		return connectWriteTool(meta, connectorConfigInputSelector, setConnectorConfig)
	case "updateConnectorState":
		return connectWriteTool(meta, connectorActionInputSelector, updateConnectorState)
	case "restartConnectorTask":
		return connectWriteTool(meta, connectorTaskInputSelector, restartConnectorTask)
	case "resetConnectorOffsets":
		return connectWriteTool(meta, connectorInputSelector, resetConnectorOffsets)
	default:
		panic("unsupported Kafka Connect MCP tool: " + meta.Name)
	}
}

func connectReadTool[In any](
	meta ToolMeta,
	cluster clusterSelector[In],
	call toolCall[In],
) ToolSpec {
	return newTool(
		meta,
		cluster,
		func(context.Context, *Executor, In) (AccessClass, error) {
			return AccessReadOnly, nil
		},
		call,
	)
}

func connectWriteTool[In any](
	meta ToolMeta,
	cluster clusterSelector[In],
	call toolCall[In],
) ToolSpec {
	return newTool(
		meta,
		cluster,
		func(context.Context, *Executor, In) (AccessClass, error) {
			return AccessWrite, nil
		},
		call,
	)
}

func connectClusterQuerySelector[Query any](input clusterOptionalQueryInput[Query]) string {
	return input.ClusterName
}

func connectAllQuerySelector[Query any](input clusterOptionalQueryInput[Query]) string {
	return input.ClusterName
}

func connectInputSelector(input connectInput) string {
	return input.ClusterName
}

func connectorInputSelector(input connectorInput) string {
	return input.ClusterName
}

func connectorPluginInputSelector(input connectorPluginBodyInput) string {
	return input.ClusterName
}

func connectNewConnectorInputSelector(input connectBodyInput[generated.NewConnector]) string {
	return input.ClusterName
}

func connectorConfigInputSelector(input connectorBodyInput[generated.ConnectorConfig]) string {
	return input.ClusterName
}

func connectorActionInputSelector(input connectorActionInput) string {
	return input.ClusterName
}

func connectorTaskInputSelector(input connectorTaskInput) string {
	return input.ClusterName
}

func getConnects(
	ctx context.Context,
	executor *Executor,
	input clusterOptionalQueryInput[generated.GetConnectsParams],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if connectStatsRequested(input.Query) {
		return nil, errInvalidRequest
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	connects, err := executor.deps.Connects.ListConnects(ctx, input.ClusterName)
	if err != nil {
		return nil, err
	}
	if len(connects) > maxMCPConnectRawItems {
		return nil, errResultTooLarge
	}
	connects = append([]domaincluster.ConnectCluster(nil), connects...)
	sort.Slice(connects, func(left, right int) bool {
		if connects[left].Name == connects[right].Name {
			return connects[left].Address < connects[right].Address
		}
		return connects[left].Name < connects[right].Name
	})
	if len(connects) > maxListItems {
		connects = connects[:maxListItems]
	}
	output := make([]generated.Connect, 0, len(connects))
	for _, connect := range connects {
		if err := validateConnectResultName(connect.Name); err != nil {
			return nil, err
		}
		address, err := safeConnectAddress(connect.Address)
		if err != nil {
			return nil, err
		}
		output = append(output, generated.Connect{
			Name:    connect.Name,
			Address: connectPointer(address),
		})
	}
	return output, nil
}

func getConnectsCSV(
	ctx context.Context,
	executor *Executor,
	input clusterOptionalQueryInput[generated.GetConnectsCsvParams],
	maxItems int,
) (any, error) {
	if connectStatsRequested(input.Query) {
		return nil, errInvalidRequest
	}
	connects, err := connectList(ctx, executor, input.ClusterName, maxItems)
	if err != nil {
		return nil, err
	}
	rows := make([][]string, 0, len(connects))
	for _, connect := range connects {
		address, err := safeConnectAddress(connect.Address)
		if err != nil {
			return nil, err
		}
		rows = append(rows, []string{connect.Name, address})
	}
	return renderConnectCSV(connectCSVHeader, rows)
}

func connectList(
	ctx context.Context,
	executor *Executor,
	clusterName string,
	maxItems int,
) ([]domaincluster.ConnectCluster, error) {
	if err := validateClusterName(clusterName); err != nil {
		return nil, err
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	connects, err := executor.deps.Connects.ListConnects(ctx, clusterName)
	if err != nil {
		return nil, err
	}
	if len(connects) > maxMCPConnectRawItems {
		return nil, errResultTooLarge
	}
	connects = append([]domaincluster.ConnectCluster(nil), connects...)
	sort.Slice(connects, func(left, right int) bool {
		if connects[left].Name == connects[right].Name {
			return connects[left].Address < connects[right].Address
		}
		return connects[left].Name < connects[right].Name
	})
	if maxItems > 0 && len(connects) > maxItems {
		connects = connects[:maxItems]
	}
	for _, connect := range connects {
		if err := validateConnectResultName(connect.Name); err != nil {
			return nil, err
		}
	}
	return connects, nil
}

func getConnectors(
	ctx context.Context,
	executor *Executor,
	input connectInput,
	maxItems int,
) (any, error) {
	if err := validateConnectInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	names, err := executor.deps.Connects.Connectors(ctx, input.ClusterName, input.ConnectName)
	if err != nil {
		return nil, err
	}
	if len(names) > maxMCPConnectRawItems {
		return nil, errResultTooLarge
	}
	names = append([]string(nil), names...)
	for _, name := range names {
		if err := validateConnectResultName(name); err != nil {
			return nil, err
		}
	}
	sort.Strings(names)
	if maxItems > 0 && len(names) > maxItems {
		names = names[:maxItems]
	}
	return names, nil
}

func getConnector(
	ctx context.Context,
	executor *Executor,
	input connectorInput,
) (any, error) {
	if err := validateConnectorInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	connector, err := executor.deps.Connects.Connector(
		ctx, input.ClusterName, input.ConnectName, input.ConnectorName,
	)
	if err != nil {
		return nil, err
	}
	converted, err := connectorToContract(connector)
	if err != nil {
		return nil, err
	}
	return redactCredentials(converted), nil
}

func getAllConnectors(
	ctx context.Context,
	executor *Executor,
	clusterName string,
	query connectorQuery,
	maxItems int,
) (any, error) {
	refs, err := connectorRefs(ctx, executor, clusterName, query, maxItems)
	if err != nil {
		return nil, err
	}
	output := make([]generated.FullConnectorInfo, 0, len(refs))
	for _, ref := range refs {
		converted, err := connectorRefToContract(ref)
		if err != nil {
			return nil, err
		}
		output = append(output, converted)
	}
	return output, nil
}

func getAllConnectorsCSV(
	ctx context.Context,
	executor *Executor,
	clusterName string,
	query connectorQuery,
	maxItems int,
) (any, error) {
	refs, err := connectorRefs(ctx, executor, clusterName, query, maxItems)
	if err != nil {
		return nil, err
	}
	rows := make([][]string, 0, len(refs))
	for _, ref := range refs {
		rows = append(rows, []string{
			ref.ConnectName,
			ref.Name,
			strings.ToUpper(ref.Type),
			connectState(ref.State),
			ref.WorkerID,
			strconv.Itoa(ref.TasksCount),
			strconv.Itoa(ref.FailedTasksCount),
		})
	}
	return renderConnectCSV(allConnectorsCSVHeader, rows)
}

func getConnectorConfig(
	ctx context.Context,
	executor *Executor,
	input connectorInput,
) (any, error) {
	if err := validateConnectorInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	config, err := executor.deps.Connects.ConnectorConfig(
		ctx, input.ClusterName, input.ConnectName, input.ConnectorName,
	)
	if err != nil {
		return nil, err
	}
	if config == nil {
		config = map[string]any{}
	}
	return redactCredentials(config), nil
}

func getConnectorTasks(
	ctx context.Context,
	executor *Executor,
	input connectorInput,
	maxItems int,
) (any, error) {
	if err := validateConnectorInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	tasks, err := executor.deps.Connects.ConnectorTasks(
		ctx, input.ClusterName, input.ConnectName, input.ConnectorName,
	)
	if err != nil {
		return nil, err
	}
	if len(tasks) > maxMCPConnectRawItems {
		return nil, errResultTooLarge
	}
	tasks = append([]domaincluster.ConnectorTask(nil), tasks...)
	sort.Slice(tasks, func(left, right int) bool {
		return tasks[left].ID < tasks[right].ID
	})
	if maxItems > 0 && len(tasks) > maxItems {
		tasks = tasks[:maxItems]
	}
	output := make([]generated.Task, 0, len(tasks))
	for _, task := range tasks {
		converted, err := connectorTaskToContract(input.ConnectorName, task)
		if err != nil {
			return nil, err
		}
		output = append(output, converted)
	}
	return redactCredentials(output), nil
}

func getConnectorPlugins(
	ctx context.Context,
	executor *Executor,
	input connectInput,
	maxItems int,
) (any, error) {
	if err := validateConnectInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	plugins, err := executor.deps.Connects.Plugins(ctx, input.ClusterName, input.ConnectName)
	if err != nil {
		return nil, err
	}
	if len(plugins) > maxMCPConnectRawItems {
		return nil, errResultTooLarge
	}
	plugins = append([]domaincluster.ConnectorPlugin(nil), plugins...)
	sort.Slice(plugins, func(left, right int) bool {
		return plugins[left].Class < plugins[right].Class
	})
	if maxItems > 0 && len(plugins) > maxItems {
		plugins = plugins[:maxItems]
	}
	output := make([]generated.ConnectorPlugin, 0, len(plugins))
	for _, plugin := range plugins {
		if err := validateConnectResultName(plugin.Class); err != nil {
			return nil, err
		}
		output = append(output, generated.ConnectorPlugin{Class: connectPointer(plugin.Class)})
	}
	return output, nil
}

func validateConnectorPluginConfig(
	ctx context.Context,
	executor *Executor,
	input connectorPluginBodyInput,
) (any, error) {
	if err := validateConnectorPluginInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	validation, err := executor.deps.Connects.ValidatePlugin(
		ctx,
		input.ClusterName,
		input.ConnectName,
		input.PluginName,
		map[string]any(input.Body),
	)
	if err != nil {
		return nil, err
	}
	return pluginValidationToContract(validation)
}

func createConnector(
	ctx context.Context,
	executor *Executor,
	input connectBodyInput[generated.NewConnector],
) (any, error) {
	if err := validateConnectInput(connectInput{
		ClusterName: input.ClusterName,
		ConnectName: input.ConnectName,
	}); err != nil {
		return nil, err
	}
	if err := validateConnectName(input.Body.Name); err != nil {
		return nil, err
	}
	if err := validateConnectorConfig(input.Body.Config); err != nil {
		return nil, err
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	connector, err := executor.deps.Connects.CreateConnector(
		ctx,
		input.ClusterName,
		input.ConnectName,
		input.Body.Name,
		map[string]any(input.Body.Config),
	)
	if err != nil {
		return nil, err
	}
	converted, err := connectorToContract(connector)
	if err != nil {
		return nil, err
	}
	return redactCredentials(converted), nil
}

func deleteConnector(
	ctx context.Context,
	executor *Executor,
	input connectorInput,
) (any, error) {
	if err := validateConnectorInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Connects.DeleteConnector(
		ctx, input.ClusterName, input.ConnectName, input.ConnectorName,
	); err != nil {
		return nil, err
	}
	return connectStatusResult{Status: "deleted"}, nil
}

func setConnectorConfig(
	ctx context.Context,
	executor *Executor,
	input connectorBodyInput[generated.ConnectorConfig],
) (any, error) {
	if err := validateConnectorInput(connectorInput{
		ClusterName: input.ClusterName, ConnectName: input.ConnectName,
		ConnectorName: input.ConnectorName,
	}); err != nil {
		return nil, err
	}
	if err := validateConnectorConfig(input.Body); err != nil {
		return nil, err
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	connector, err := executor.deps.Connects.SetConnectorConfig(
		ctx,
		input.ClusterName,
		input.ConnectName,
		input.ConnectorName,
		map[string]any(input.Body),
	)
	if err != nil {
		return nil, err
	}
	converted, err := connectorToContract(connector)
	if err != nil {
		return nil, err
	}
	return redactCredentials(converted), nil
}

func updateConnectorState(
	ctx context.Context,
	executor *Executor,
	input connectorActionInput,
) (any, error) {
	if err := validateConnectorInput(connectorInput{
		ClusterName: input.ClusterName, ConnectName: input.ConnectName,
		ConnectorName: input.ConnectorName,
	}); err != nil {
		return nil, err
	}
	if !input.Action.Valid() {
		return nil, errInvalidRequest
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Connects.UpdateConnectorState(
		ctx,
		input.ClusterName,
		input.ConnectName,
		input.ConnectorName,
		string(input.Action),
	); err != nil {
		return nil, err
	}
	return connectorActionResult{Status: "updated", Action: string(input.Action)}, nil
}

func restartConnectorTask(
	ctx context.Context,
	executor *Executor,
	input connectorTaskInput,
) (any, error) {
	if err := validateConnectorInput(connectorInput{
		ClusterName: input.ClusterName, ConnectName: input.ConnectName,
		ConnectorName: input.ConnectorName,
	}); err != nil {
		return nil, err
	}
	if input.TaskID < 0 {
		return nil, errInvalidRequest
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Connects.RestartConnectorTask(
		ctx,
		input.ClusterName,
		input.ConnectName,
		input.ConnectorName,
		int(input.TaskID),
	); err != nil {
		return nil, err
	}
	return connectorTaskActionResult{Status: "restarted", TaskID: input.TaskID}, nil
}

func resetConnectorOffsets(
	ctx context.Context,
	executor *Executor,
	input connectorInput,
) (any, error) {
	if err := validateConnectorInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Connects.ResetConnectorOffsets(
		ctx, input.ClusterName, input.ConnectName, input.ConnectorName,
	); err != nil {
		return nil, err
	}
	return connectStatusResult{Status: "reset"}, nil
}

type connectorQuery struct {
	Search    string
	OrderBy   generated.ConnectorColumnsToSort
	SortOrder generated.SortOrder
}

func connectorListQuery(params *generated.GetAllConnectorsParams) (connectorQuery, error) {
	if params == nil {
		return connectorQuery{}, nil
	}
	return validatedConnectorQuery(params.Search, params.OrderBy, params.SortOrder, params.Fts)
}

func connectorCSVQuery(params *generated.GetAllConnectorsCsvParams) (connectorQuery, error) {
	if params == nil {
		return connectorQuery{}, nil
	}
	return validatedConnectorQuery(params.Search, params.OrderBy, params.SortOrder, params.Fts)
}

func validatedConnectorQuery(
	search *string,
	orderBy *generated.ConnectorColumnsToSort,
	sortOrder *generated.SortOrder,
	fts *bool,
) (connectorQuery, error) {
	query := connectorQuery{}
	if search != nil {
		if len(*search) > maxClusterBrokerNameBytes {
			return connectorQuery{}, errInvalidRequest
		}
		query.Search = *search
	}
	if orderBy != nil {
		if !orderBy.Valid() {
			return connectorQuery{}, errInvalidRequest
		}
		query.OrderBy = *orderBy
	}
	if sortOrder != nil {
		if !sortOrder.Valid() {
			return connectorQuery{}, errInvalidRequest
		}
		query.SortOrder = *sortOrder
	}
	if fts != nil && *fts {
		return connectorQuery{}, errInvalidRequest
	}
	return query, nil
}

func connectorRefs(
	ctx context.Context,
	executor *Executor,
	clusterName string,
	query connectorQuery,
	maxItems int,
) ([]domaincluster.ConnectorRef, error) {
	if err := validateClusterName(clusterName); err != nil {
		return nil, err
	}
	if executor.deps.Connects == nil {
		return nil, errOperationFailed
	}
	refs, err := executor.deps.Connects.AllConnectors(ctx, clusterName)
	if err != nil {
		return nil, err
	}
	if len(refs) > maxMCPConnectRawItems {
		return nil, errResultTooLarge
	}
	refs = append([]domaincluster.ConnectorRef(nil), refs...)
	if query.Search != "" {
		search := strings.ToLower(query.Search)
		filtered := make([]domaincluster.ConnectorRef, 0, len(refs))
		for _, ref := range refs {
			if strings.Contains(strings.ToLower(ref.Name), search) ||
				strings.Contains(strings.ToLower(ref.ConnectName), search) ||
				strings.Contains(strings.ToLower(ref.Type), search) ||
				strings.Contains(strings.ToLower(ref.State), search) {
				filtered = append(filtered, ref)
			}
		}
		refs = filtered
	}
	for _, ref := range refs {
		if err := validateConnectorRef(ref); err != nil {
			return nil, err
		}
	}
	sort.SliceStable(refs, func(left, right int) bool {
		comparison := compareConnectorRefs(refs[left], refs[right], query.OrderBy)
		if query.SortOrder == generated.DESC {
			comparison = -comparison
		}
		return comparison < 0
	})
	if maxItems > 0 && len(refs) > maxItems {
		refs = refs[:maxItems]
	}
	return refs, nil
}

func compareConnectorRefs(left, right domaincluster.ConnectorRef, orderBy generated.ConnectorColumnsToSort) int {
	var primary int
	switch orderBy {
	case generated.ConnectorColumnsToSortNAME:
		primary = strings.Compare(left.Name, right.Name)
	case generated.ConnectorColumnsToSortSTATUS:
		primary = strings.Compare(left.State, right.State)
	case generated.ConnectorColumnsToSortTYPE:
		primary = strings.Compare(left.Type, right.Type)
	default:
		primary = strings.Compare(left.ConnectName, right.ConnectName)
	}
	if primary != 0 {
		return primary
	}
	if comparison := strings.Compare(left.ConnectName, right.ConnectName); comparison != 0 {
		return comparison
	}
	return strings.Compare(left.Name, right.Name)
}

func validateConnectInput(input connectInput) error {
	if err := validateClusterName(input.ClusterName); err != nil {
		return err
	}
	return validateConnectName(input.ConnectName)
}

func validateConnectorInput(input connectorInput) error {
	if err := validateConnectInput(connectInput{
		ClusterName: input.ClusterName,
		ConnectName: input.ConnectName,
	}); err != nil {
		return err
	}
	return validateConnectName(input.ConnectorName)
}

func validateConnectorPluginInput(input connectorPluginBodyInput) error {
	if err := validateConnectInput(connectInput{
		ClusterName: input.ClusterName,
		ConnectName: input.ConnectName,
	}); err != nil {
		return err
	}
	if err := validateConnectName(input.PluginName); err != nil {
		return err
	}
	return validateConnectorConfig(input.Body)
}

func validateConnectName(name string) error {
	return validateBoundedName(name)
}

func validateConnectResultName(name string) error {
	if err := validateConnectName(name); err != nil {
		return errOperationFailed
	}
	return nil
}

func validateConnectorConfig(config generated.ConnectorConfig) error {
	if len(config) > maxMCPConnectorConfigKeys {
		return errInvalidRequest
	}
	raw, err := json.Marshal(config)
	if err != nil || len(raw) > maxMCPConnectorConfigBytes {
		return errInvalidRequest
	}
	return nil
}

func validateConnectorRef(ref domaincluster.ConnectorRef) error {
	if validateConnectResultName(ref.ConnectName) != nil ||
		validateConnectResultName(ref.Name) != nil ||
		ref.TasksCount < 0 ||
		ref.FailedTasksCount < 0 {
		return errOperationFailed
	}
	if ref.Type != "" {
		connectorType := generated.ConnectorType(strings.ToUpper(ref.Type))
		if !connectorType.Valid() {
			return errOperationFailed
		}
	}
	if ref.State != "" {
		state := generated.ConnectorState(ref.State)
		if !state.Valid() {
			return errOperationFailed
		}
	}
	return nil
}

func connectorRefToContract(ref domaincluster.ConnectorRef) (generated.FullConnectorInfo, error) {
	if err := validateConnectorRef(ref); err != nil {
		return generated.FullConnectorInfo{}, err
	}
	output := generated.FullConnectorInfo{
		Connect: ref.ConnectName,
		Name:    ref.Name,
		Status: generated.ConnectorStatus{
			State:    generated.ConnectorState(connectState(ref.State)),
			WorkerId: connectPointer(ref.WorkerID),
		},
		TasksCount:       connectPointer(ref.TasksCount),
		FailedTasksCount: connectPointer(ref.FailedTasksCount),
	}
	if ref.Type != "" {
		output.Type = connectPointer(generated.ConnectorType(strings.ToUpper(ref.Type)))
	}
	return output, nil
}

func connectorToContract(connector domaincluster.Connector) (generated.Connector, error) {
	if validateConnectResultName(connector.Name) != nil ||
		validateConnectResultName(connector.ConnectName) != nil {
		return generated.Connector{}, errOperationFailed
	}
	connectorType := generated.ConnectorType(strings.ToUpper(connector.Type))
	if !connectorType.Valid() {
		connectorType = generated.ConnectorTypeUNKNOWN
	}
	state := generated.ConnectorState(connector.State)
	if !state.Valid() {
		return generated.Connector{}, errOperationFailed
	}
	config := connector.Config
	if config == nil {
		config = map[string]any{}
	}
	output := generated.Connector{
		Config:  generated.ConnectorConfig(config),
		Connect: connector.ConnectName,
		Name:    connector.Name,
		Status: generated.ConnectorStatus{
			State:    state,
			Trace:    connectPointer(redactConnectTrace(connector.Trace)),
			WorkerId: connectPointer(connector.WorkerID),
		},
		Type: connectorType,
	}
	if len(connector.TaskIDs) > maxMCPConnectRawItems ||
		len(connector.Topics) > maxMCPConnectRawItems {
		return generated.Connector{}, errResultTooLarge
	}
	taskIDs := append([]int(nil), connector.TaskIDs...)
	sort.Ints(taskIDs)
	if len(taskIDs) > maxListItems {
		taskIDs = taskIDs[:maxListItems]
	}
	if len(taskIDs) > 0 {
		tasks := make([]generated.TaskId, 0, len(taskIDs))
		for _, taskID := range taskIDs {
			if taskID < 0 || int64(taskID) > math.MaxInt32 {
				return generated.Connector{}, errOperationFailed
			}
			tasks = append(tasks, generated.TaskId{
				Connector: connectPointer(connector.Name),
				Task:      connectPointer(int32(taskID)),
			})
		}
		output.Tasks = &tasks
	}
	topics := append([]string(nil), connector.Topics...)
	sort.Strings(topics)
	if len(topics) > maxListItems {
		topics = topics[:maxListItems]
	}
	if len(topics) > 0 {
		output.Topics = &topics
	}
	return output, nil
}

func connectorTaskToContract(
	connectorName string,
	task domaincluster.ConnectorTask,
) (generated.Task, error) {
	if task.ID < 0 || int64(task.ID) > math.MaxInt32 {
		return generated.Task{}, errOperationFailed
	}
	state := generated.ConnectorTaskStatus(task.State)
	if !state.Valid() {
		return generated.Task{}, errOperationFailed
	}
	taskID := int32(task.ID)
	output := generated.Task{
		Id: &generated.TaskId{
			Connector: connectPointer(connectorName),
			Task:      connectPointer(taskID),
		},
		Status: generated.TaskStatus{
			Id:       taskID,
			State:    state,
			Trace:    connectPointer(redactConnectTrace(task.Trace)),
			WorkerId: task.WorkerID,
		},
	}
	if len(task.Config) > 0 {
		config := generated.ConnectorConfig(task.Config)
		output.Config = &config
	}
	return output, nil
}

func pluginValidationToContract(
	validation domaincluster.PluginValidation,
) (any, error) {
	if validation.ErrorCount < 0 || int64(validation.ErrorCount) > math.MaxInt32 {
		return nil, errOperationFailed
	}
	if len(validation.Groups) > maxListItems {
		return nil, errResultTooLarge
	}
	if len(validation.Configs) > maxMCPConnectorConfigKeys {
		return nil, errResultTooLarge
	}
	groups := append([]string(nil), validation.Groups...)
	sort.Strings(groups)
	configs := append([]domaincluster.PluginConfigEntry(nil), validation.Configs...)
	sort.SliceStable(configs, func(left, right int) bool {
		return configs[left].Definition.Name < configs[right].Definition.Name
	})
	converted := make([]generated.ConnectorPluginConfig, 0, len(configs))
	for _, config := range configs {
		if validateConnectResultName(config.Definition.Name) != nil ||
			validateConnectResultName(config.Value.Name) != nil {
			return nil, errOperationFailed
		}
		if int64(config.Definition.Order) < math.MinInt32 ||
			int64(config.Definition.Order) > math.MaxInt32 {
			return nil, errOperationFailed
		}
		if len(config.Definition.Dependents) > maxListItems ||
			len(config.Value.RecommendedValues) > maxListItems ||
			len(config.Value.Errors) > maxListItems {
			return nil, errResultTooLarge
		}
		configType := generated.ConnectorPluginConfigDefinitionType(config.Definition.Type)
		importance := generated.ConnectorPluginConfigDefinitionImportance(config.Definition.Importance)
		width := generated.ConnectorPluginConfigDefinitionWidth(config.Definition.Width)
		if !configType.Valid() || !importance.Valid() || !width.Valid() {
			return nil, errOperationFailed
		}
		definition := generated.ConnectorPluginConfigDefinition{
			Name:          connectPointer(config.Definition.Name),
			Type:          connectPointer(configType),
			Required:      connectPointer(config.Definition.Required),
			DefaultValue:  connectPointer(config.Definition.DefaultValue),
			Importance:    connectPointer(importance),
			Documentation: connectPointer(config.Definition.Documentation),
			Group:         connectPointer(config.Definition.Group),
			Width:         connectPointer(width),
			DisplayName:   connectPointer(config.Definition.DisplayName),
			Order:         connectPointer(int32(config.Definition.Order)),
		}
		if len(config.Definition.Dependents) > 0 {
			dependents := append([]string(nil), config.Definition.Dependents...)
			sort.Strings(dependents)
			definition.Dependents = &dependents
		}
		value := generated.ConnectorPluginConfigValue{
			Name:    connectPointer(config.Value.Name),
			Value:   connectPointer(config.Value.Value),
			Visible: connectPointer(config.Value.Visible),
		}
		credential := isCredentialKey(config.Definition.Name) ||
			isCredentialKey(config.Value.Name) ||
			strings.EqualFold(config.Definition.Type, "PASSWORD")
		if credential {
			definition.DefaultValue = connectPointer(redactedValue)
			value.Value = connectPointer(redactedValue)
		}
		if len(config.Value.RecommendedValues) > 0 {
			recommended := append([]string(nil), config.Value.RecommendedValues...)
			if credential {
				for index := range recommended {
					recommended[index] = redactedValue
				}
			} else {
				sort.Strings(recommended)
			}
			value.RecommendedValues = &recommended
		}
		if len(config.Value.Errors) > 0 {
			configErrors := append([]string(nil), config.Value.Errors...)
			for index := range configErrors {
				if configErrors[index] != "" {
					configErrors[index] = redactedValue
				}
			}
			value.Errors = &configErrors
		}
		converted = append(converted, generated.ConnectorPluginConfig{
			Definition: &definition,
			Value:      &value,
		})
	}
	output := generated.ConnectorPluginConfigValidationResponse{
		Name:       connectPointer(validation.Name),
		ErrorCount: connectPointer(int32(validation.ErrorCount)),
	}
	if len(groups) > 0 {
		output.Groups = &groups
	}
	if len(converted) > 0 {
		output.Configs = &converted
	}
	return redactCredentials(output), nil
}

func connectState(state string) string {
	if state == "" {
		return string(generated.ConnectorStateUNASSIGNED)
	}
	return state
}

func connectStatsRequested[Params interface {
	generated.GetConnectsParams | generated.GetConnectsCsvParams
}](params *Params) bool {
	if params == nil {
		return false
	}
	switch typed := any(params).(type) {
	case *generated.GetConnectsParams:
		return typed.WithStats != nil && *typed.WithStats
	case *generated.GetConnectsCsvParams:
		return typed.WithStats != nil && *typed.WithStats
	default:
		return false
	}
}

func redactConnectTrace(trace string) string {
	if trace == "" {
		return ""
	}
	return redactedValue
}

func safeConnectAddress(address string) (string, error) {
	parsed, err := url.Parse(address)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errOperationFailed
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", errOperationFailed
	}
	parsed.User = nil
	query := parsed.Query()
	for key := range query {
		if isCredentialKey(key) {
			query.Set(key, redactedValue)
		}
	}
	parsed.RawQuery = query.Encode()
	parsed.Fragment = ""
	return parsed.String(), nil
}

func renderConnectCSV(header []string, rows [][]string) (string, error) {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	if err := writer.Write(header); err != nil {
		return "", errOperationFailed
	}
	for _, row := range rows {
		safe := make([]string, len(row))
		for index, value := range row {
			safe[index] = connectCSVCell(value)
		}
		if err := writer.Write(safe); err != nil {
			return "", errOperationFailed
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", errOperationFailed
	}
	return buffer.String(), nil
}

func connectCSVCell(value string) string {
	if value == "" {
		return value
	}
	switch value[0] {
	case '=', '+', '-', '@', '\t', '\r':
		return "'" + value
	default:
		return value
	}
}

func connectPointer[Value any](value Value) *Value {
	return &value
}
