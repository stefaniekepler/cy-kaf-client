package mcpserver

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domainanalysis "github.com/cy-kaf/cy-kaf-client/internal/domain/analysis"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

const (
	defaultMCPTopicPage       = 1
	defaultMCPTopicPerPage    = 25
	maxMCPTopicPerPage        = 100
	maxTopicNameBytes         = 249
	maxTopicConfigNameBytes   = 1024
	maxTopicConfigEntries     = maxListItems
	maxTopicConfigValueBytes  = maxResultBytes
	maxTopicConfigPayloadSize = maxResultBytes
)

var topicCSVHeader = []string{
	"bytesInPerSec",
	"bytesOutPerSec",
	"cleanUpPolicy",
	"inSyncReplicas",
	"internal",
	"messagesCount",
	"name",
	"partitionCount",
	"partitions",
	"replicas",
	"replicationFactor",
	"segmentCount",
	"segmentSize",
	"underReplicatedPartitions",
}

type topicPageResult struct {
	Items     []generated.Topic `json:"items"`
	Page      int               `json:"page"`
	PageCount int               `json:"pageCount"`
	Truncated bool              `json:"truncated"`
}

type topicConnectorsResult struct {
	Connectors []generated.FullConnectorInfo `json:"connectors"`
}

func topicTool(meta ToolMeta) ToolSpec {
	switch meta.Name {
	case "getTopics":
		return topicReadTool(meta, topicListInputSelector,
			func(ctx context.Context, executor *Executor, input clusterOptionalQueryInput[generated.GetTopicsParams]) (any, error) {
				return getTopics(ctx, executor, input)
			})
	case "getTopicsCsv":
		return topicReadTool(meta, topicCSVInputSelector,
			func(ctx context.Context, executor *Executor, input clusterOptionalQueryInput[generated.GetTopicsCsvParams]) (any, error) {
				return getTopicsCSV(ctx, executor, input, meta.MaxItems)
			})
	case "getTopicConfigs":
		return topicReadTool(meta, topicInputSelector, getTopicConfigs)
	case "getTopicDetails":
		return topicReadTool(meta, topicInputSelector, getTopicDetails)
	case "listTopicAcls":
		return topicReadTool(meta, topicInputSelector, listTopicAcls)
	case "analyzeTopic":
		return topicReadTool(meta, topicInputSelector, analyzeTopic)
	case "cancelTopicAnalysis":
		return topicReadTool(meta, topicInputSelector, cancelTopicAnalysis)
	case "getTopicAnalysis":
		return topicReadTool(meta, topicInputSelector, getTopicAnalysis)
	case "getActiveProducerStates":
		return topicReadTool(meta, topicInputSelector, getActiveProducerStates)
	case "getTopicConnectors":
		return topicReadTool(meta, topicInputSelector, getTopicConnectors)
	case "createTopic":
		return topicWriteToolWithSchema(meta, topicCreateInputSchema(), topicCreateInputSelector, createTopic)
	case "recreateTopic":
		return topicWriteTool(meta, topicInputSelector, recreateTopic)
	case "cloneTopic":
		return topicWriteTool(meta, topicCloneInputSelector, cloneTopic)
	case "deleteTopic":
		return topicWriteTool(meta, topicInputSelector, deleteTopic)
	case "updateTopic":
		return topicWriteToolWithSchema(meta, topicUpdateInputSchema(), topicUpdateInputSelector, updateTopic)
	case "increaseTopicPartitions":
		return topicWriteTool(meta, topicIncreaseInputSelector, increaseTopicPartitions)
	case "changeReplicationFactor":
		return topicWriteTool(meta, topicReplicationInputSelector, changeReplicationFactor)
	default:
		panic("unsupported Topics MCP tool: " + meta.Name)
	}
}

func topicReadTool[In any](
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

func topicWriteTool[In any](
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

func topicWriteToolWithSchema[In any](
	meta ToolMeta,
	inputSchema any,
	cluster clusterSelector[In],
	call toolCall[In],
) ToolSpec {
	return newToolWithInputSchema(
		meta,
		inputSchema,
		cluster,
		func(context.Context, *Executor, In) (AccessClass, error) {
			return AccessWrite, nil
		},
		call,
	)
}

func topicInputSelector(input topicInput) string {
	return input.ClusterName
}

func topicListInputSelector(input clusterOptionalQueryInput[generated.GetTopicsParams]) string {
	return input.ClusterName
}

func topicCSVInputSelector(input clusterOptionalQueryInput[generated.GetTopicsCsvParams]) string {
	return input.ClusterName
}

func topicCreateInputSelector(input clusterBodyInput[topicCreationLenientInput]) string {
	return input.ClusterName
}

func topicCloneInputSelector(input topicQueryInput[generated.CloneTopicParams]) string {
	return input.ClusterName
}

func topicUpdateInputSelector(input topicBodyInput[topicUpdateLenientInput]) string {
	return input.ClusterName
}

func topicIncreaseInputSelector(input topicBodyInput[generated.PartitionsIncrease]) string {
	return input.ClusterName
}

func topicReplicationInputSelector(input topicBodyInput[generated.ReplicationFactorChange]) string {
	return input.ClusterName
}

func getTopics(
	ctx context.Context,
	executor *Executor,
	input clusterOptionalQueryInput[generated.GetTopicsParams],
) (any, error) {
	query, err := topicListQuery(input.Query)
	if err != nil {
		return nil, err
	}
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	page, err := executor.deps.Topics.List(ctx, input.ClusterName, query)
	if err != nil {
		return nil, err
	}

	topics := append([]domaincluster.TopicState(nil), page.Topics...)
	sortTopicStates(topics, query)
	truncated := query.Page < page.PageCount
	if len(topics) > query.PerPage {
		topics = topics[:query.PerPage]
		truncated = true
	}
	pageNumber := query.Page
	if page.PageCount == 0 {
		pageNumber = 1
	} else if pageNumber > page.PageCount {
		pageNumber = page.PageCount
	}
	return topicPageResult{
		Items:     topicsFrom(topics),
		Page:      pageNumber,
		PageCount: page.PageCount,
		Truncated: truncated,
	}, nil
}

func getTopicsCSV(
	ctx context.Context,
	executor *Executor,
	input clusterOptionalQueryInput[generated.GetTopicsCsvParams],
	maxItems int,
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	query, err := topicCSVQuery(input.Query, maxItems)
	if err != nil {
		return nil, err
	}
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	page, err := executor.deps.Topics.List(ctx, input.ClusterName, query)
	if err != nil {
		return nil, err
	}
	topics := append([]domaincluster.TopicState(nil), page.Topics...)
	sortTopicStates(topics, query)
	if maxItems > 0 && len(topics) > maxItems {
		topics = topics[:maxItems]
	}
	return topicRowsCSV(topicsFrom(topics))
}

func getTopicConfigs(ctx context.Context, executor *Executor, input topicInput) (any, error) {
	if err := validateTopicInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	configs, err := executor.deps.Topics.Configs(ctx, input.ClusterName, input.TopicName)
	if err != nil {
		return nil, err
	}
	configs = append([]domaincluster.ConfigEntry(nil), configs...)
	sort.Slice(configs, func(left, right int) bool {
		return configs[left].Name < configs[right].Name
	})
	if len(configs) > maxListItems {
		configs = configs[:maxListItems]
	}
	output := make([]generated.TopicConfig, 0, len(configs))
	for _, config := range configs {
		output = append(output, topicConfigFrom(config))
	}
	redactTopicConfigValues(output)
	return redactCredentials(output), nil
}

func getTopicDetails(ctx context.Context, executor *Executor, input topicInput) (any, error) {
	if err := validateTopicInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	state, configs, err := executor.deps.Topics.Details(ctx, input.ClusterName, input.TopicName)
	if err != nil {
		return nil, err
	}
	return topicDetailsFrom(state, configs), nil
}

func listTopicAcls(ctx context.Context, executor *Executor, input topicInput) (any, error) {
	if err := validateTopicInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	acls, err := executor.deps.Topics.Acls(ctx, input.ClusterName, input.TopicName)
	if err != nil {
		return nil, err
	}
	acls = append([]domaincluster.AclBinding(nil), acls...)
	sort.Slice(acls, func(left, right int) bool {
		return topicACLKey(acls[left]) < topicACLKey(acls[right])
	})
	if len(acls) > maxListItems {
		acls = acls[:maxListItems]
	}
	output := make([]generated.KafkaAcl, 0, len(acls))
	for _, acl := range acls {
		output = append(output, topicACLFrom(acl))
	}
	return output, nil
}

func analyzeTopic(ctx context.Context, executor *Executor, input topicInput) (any, error) {
	if err := validateTopicInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Analysis == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Analysis.Analyze(ctx, input.ClusterName, input.TopicName); err != nil {
		return nil, err
	}
	return nil, nil
}

func cancelTopicAnalysis(ctx context.Context, executor *Executor, input topicInput) (any, error) {
	if err := validateTopicInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Analysis == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Analysis.Cancel(ctx, input.ClusterName, input.TopicName); err != nil {
		return nil, err
	}
	return nil, nil
}

func getTopicAnalysis(_ context.Context, executor *Executor, input topicInput) (any, error) {
	if err := validateTopicInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Analysis == nil {
		return nil, errOperationFailed
	}
	view, found, err := executor.deps.Analysis.Get(input.ClusterName, input.TopicName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, appcluster.ErrAnalysisTopicNotFound
	}
	return topicAnalysisFrom(view), nil
}

func getActiveProducerStates(ctx context.Context, executor *Executor, input topicInput) (any, error) {
	if err := validateTopicInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	producers, err := executor.deps.Topics.ActiveProducers(ctx, input.ClusterName, input.TopicName)
	if err != nil {
		return nil, err
	}
	producers = append([]domaincluster.ProducerState(nil), producers...)
	sort.Slice(producers, func(left, right int) bool {
		if producers[left].Partition != producers[right].Partition {
			return producers[left].Partition < producers[right].Partition
		}
		return producers[left].ProducerID < producers[right].ProducerID
	})
	if len(producers) > maxListItems {
		producers = producers[:maxListItems]
	}
	output := make([]generated.TopicProducerState, 0, len(producers))
	for _, producer := range producers {
		output = append(output, topicProducerFrom(producer))
	}
	return output, nil
}

func getTopicConnectors(ctx context.Context, executor *Executor, input topicInput) (any, error) {
	if err := validateTopicInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Topics.Connectors(ctx, input.ClusterName, input.TopicName); err != nil {
		return nil, err
	}
	return topicConnectorsResult{Connectors: []generated.FullConnectorInfo{}}, nil
}

func createTopic(
	ctx context.Context,
	executor *Executor,
	input clusterBodyInput[topicCreationLenientInput],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	configs, err := coerceTopicConfigs(input.Body.Configs)
	if err != nil {
		return nil, err
	}
	body := generated.TopicCreation{
		Name:              input.Body.Name,
		Partitions:        input.Body.Partitions,
		ReplicationFactor: input.Body.ReplicationFactor,
		Configs:           mapPointer(configs, input.Body.Configs != nil),
	}
	spec, err := topicSpecFrom(body)
	if err != nil {
		return nil, err
	}
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	state, err := executor.deps.Topics.Create(ctx, input.ClusterName, spec)
	if err != nil {
		return nil, err
	}
	return topicFrom(state), nil
}

func recreateTopic(ctx context.Context, executor *Executor, input topicInput) (any, error) {
	if err := validateTopicInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	state, err := executor.deps.Topics.Recreate(ctx, input.ClusterName, input.TopicName)
	if err != nil {
		return nil, err
	}
	return topicFrom(state), nil
}

func cloneTopic(
	ctx context.Context,
	executor *Executor,
	input topicQueryInput[generated.CloneTopicParams],
) (any, error) {
	if err := validateTopicInput(topicInput{ClusterName: input.ClusterName, TopicName: input.TopicName}); err != nil {
		return nil, err
	}
	if err := validateTopicName(input.Query.NewTopicName); err != nil {
		return nil, err
	}
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	state, err := executor.deps.Topics.Clone(
		ctx,
		input.ClusterName,
		input.TopicName,
		input.Query.NewTopicName,
	)
	if err != nil {
		return nil, err
	}
	return topicFrom(state), nil
}

func deleteTopic(ctx context.Context, executor *Executor, input topicInput) (any, error) {
	if err := validateTopicInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Topics.Delete(ctx, input.ClusterName, input.TopicName); err != nil {
		return nil, err
	}
	return nil, nil
}

func updateTopic(
	ctx context.Context,
	executor *Executor,
	input topicBodyInput[topicUpdateLenientInput],
) (any, error) {
	if err := validateTopicInput(topicInput{ClusterName: input.ClusterName, TopicName: input.TopicName}); err != nil {
		return nil, err
	}
	configs, err := coerceTopicConfigs(input.Body.Configs)
	if err != nil {
		return nil, err
	}
	body := generated.TopicUpdate{Configs: mapPointer(configs, input.Body.Configs != nil)}
	desired := map[string]string(nil)
	if body.Configs != nil {
		desired = *body.Configs
	}
	if err := validateTopicConfigs(desired); err != nil {
		return nil, err
	}
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	state, err := executor.deps.Topics.UpdateConfigs(ctx, input.ClusterName, input.TopicName, desired)
	if err != nil {
		return nil, err
	}
	return topicFrom(state), nil
}

func increaseTopicPartitions(
	ctx context.Context,
	executor *Executor,
	input topicBodyInput[generated.PartitionsIncrease],
) (any, error) {
	if err := validateTopicInput(topicInput{ClusterName: input.ClusterName, TopicName: input.TopicName}); err != nil {
		return nil, err
	}
	if input.Body.TotalPartitionsCount == nil ||
		*input.Body.TotalPartitionsCount < 1 ||
		int64(*input.Body.TotalPartitionsCount) > math.MaxInt32 {
		return nil, errInvalidRequest
	}
	total := int32(*input.Body.TotalPartitionsCount)
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Topics.IncreasePartitions(ctx, input.ClusterName, input.TopicName, total); err != nil {
		return nil, err
	}
	return generated.PartitionsIncreaseResponse{
		TopicName: input.TopicName, TotalPartitionsCount: int(total),
	}, nil
}

func changeReplicationFactor(
	ctx context.Context,
	executor *Executor,
	input topicBodyInput[generated.ReplicationFactorChange],
) (any, error) {
	if err := validateTopicInput(topicInput{ClusterName: input.ClusterName, TopicName: input.TopicName}); err != nil {
		return nil, err
	}
	if input.Body.TotalReplicationFactor == nil ||
		*input.Body.TotalReplicationFactor < 1 ||
		*input.Body.TotalReplicationFactor > math.MaxInt16 {
		return nil, errInvalidRequest
	}
	target := int16(*input.Body.TotalReplicationFactor)
	if executor.deps.Topics == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Topics.ChangeReplicationFactor(ctx, input.ClusterName, input.TopicName, target); err != nil {
		return nil, err
	}
	return generated.ReplicationFactorChangeResponse{
		TopicName: input.TopicName, TotalReplicationFactor: int(target),
	}, nil
}

func topicListQuery(params *generated.GetTopicsParams) (appcluster.TopicListQuery, error) {
	query := appcluster.TopicListQuery{Page: defaultMCPTopicPage, PerPage: defaultMCPTopicPerPage}
	if params == nil {
		return query, nil
	}
	if params.Page != nil {
		if *params.Page < 1 {
			return appcluster.TopicListQuery{}, errInvalidRequest
		}
		query.Page = int(*params.Page)
	}
	if params.PerPage != nil {
		if *params.PerPage < 1 || *params.PerPage > maxMCPTopicPerPage {
			return appcluster.TopicListQuery{}, errInvalidRequest
		}
		query.PerPage = int(*params.PerPage)
	}
	if err := applyTopicListFields(
		&query,
		params.ShowInternal,
		params.Search,
		params.OrderBy,
		params.SortOrder,
	); err != nil {
		return appcluster.TopicListQuery{}, err
	}
	return query, nil
}

func topicCSVQuery(params *generated.GetTopicsCsvParams, maxItems int) (appcluster.TopicListQuery, error) {
	query := appcluster.TopicListQuery{Page: defaultMCPTopicPage, PerPage: maxItems}
	if maxItems <= 0 {
		return appcluster.TopicListQuery{}, errInvalidRequest
	}
	if params == nil {
		return query, nil
	}
	if err := applyTopicListFields(
		&query,
		params.ShowInternal,
		params.Search,
		params.OrderBy,
		params.SortOrder,
	); err != nil {
		return appcluster.TopicListQuery{}, err
	}
	return query, nil
}

func applyTopicListFields(
	query *appcluster.TopicListQuery,
	showInternal *bool,
	search *string,
	orderBy *generated.TopicColumnsToSort,
	sortOrder *generated.SortOrder,
) error {
	if showInternal != nil {
		query.ShowInternal = *showInternal
	}
	if search != nil {
		if len(*search) > maxClusterBrokerNameBytes {
			return errInvalidRequest
		}
		query.Search = *search
	}
	if orderBy != nil {
		if !orderBy.Valid() {
			return errInvalidRequest
		}
		query.OrderBy = string(*orderBy)
	}
	if sortOrder != nil {
		if !sortOrder.Valid() {
			return errInvalidRequest
		}
		query.SortOrder = string(*sortOrder)
	}
	return nil
}

func topicSpecFrom(body generated.TopicCreation) (domaincluster.TopicSpec, error) {
	if err := validateTopicName(body.Name); err != nil {
		return domaincluster.TopicSpec{}, err
	}
	if body.Partitions != -1 && body.Partitions < 1 {
		return domaincluster.TopicSpec{}, errInvalidRequest
	}
	replicationFactor := int16(-1)
	if body.ReplicationFactor != nil {
		if *body.ReplicationFactor != -1 &&
			(*body.ReplicationFactor < 1 || *body.ReplicationFactor > math.MaxInt16) {
			return domaincluster.TopicSpec{}, errInvalidRequest
		}
		replicationFactor = int16(*body.ReplicationFactor)
	}
	var configs map[string]string
	if body.Configs != nil {
		configs = *body.Configs
	}
	if err := validateTopicConfigs(configs); err != nil {
		return domaincluster.TopicSpec{}, err
	}
	return domaincluster.TopicSpec{
		Name:              body.Name,
		Partitions:        body.Partitions,
		ReplicationFactor: replicationFactor,
		Configs:           configs,
	}, nil
}

func coerceTopicConfigs(raw map[string]json.RawMessage) (map[string]string, error) {
	if raw == nil {
		return nil, nil
	}
	if len(raw) > maxTopicConfigEntries {
		return nil, errInvalidRequest
	}
	output := make(map[string]string, len(raw))
	for name, encoded := range raw {
		if strings.TrimSpace(name) == "" || len(name) > maxTopicConfigNameBytes {
			return nil, errInvalidRequest
		}
		encoded = bytes.TrimSpace(encoded)
		if len(encoded) == 0 {
			return nil, errInvalidRequest
		}
		if bytes.Equal(encoded, []byte("null")) {
			continue
		}
		var value any
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, errInvalidRequest
		}
		switch typed := value.(type) {
		case string:
			output[name] = typed
		case bool:
			output[name] = strconv.FormatBool(typed)
		case json.Number:
			output[name] = typed.String()
		default:
			return nil, errInvalidRequest
		}
	}
	return output, nil
}

func validateTopicInput(input topicInput) error {
	if err := validateClusterName(input.ClusterName); err != nil {
		return err
	}
	return validateTopicName(input.TopicName)
}

func validateClusterName(name string) error {
	return validateBoundedName(name)
}

func validateTopicName(name string) error {
	if strings.TrimSpace(name) == "" ||
		len(name) > maxTopicNameBytes ||
		name == "." ||
		name == ".." {
		return errInvalidRequest
	}
	for _, char := range name {
		if (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '.' ||
			char == '_' ||
			char == '-' {
			continue
		}
		return errInvalidRequest
	}
	return nil
}

func validateTopicConfigs(configs map[string]string) error {
	if len(configs) > maxTopicConfigEntries {
		return errInvalidRequest
	}
	total := 0
	for name, value := range configs {
		if strings.TrimSpace(name) == "" || len(name) > maxTopicConfigNameBytes {
			return errInvalidRequest
		}
		if len(value) > maxTopicConfigValueBytes {
			return errInvalidRequest
		}
		total += len(name) + len(value)
		if total > maxTopicConfigPayloadSize {
			return errInvalidRequest
		}
	}
	return nil
}

func mapPointer(configs map[string]string, present bool) *map[string]string {
	if !present {
		return nil
	}
	return &configs
}

func topicCreateInputSchema() map[string]any {
	return topicLenientInputSchema(false)
}

func topicUpdateInputSchema() map[string]any {
	return topicLenientInputSchema(true)
}

func topicLenientInputSchema(update bool) map[string]any {
	scalarConfig := map[string]any{
		"oneOf": []any{
			map[string]any{"type": "string"},
			map[string]any{"type": "number"},
			map[string]any{"type": "boolean"},
			map[string]any{"type": "null"},
		},
	}
	configs := map[string]any{
		"type":                 "object",
		"additionalProperties": scalarConfig,
	}
	bodyProperties := map[string]any{"configs": configs}
	bodyRequired := []string{}
	rootProperties := map[string]any{
		"clusterName": map[string]any{"type": "string"},
	}
	rootRequired := []string{"clusterName", "body"}
	if update {
		rootProperties["topicName"] = map[string]any{"type": "string"}
		rootRequired = []string{"clusterName", "topicName", "body"}
	} else {
		bodyProperties["name"] = map[string]any{"type": "string"}
		bodyProperties["partitions"] = map[string]any{"type": "integer"}
		bodyProperties["replicationFactor"] = map[string]any{
			"type": []string{"integer", "null"},
		}
		bodyRequired = []string{"name", "partitions"}
	}
	rootProperties["body"] = map[string]any{
		"type":                 "object",
		"properties":           bodyProperties,
		"required":             bodyRequired,
		"additionalProperties": false,
	}
	return map[string]any{
		"type":                 "object",
		"properties":           rootProperties,
		"required":             rootRequired,
		"additionalProperties": false,
	}
}

func sortTopicStates(topics []domaincluster.TopicState, query appcluster.TopicListQuery) {
	descending := query.SortOrder == string(generated.DESC)
	sort.SliceStable(topics, func(left, right int) bool {
		a, b := topics[left], topics[right]
		var comparison int
		switch query.OrderBy {
		case string(generated.MESSAGESCOUNT):
			ac, ak := a.MessagesCount()
			bc, bk := b.MessagesCount()
			switch {
			case ak != bk:
				return ak
			case ak && ac < bc:
				comparison = -1
			case ak && ac > bc:
				comparison = 1
			}
		case string(generated.TOTALPARTITIONS):
			comparison = compareInts(len(a.Partitions), len(b.Partitions))
		case string(generated.REPLICATIONFACTOR):
			comparison = compareInts(a.ReplicationFactor, b.ReplicationFactor)
		case string(generated.SIZE):
			comparison = compareInt64s(a.SegmentSize, b.SegmentSize)
		default:
			comparison = strings.Compare(a.Name, b.Name)
		}
		if comparison == 0 {
			return a.Name < b.Name
		}
		if descending {
			return comparison > 0
		}
		return comparison < 0
	})
}

func compareInts(left, right int) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareInt64s(left, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func topicRowsCSV(topics []generated.Topic) (string, error) {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	if err := writer.Write(topicCSVHeader); err != nil {
		return "", errOperationFailed
	}
	for _, topic := range topics {
		if err := writer.Write([]string{
			topicFloat64Cell(topic.BytesInPerSec),
			topicFloat64Cell(topic.BytesOutPerSec),
			topicStringerCell(topic.CleanUpPolicy),
			topicInt32Cell(topic.InSyncReplicas),
			topicBoolCell(topic.Internal),
			topicInt64Cell(topic.MessagesCount),
			topic.Name,
			topicInt32Cell(topic.PartitionCount),
			"",
			topicInt32Cell(topic.Replicas),
			topicInt32Cell(topic.ReplicationFactor),
			topicInt32Cell(topic.SegmentCount),
			topicInt64Cell(topic.SegmentSize),
			topicInt32Cell(topic.UnderReplicatedPartitions),
		}); err != nil {
			return "", errOperationFailed
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", errOperationFailed
	}
	return buffer.String(), nil
}

func topicStringerCell[Value ~string](value *Value) string {
	if value == nil {
		return ""
	}
	return string(*value)
}

func topicInt32Cell(value *int32) string {
	if value == nil {
		return ""
	}
	return strconv.FormatInt(int64(*value), 10)
}

func topicInt64Cell(value *int64) string {
	if value == nil {
		return ""
	}
	return strconv.FormatInt(*value, 10)
}

func topicFloat64Cell(value *float64) string {
	if value == nil {
		return ""
	}
	return strconv.FormatFloat(*value, 'f', -1, 64)
}

func topicBoolCell(value *bool) string {
	if value == nil {
		return ""
	}
	return strconv.FormatBool(*value)
}

func topicFrom(state domaincluster.TopicState) generated.Topic {
	replicas, inSync, underReplicated := topicTalliesFrom(state)
	partitionCount, replicationFactor := topicCountPointers(state)
	return generated.Topic{
		Name:                      state.Name,
		Internal:                  topicPointer(state.Internal),
		PartitionCount:            partitionCount,
		ReplicationFactor:         replicationFactor,
		Replicas:                  topicPointer(replicas),
		InSyncReplicas:            topicPointer(inSync),
		SegmentSize:               topicPointer(state.SegmentSize),
		SegmentCount:              topicPointer(boundedInt32Count(state.SegmentCount)),
		UnderReplicatedPartitions: topicPointer(underReplicated),
		MessagesCount:             topicMessagesCountFrom(state),
	}
}

func topicsFrom(states []domaincluster.TopicState) []generated.Topic {
	output := make([]generated.Topic, 0, len(states))
	for _, state := range states {
		output = append(output, topicFrom(state))
	}
	return output
}

func topicDetailsFrom(state domaincluster.TopicState, configs []domaincluster.ConfigEntry) generated.TopicDetails {
	replicas, inSync, underReplicated := topicTalliesFrom(state)
	partitionCount, replicationFactor := topicCountPointers(state)
	output := generated.TopicDetails{
		Name:                      state.Name,
		Internal:                  topicPointer(state.Internal),
		PartitionCount:            partitionCount,
		ReplicationFactor:         replicationFactor,
		Replicas:                  topicPointer(replicas),
		InSyncReplicas:            topicPointer(inSync),
		SegmentSize:               topicPointer(state.SegmentSize),
		SegmentCount:              topicPointer(boundedInt32Count(state.SegmentCount)),
		UnderReplicatedPartitions: topicPointer(underReplicated),
	}
	if len(state.Partitions) > 0 {
		partitions := topicPartitionsFrom(state.Partitions)
		output.Partitions = &partitions
	}
	if policy := topicCleanupPolicy(configs); policy != "" {
		output.CleanUpPolicy = &policy
	}
	return output
}

func topicTalliesFrom(state domaincluster.TopicState) (replicas, inSync, underReplicated int32) {
	for _, partition := range state.Partitions {
		replicas += boundedInt32Count(len(partition.Replicas))
		inSync += boundedInt32Count(len(partition.ISR))
		if len(partition.ISR) < len(partition.Replicas) {
			underReplicated++
		}
	}
	return replicas, inSync, underReplicated
}

func topicCountPointers(state domaincluster.TopicState) (*int32, *int32) {
	var partitionCount, replicationFactor *int32
	if len(state.Partitions) > 0 {
		partitionCount = topicPointer(boundedInt32Count(len(state.Partitions)))
	}
	if state.ReplicationFactor > 0 {
		replicationFactor = topicPointer(boundedInt32Count(state.ReplicationFactor))
	}
	return partitionCount, replicationFactor
}

func topicMessagesCountFrom(state domaincluster.TopicState) *int64 {
	count, known := state.MessagesCount()
	if !known {
		return nil
	}
	return &count
}

func topicPartitionsFrom(input []domaincluster.PartitionState) []generated.Partition {
	partitions := append([]domaincluster.PartitionState(nil), input...)
	sort.Slice(partitions, func(left, right int) bool {
		return partitions[left].ID < partitions[right].ID
	})
	if len(partitions) > maxListItems {
		partitions = partitions[:maxListItems]
	}
	output := make([]generated.Partition, 0, len(partitions))
	for _, partition := range partitions {
		replicas := append([]int32(nil), partition.Replicas...)
		if len(replicas) > maxListItems {
			replicas = replicas[:maxListItems]
		}
		inSync := make(map[int32]bool, len(partition.ISR))
		for _, broker := range partition.ISR {
			inSync[broker] = true
		}
		generatedReplicas := make([]generated.Replica, 0, len(replicas))
		for _, broker := range replicas {
			generatedReplicas = append(generatedReplicas, generated.Replica{
				Broker: topicPointer(broker),
				Leader: topicPointer(broker == partition.Leader),
				InSync: topicPointer(inSync[broker]),
			})
		}
		output = append(output, generated.Partition{
			Partition: partition.ID,
			Leader:    topicPointer(partition.Leader),
			OffsetMin: partition.StartOffset,
			OffsetMax: partition.EndOffset,
			Replicas:  &generatedReplicas,
		})
	}
	return output
}

func topicCleanupPolicy(configs []domaincluster.ConfigEntry) generated.CleanUpPolicy {
	for _, config := range configs {
		if config.Name != "cleanup.policy" {
			continue
		}
		switch config.Value {
		case "delete":
			return generated.CleanUpPolicyDELETE
		case "compact":
			return generated.CleanUpPolicyCOMPACT
		case "compact,delete", "delete,compact":
			return generated.CleanUpPolicyCOMPACTDELETE
		default:
			return generated.CleanUpPolicyUNKNOWN
		}
	}
	return ""
}

func topicConfigFrom(config domaincluster.ConfigEntry) generated.TopicConfig {
	source := brokerConfigSourceFrom(config.Source)
	output := generated.TopicConfig{
		Name:        config.Name,
		Value:       topicPointer(config.Value),
		Source:      &source,
		IsSensitive: topicPointer(config.IsSensitive),
		IsReadOnly:  topicPointer(config.IsReadOnly),
	}
	for _, synonym := range config.Synonyms {
		if synonym.Source == "DEFAULT_CONFIG" && output.DefaultValue == nil {
			defaultValue := synonym.Value
			if isCredentialKey(synonym.Name) {
				defaultValue = redactedValue
			}
			output.DefaultValue = topicPointer(defaultValue)
		}
	}
	if len(config.Synonyms) > 0 {
		domainSynonyms := append([]domaincluster.ConfigSynonym(nil), config.Synonyms...)
		sort.Slice(domainSynonyms, func(left, right int) bool {
			if domainSynonyms[left].Name != domainSynonyms[right].Name {
				return domainSynonyms[left].Name < domainSynonyms[right].Name
			}
			if domainSynonyms[left].Source != domainSynonyms[right].Source {
				return domainSynonyms[left].Source < domainSynonyms[right].Source
			}
			return domainSynonyms[left].Value < domainSynonyms[right].Value
		})
		if len(domainSynonyms) > maxListItems {
			domainSynonyms = domainSynonyms[:maxListItems]
		}
		synonyms := make([]generated.ConfigSynonym, 0, len(domainSynonyms))
		for _, synonym := range domainSynonyms {
			synonymSource := brokerConfigSourceFrom(synonym.Source)
			synonyms = append(synonyms, generated.ConfigSynonym{
				Name:   topicPointer(synonym.Name),
				Value:  topicPointer(synonym.Value),
				Source: &synonymSource,
			})
		}
		output.Synonyms = &synonyms
	}
	return output
}

func redactTopicConfigValues(configs []generated.TopicConfig) {
	for index := range configs {
		sensitive := (configs[index].IsSensitive != nil && *configs[index].IsSensitive) ||
			isCredentialKey(configs[index].Name)
		if sensitive {
			if configs[index].Value != nil {
				*configs[index].Value = redactedValue
			}
			if configs[index].DefaultValue != nil {
				*configs[index].DefaultValue = redactedValue
			}
		}
		if configs[index].Synonyms == nil {
			continue
		}
		for synonymIndex := range *configs[index].Synonyms {
			synonym := &(*configs[index].Synonyms)[synonymIndex]
			if synonym.Value == nil {
				continue
			}
			if sensitive || (synonym.Name != nil && isCredentialKey(*synonym.Name)) {
				*synonym.Value = redactedValue
			}
		}
	}
}

func topicACLFrom(acl domaincluster.AclBinding) generated.KafkaAcl {
	return generated.KafkaAcl{
		Principal:       acl.Principal,
		Host:            acl.Host,
		ResourceType:    generated.KafkaAclResourceType(acl.ResourceType),
		ResourceName:    acl.ResourceName,
		NamePatternType: generated.KafkaAclNamePatternType(acl.PatternType),
		Operation:       generated.KafkaAclOperation(acl.Operation),
		Permission:      generated.KafkaAclPermission(acl.Permission),
	}
}

func topicACLKey(acl domaincluster.AclBinding) string {
	return strings.Join([]string{
		acl.ResourceType,
		acl.ResourceName,
		acl.PatternType,
		acl.Principal,
		acl.Host,
		acl.Operation,
		acl.Permission,
	}, "\x00")
}

func topicProducerFrom(producer domaincluster.ProducerState) generated.TopicProducerState {
	return generated.TopicProducerState{
		Partition:                     topicPointer(producer.Partition),
		ProducerId:                    topicPointer(producer.ProducerID),
		ProducerEpoch:                 topicPointer(producer.ProducerEpoch),
		LastSequence:                  topicPointer(producer.LastSequence),
		LastTimestampMs:               topicPointer(producer.LastTimestamp),
		CoordinatorEpoch:              topicPointer(producer.CoordinatorEpoch),
		CurrentTransactionStartOffset: topicPointer(producer.CurrentTransactionStartOffset),
	}
}

func topicAnalysisFrom(view appcluster.AnalysisView) generated.TopicAnalysis {
	if view.Progress != nil {
		return generated.TopicAnalysis{Progress: &generated.TopicAnalysisProgress{
			StartedAt:           topicPointer(view.Progress.StartedAt),
			CompletenessPercent: topicPointer(view.Progress.CompletenessPercent),
			MsgsScanned:         topicPointer(view.Progress.MsgsScanned),
			BytesScanned:        topicPointer(view.Progress.BytesScanned),
		}}
	}
	if view.Result == nil {
		return generated.TopicAnalysis{}
	}
	result := topicAnalysisResultFrom(*view.Result)
	return generated.TopicAnalysis{Result: &result}
}

func topicAnalysisResultFrom(result appcluster.AnalysisResult) generated.TopicAnalysisResult {
	stats := append([]domainanalysis.Stats(nil), result.PartitionStats...)
	sort.Slice(stats, func(left, right int) bool {
		if stats[left].Partition == nil {
			return stats[right].Partition != nil
		}
		if stats[right].Partition == nil {
			return false
		}
		return *stats[left].Partition < *stats[right].Partition
	})
	if len(stats) > maxListItems {
		stats = stats[:maxListItems]
	}
	partitions := make([]generated.TopicAnalysisStats, 0, len(stats))
	for _, partition := range stats {
		partitions = append(partitions, topicAnalysisStatsFrom(partition))
	}
	output := generated.TopicAnalysisResult{
		StartedAt:      topicPointer(result.StartedAt),
		FinishedAt:     topicPointer(result.FinishedAt),
		TotalStats:     topicPointer(topicAnalysisStatsFrom(result.TotalStats)),
		PartitionStats: &partitions,
	}
	if result.Error != "" {
		output.Error = topicPointer(errOperationFailed.Error())
	}
	return output
}

func topicAnalysisStatsFrom(stats domainanalysis.Stats) generated.TopicAnalysisStats {
	output := generated.TopicAnalysisStats{}
	if stats.Partition != nil {
		output.Partition = topicPointer(*stats.Partition)
	}
	if !stats.HasData {
		return output
	}
	output.TotalMsgs = topicPointer(stats.TotalMsgs)
	output.MinOffset = topicPointer(stats.MinOffset)
	output.MaxOffset = topicPointer(stats.MaxOffset)
	output.MinTimestamp = topicPointer(stats.MinTimestamp)
	output.MaxTimestamp = topicPointer(stats.MaxTimestamp)
	output.NullKeys = topicPointer(stats.NullKeys)
	output.NullValues = topicPointer(stats.NullValues)
	output.ApproxUniqKeys = topicPointer(stats.ApproxUniqKeys)
	output.ApproxUniqValues = topicPointer(stats.ApproxUniqValues)
	output.KeySize = topicPointer(topicAnalysisSizeFrom(stats.KeySize))
	output.ValueSize = topicPointer(topicAnalysisSizeFrom(stats.ValueSize))
	if len(stats.HourlyMsgCounts) > 0 {
		domainHours := append([]domainanalysis.HourCount(nil), stats.HourlyMsgCounts...)
		sort.Slice(domainHours, func(left, right int) bool {
			return domainHours[left].HourStart < domainHours[right].HourStart
		})
		if len(domainHours) > maxListItems {
			domainHours = domainHours[:maxListItems]
		}
		hours := make([]struct {
			Count     *int64 `json:"count,omitempty"`
			HourStart *int64 `json:"hourStart,omitempty"`
		}, len(domainHours))
		for index, hour := range domainHours {
			hours[index].Count = topicPointer(hour.Count)
			hours[index].HourStart = topicPointer(hour.HourStart)
		}
		output.HourlyMsgCounts = &hours
	}
	return output
}

func topicAnalysisSizeFrom(stats domainanalysis.SizeStats) generated.TopicAnalysisSizeStats {
	return generated.TopicAnalysisSizeStats{
		Sum:      topicPointer(stats.Sum),
		Min:      topicPointer(stats.Min),
		Max:      topicPointer(stats.Max),
		Avg:      topicPointer(stats.Avg),
		Prctl50:  topicPointer(stats.Prctl50),
		Prctl75:  topicPointer(stats.Prctl75),
		Prctl95:  topicPointer(stats.Prctl95),
		Prctl99:  topicPointer(stats.Prctl99),
		Prctl999: topicPointer(stats.Prctl999),
	}
}

func topicPointer[Value any](value Value) *Value {
	return &value
}
