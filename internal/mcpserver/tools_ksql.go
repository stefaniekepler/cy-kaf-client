package mcpserver

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

const (
	maxMCPKsqlSQLBytes           = 64 << 10
	maxMCPKsqlPropertyCount      = 32
	maxMCPKsqlPropertyKeyBytes   = 256
	maxMCPKsqlPropertyValueBytes = 4 << 10
	maxMCPKsqlPipeIDBytes        = 1024
	maxMCPKsqlRawListItems       = 2000
	maxMCPKsqlDescriptionBytes   = 64 << 10
	maxMCPKsqlFrames             = maxKsqlRows
	maxMCPKsqlColumns            = 512
	maxMCPKsqlValueDepth         = 32
	maxMCPKsqlValueNodes         = maxKsqlRows*maxMCPKsqlColumns + maxKsqlRows
	ksqlResultEnvelopeReserve    = 128
	ksqlResultFrameReserve       = 96
)

var allowedMCPKsqlStreamsProperties = map[string]map[string]struct{}{
	// This is the request-local property used by the existing UI contract.
	// Network, TLS, authentication, file and arbitrary server properties are
	// deliberately not forwarded from an MCP caller.
	"auto.offset.reset": {
		"earliest": {},
		"latest":   {},
	},
}

type ksqlCollectionLimitError struct{}

func (*ksqlCollectionLimitError) Error() string {
	return "ksql result collection limit reached"
}

var errKsqlCollectionLimit = &ksqlCollectionLimitError{}

type ksqlResultCollector struct {
	frames        []generated.KsqlResponse
	schemaColumns []string
	rows          int
	maxRows       int
	maxBytes      int
	budget        ksqlValueBudget
}

type ksqlValueVisit struct {
	kind reflect.Kind
	ptr  uintptr
}

type ksqlValueBudget struct {
	nodes    int
	bytes    int
	visiting map[ksqlValueVisit]struct{}
}

func ksqlTool(meta ToolMeta) ToolSpec {
	switch meta.Name {
	case "executeKsql":
		return newTool(
			meta,
			func(input clusterBodyInput[generated.KsqlCommandV2]) string {
				return input.ClusterName
			},
			classifyMCPKsqlAccess,
			executeMCPKsql,
		)
	case "openKsqlResponsePipe":
		return newTool(
			meta,
			func(input clusterQueryInput[generated.OpenKsqlResponsePipeParams]) string {
				return input.ClusterName
			},
			func(context.Context, *Executor, clusterQueryInput[generated.OpenKsqlResponsePipeParams]) (AccessClass, error) {
				return AccessReadOnly, nil
			},
			func(
				ctx context.Context,
				executor *Executor,
				input clusterQueryInput[generated.OpenKsqlResponsePipeParams],
			) (any, error) {
				return openMCPKsql(ctx, executor, meta, input)
			},
		)
	case "listStreams":
		return newMCPKsqlReadTool(meta, listMCPKsqlStreams)
	case "listTables":
		return newMCPKsqlReadTool(meta, listMCPKsqlTables)
	default:
		panic("unsupported KSQL MCP tool: " + meta.Name)
	}
}

func newMCPKsqlReadTool(
	meta ToolMeta,
	call toolCall[clusterInput],
) ToolSpec {
	return newTool(
		meta,
		func(input clusterInput) string { return input.ClusterName },
		func(context.Context, *Executor, clusterInput) (AccessClass, error) {
			return AccessReadOnly, nil
		},
		call,
	)
}

func classifyMCPKsqlAccess(
	_ context.Context,
	executor *Executor,
	input clusterBodyInput[generated.KsqlCommandV2],
) (AccessClass, error) {
	if err := validateMCPKsqlCommandInput(input); err != nil {
		return AccessConditionalKSQL, err
	}
	if executor.deps.KSQLClassifier == nil {
		return AccessConditionalKSQL, errOperationFailed
	}
	kind, err := executor.deps.KSQLClassifier.Classify(input.Body.Ksql)
	if err != nil {
		return AccessConditionalKSQL, errInvalidRequest
	}
	return ksqlAccessForKind(kind)
}

func ksqlAccessForKind(kind domaincluster.KsqlStatementKind) (AccessClass, error) {
	switch kind {
	case domaincluster.KsqlQuery:
		return AccessReadOnly, nil
	case domaincluster.KsqlStatement:
		return AccessWrite, nil
	default:
		return AccessConditionalKSQL, errInvalidRequest
	}
}

func executeMCPKsql(
	ctx context.Context,
	executor *Executor,
	input clusterBodyInput[generated.KsqlCommandV2],
) (any, error) {
	if executor.deps.KSQL == nil {
		return nil, errOperationFailed
	}
	properties := make(map[string]string)
	if input.Body.StreamsProperties != nil {
		properties = cloneMCPKsqlProperties(*input.Body.StreamsProperties)
	}
	pipeID, err := executor.deps.KSQL.Register(
		ctx,
		input.ClusterName,
		input.Body.Ksql,
		properties,
	)
	if err != nil {
		return nil, err
	}
	if err := validateMCPKsqlPipeID(pipeID); err != nil {
		return nil, errOperationFailed
	}
	return generated.KsqlCommandV2Response{PipeId: pipeID}, nil
}

func openMCPKsql(
	ctx context.Context,
	executor *Executor,
	meta ToolMeta,
	input clusterQueryInput[generated.OpenKsqlResponsePipeParams],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if err := validateMCPKsqlPipeID(input.Query.PipeId); err != nil {
		return nil, err
	}
	if executor.deps.KSQL == nil {
		return nil, errOperationFailed
	}

	collector := ksqlResultCollector{
		frames:   make([]generated.KsqlResponse, 0, min(meta.MaxItems, maxMCPKsqlFrames)),
		maxRows:  min(meta.MaxItems, maxKsqlRows),
		maxBytes: min(meta.MaxBytes, maxResultBytes),
		budget: ksqlValueBudget{
			bytes:    ksqlResultEnvelopeReserve,
			visiting: make(map[ksqlValueVisit]struct{}),
		},
	}
	if collector.maxRows <= 0 || collector.maxBytes <= 0 {
		return nil, errOperationFailed
	}

	err := executor.deps.KSQL.OpenAuthorized(
		ctx,
		input.ClusterName,
		input.Query.PipeId,
		func(kind domaincluster.KsqlStatementKind) error {
			access, err := ksqlAccessForKind(kind)
			if err != nil {
				return err
			}
			// This is deliberately a fresh policy load after the one-shot pipe
			// has been claimed. A cached write approval cannot execute here.
			return executor.authorize(ctx, meta, input.ClusterName, access)
		},
		collector.collect,
	)
	if err != nil {
		classification := classifyKsqlCollectionLimitErrorTree(err)
		if !classification.complete || !classification.onlyLimit {
			if classification.containsLimit {
				return nil, errOperationFailed
			}
			return nil, err
		}
		return nil, errResultTooLarge
	}
	return collector.frames, nil
}

type ksqlLimitErrorTree struct {
	onlyLimit     bool
	containsLimit bool
	complete      bool
}

func classifyKsqlCollectionLimitErrorTree(err error) ksqlLimitErrorTree {
	nodes := 0
	return classifyKsqlCollectionLimitError(err, 0, &nodes)
}

func classifyKsqlCollectionLimitError(err error, depth int, nodes *int) ksqlLimitErrorTree {
	const (
		maxDepth = 32
		maxNodes = 128
	)
	if err == nil {
		return ksqlLimitErrorTree{complete: true}
	}
	if depth >= maxDepth || *nodes >= maxNodes {
		return ksqlLimitErrorTree{}
	}
	*nodes++
	if limitErr, ok := err.(*ksqlCollectionLimitError); ok && limitErr == errKsqlCollectionLimit {
		return ksqlLimitErrorTree{onlyLimit: true, containsLimit: true, complete: true}
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() error }:
		return classifyKsqlCollectionLimitError(wrapped.Unwrap(), depth+1, nodes)
	case interface{ Unwrap() []error }:
		children := wrapped.Unwrap()
		if len(children) == 0 {
			return ksqlLimitErrorTree{complete: true}
		}
		result := ksqlLimitErrorTree{onlyLimit: true, complete: true}
		for _, child := range children {
			classification := classifyKsqlCollectionLimitError(child, depth+1, nodes)
			if !classification.complete {
				return ksqlLimitErrorTree{}
			}
			result.onlyLimit = result.onlyLimit && classification.onlyLimit
			result.containsLimit = result.containsLimit || classification.containsLimit
		}
		return result
	default:
		return ksqlLimitErrorTree{complete: true}
	}
}

func (c *ksqlResultCollector) collect(table domaincluster.KsqlTable) error {
	if table.IsError {
		return errOperationFailed
	}
	if len(c.frames) >= maxMCPKsqlFrames ||
		len(table.ColumnNames) > maxMCPKsqlColumns ||
		len(table.Values) > c.maxRows-c.rows {
		return errKsqlCollectionLimit
	}

	columns := make([]string, len(table.ColumnNames))
	for index, column := range table.ColumnNames {
		cloned, err := c.budget.cloneString(column)
		if err != nil {
			return errKsqlCollectionLimit
		}
		columns[index] = cloned
	}
	sensitivityColumns := columns
	if len(sensitivityColumns) == 0 {
		sensitivityColumns = c.schemaColumns
	}

	values := make([][]any, len(table.Values))
	for rowIndex, row := range table.Values {
		if len(row) > maxMCPKsqlColumns {
			return errKsqlCollectionLimit
		}
		if len(row) > 0 &&
			(len(sensitivityColumns) == 0 || len(row) != len(sensitivityColumns)) {
			return errOperationFailed
		}
		values[rowIndex] = make([]any, len(row))
		for columnIndex, value := range row {
			sensitive := isCredentialKey(sensitivityColumns[columnIndex])
			cloned, err := c.budget.clone(value, 0, sensitive)
			if err != nil {
				return err
			}
			values[rowIndex][columnIndex] = cloned
		}
	}

	header, err := c.budget.cloneString(table.Header)
	if err != nil {
		return errKsqlCollectionLimit
	}
	frame := generated.KsqlResponse{Table: &generated.KsqlTableResponse{
		Header:      &header,
		ColumnNames: &columns,
		Values:      &values,
	}}
	candidate := append(c.frames, frame)
	raw, err := json.Marshal(map[string]any{"result": candidate})
	if err != nil {
		return errOperationFailed
	}
	if len(raw) > c.maxBytes {
		return errKsqlCollectionLimit
	}
	c.frames = candidate
	c.rows += len(values)
	if len(columns) > 0 {
		c.schemaColumns = append([]string(nil), columns...)
	}
	return nil
}

func (b *ksqlValueBudget) clone(value any, depth int, sensitive bool) (any, error) {
	if depth >= maxMCPKsqlValueDepth || b.nodes >= maxMCPKsqlValueNodes {
		return nil, errKsqlCollectionLimit
	}
	b.nodes++
	if sensitive {
		return b.cloneString(redactedValue)
	}

	switch typed := value.(type) {
	case nil, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, json.Number:
		return typed, nil
	case string:
		return b.cloneString(typed)
	case []any:
		if len(typed) > maxMCPKsqlValueNodes-b.nodes {
			return nil, errKsqlCollectionLimit
		}
		visit := ksqlValueVisit{kind: reflect.Slice, ptr: reflect.ValueOf(typed).Pointer()}
		if visit.ptr != 0 {
			if _, exists := b.visiting[visit]; exists {
				return nil, errOperationFailed
			}
			b.visiting[visit] = struct{}{}
			defer delete(b.visiting, visit)
		}
		output := make([]any, len(typed))
		for index, nested := range typed {
			cloned, err := b.clone(nested, depth+1, false)
			if err != nil {
				return nil, err
			}
			output[index] = cloned
		}
		return output, nil
	case map[string]any:
		if len(typed) > maxMCPKsqlValueNodes-b.nodes {
			return nil, errKsqlCollectionLimit
		}
		visit := ksqlValueVisit{kind: reflect.Map, ptr: reflect.ValueOf(typed).Pointer()}
		if _, exists := b.visiting[visit]; exists {
			return nil, errOperationFailed
		}
		b.visiting[visit] = struct{}{}
		defer delete(b.visiting, visit)

		output := make(map[string]any, len(typed))
		for key, nested := range typed {
			clonedKey, err := b.cloneString(key)
			if err != nil {
				return nil, errKsqlCollectionLimit
			}
			cloned, err := b.clone(nested, depth+1, isCredentialKey(key))
			if err != nil {
				return nil, err
			}
			output[clonedKey] = cloned
		}
		return output, nil
	default:
		return nil, errOperationFailed
	}
}

func (b *ksqlValueBudget) cloneString(value string) (string, error) {
	if !utf8.ValidString(value) {
		return "", errOperationFailed
	}
	size := jsonEncodedStringSize(value)
	if size < 0 || b.bytes > maxResultBytes-size {
		return "", errKsqlCollectionLimit
	}
	b.bytes += size
	return strings.Clone(value), nil
}

func jsonEncodedStringSize(value string) int {
	size := 2
	for _, char := range value {
		switch {
		case char == '"' || char == '\\' || char == '\b' || char == '\f' ||
			char == '\n' || char == '\r' || char == '\t':
			size += 2
		case char < 0x20 || char == '<' || char == '>' || char == '&' ||
			char == '\u2028' || char == '\u2029':
			size += 6
		default:
			size += utf8.RuneLen(char)
		}
	}
	return size
}

func listMCPKsqlStreams(
	ctx context.Context,
	executor *Executor,
	input clusterInput,
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if executor.deps.KSQL == nil {
		return nil, errOperationFailed
	}
	streams, err := executor.deps.KSQL.ListStreams(ctx, input.ClusterName)
	if err != nil {
		return nil, err
	}
	if err := validateMCPKsqlStreamDescriptions(streams); err != nil {
		return nil, err
	}
	output := make([]generated.KsqlStreamDescription, len(streams))
	for index, stream := range streams {
		output[index] = generated.KsqlStreamDescription{
			Name:        cloneMCPKsqlStringPointer(stream.Name),
			Topic:       cloneMCPKsqlStringPointer(stream.Topic),
			KeyFormat:   cloneMCPKsqlStringPointer(stream.KeyFormat),
			ValueFormat: cloneMCPKsqlStringPointer(stream.ValueFormat),
		}
	}
	sort.SliceStable(output, func(left, right int) bool {
		return compareMCPKsqlStreams(output[left], output[right]) < 0
	})
	if len(output) > maxListItems {
		output = output[:maxListItems]
	}
	return output, nil
}

func listMCPKsqlTables(
	ctx context.Context,
	executor *Executor,
	input clusterInput,
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if executor.deps.KSQL == nil {
		return nil, errOperationFailed
	}
	tables, err := executor.deps.KSQL.ListTables(ctx, input.ClusterName)
	if err != nil {
		return nil, err
	}
	if err := validateMCPKsqlTableDescriptions(tables); err != nil {
		return nil, err
	}
	output := make([]generated.KsqlTableDescription, len(tables))
	for index, table := range tables {
		output[index] = generated.KsqlTableDescription{
			Name:        cloneMCPKsqlStringPointer(table.Name),
			Topic:       cloneMCPKsqlStringPointer(table.Topic),
			KeyFormat:   cloneMCPKsqlStringPointer(table.KeyFormat),
			ValueFormat: cloneMCPKsqlStringPointer(table.ValueFormat),
			IsWindowed:  cloneMCPKsqlBoolPointer(table.IsWindowed),
		}
	}
	sort.SliceStable(output, func(left, right int) bool {
		return compareMCPKsqlTables(output[left], output[right]) < 0
	})
	if len(output) > maxListItems {
		output = output[:maxListItems]
	}
	return output, nil
}

func validateMCPKsqlCommandInput(input clusterBodyInput[generated.KsqlCommandV2]) error {
	if err := validateClusterName(input.ClusterName); err != nil {
		return err
	}
	if strings.TrimSpace(input.Body.Ksql) == "" ||
		len(input.Body.Ksql) > maxMCPKsqlSQLBytes ||
		!utf8.ValidString(input.Body.Ksql) {
		return errInvalidRequest
	}
	if input.Body.StreamsProperties == nil {
		return nil
	}
	properties := *input.Body.StreamsProperties
	if len(properties) > maxMCPKsqlPropertyCount {
		return errInvalidRequest
	}
	totalBytes := 0
	for key, value := range properties {
		if len(key) == 0 ||
			len(key) > maxMCPKsqlPropertyKeyBytes ||
			len(value) > maxMCPKsqlPropertyValueBytes ||
			!utf8.ValidString(key) ||
			!utf8.ValidString(value) {
			return errInvalidRequest
		}
		allowedValues, allowed := allowedMCPKsqlStreamsProperties[key]
		if !allowed {
			return errInvalidRequest
		}
		if _, allowed = allowedValues[value]; !allowed {
			return errInvalidRequest
		}
		if totalBytes > maxMCPKsqlPropertyCount*(maxMCPKsqlPropertyKeyBytes+maxMCPKsqlPropertyValueBytes)-len(key)-len(value) {
			return errInvalidRequest
		}
		totalBytes += len(key) + len(value)
	}
	return nil
}

func validateMCPKsqlPipeID(pipeID string) error {
	if strings.TrimSpace(pipeID) == "" ||
		len(pipeID) > maxMCPKsqlPipeIDBytes ||
		!utf8.ValidString(pipeID) {
		return errInvalidRequest
	}
	for _, char := range pipeID {
		if (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '-' {
			continue
		}
		return errInvalidRequest
	}
	return nil
}

func cloneMCPKsqlProperties(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func validateMCPKsqlStreamDescriptions(streams []domaincluster.KsqlStreamDescription) error {
	if len(streams) > maxMCPKsqlRawListItems {
		return errResultTooLarge
	}
	bytes := 0
	for _, stream := range streams {
		if !consumeMCPKsqlDescription(&bytes, stream.Name, stream.Topic, stream.KeyFormat, stream.ValueFormat) {
			return errResultTooLarge
		}
	}
	return nil
}

func validateMCPKsqlTableDescriptions(tables []domaincluster.KsqlTableDescription) error {
	if len(tables) > maxMCPKsqlRawListItems {
		return errResultTooLarge
	}
	bytes := 0
	for _, table := range tables {
		if !consumeMCPKsqlDescription(&bytes, table.Name, table.Topic, table.KeyFormat, table.ValueFormat) {
			return errResultTooLarge
		}
	}
	return nil
}

func consumeMCPKsqlDescription(bytes *int, values ...*string) bool {
	for _, value := range values {
		if value == nil {
			continue
		}
		if len(*value) > maxMCPKsqlDescriptionBytes || !utf8.ValidString(*value) {
			return false
		}
		size := jsonEncodedStringSize(*value)
		if size < 0 || *bytes > maxResultBytes-size {
			return false
		}
		*bytes += size
	}
	return true
}

func cloneMCPKsqlStringPointer(input *string) *string {
	if input == nil {
		return nil
	}
	value := strings.Clone(*input)
	return &value
}

func cloneMCPKsqlBoolPointer(input *bool) *bool {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}

func compareMCPKsqlStreams(left, right generated.KsqlStreamDescription) int {
	for _, pair := range [][2]*string{
		{left.Name, right.Name},
		{left.Topic, right.Topic},
		{left.KeyFormat, right.KeyFormat},
		{left.ValueFormat, right.ValueFormat},
	} {
		if compared := compareMCPKsqlStringPointers(pair[0], pair[1]); compared != 0 {
			return compared
		}
	}
	return 0
}

func compareMCPKsqlTables(left, right generated.KsqlTableDescription) int {
	for _, pair := range [][2]*string{
		{left.Name, right.Name},
		{left.Topic, right.Topic},
		{left.KeyFormat, right.KeyFormat},
		{left.ValueFormat, right.ValueFormat},
	} {
		if compared := compareMCPKsqlStringPointers(pair[0], pair[1]); compared != 0 {
			return compared
		}
	}
	return compareMCPKsqlBoolPointers(left.IsWindowed, right.IsWindowed)
}

func compareMCPKsqlStringPointers(left, right *string) int {
	if left == nil {
		if right == nil {
			return 0
		}
		return -1
	}
	if right == nil {
		return 1
	}
	return strings.Compare(*left, *right)
}

func compareMCPKsqlBoolPointers(left, right *bool) int {
	if left == nil {
		if right == nil {
			return 0
		}
		return -1
	}
	if right == nil {
		return 1
	}
	if *left == *right {
		return 0
	}
	if !*left {
		return -1
	}
	return 1
}
