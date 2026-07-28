package mcpserver

import (
	"context"
	"encoding/json"
	"time"
	"unicode/utf8"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

const (
	maxMessageFilterBytes      = 64 << 10
	maxMessageHeaderNameBytes  = 1024
	maxMessageHeaderValueBytes = 256 << 10
	maxMessageSerdeNameBytes   = 1024
	maxMessageSerdeTextBytes   = 64 << 10
	maxMessageSerdeSchemaBytes = 256 << 10
	maxMessageCursorBytes      = 4 << 10
	messageResultReserveBytes  = maxMessageCursorBytes + 128
	messageSerdeResultReserve  = 256
)

type messageBrowseResult struct {
	Items     []generated.TopicMessage `json:"items"`
	Truncated bool                     `json:"truncated"`
	Cursor    string                   `json:"cursor"`
}

type messageCollectionStopError struct{}

func (*messageCollectionStopError) Error() string {
	return "message collection stopped"
}

var errMessageCollectionStop = &messageCollectionStopError{}

type messageBrowseCollector struct {
	parentCtx context.Context
	cancel    context.CancelFunc
	result    *messageBrowseResult
	maxItems  int
	bytes     int
	accepted  int
	stopped   bool
}

type messageSerdeBudget struct {
	items int
	bytes int
	limit int
}

func messageTool(meta ToolMeta) ToolSpec {
	switch meta.Name {
	case "getTopicMessagesV2":
		return newMessageReadTool(meta, messageBrowseCluster,
			func(ctx context.Context, executor *Executor, input topicOptionalQueryInput[generated.GetTopicMessagesV2Params]) (any, error) {
				return browseTopicMessages(ctx, executor, input, meta.MaxItems)
			})
	case "getSerdes":
		return newMessageReadTool(meta, messageSerdeCluster,
			func(ctx context.Context, executor *Executor, input topicQueryInput[generated.GetSerdesParams]) (any, error) {
				return getMessageSerdes(ctx, executor, input, meta.MaxItems)
			})
	case "registerFilter":
		return newMessageReadTool(meta, messageRegisterFilterCluster, registerMessageFilter)
	case "executeSmartFilterTest":
		return newMessageReadTool(meta, nil, executeMessageSmartFilterTest)
	case "deleteTopicMessages":
		return newMessageWriteTool(meta, messageDeleteCluster, deleteTopicMessages)
	case "sendTopicMessages":
		return newMessageWriteTool(meta, messageSendCluster, sendTopicMessage)
	default:
		panic("unsupported Messages MCP tool: " + meta.Name)
	}
}

func newMessageReadTool[In any](
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

func newMessageWriteTool[In any](
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

func messageBrowseCluster(input topicOptionalQueryInput[generated.GetTopicMessagesV2Params]) string {
	return input.ClusterName
}

func messageSerdeCluster(input topicQueryInput[generated.GetSerdesParams]) string {
	return input.ClusterName
}

func messageRegisterFilterCluster(input topicBodyInput[generated.MessageFilterRegistration]) string {
	return input.ClusterName
}

func messageDeleteCluster(input topicOptionalQueryInput[generated.DeleteTopicMessagesParams]) string {
	return input.ClusterName
}

func messageSendCluster(input topicBodyInput[generated.CreateTopicMessage]) string {
	return input.ClusterName
}

func browseTopicMessages(
	ctx context.Context,
	executor *Executor,
	input topicOptionalQueryInput[generated.GetTopicMessagesV2Params],
	maxItems int,
) (any, error) {
	if err := validateTopicInput(topicInput{
		ClusterName: input.ClusterName,
		TopicName:   input.TopicName,
	}); err != nil {
		return nil, err
	}
	spec, err := messageBrowseSpec(input.Query, maxItems)
	if err != nil {
		return nil, err
	}
	if executor.deps.Messages == nil {
		return nil, errOperationFailed
	}

	result := messageBrowseResult{
		Items:  make([]generated.TopicMessage, 0, spec.Limit),
		Cursor: spec.Cursor,
	}
	browseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	collector := &messageBrowseCollector{
		parentCtx: ctx,
		cancel:    cancel,
		result:    &result,
		maxItems:  spec.Limit,
	}
	err = executor.deps.Messages.Browse(
		browseCtx,
		input.ClusterName,
		input.TopicName,
		spec,
		collector.collect,
	)
	if err != nil {
		classification := classifyMessageLocalStopErrorTree(err)
		if !classification.complete {
			return nil, errOperationFailed
		}
		if !collector.stopped || ctx.Err() != nil || !classification.onlyLocal {
			return nil, err
		}
	}
	return result, nil
}

type messageLocalStopErrorTree struct {
	onlyLocal bool
	complete  bool
}

func classifyMessageLocalStopErrorTree(err error) messageLocalStopErrorTree {
	nodes := 0
	return classifyMessageLocalStopError(err, 0, &nodes)
}

func classifyMessageLocalStopError(
	err error,
	depth int,
	nodes *int,
) messageLocalStopErrorTree {
	const (
		maxDepth = 32
		maxNodes = 128
	)
	if err == nil {
		return messageLocalStopErrorTree{complete: true}
	}
	if depth >= maxDepth || *nodes >= maxNodes {
		return messageLocalStopErrorTree{}
	}
	*nodes++

	if stopErr, ok := err.(*messageCollectionStopError); ok && stopErr == errMessageCollectionStop {
		return messageLocalStopErrorTree{onlyLocal: true, complete: true}
	}
	if err == context.Canceled {
		return messageLocalStopErrorTree{onlyLocal: true, complete: true}
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() error }:
		return classifyMessageLocalStopError(wrapped.Unwrap(), depth+1, nodes)
	case interface{ Unwrap() []error }:
		children := wrapped.Unwrap()
		if len(children) == 0 {
			return messageLocalStopErrorTree{complete: true}
		}
		result := messageLocalStopErrorTree{onlyLocal: true, complete: true}
		for _, child := range children {
			classification := classifyMessageLocalStopError(child, depth+1, nodes)
			if !classification.complete {
				return messageLocalStopErrorTree{}
			}
			if !classification.onlyLocal {
				result.onlyLocal = false
			}
		}
		return result
	}
	return messageLocalStopErrorTree{complete: true}
}

func (c *messageBrowseCollector) collect(event appcluster.BrowseEvent) error {
	if c.stopped {
		return errMessageCollectionStop
	}
	if err := c.parentCtx.Err(); err != nil {
		return err
	}
	switch event.Kind {
	case appcluster.EventDone:
		return c.collectDone(event.CursorID)
	case appcluster.EventMessage:
		return c.collectMessage(event)
	default:
		return nil
	}
}

func (c *messageBrowseCollector) collectDone(cursor string) error {
	if cursor == "" {
		c.result.Cursor = ""
		return nil
	}
	if err := validateMessageOutputCursor(cursor); err != nil {
		return err
	}
	c.result.Cursor = cursor
	return nil
}

func (c *messageBrowseCollector) collectMessage(event appcluster.BrowseEvent) error {
	if event.Message == nil {
		return nil
	}
	if c.accepted >= c.maxItems {
		return c.stop()
	}
	item, ok := boundedTopicMessage(event.Message)
	if !ok {
		return c.stop()
	}
	raw, err := json.Marshal(item)
	if err != nil {
		return errOperationFailed
	}
	separatorBytes := 0
	if c.accepted > 0 {
		separatorBytes = 1
	}
	if c.bytes+separatorBytes+len(raw) > maxResultBytes-messageResultReserveBytes {
		return c.stop()
	}
	if event.CursorID != "" {
		if err := validateMessageOutputCursor(event.CursorID); err != nil {
			return err
		}
	}

	c.result.Items = append(c.result.Items, item)
	c.bytes += separatorBytes + len(raw)
	c.accepted++
	c.result.Cursor = event.CursorID
	return nil
}

func (c *messageBrowseCollector) stop() error {
	c.result.Truncated = true
	c.stopped = true
	c.cancel()
	return errMessageCollectionStop
}

func messageBrowseSpec(
	query *generated.GetTopicMessagesV2Params,
	maxItems int,
) (appcluster.BrowseSpec, error) {
	if maxItems <= 0 {
		return appcluster.BrowseSpec{}, errOperationFailed
	}
	spec := appcluster.BrowseSpec{
		Mode:  appcluster.ModeLatest,
		Limit: maxItems,
	}
	if query == nil {
		return spec, nil
	}

	mode, ok := messageBrowseMode(query.Mode)
	if !ok {
		return appcluster.BrowseSpec{}, errInvalidRequest
	}
	spec.Mode = mode
	if query.Limit != nil {
		if *query.Limit <= 0 || int(*query.Limit) > maxItems {
			return appcluster.BrowseSpec{}, errInvalidRequest
		}
		spec.Limit = int(*query.Limit)
	}
	if query.Partitions != nil {
		partitions, err := boundedMessagePartitions(*query.Partitions, maxItems)
		if err != nil {
			return appcluster.BrowseSpec{}, err
		}
		spec.Partitions = partitions
	}
	if query.Offset != nil {
		spec.Offset = *query.Offset
	}
	if query.Timestamp != nil {
		spec.TimestampMs = *query.Timestamp
	}
	if query.StringFilter != nil {
		if len(*query.StringFilter) > maxMessageFilterBytes {
			return appcluster.BrowseSpec{}, errInvalidRequest
		}
		spec.StringFilter = *query.StringFilter
	}
	if query.SmartFilterId != nil {
		if len(*query.SmartFilterId) > maxMessageFilterBytes {
			return appcluster.BrowseSpec{}, errInvalidRequest
		}
		spec.SmartFilterID = *query.SmartFilterId
	}
	if query.KeySerde != nil {
		if len(*query.KeySerde) > maxMessageSerdeNameBytes {
			return appcluster.BrowseSpec{}, errInvalidRequest
		}
		spec.KeySerde = *query.KeySerde
	}
	if query.ValueSerde != nil {
		if len(*query.ValueSerde) > maxMessageSerdeNameBytes {
			return appcluster.BrowseSpec{}, errInvalidRequest
		}
		spec.ValueSerde = *query.ValueSerde
	}
	if query.Cursor != nil {
		if err := validateMessageInputCursor(*query.Cursor); err != nil {
			return appcluster.BrowseSpec{}, err
		}
		spec.Cursor = *query.Cursor
	}
	return spec, nil
}

func validateMessageInputCursor(cursor string) error {
	size, ok := messageCursorJSONSize(cursor)
	if !ok || size > maxMessageCursorBytes {
		return errInvalidRequest
	}
	return nil
}

func validateMessageOutputCursor(cursor string) error {
	size, ok := messageCursorJSONSize(cursor)
	if !ok {
		return errOperationFailed
	}
	if size > maxMessageCursorBytes {
		return errResultTooLarge
	}
	return nil
}

func messageCursorJSONSize(cursor string) (int, bool) {
	if !utf8.ValidString(cursor) {
		return 0, false
	}
	raw, err := json.Marshal(cursor)
	if err != nil {
		return 0, false
	}
	var roundTrip string
	if err := json.Unmarshal(raw, &roundTrip); err != nil || roundTrip != cursor {
		return 0, false
	}
	return len(raw), true
}

func messageBrowseMode(mode *generated.PollingMode) (appcluster.BrowseMode, bool) {
	if mode == nil {
		return appcluster.ModeLatest, true
	}
	switch *mode {
	case generated.PollingModeLATEST:
		return appcluster.ModeLatest, true
	case generated.PollingModeEARLIEST:
		return appcluster.ModeEarliest, true
	case generated.PollingModeTAILING:
		return appcluster.ModeTailing, true
	case generated.PollingModeFROMOFFSET:
		return appcluster.ModeFromOffset, true
	case generated.PollingModeTOOFFSET:
		return appcluster.ModeToOffset, true
	case generated.PollingModeFROMTIMESTAMP:
		return appcluster.ModeFromTimestamp, true
	case generated.PollingModeTOTIMESTAMP:
		return appcluster.ModeToTimestamp, true
	default:
		return 0, false
	}
}

func boundedTopicMessage(message *appcluster.DecodedMessage) (generated.TopicMessage, bool) {
	if len(message.Key) > maxResultBytes ||
		len(message.Value) > maxResultBytes ||
		len(message.KeySerde) > maxMessageSerdeNameBytes ||
		len(message.ValueSerde) > maxMessageSerdeNameBytes ||
		len(message.Headers) > maxMessageItems {
		return generated.TopicMessage{}, false
	}
	for name, value := range message.Headers {
		if len(name) > maxMessageHeaderNameBytes || len(value) > maxMessageHeaderValueBytes {
			return generated.TopicMessage{}, false
		}
	}

	result := generated.TopicMessage{
		Partition:   message.Partition,
		Offset:      message.Offset,
		Timestamp:   time.UnixMilli(message.TimestampMs).UTC(),
		Key:         pointerTo(message.Key),
		Value:       pointerTo(message.Value),
		KeySize:     pointerTo(message.KeySize),
		ValueSize:   pointerTo(message.ValueSize),
		HeadersSize: pointerTo(message.HeadersSize),
		KeySerde:    pointerTo(message.KeySerde),
		ValueSerde:  pointerTo(message.ValueSerde),
	}
	if message.TimestampType != "" {
		timestampType := generated.TopicMessageTimestampType(message.TimestampType)
		result.TimestampType = &timestampType
	}
	if len(message.Headers) > 0 {
		headers := make(map[string]string, len(message.Headers))
		for name, value := range message.Headers {
			headers[name] = value
		}
		result.Headers = &headers
	}
	return result, true
}

func getMessageSerdes(
	ctx context.Context,
	executor *Executor,
	input topicQueryInput[generated.GetSerdesParams],
	maxItems int,
) (any, error) {
	if err := validateTopicInput(topicInput{
		ClusterName: input.ClusterName,
		TopicName:   input.TopicName,
	}); err != nil {
		return nil, err
	}
	use, ok := messageSerdeUsage(input.Query.Use)
	if !ok {
		return nil, errInvalidRequest
	}
	if executor.deps.Serdes == nil {
		return nil, errOperationFailed
	}
	suggestion, err := executor.deps.Serdes.Suggest(
		ctx,
		input.ClusterName,
		input.TopicName,
		use,
	)
	if err != nil {
		return nil, err
	}
	return messageSerdeSuggestion(suggestion, maxItems)
}

func messageSerdeUsage(use generated.SerdeUsage) (serde.Usage, bool) {
	switch use {
	case generated.SERIALIZE:
		return serde.UsageSerialize, true
	case generated.DESERIALIZE:
		return serde.UsageDeserialize, true
	default:
		return 0, false
	}
}

func messageSerdeSuggestion(
	suggestion serde.Suggestion,
	maxItems int,
) (generated.TopicSerdeSuggestion, error) {
	if maxItems <= 0 {
		return generated.TopicSerdeSuggestion{}, errOperationFailed
	}
	budget := messageSerdeBudget{
		bytes: messageSerdeResultReserve,
		limit: maxItems,
	}
	if err := budget.consumeDescriptions(suggestion.Key); err != nil {
		return generated.TopicSerdeSuggestion{}, err
	}
	if err := budget.consumeDescriptions(suggestion.Value); err != nil {
		return generated.TopicSerdeSuggestion{}, err
	}

	key := messageSerdeDescriptions(suggestion.Key)
	value := messageSerdeDescriptions(suggestion.Value)
	return generated.TopicSerdeSuggestion{Key: &key, Value: &value}, nil
}

func (b *messageSerdeBudget) consumeDescriptions(descriptions []serde.Description) error {
	for _, description := range descriptions {
		if err := b.consumeItem(128); err != nil {
			return err
		}
		if err := b.consumeString(description.Name, maxMessageSerdeNameBytes); err != nil {
			return err
		}
		if err := b.consumeString(description.Description, maxMessageSerdeTextBytes); err != nil {
			return err
		}
		if description.Schema != nil {
			if err := b.consumeString(*description.Schema, maxMessageSerdeSchemaBytes); err != nil {
				return err
			}
		}
		for _, param := range description.Params {
			if err := b.consumeItem(128); err != nil {
				return err
			}
			if err := b.consumeString(param.Name, maxMessageSerdeNameBytes); err != nil {
				return err
			}
			if err := b.consumeString(param.VisibleName, maxMessageSerdeNameBytes); err != nil {
				return err
			}
			for _, allowed := range param.AllowedValues {
				if err := b.consumeItem(8); err != nil {
					return err
				}
				if err := b.consumeString(allowed, maxMessageSerdeTextBytes); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (b *messageSerdeBudget) consumeItem(overhead int) error {
	if b.items >= b.limit {
		return errResultTooLarge
	}
	b.items++
	return b.consumeBytes(overhead)
}

func (b *messageSerdeBudget) consumeString(value string, maxFieldBytes int) error {
	if len(value) > maxFieldBytes {
		return errResultTooLarge
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return errOperationFailed
	}
	return b.consumeBytes(len(raw))
}

func (b *messageSerdeBudget) consumeBytes(size int) error {
	if size < 0 || b.bytes > maxResultBytes-size {
		return errResultTooLarge
	}
	b.bytes += size
	return nil
}

func messageSerdeDescriptions(descriptions []serde.Description) []generated.SerdeDescription {
	result := make([]generated.SerdeDescription, 0, len(descriptions))
	for _, description := range descriptions {
		item := generated.SerdeDescription{
			Name:        pointerTo(description.Name),
			Description: pointerTo(description.Description),
			Preferred:   pointerTo(description.Preferred),
			Schema:      description.Schema,
		}
		params := description.Params
		if len(params) > 0 {
			mapped := make([]generated.SerdeParameter, 0, len(params))
			for _, param := range params {
				allowed := param.AllowedValues
				mappedParam := generated.SerdeParameter{Name: param.Name}
				if param.VisibleName != "" {
					mappedParam.VisibleName = pointerTo(param.VisibleName)
				}
				if len(allowed) > 0 {
					allowedCopy := append([]string(nil), allowed...)
					mappedParam.AllowedValues = &allowedCopy
				}
				mapped = append(mapped, mappedParam)
			}
			item.Parameters = &mapped
		}
		result = append(result, item)
	}
	return result
}

func registerMessageFilter(
	_ context.Context,
	executor *Executor,
	input topicBodyInput[generated.MessageFilterRegistration],
) (any, error) {
	if err := validateTopicInput(topicInput{
		ClusterName: input.ClusterName,
		TopicName:   input.TopicName,
	}); err != nil {
		return nil, err
	}
	code := ""
	if input.Body.FilterCode != nil {
		code = *input.Body.FilterCode
	}
	if len(code) > maxMessageFilterBytes {
		return nil, errInvalidRequest
	}
	if executor.deps.SmartFilters == nil {
		return nil, errOperationFailed
	}
	id, err := executor.deps.SmartFilters.Register(code)
	if err != nil {
		// Register failures are compile/validation failures for caller-supplied
		// CEL. Preserve that classification without returning the compiler's
		// raw expression-bearing error text.
		return nil, errInvalidRequest
	}
	if len(id) > maxMessageCursorBytes {
		return nil, errOperationFailed
	}
	return generated.MessageFilterId{Id: &id}, nil
}

func executeMessageSmartFilterTest(
	_ context.Context,
	executor *Executor,
	input globalBodyInput[generated.SmartFilterTestExecution],
) (any, error) {
	test, err := messageSmartFilterTest(input.Body)
	if err != nil {
		return nil, err
	}
	if executor.deps.SmartFilters == nil {
		return nil, errOperationFailed
	}
	matched, evaluationError, compileError := executor.deps.SmartFilters.Test(test)
	if compileError != nil || evaluationError != "" {
		return nil, errInvalidRequest
	}
	return generated.SmartFilterTestExecutionResult{Result: &matched}, nil
}

func messageSmartFilterTest(body generated.SmartFilterTestExecution) (appcluster.SmartFilterTest, error) {
	if len(body.FilterCode) > maxMessageFilterBytes {
		return appcluster.SmartFilterTest{}, errInvalidRequest
	}
	result := appcluster.SmartFilterTest{FilterCode: body.FilterCode}
	if body.Key != nil {
		result.Key = *body.Key
	}
	if body.Value != nil {
		result.Value = *body.Value
	}
	if body.Headers != nil {
		result.Headers = make(map[string]string, len(*body.Headers))
		for name, value := range *body.Headers {
			result.Headers[name] = value
		}
	}
	if body.Partition != nil {
		result.Partition = *body.Partition
	}
	if body.Offset != nil {
		result.Offset = *body.Offset
	}
	if body.TimestampMs != nil {
		result.TimestampMs = *body.TimestampMs
	}
	if err := validateMessageEnvelope(
		pointerTo(result.Key),
		pointerTo(result.Value),
		result.Headers,
		nil,
		nil,
	); err != nil {
		return appcluster.SmartFilterTest{}, err
	}
	return result, nil
}

func sendTopicMessage(
	ctx context.Context,
	executor *Executor,
	input topicBodyInput[generated.CreateTopicMessage],
) (any, error) {
	if err := validateTopicInput(topicInput{
		ClusterName: input.ClusterName,
		TopicName:   input.TopicName,
	}); err != nil {
		return nil, err
	}
	spec, err := messageSendSpec(input.Body)
	if err != nil {
		return nil, err
	}
	if executor.deps.Messages == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Messages.Send(ctx, input.ClusterName, input.TopicName, spec); err != nil {
		return nil, err
	}
	return nil, nil
}

func messageSendSpec(body generated.CreateTopicMessage) (appcluster.SendSpec, error) {
	if body.Partition < 0 ||
		(body.KeySerde != nil && len(*body.KeySerde) > maxMessageSerdeNameBytes) ||
		(body.ValueSerde != nil && len(*body.ValueSerde) > maxMessageSerdeNameBytes) {
		return appcluster.SendSpec{}, errInvalidRequest
	}
	if err := validateMessageEnvelope(
		body.Key,
		body.Value,
		mapValue(body.Headers),
		mapValue(body.KeySerdeProperties),
		mapValue(body.ValueSerdeProperties),
	); err != nil {
		return appcluster.SendSpec{}, err
	}

	spec := appcluster.SendSpec{
		Partition: body.Partition,
		Key:       body.Key,
		Value:     body.Value,
	}
	if body.Headers != nil {
		spec.Headers = copyStringMap(*body.Headers)
	}
	if body.KeySerde != nil {
		spec.KeySerde = *body.KeySerde
	}
	if body.ValueSerde != nil {
		spec.ValueSerde = *body.ValueSerde
	}
	if body.KeySerdeProperties != nil {
		spec.KeySerdeProps = copyAnyMap(*body.KeySerdeProperties)
	}
	if body.ValueSerdeProperties != nil {
		spec.ValueSerdeProps = copyAnyMap(*body.ValueSerdeProperties)
	}
	return spec, nil
}

func validateMessageEnvelope(
	key *string,
	value *string,
	headers map[string]string,
	keySerdeProperties map[string]any,
	valueSerdeProperties map[string]any,
) error {
	if (key != nil && len(*key) > maxResultBytes) ||
		(value != nil && len(*value) > maxResultBytes) ||
		len(headers) > maxMessageItems {
		return errInvalidRequest
	}
	for name, headerValue := range headers {
		if name == "" ||
			len(name) > maxMessageHeaderNameBytes ||
			len(headerValue) > maxMessageHeaderValueBytes {
			return errInvalidRequest
		}
	}
	envelope := struct {
		Key                  *string           `json:"key,omitempty"`
		Value                *string           `json:"value,omitempty"`
		Headers              map[string]string `json:"headers,omitempty"`
		KeySerdeProperties   map[string]any    `json:"keySerdeProperties,omitempty"`
		ValueSerdeProperties map[string]any    `json:"valueSerdeProperties,omitempty"`
	}{
		Key:                  key,
		Value:                value,
		Headers:              headers,
		KeySerdeProperties:   keySerdeProperties,
		ValueSerdeProperties: valueSerdeProperties,
	}
	raw, err := json.Marshal(envelope)
	if err != nil || len(raw) > maxResultBytes {
		return errInvalidRequest
	}
	return nil
}

func deleteTopicMessages(
	ctx context.Context,
	executor *Executor,
	input topicOptionalQueryInput[generated.DeleteTopicMessagesParams],
) (any, error) {
	if err := validateTopicInput(topicInput{
		ClusterName: input.ClusterName,
		TopicName:   input.TopicName,
	}); err != nil {
		return nil, err
	}
	var partitions []int32
	if input.Query != nil && input.Query.Partitions != nil {
		var err error
		partitions, err = boundedMessagePartitions(*input.Query.Partitions, maxMessageItems)
		if err != nil {
			return nil, err
		}
	}
	if executor.deps.Messages == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Messages.Delete(
		ctx,
		input.ClusterName,
		input.TopicName,
		partitions,
	); err != nil {
		return nil, err
	}
	return nil, nil
}

func boundedMessagePartitions(partitions []int32, maxItems int) ([]int32, error) {
	if len(partitions) > maxItems {
		return nil, errInvalidRequest
	}
	result := make([]int32, 0, len(partitions))
	seen := make(map[int32]struct{}, len(partitions))
	for _, partition := range partitions {
		if partition < 0 {
			return nil, errInvalidRequest
		}
		if _, exists := seen[partition]; exists {
			return nil, errInvalidRequest
		}
		seen[partition] = struct{}{}
		result = append(result, partition)
	}
	return result, nil
}

func mapValue[T any](value *map[string]T) map[string]T {
	if value == nil {
		return nil
	}
	return *value
}

func copyStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func copyAnyMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func pointerTo[T any](value T) *T {
	return &value
}
