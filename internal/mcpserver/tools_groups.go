package mcpserver

import (
	"bytes"
	"context"
	"encoding/csv"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

const (
	defaultMCPGroupPage       = 1
	defaultMCPGroupPerPage    = 25
	maxMCPGroupPerPage        = 100
	maxMCPGroupLagIDs         = 100
	maxMCPGroupResetEntries   = maxListItems
	maxMCPGroupOutputChildren = maxListItems
)

var groupCSVHeader = []string{
	"groupId",
	"state",
	"members",
	"topics",
	"partitionAssignor",
	"coordinatorId",
	"consumerLag",
}

type groupPageResult struct {
	Items     []generated.ConsumerGroup `json:"items"`
	Page      int                       `json:"page"`
	PageCount int                       `json:"pageCount"`
	Truncated bool                      `json:"truncated"`
}

// generated.ConsumerGroupDetails aliases generated.ConsumerGroup and drops
// the contract's sibling partitions property. Keep the same local wrapper the
// reviewed HTTP adapter uses so MCP returns the actual SDK wire shape.
type groupDetailsResult struct {
	generated.ConsumerGroup
	Partitions *[]generated.ConsumerGroupTopicPartition `json:"partitions,omitempty"`
}

func consumerGroupTool(meta ToolMeta) ToolSpec {
	switch meta.Name {
	case "getConsumerGroup":
		return consumerGroupReadTool(meta, consumerGroupInputSelector, getConsumerGroup)
	case "getConsumerGroupsLag":
		return consumerGroupReadTool(meta, consumerGroupLagInputSelector, getConsumerGroupsLag)
	case "getTopicConsumerGroups":
		return consumerGroupReadTool(meta, consumerGroupTopicInputSelector,
			func(ctx context.Context, executor *Executor, input topicInput) (any, error) {
				return getTopicConsumerGroups(ctx, executor, input, meta.MaxItems)
			})
	case "getConsumerGroupsPage":
		return consumerGroupReadTool(meta, consumerGroupPageInputSelector, getConsumerGroupsPage)
	case "getConsumerGroupsCsv":
		return consumerGroupReadTool(meta, consumerGroupCSVInputSelector,
			func(ctx context.Context, executor *Executor, input clusterOptionalQueryInput[generated.GetConsumerGroupsCsvParams]) (any, error) {
				return getConsumerGroupsCSV(ctx, executor, input, meta.MaxItems)
			})
	case "deleteConsumerGroup":
		return consumerGroupWriteTool(meta, consumerGroupInputSelector, deleteConsumerGroup)
	case "deleteConsumerGroupOffsets":
		return consumerGroupWriteTool(meta, consumerGroupOffsetsInputSelector, deleteConsumerGroupOffsets)
	case "resetConsumerGroupOffsets":
		return consumerGroupWriteTool(meta, consumerGroupResetInputSelector, resetConsumerGroupOffsets)
	default:
		panic("unsupported Consumer Groups MCP tool: " + meta.Name)
	}
}

func consumerGroupReadTool[In any](
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

func consumerGroupWriteTool[In any](
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

func consumerGroupInputSelector(input consumerGroupInput) string {
	return input.ClusterName
}

func consumerGroupLagInputSelector(input clusterQueryInput[generated.GetConsumerGroupsLagParams]) string {
	return input.ClusterName
}

func consumerGroupTopicInputSelector(input topicInput) string {
	return input.ClusterName
}

func consumerGroupPageInputSelector(input clusterOptionalQueryInput[generated.GetConsumerGroupsPageParams]) string {
	return input.ClusterName
}

func consumerGroupCSVInputSelector(input clusterOptionalQueryInput[generated.GetConsumerGroupsCsvParams]) string {
	return input.ClusterName
}

func consumerGroupOffsetsInputSelector(input consumerGroupTopicInput) string {
	return input.ClusterName
}

func consumerGroupResetInputSelector(input consumerGroupBodyInput[generated.ConsumerGroupOffsetsReset]) string {
	return input.ClusterName
}

func getConsumerGroup(
	ctx context.Context,
	executor *Executor,
	input consumerGroupInput,
) (any, error) {
	if err := validateConsumerGroupInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Groups == nil {
		return nil, errOperationFailed
	}
	group, err := executor.deps.Groups.Get(ctx, input.ClusterName, input.ID)
	if err != nil {
		return nil, err
	}
	if err := validateConsumerGroupResult(group); err != nil {
		return nil, err
	}
	return consumerGroupDetailsFrom(group), nil
}

func getConsumerGroupsLag(
	ctx context.Context,
	executor *Executor,
	input clusterQueryInput[generated.GetConsumerGroupsLagParams],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	ids, err := validatedConsumerGroupLagIDs(input.Query.Ids)
	if err != nil {
		return nil, err
	}
	if executor.deps.Groups == nil {
		return nil, errOperationFailed
	}
	groups, err := executor.deps.Groups.Lag(ctx, input.ClusterName, ids)
	if err != nil {
		return nil, err
	}
	if len(groups) > maxMCPGroupLagIDs {
		return nil, errResultTooLarge
	}
	includePartitions := input.Query.IncludePartitions != nil && *input.Query.IncludePartitions
	output := generated.ConsumerGroupsLagResponse{
		ConsumerGroups:  make(map[string]generated.ConsumerGroupLag, len(groups)),
		UpdateTimestamp: time.Now().UnixMilli(),
	}
	for _, group := range groups {
		if err := validateConsumerGroupResult(group); err != nil {
			return nil, err
		}
		output.ConsumerGroups[group.ID] = consumerGroupLagFrom(group, includePartitions)
	}
	return output, nil
}

func getTopicConsumerGroups(
	ctx context.Context,
	executor *Executor,
	input topicInput,
	maxItems int,
) (any, error) {
	if err := validateTopicInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Groups == nil {
		return nil, errOperationFailed
	}
	groups, err := executor.deps.Groups.ForTopic(ctx, input.ClusterName, input.TopicName)
	if err != nil {
		return nil, err
	}
	groups = append([]domaincluster.GroupState(nil), groups...)
	sort.Slice(groups, func(left, right int) bool {
		return groups[left].ID < groups[right].ID
	})
	if maxItems > 0 && len(groups) > maxItems {
		groups = groups[:maxItems]
	}
	output := make([]generated.ConsumerGroup, 0, len(groups))
	for _, group := range groups {
		if err := validateConsumerGroupResult(group); err != nil {
			return nil, err
		}
		output = append(output, topicConsumerGroupFrom(group, input.TopicName))
	}
	return output, nil
}

func getConsumerGroupsPage(
	ctx context.Context,
	executor *Executor,
	input clusterOptionalQueryInput[generated.GetConsumerGroupsPageParams],
) (any, error) {
	query, err := consumerGroupPageQuery(input.Query)
	if err != nil {
		return nil, err
	}
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if executor.deps.Groups == nil {
		return nil, errOperationFailed
	}
	page, err := executor.deps.Groups.Page(ctx, input.ClusterName, query)
	if err != nil {
		return nil, err
	}

	groups := append([]domaincluster.GroupState(nil), page.Groups...)
	sortConsumerGroupStates(groups, query)
	truncated := query.Page < page.PageCount
	if len(groups) > query.PerPage {
		groups = groups[:query.PerPage]
		truncated = true
	}
	items := make([]generated.ConsumerGroup, 0, len(groups))
	for _, group := range groups {
		if err := validateConsumerGroupResult(group); err != nil {
			return nil, err
		}
		items = append(items, consumerGroupFrom(group))
	}

	pageNumber := query.Page
	if page.PageCount == 0 {
		pageNumber = 1
	} else if pageNumber > page.PageCount {
		pageNumber = page.PageCount
	}
	return groupPageResult{
		Items:     items,
		Page:      pageNumber,
		PageCount: page.PageCount,
		Truncated: truncated,
	}, nil
}

func getConsumerGroupsCSV(
	ctx context.Context,
	executor *Executor,
	input clusterOptionalQueryInput[generated.GetConsumerGroupsCsvParams],
	maxItems int,
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	query, err := consumerGroupCSVQuery(input.Query, maxItems)
	if err != nil {
		return nil, err
	}
	if executor.deps.Groups == nil {
		return nil, errOperationFailed
	}

	groups := make([]domaincluster.GroupState, 0, maxItems)
	seenGroupIDs := make(map[string]struct{}, maxItems)
	// PageCount can be stale across live Kafka listings, so termination uses
	// observable progress plus this catalog-derived hard call bound.
	maxPages := (maxItems + maxMCPGroupPerPage - 1) / maxMCPGroupPerPage
	for pageNumber := 1; pageNumber <= maxPages && len(groups) < maxItems; pageNumber++ {
		query.Page = pageNumber
		query.PerPage = min(maxMCPGroupPerPage, maxItems-len(groups))
		page, pageErr := executor.deps.Groups.Page(ctx, input.ClusterName, query)
		if pageErr != nil {
			return nil, pageErr
		}
		if len(page.Groups) == 0 {
			break
		}

		pageGroups := page.Groups
		if len(pageGroups) > query.PerPage {
			pageGroups = pageGroups[:query.PerPage]
		}
		added := 0
		for _, group := range pageGroups {
			if _, duplicate := seenGroupIDs[group.ID]; duplicate {
				continue
			}
			seenGroupIDs[group.ID] = struct{}{}
			groups = append(groups, group)
			added++
		}
		if added == 0 || len(page.Groups) < query.PerPage {
			break
		}
	}

	sortConsumerGroupStates(groups, query)
	for _, group := range groups {
		if err := validateConsumerGroupResult(group); err != nil {
			return nil, err
		}
	}
	return consumerGroupsCSV(groups)
}

func deleteConsumerGroup(
	ctx context.Context,
	executor *Executor,
	input consumerGroupInput,
) (any, error) {
	if err := validateConsumerGroupInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Groups == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Groups.Delete(ctx, input.ClusterName, input.ID); err != nil {
		return nil, err
	}
	return nil, nil
}

func deleteConsumerGroupOffsets(
	ctx context.Context,
	executor *Executor,
	input consumerGroupTopicInput,
) (any, error) {
	if err := validateConsumerGroupTopicInput(input); err != nil {
		return nil, err
	}
	if executor.deps.Groups == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Groups.DeleteOffsets(
		ctx,
		input.ClusterName,
		input.ID,
		input.TopicName,
	); err != nil {
		return nil, err
	}
	return nil, nil
}

func resetConsumerGroupOffsets(
	ctx context.Context,
	executor *Executor,
	input consumerGroupBodyInput[generated.ConsumerGroupOffsetsReset],
) (any, error) {
	if err := validateConsumerGroupInput(consumerGroupInput{
		ClusterName: input.ClusterName,
		ID:          input.ID,
	}); err != nil {
		return nil, err
	}
	spec, err := consumerGroupResetSpec(input.Body)
	if err != nil {
		return nil, err
	}
	if executor.deps.Groups == nil {
		return nil, errOperationFailed
	}
	if err := executor.deps.Groups.Reset(ctx, input.ClusterName, input.ID, spec); err != nil {
		return nil, err
	}
	return nil, nil
}

func consumerGroupPageQuery(
	params *generated.GetConsumerGroupsPageParams,
) (appcluster.GroupPageQuery, error) {
	query := appcluster.GroupPageQuery{
		Page:    defaultMCPGroupPage,
		PerPage: defaultMCPGroupPerPage,
	}
	if params == nil {
		return query, nil
	}
	if params.Page != nil {
		if *params.Page < 1 {
			return appcluster.GroupPageQuery{}, errInvalidRequest
		}
		query.Page = int(*params.Page)
	}
	if params.PerPage != nil {
		if *params.PerPage < 1 || *params.PerPage > maxMCPGroupPerPage {
			return appcluster.GroupPageQuery{}, errInvalidRequest
		}
		query.PerPage = int(*params.PerPage)
	}
	if err := applyConsumerGroupQueryFields(
		&query,
		params.Search,
		params.OrderBy,
		params.SortOrder,
		params.State,
	); err != nil {
		return appcluster.GroupPageQuery{}, err
	}
	return query, nil
}

func consumerGroupCSVQuery(
	params *generated.GetConsumerGroupsCsvParams,
	maxItems int,
) (appcluster.GroupPageQuery, error) {
	if maxItems < 1 {
		return appcluster.GroupPageQuery{}, errOperationFailed
	}
	query := appcluster.GroupPageQuery{
		Page: 1, PerPage: min(maxMCPGroupPerPage, maxItems),
	}
	if params == nil {
		return query, nil
	}
	if err := applyConsumerGroupQueryFields(
		&query,
		params.Search,
		params.OrderBy,
		params.SortOrder,
		params.State,
	); err != nil {
		return appcluster.GroupPageQuery{}, err
	}
	// HTTP accepts page/perPage on this generated parameter type but ignores
	// them for an all-rows CSV export. MCP preserves that projection while
	// replacing the HTTP MaxInt32 request with bounded 100-row pages.
	return query, nil
}

func applyConsumerGroupQueryFields(
	query *appcluster.GroupPageQuery,
	search *string,
	orderBy *generated.ConsumerGroupOrdering,
	sortOrder *generated.SortOrder,
	states *[]generated.ConsumerGroupState,
) error {
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
	if states != nil {
		if len(*states) > maxMCPGroupPerPage {
			return errInvalidRequest
		}
		seen := make(map[string]struct{}, len(*states))
		for _, state := range *states {
			if !state.Valid() {
				return errInvalidRequest
			}
			value := string(state)
			if _, exists := seen[value]; exists {
				continue
			}
			seen[value] = struct{}{}
			query.States = append(query.States, value)
		}
	}
	return nil
}

func validatedConsumerGroupLagIDs(ids []string) ([]string, error) {
	if len(ids) < 1 || len(ids) > maxMCPGroupLagIDs {
		return nil, errInvalidRequest
	}
	output := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if err := validateConsumerGroupID(id); err != nil {
			return nil, err
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		output = append(output, id)
	}
	return output, nil
}

func consumerGroupResetSpec(
	body generated.ConsumerGroupOffsetsReset,
) (domaincluster.ResetSpec, error) {
	if !body.ResetType.Valid() {
		return domaincluster.ResetSpec{}, errInvalidRequest
	}
	if err := validateTopicName(body.Topic); err != nil {
		return domaincluster.ResetSpec{}, err
	}

	partitions, err := validatedResetPartitions(body.Partitions)
	if err != nil {
		return domaincluster.ResetSpec{}, err
	}
	offsets, err := validatedResetPartitionOffsets(body.PartitionsOffsets)
	if err != nil {
		return domaincluster.ResetSpec{}, err
	}

	spec := domaincluster.ResetSpec{
		Topic:      body.Topic,
		ResetType:  string(body.ResetType),
		Partitions: partitions,
	}
	switch body.ResetType {
	case generated.ConsumerGroupOffsetsResetTypeEARLIEST,
		generated.ConsumerGroupOffsetsResetTypeLATEST:
		if body.PartitionsOffsets != nil || body.ResetToTimestamp != nil {
			return domaincluster.ResetSpec{}, errInvalidRequest
		}
	case generated.ConsumerGroupOffsetsResetTypeOFFSET:
		if body.Partitions != nil ||
			body.ResetToTimestamp != nil ||
			body.PartitionsOffsets == nil ||
			len(*body.PartitionsOffsets) == 0 {
			return domaincluster.ResetSpec{}, errInvalidRequest
		}
		spec.Partitions = nil
		spec.PartitionsOffsets = offsets
	case generated.ConsumerGroupOffsetsResetTypeTIMESTAMP:
		if body.PartitionsOffsets != nil ||
			body.ResetToTimestamp == nil ||
			*body.ResetToTimestamp < 0 {
			return domaincluster.ResetSpec{}, errInvalidRequest
		}
		spec.Timestamp = *body.ResetToTimestamp
	default:
		return domaincluster.ResetSpec{}, errInvalidRequest
	}
	return spec, nil
}

func validatedResetPartitions(partitions *[]int32) ([]int32, error) {
	if partitions == nil {
		return nil, nil
	}
	if len(*partitions) == 0 || len(*partitions) > maxMCPGroupResetEntries {
		return nil, errInvalidRequest
	}
	output := append([]int32(nil), (*partitions)...)
	seen := make(map[int32]struct{}, len(output))
	for _, partition := range output {
		if partition < 0 {
			return nil, errInvalidRequest
		}
		if _, exists := seen[partition]; exists {
			return nil, errInvalidRequest
		}
		seen[partition] = struct{}{}
	}
	return output, nil
}

func validatedResetPartitionOffsets(
	offsets *[]generated.PartitionOffset,
) (map[int32]int64, error) {
	if offsets == nil {
		return nil, nil
	}
	if len(*offsets) > maxMCPGroupResetEntries {
		return nil, errInvalidRequest
	}
	output := make(map[int32]int64, len(*offsets))
	for _, pair := range *offsets {
		if pair.Partition < 0 || pair.Offset == nil || *pair.Offset < 0 {
			return nil, errInvalidRequest
		}
		if _, exists := output[pair.Partition]; exists {
			return nil, errInvalidRequest
		}
		output[pair.Partition] = *pair.Offset
	}
	return output, nil
}

func validateConsumerGroupInput(input consumerGroupInput) error {
	if err := validateClusterName(input.ClusterName); err != nil {
		return err
	}
	return validateConsumerGroupID(input.ID)
}

func validateConsumerGroupTopicInput(input consumerGroupTopicInput) error {
	if err := validateConsumerGroupInput(consumerGroupInput{
		ClusterName: input.ClusterName,
		ID:          input.ID,
	}); err != nil {
		return err
	}
	return validateTopicName(input.TopicName)
}

func validateConsumerGroupID(id string) error {
	return validateBoundedName(id)
}

func validateConsumerGroupResult(group domaincluster.GroupState) error {
	for _, value := range []string{
		group.ID,
		group.State,
		group.Coordinator,
		group.Protocol,
	} {
		if len(value) > maxClusterBrokerNameBytes {
			return errResultTooLarge
		}
	}
	if len(group.Members) > maxMCPGroupOutputChildren ||
		len(group.Offsets) > maxMCPGroupOutputChildren {
		return errResultTooLarge
	}
	for _, member := range group.Members {
		if len(member.MemberID) > maxClusterBrokerNameBytes ||
			len(member.Host) > maxClusterBrokerNameBytes ||
			len(member.Assignments) > maxMCPGroupOutputChildren {
			return errResultTooLarge
		}
		for _, assignment := range member.Assignments {
			if len(assignment.Topic) > maxTopicNameBytes ||
				len(assignment.Partitions) > maxMCPGroupOutputChildren {
				return errResultTooLarge
			}
		}
	}
	for _, offset := range group.Offsets {
		if len(offset.Topic) > maxTopicNameBytes {
			return errResultTooLarge
		}
	}
	return nil
}

func sortConsumerGroupStates(
	groups []domaincluster.GroupState,
	query appcluster.GroupPageQuery,
) {
	sort.SliceStable(groups, func(left, right int) bool {
		a, b := groups[left], groups[right]
		comparison := 0
		switch query.OrderBy {
		case string(generated.ConsumerGroupOrderingMEMBERS):
			comparison = compareInts(len(a.Members), len(b.Members))
		case string(generated.ConsumerGroupOrderingSTATE):
			comparison = strings.Compare(a.State, b.State)
		case string(generated.ConsumerGroupOrderingMESSAGESBEHIND):
			comparison = compareInt64s(a.Lag(), b.Lag())
		case string(generated.ConsumerGroupOrderingTOPICNUM):
			comparison = compareInts(a.TopicCount(), b.TopicCount())
		default:
			comparison = strings.Compare(a.ID, b.ID)
		}
		if comparison == 0 {
			return a.ID < b.ID
		}
		if query.SortOrder == string(generated.DESC) {
			return comparison > 0
		}
		return comparison < 0
	})
}

func consumerGroupFrom(group domaincluster.GroupState) generated.ConsumerGroup {
	output := generated.ConsumerGroup{
		GroupId: group.ID,
		Inherit: "ConsumerGroup",
		Coordinator: &generated.Broker{
			Id: group.CoordinatorID,
		},
	}
	if group.State != "" {
		state := generated.ConsumerGroupState(group.State)
		if !state.Valid() {
			state = generated.ConsumerGroupStateUNKNOWN
		}
		output.State = &state
	}
	if len(group.Members) > 0 {
		output.Members = consumerGroupPointer(boundedInt32Count(len(group.Members)))
	}
	if topics := group.TopicCount(); topics > 0 {
		output.Topics = consumerGroupPointer(boundedInt32Count(topics))
	}
	if group.Protocol != "" {
		output.PartitionAssignor = consumerGroupPointer(group.Protocol)
	}
	if group.Coordinator != "" {
		output.Coordinator.Host = consumerGroupPointer(group.Coordinator)
	}
	if consumerGroupHasCommit(group) {
		output.ConsumerLag = consumerGroupPointer(group.Lag())
	}
	return output
}

func consumerGroupDetailsFrom(group domaincluster.GroupState) groupDetailsResult {
	output := groupDetailsResult{ConsumerGroup: consumerGroupFrom(group)}
	output.Inherit = "details"
	if len(group.Offsets) == 0 {
		return output
	}

	offsets := append([]domaincluster.GroupOffset(nil), group.Offsets...)
	sort.SliceStable(offsets, func(left, right int) bool {
		if offsets[left].Topic != offsets[right].Topic {
			return offsets[left].Topic < offsets[right].Topic
		}
		return offsets[left].Partition < offsets[right].Partition
	})
	partitions := make([]generated.ConsumerGroupTopicPartition, 0, len(offsets))
	for _, offset := range offsets {
		partitions = append(partitions, consumerGroupPartitionFrom(group.Members, offset))
	}
	output.Partitions = &partitions
	return output
}

func consumerGroupPartitionFrom(
	members []domaincluster.GroupMember,
	offset domaincluster.GroupOffset,
) generated.ConsumerGroupTopicPartition {
	output := generated.ConsumerGroupTopicPartition{
		Partition: offset.Partition,
		Topic:     offset.Topic,
	}
	if offset.End >= 0 {
		output.EndOffset = consumerGroupPointer(offset.End)
	}
	if offset.Committed >= 0 {
		output.CurrentOffset = consumerGroupPointer(offset.Committed)
		if offset.End >= 0 {
			lag := offset.End - offset.Committed
			if lag < 0 {
				lag = 0
			}
			output.ConsumerLag = consumerGroupPointer(lag)
		}
	}
	if member := consumerGroupMemberFor(members, offset.Topic, offset.Partition); member != nil {
		output.ConsumerId = consumerGroupPointer(member.MemberID)
		if member.Host != "" {
			output.Host = consumerGroupPointer(member.Host)
		}
	}
	return output
}

func consumerGroupMemberFor(
	members []domaincluster.GroupMember,
	topic string,
	partition int32,
) *domaincluster.GroupMember {
	for index := range members {
		for _, assignment := range members[index].Assignments {
			if assignment.Topic != topic {
				continue
			}
			for _, assigned := range assignment.Partitions {
				if assigned == partition {
					return &members[index]
				}
			}
		}
	}
	return nil
}

func topicConsumerGroupFrom(
	group domaincluster.GroupState,
	topic string,
) generated.ConsumerGroup {
	output := consumerGroupFrom(group)
	var members int32
	for _, member := range group.Members {
		if consumerGroupMemberUsesTopic(member, topic) {
			members++
		}
	}
	output.Members = consumerGroupPointer(members)
	output.Topics = consumerGroupPointer(int32(1))
	output.ConsumerLag = nil

	var lag int64
	known := false
	for _, offset := range group.Offsets {
		if offset.Topic != topic || offset.Committed < 0 {
			continue
		}
		known = true
		if delta := offset.End - offset.Committed; delta > 0 {
			lag += delta
		}
	}
	if known {
		output.ConsumerLag = consumerGroupPointer(lag)
	}
	return output
}

func consumerGroupMemberUsesTopic(member domaincluster.GroupMember, topic string) bool {
	for _, assignment := range member.Assignments {
		if assignment.Topic == topic {
			return true
		}
	}
	return false
}

func consumerGroupLagFrom(
	group domaincluster.GroupState,
	includePartitions bool,
) generated.ConsumerGroupLag {
	topics := make(map[string]int64)
	var byTopic map[string]map[string]int64
	if includePartitions {
		byTopic = make(map[string]map[string]int64)
	}
	for _, offset := range group.Offsets {
		if offset.Committed < 0 || offset.End < 0 {
			continue
		}
		lag := offset.End - offset.Committed
		if lag < 0 {
			lag = 0
		}
		topics[offset.Topic] += lag
		if includePartitions {
			if byTopic[offset.Topic] == nil {
				byTopic[offset.Topic] = make(map[string]int64)
			}
			byTopic[offset.Topic][strconv.Itoa(int(offset.Partition))] = lag
		}
	}
	output := generated.ConsumerGroupLag{Lag: group.Lag(), Topics: topics}
	if includePartitions && len(byTopic) > 0 {
		topicPartitions := make(
			map[string]generated.ConsumerGroupTopicLag,
			len(byTopic),
		)
		for topic, partitions := range byTopic {
			topicPartitions[topic] = generated.ConsumerGroupTopicLag{
				Partitions: &partitions,
			}
		}
		output.TopicPartitions = &topicPartitions
	}
	return output
}

func consumerGroupsCSV(groups []domaincluster.GroupState) (string, error) {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	if err := writer.Write(groupCSVHeader); err != nil {
		return "", errOperationFailed
	}
	for _, group := range groups {
		var members, topics, assignor, lag string
		if len(group.Members) > 0 {
			members = strconv.Itoa(len(group.Members))
		}
		if count := group.TopicCount(); count > 0 {
			topics = strconv.Itoa(count)
		}
		if group.Protocol != "" {
			assignor = group.Protocol
		}
		if consumerGroupHasCommit(group) {
			lag = strconv.FormatInt(group.Lag(), 10)
		}
		if err := writer.Write([]string{
			group.ID,
			group.State,
			members,
			topics,
			assignor,
			strconv.FormatInt(int64(group.CoordinatorID), 10),
			lag,
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

func consumerGroupHasCommit(group domaincluster.GroupState) bool {
	for _, offset := range group.Offsets {
		if offset.Committed >= 0 {
			return true
		}
	}
	return false
}

func consumerGroupPointer[Value any](value Value) *Value {
	return &value
}
