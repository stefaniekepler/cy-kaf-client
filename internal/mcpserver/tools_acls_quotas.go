package mcpserver

import (
	"context"
	"encoding/csv"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"unicode"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

const (
	maxMCPAclRawItems      = 2000
	maxMCPQuotaRawItems    = 2000
	maxMCPAclMutationItems = 500
	maxMCPQuotaConfigKeys  = 32
	maxMCPAclFieldBytes    = 1024
	maxMCPQuotaEntityBytes = 1024
)

var mcpAclFilterResourceTypes = map[string]struct{}{
	"UNKNOWN": {}, "TOPIC": {}, "GROUP": {}, "CLUSTER": {},
	"TRANSACTIONAL_ID": {}, "DELEGATION_TOKEN": {}, "USER": {},
}

var mcpAclWriteResourceTypes = map[string]struct{}{
	"TOPIC": {}, "GROUP": {}, "CLUSTER": {},
	"TRANSACTIONAL_ID": {}, "DELEGATION_TOKEN": {},
}

var mcpAclPatternTypes = map[string]struct{}{
	"MATCH": {}, "LITERAL": {}, "PREFIXED": {},
}

var mcpAclOperations = map[string]struct{}{
	"ALL": {}, "READ": {}, "WRITE": {}, "CREATE": {}, "DELETE": {},
	"ALTER": {}, "DESCRIBE": {}, "CLUSTER_ACTION": {},
	"DESCRIBE_CONFIGS": {}, "ALTER_CONFIGS": {}, "IDEMPOTENT_WRITE": {},
	"CREATE_TOKENS": {}, "DESCRIBE_TOKENS": {},
}

var mcpAclPermissions = map[string]struct{}{"ALLOW": {}, "DENY": {}}

var mcpQuotaConfigKeys = map[string]struct{}{
	"producer_byte_rate":       {},
	"consumer_byte_rate":       {},
	"request_percentage":       {},
	"controller_mutation_rate": {},
	"connection_creation_rate": {},
}

func aclQuotaTool(meta ToolMeta) ToolSpec {
	switch meta.Name {
	case "listAcls":
		return aclQuotaReadTool(
			meta,
			aclQueryInputSelector[generated.ListAclsParams],
			func(ctx context.Context, executor *Executor, input clusterOptionalQueryInput[generated.ListAclsParams]) (any, error) {
				bindings, err := listMCPAcls(ctx, executor, input.ClusterName, aclFilterFromListParams(input.Query), meta.MaxItems)
				if err != nil {
					return nil, err
				}
				return aclBindingsToContract(bindings)
			},
		)
	case "getAclAsCsv":
		return aclQuotaReadTool(
			meta,
			aclQueryInputSelector[generated.GetAclAsCsvParams],
			func(ctx context.Context, executor *Executor, input clusterOptionalQueryInput[generated.GetAclAsCsvParams]) (any, error) {
				bindings, err := listMCPAcls(ctx, executor, input.ClusterName, aclFilterFromCSVParams(input.Query), meta.MaxItems)
				if err != nil {
					return nil, err
				}
				if executor.deps.Acls == nil {
					return nil, errOperationFailed
				}
				safeBindings, err := formulaSafeMCPAcls(bindings)
				if err != nil {
					return nil, err
				}
				text, err := executor.deps.Acls.FormatAclCSV(safeBindings)
				if err != nil {
					return nil, err
				}
				if len(text) > maxCSVBytes {
					return nil, errResultTooLarge
				}
				return text, nil
			},
		)
	case "createAcl":
		return aclQuotaWriteTool(meta, aclBodyInputSelector[generated.KafkaAcl], createMCPAcl)
	case "deleteAcl":
		return aclQuotaWriteTool(meta, aclBodyInputSelector[generated.KafkaAcl], deleteMCPAcl)
	case "syncAclsCsv":
		return aclQuotaWriteTool(meta, aclBodyInputSelector[generated.SyncAclsCsvTextBody], syncMCPAclsCSV)
	case "createConsumerAcl":
		return aclQuotaWriteTool(meta, aclBodyInputSelector[generated.CreateConsumerAcl], createMCPConsumerAcl)
	case "createProducerAcl":
		return aclQuotaWriteTool(meta, aclBodyInputSelector[generated.CreateProducerAcl], createMCPProducerAcl)
	case "createStreamAppAcl":
		return aclQuotaWriteTool(meta, aclBodyInputSelector[generated.CreateStreamAppAcl], createMCPStreamAppAcl)
	case "listQuotas":
		return aclQuotaReadTool(meta, clusterInputSelector, listMCPQuotas)
	case "upsertClientQuotas":
		return aclQuotaWriteTool(meta, aclBodyInputSelector[generated.ClientQuotas], upsertMCPClientQuotas)
	default:
		panic("unsupported ACL or Client Quotas MCP tool: " + meta.Name)
	}
}

func aclQuotaReadTool[In any](
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

func aclQuotaWriteTool[In any](
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

func aclQueryInputSelector[Query any](input clusterOptionalQueryInput[Query]) string {
	return input.ClusterName
}

func aclBodyInputSelector[Body any](input clusterBodyInput[Body]) string {
	return input.ClusterName
}

func aclFilterFromListParams(params *generated.ListAclsParams) domaincluster.AclFilter {
	if params == nil {
		return domaincluster.AclFilter{}
	}
	return aclFilterFromPointers(
		params.ResourceType,
		params.ResourceName,
		params.NamePatternType,
		params.Search,
		params.Fts,
	)
}

func aclFilterFromCSVParams(params *generated.GetAclAsCsvParams) domaincluster.AclFilter {
	if params == nil {
		return domaincluster.AclFilter{}
	}
	return aclFilterFromPointers(
		params.ResourceType,
		params.ResourceName,
		params.NamePatternType,
		params.Search,
		params.Fts,
	)
}

func aclFilterFromPointers(
	resourceType *generated.KafkaAclResourceType,
	resourceName *string,
	patternType *generated.KafkaAclNamePatternType,
	search *string,
	fts *bool,
) domaincluster.AclFilter {
	filter := domaincluster.AclFilter{}
	if resourceType != nil {
		filter.ResourceType = string(*resourceType)
	}
	if resourceName != nil {
		filter.ResourceName = *resourceName
	}
	if patternType != nil {
		filter.PatternType = string(*patternType)
	}
	if search != nil {
		filter.Search = *search
	}
	if fts != nil {
		filter.Fts = *fts
	}
	return filter
}

func validateMCPAclFilter(filter domaincluster.AclFilter) error {
	if filter.ResourceType != "" {
		if _, ok := mcpAclFilterResourceTypes[filter.ResourceType]; !ok {
			return errInvalidRequest
		}
	}
	if filter.PatternType != "" {
		if _, ok := mcpAclPatternTypes[filter.PatternType]; !ok {
			return errInvalidRequest
		}
	}
	if len(filter.ResourceName) > maxMCPAclFieldBytes ||
		len(filter.Search) > maxMCPAclFieldBytes ||
		hasUnsafeControl(filter.ResourceName) ||
		hasUnsafeControl(filter.Search) {
		return errInvalidRequest
	}
	return nil
}

func listMCPAcls(
	ctx context.Context,
	executor *Executor,
	clusterName string,
	filter domaincluster.AclFilter,
	maxItems int,
) ([]domaincluster.AclBinding, error) {
	if err := validateClusterName(clusterName); err != nil {
		return nil, err
	}
	if err := validateMCPAclFilter(filter); err != nil {
		return nil, err
	}
	if executor.deps.Acls == nil {
		return nil, errOperationFailed
	}
	bindings, err := executor.deps.Acls.List(ctx, clusterName, filter)
	if err != nil {
		return nil, err
	}
	// The port response has already been materialized. Bound all additional
	// local validation, copying, sorting, and conversion before touching rows.
	if len(bindings) > maxMCPAclRawItems {
		return nil, errResultTooLarge
	}
	for _, binding := range bindings {
		if err := validateMCPAclResultBinding(binding); err != nil {
			return nil, errOperationFailed
		}
	}
	bindings = append([]domaincluster.AclBinding(nil), bindings...)
	sort.SliceStable(bindings, func(left, right int) bool {
		return appcluster.AclSortKey(bindings[left]) < appcluster.AclSortKey(bindings[right])
	})
	if maxItems > 0 && len(bindings) > maxItems {
		bindings = bindings[:maxItems]
	}
	return bindings, nil
}

func validateMCPAclResultBinding(binding domaincluster.AclBinding) error {
	if err := validateMCPAclActor(binding.Principal, binding.Host); err != nil {
		return err
	}
	if err := validateMCPAclResourceName(binding.ResourceName); err != nil {
		return err
	}
	if _, ok := mcpAclFilterResourceTypes[binding.ResourceType]; !ok {
		return errInvalidRequest
	}
	if _, ok := mcpAclPatternTypes[binding.PatternType]; !ok {
		return errInvalidRequest
	}
	if binding.Operation != "UNKNOWN" {
		if _, ok := mcpAclOperations[binding.Operation]; !ok {
			return errInvalidRequest
		}
	}
	if _, ok := mcpAclPermissions[binding.Permission]; !ok {
		return errInvalidRequest
	}
	return nil
}

func aclBindingsToContract(bindings []domaincluster.AclBinding) ([]generated.KafkaAcl, error) {
	output := make([]generated.KafkaAcl, 0, len(bindings))
	for _, binding := range bindings {
		output = append(output, generated.KafkaAcl{
			Principal:       binding.Principal,
			Host:            binding.Host,
			ResourceType:    generated.KafkaAclResourceType(binding.ResourceType),
			ResourceName:    binding.ResourceName,
			NamePatternType: generated.KafkaAclNamePatternType(binding.PatternType),
			Operation:       generated.KafkaAclOperation(binding.Operation),
			Permission:      generated.KafkaAclPermission(binding.Permission),
		})
	}
	return output, nil
}

func formulaSafeMCPAcls(bindings []domaincluster.AclBinding) ([]domaincluster.AclBinding, error) {
	if len(bindings) > maxMCPAclMutationItems {
		return nil, errResultTooLarge
	}
	output := append([]domaincluster.AclBinding(nil), bindings...)
	for index := range output {
		output[index].Principal = connectCSVCell(output[index].Principal)
		output[index].ResourceType = connectCSVCell(output[index].ResourceType)
		output[index].PatternType = connectCSVCell(output[index].PatternType)
		output[index].ResourceName = connectCSVCell(output[index].ResourceName)
		output[index].Operation = connectCSVCell(output[index].Operation)
		output[index].Permission = connectCSVCell(output[index].Permission)
		output[index].Host = connectCSVCell(output[index].Host)
	}
	return output, nil
}

func aclBindingFromMCP(body generated.KafkaAcl, allowMatch bool) (domaincluster.AclBinding, error) {
	binding := domaincluster.AclBinding{
		Principal:    body.Principal,
		Host:         body.Host,
		ResourceType: string(body.ResourceType),
		ResourceName: body.ResourceName,
		PatternType:  string(body.NamePatternType),
		Operation:    string(body.Operation),
		Permission:   string(body.Permission),
	}
	if err := validateMCPAclActor(binding.Principal, binding.Host); err != nil {
		return domaincluster.AclBinding{}, err
	}
	if err := validateMCPAclResourceName(binding.ResourceName); err != nil {
		return domaincluster.AclBinding{}, err
	}
	if _, ok := mcpAclWriteResourceTypes[binding.ResourceType]; !ok {
		return domaincluster.AclBinding{}, errInvalidRequest
	}
	if _, ok := mcpAclOperations[binding.Operation]; !ok {
		return domaincluster.AclBinding{}, errInvalidRequest
	}
	if _, ok := mcpAclPermissions[binding.Permission]; !ok {
		return domaincluster.AclBinding{}, errInvalidRequest
	}
	validPattern := binding.PatternType == "LITERAL" ||
		binding.PatternType == "PREFIXED" ||
		allowMatch && binding.PatternType == "MATCH"
	if !validPattern {
		return domaincluster.AclBinding{}, errInvalidRequest
	}
	if binding.ResourceType == "CLUSTER" &&
		(binding.ResourceName != "kafka-cluster" || binding.PatternType != "LITERAL") {
		return domaincluster.AclBinding{}, errInvalidRequest
	}
	return binding, nil
}

func createMCPAcl(
	ctx context.Context,
	executor *Executor,
	input clusterBodyInput[generated.KafkaAcl],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	binding, err := aclBindingFromMCP(input.Body, false)
	if err != nil {
		return nil, err
	}
	if executor.deps.Acls == nil {
		return nil, errOperationFailed
	}
	return nil, executor.deps.Acls.CreateAcl(ctx, input.ClusterName, binding)
}

func deleteMCPAcl(
	ctx context.Context,
	executor *Executor,
	input clusterBodyInput[generated.KafkaAcl],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	binding, err := aclBindingFromMCP(input.Body, true)
	if err != nil {
		return nil, err
	}
	if executor.deps.Acls == nil {
		return nil, errOperationFailed
	}
	deleted, err := executor.deps.Acls.DeleteAcl(ctx, input.ClusterName, binding)
	if err != nil {
		return nil, err
	}
	if deleted <= 0 {
		return nil, errNotFound
	}
	return nil, nil
}

func syncMCPAclsCSV(
	ctx context.Context,
	executor *Executor,
	input clusterBodyInput[generated.SyncAclsCsvTextBody],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if len(input.Body) == 0 || len(input.Body) > maxCSVBytes {
		return nil, errInvalidRequest
	}
	if err := validateMCPAClCSVLogicalRows(input.Body, maxMCPAclMutationItems); err != nil {
		return nil, err
	}
	bindings, err := appcluster.ParseAclCSV(input.Body)
	if err != nil {
		return nil, errInvalidRequest
	}
	if len(bindings) > maxMCPAclMutationItems {
		return nil, errInvalidRequest
	}
	for _, binding := range bindings {
		body := generated.KafkaAcl{
			Principal: binding.Principal, Host: binding.Host,
			ResourceType:    generated.KafkaAclResourceType(binding.ResourceType),
			ResourceName:    binding.ResourceName,
			NamePatternType: generated.KafkaAclNamePatternType(binding.PatternType),
			Operation:       generated.KafkaAclOperation(binding.Operation),
			Permission:      generated.KafkaAclPermission(binding.Permission),
		}
		if _, err := aclBindingFromMCP(body, false); err != nil {
			return nil, errInvalidRequest
		}
	}
	if executor.deps.Acls == nil {
		return nil, errOperationFailed
	}
	return nil, executor.deps.Acls.SyncCSV(ctx, input.ClusterName, input.Body)
}

func validateMCPAClCSVLogicalRows(text string, maxRows int) error {
	if maxRows < 1 {
		return errInvalidRequest
	}
	reader := csv.NewReader(strings.NewReader(text))
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true
	headerSeen := false
	rows := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errInvalidRequest
		}
		if len(record) == 1 && strings.TrimSpace(record[0]) == "" {
			continue
		}
		if !headerSeen {
			headerSeen = true
			continue
		}
		rows++
		if rows > maxRows {
			return errInvalidRequest
		}
	}
	if !headerSeen {
		return errInvalidRequest
	}
	return nil
}

func createMCPConsumerAcl(
	ctx context.Context,
	executor *Executor,
	input clusterBodyInput[generated.CreateConsumerAcl],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if input.Body.Principal == nil {
		return nil, errInvalidRequest
	}
	host := mcpAclHost(input.Body.Host)
	if err := validateMCPAclActor(*input.Body.Principal, host); err != nil {
		return nil, err
	}
	topics, err := mcpAclResources(input.Body.Topics)
	if err != nil {
		return nil, err
	}
	groups, err := mcpAclResources(input.Body.ConsumerGroups)
	if err != nil {
		return nil, err
	}
	topicPrefix, err := mcpAclOptionalResource(input.Body.TopicsPrefix)
	if err != nil {
		return nil, err
	}
	groupPrefix, err := mcpAclOptionalResource(input.Body.ConsumerGroupsPrefix)
	if err != nil {
		return nil, err
	}
	expanded := 2 * (len(topics) + len(groups) +
		nonEmptyCount(topicPrefix, groupPrefix))
	if err := validateMCPAclExpansion(expanded); err != nil {
		return nil, errInvalidRequest
	}
	if executor.deps.Acls == nil {
		return nil, errOperationFailed
	}
	spec := appcluster.ConsumerAclSpec{
		Principal: *input.Body.Principal, Host: host,
		Topics: topics, TopicsPrefix: topicPrefix,
		ConsumerGroups: groups, ConsumerGroupsPrefix: groupPrefix,
	}
	return nil, executor.deps.Acls.CreateConsumerAcl(ctx, input.ClusterName, spec)
}

func createMCPProducerAcl(
	ctx context.Context,
	executor *Executor,
	input clusterBodyInput[generated.CreateProducerAcl],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if input.Body.Principal == nil {
		return nil, errInvalidRequest
	}
	host := mcpAclHost(input.Body.Host)
	if err := validateMCPAclActor(*input.Body.Principal, host); err != nil {
		return nil, err
	}
	topics, err := mcpAclResources(input.Body.Topics)
	if err != nil {
		return nil, err
	}
	topicPrefix, err := mcpAclOptionalResource(input.Body.TopicsPrefix)
	if err != nil {
		return nil, err
	}
	transactionalID, err := mcpAclOptionalResource(input.Body.TransactionalId)
	if err != nil {
		return nil, err
	}
	transactionalPrefix, err := mcpAclOptionalResource(input.Body.TransactionsIdPrefix)
	if err != nil {
		return nil, err
	}
	idempotent := input.Body.Idempotent != nil && *input.Body.Idempotent
	expanded := 3*(len(topics)+nonEmptyCount(topicPrefix)) +
		2*nonEmptyCount(transactionalID, transactionalPrefix) +
		boolCount(idempotent)
	if err := validateMCPAclExpansion(expanded); err != nil {
		return nil, errInvalidRequest
	}
	if executor.deps.Acls == nil {
		return nil, errOperationFailed
	}
	spec := appcluster.ProducerAclSpec{
		Principal: *input.Body.Principal, Host: host,
		Topics: topics, TopicsPrefix: topicPrefix,
		TransactionalID: transactionalID, TransactionsIDPrefix: transactionalPrefix,
		Idempotent: idempotent,
	}
	return nil, executor.deps.Acls.CreateProducerAcl(ctx, input.ClusterName, spec)
}

func createMCPStreamAppAcl(
	ctx context.Context,
	executor *Executor,
	input clusterBodyInput[generated.CreateStreamAppAcl],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if input.Body.Principal == nil {
		return nil, errInvalidRequest
	}
	host := mcpAclHost(input.Body.Host)
	if err := validateMCPAclActor(*input.Body.Principal, host); err != nil {
		return nil, err
	}
	inputTopics, err := mcpAclResources(input.Body.InputTopics)
	if err != nil {
		return nil, err
	}
	outputTopics, err := mcpAclResources(input.Body.OutputTopics)
	if err != nil {
		return nil, err
	}
	applicationID, err := mcpAclOptionalResource(input.Body.ApplicationId)
	if err != nil {
		return nil, err
	}
	expanded := len(inputTopics) + len(outputTopics) + 2*nonEmptyCount(applicationID)
	if err := validateMCPAclExpansion(expanded); err != nil {
		return nil, errInvalidRequest
	}
	if executor.deps.Acls == nil {
		return nil, errOperationFailed
	}
	spec := appcluster.StreamAppAclSpec{
		Principal: *input.Body.Principal, Host: host,
		InputTopics: inputTopics, OutputTopics: outputTopics,
		ApplicationID: applicationID,
	}
	return nil, executor.deps.Acls.CreateStreamAppAcl(ctx, input.ClusterName, spec)
}

func mcpAclHost(host *string) string {
	if host == nil {
		return "*"
	}
	return *host
}

func mcpAclResources(resources *[]string) ([]string, error) {
	if resources == nil {
		return nil, nil
	}
	if len(*resources) > maxMCPAclMutationItems {
		return nil, errInvalidRequest
	}
	output := make([]string, 0, len(*resources))
	seen := make(map[string]struct{}, len(*resources))
	for _, resource := range *resources {
		if err := validateMCPAclResourceName(resource); err != nil {
			return nil, err
		}
		if _, duplicate := seen[resource]; duplicate {
			return nil, errInvalidRequest
		}
		seen[resource] = struct{}{}
		output = append(output, resource)
	}
	return output, nil
}

func mcpAclOptionalResource(resource *string) (string, error) {
	if resource == nil || *resource == "" {
		return "", nil
	}
	if err := validateMCPAclResourceName(*resource); err != nil {
		return "", err
	}
	return *resource, nil
}

func nonEmptyCount(values ...string) int {
	total := 0
	for _, value := range values {
		if value != "" {
			total++
		}
	}
	return total
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func validateMCPAclExpansion(expanded int) error {
	if expanded < 1 || expanded > maxMCPAclMutationItems {
		return errInvalidRequest
	}
	return nil
}

func validateMCPAclActor(principal, host string) error {
	if len(principal) == 0 || len(principal) > maxMCPAclFieldBytes ||
		strings.TrimSpace(principal) != principal || hasUnsafeControl(principal) {
		return errInvalidRequest
	}
	separator := strings.IndexByte(principal, ':')
	if separator <= 0 || separator == len(principal)-1 {
		return errInvalidRequest
	}
	if !isAclPrincipalType(principal[:separator]) ||
		strings.TrimSpace(principal[separator+1:]) != principal[separator+1:] {
		return errInvalidRequest
	}
	if len(host) == 0 || len(host) > maxMCPAclFieldBytes ||
		strings.TrimSpace(host) != host || hasUnsafeControl(host) {
		return errInvalidRequest
	}
	if host != "*" && net.ParseIP(host) == nil {
		return errInvalidRequest
	}
	return nil
}

func isAclPrincipalType(value string) bool {
	for _, current := range value {
		if unicode.IsLetter(current) || unicode.IsDigit(current) ||
			current == '_' || current == '-' || current == '.' {
			continue
		}
		return false
	}
	return value != ""
}

func validateMCPAclResourceName(value string) error {
	if len(value) == 0 || len(value) > maxMCPAclFieldBytes ||
		strings.TrimSpace(value) != value || hasUnsafeControl(value) {
		return errInvalidRequest
	}
	return nil
}

func hasUnsafeControl(value string) bool {
	return strings.IndexFunc(value, func(current rune) bool {
		return unicode.IsControl(current)
	}) >= 0
}

func listMCPQuotas(
	ctx context.Context,
	executor *Executor,
	input clusterInput,
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	if executor.deps.Quotas == nil {
		return nil, errOperationFailed
	}
	quotas, err := executor.deps.Quotas.ListQuotas(ctx, input.ClusterName)
	if err != nil {
		return nil, err
	}
	if len(quotas) > maxMCPQuotaRawItems {
		return nil, errResultTooLarge
	}

	type entityKey struct{ user, clientID, ip string }
	seen := make(map[entityKey]struct{}, len(quotas))
	for _, quota := range quotas {
		if err := validateMCPClientQuotaResult(quota); err != nil {
			return nil, errOperationFailed
		}
		key := entityKey{user: quota.User, clientID: quota.ClientID, ip: quota.IP}
		if _, duplicate := seen[key]; duplicate {
			return nil, errOperationFailed
		}
		seen[key] = struct{}{}
	}
	quotas = cloneClientQuotas(quotas)
	sort.SliceStable(quotas, func(left, right int) bool {
		leftFields := [...]string{quotas[left].User, quotas[left].ClientID, quotas[left].IP}
		rightFields := [...]string{quotas[right].User, quotas[right].ClientID, quotas[right].IP}
		for index := range leftFields {
			if leftFields[index] != rightFields[index] {
				return leftFields[index] < rightFields[index]
			}
		}
		return false
	})
	if len(quotas) > maxListItems {
		quotas = quotas[:maxListItems]
	}
	output := make([]generated.ClientQuotas, 0, len(quotas))
	for _, quota := range quotas {
		converted, err := clientQuotaToMCP(quota)
		if err != nil {
			return nil, errOperationFailed
		}
		output = append(output, converted)
	}
	return output, nil
}

func upsertMCPClientQuotas(
	ctx context.Context,
	executor *Executor,
	input clusterBodyInput[generated.ClientQuotas],
) (any, error) {
	if err := validateClusterName(input.ClusterName); err != nil {
		return nil, err
	}
	quota, err := clientQuotaFromMCP(input.Body)
	if err != nil {
		return nil, err
	}
	if executor.deps.Quotas == nil {
		return nil, errOperationFailed
	}
	return nil, executor.deps.Quotas.UpsertQuotas(ctx, input.ClusterName, quota)
}

func clientQuotaFromMCP(body generated.ClientQuotas) (domaincluster.ClientQuota, error) {
	if err := validateMCPClientQuotaWriteRequest(body); err != nil {
		return domaincluster.ClientQuota{}, err
	}
	quota := domaincluster.ClientQuota{Quotas: make(map[string]float64)}
	if body.User != nil {
		quota.User = *body.User
	}
	if body.ClientId != nil {
		quota.ClientID = *body.ClientId
	}
	if body.Ip != nil {
		quota.IP = *body.Ip
	}
	if body.Quotas != nil {
		for name, value := range *body.Quotas {
			quota.Quotas[name] = float64(value)
		}
	}
	return quota, nil
}

func clientQuotaToMCP(quota domaincluster.ClientQuota) (generated.ClientQuotas, error) {
	if err := validateMCPClientQuotaResult(quota); err != nil {
		return generated.ClientQuotas{}, err
	}
	output := generated.ClientQuotas{}
	if quota.User != "" {
		output.User = stringPointer(quota.User)
	}
	if quota.ClientID != "" {
		output.ClientId = stringPointer(quota.ClientID)
	}
	if quota.IP != "" {
		output.Ip = stringPointer(quota.IP)
	}
	if len(quota.Quotas) > 0 {
		values := make(map[string]float32, len(quota.Quotas))
		for name, value := range quota.Quotas {
			if value > math.MaxFloat32 {
				return generated.ClientQuotas{}, errInvalidRequest
			}
			values[name] = float32(value)
		}
		output.Quotas = &values
	}
	return output, nil
}

func validateMCPClientQuotaWriteRequest(body generated.ClientQuotas) error {
	present := false
	for _, dimension := range []*string{body.User, body.ClientId, body.Ip} {
		if dimension == nil {
			continue
		}
		present = true
		if *dimension == "" || !validMCPQuotaDimension(*dimension) {
			return errInvalidRequest
		}
	}
	if !present {
		return errInvalidRequest
	}
	values := map[string]float64{}
	if body.Quotas != nil {
		if len(*body.Quotas) > maxMCPQuotaConfigKeys {
			return errInvalidRequest
		}
		for name, value := range *body.Quotas {
			values[name] = float64(value)
		}
	}
	return validateMCPQuotaValues(values)
}

func validateMCPClientQuotaResult(quota domaincluster.ClientQuota) error {
	for _, dimension := range []string{quota.User, quota.ClientID, quota.IP} {
		if dimension == "" {
			continue
		}
		if !validMCPQuotaDimension(dimension) {
			return errInvalidRequest
		}
	}
	return validateMCPQuotaValues(quota.Quotas)
}

func validMCPQuotaDimension(dimension string) bool {
	return len(dimension) <= maxMCPQuotaEntityBytes &&
		strings.TrimSpace(dimension) == dimension &&
		!hasUnsafeControl(dimension)
}

func validateMCPQuotaValues(values map[string]float64) error {
	if len(values) > maxMCPQuotaConfigKeys {
		return errInvalidRequest
	}
	for name, value := range values {
		if _, ok := mcpQuotaConfigKeys[name]; !ok ||
			math.IsNaN(value) || math.IsInf(value, 0) ||
			value < 0 || value > math.MaxFloat32 {
			return errInvalidRequest
		}
	}
	return nil
}

func cloneClientQuotas(quotas []domaincluster.ClientQuota) []domaincluster.ClientQuota {
	output := make([]domaincluster.ClientQuota, len(quotas))
	for index, quota := range quotas {
		output[index] = quota
		output[index].Quotas = make(map[string]float64, len(quota.Quotas))
		for name, value := range quota.Quotas {
			output[index].Quotas[name] = value
		}
	}
	return output
}

func stringPointer(value string) *string {
	return &value
}
