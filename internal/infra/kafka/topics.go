package kafka

// topics.go implements cluster.TopicAdminPort (P1b Task 4's topic read
// surface: configs, ACLs, active producers) against the pooled kadm client —
// same shape as state.go's BrokerAdminPort methods: clientFor, one kadm call,
// map the result onto domain types, no caching (every request hits the live
// cluster). Task 5 appends this file's write methods to the same interface.

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// TopicConfigs reports one topic's configuration entries (dynamic/static/
// default, with synonyms — kadm's IncludeSynonyms is always on, same as
// BrokerConfigs), reusing the shared Config->ConfigEntry mapping
// (resourceConfigsToEntries, state.go).
func (p *Pool) TopicConfigs(ctx context.Context, def cluster.Definition, topic string) ([]cluster.ConfigEntry, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	rcs, err := c.adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("describe topic configs: %w", err)
	}
	return resourceConfigsToEntries(rcs)
}

// TopicAcls reports every ACL bound to topic by literal resource-name match
// (kadm.ACLPatternLiteral, set explicitly below — prefixed/wildcard ACLs
// that would *also* apply to this topic are out of scope for P1b Task 4).
// Allow/AllowHosts/Deny/DenyHosts/Operations are all called with no
// arguments, which kadm's ACLBuilder treats as "any" for each — describing
// with all four collapses (per kadm's acls.go createDelDescACL) into a
// single broad DescribeACLs call that returns every ACL on the topic
// regardless of principal/host/operation/permission, rather than one call
// per combination.
func (p *Pool) TopicAcls(ctx context.Context, def cluster.Definition, topic string) ([]cluster.AclBinding, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	builder := kadm.NewACLs().Topics(topic).ResourcePatternType(kadm.ACLPatternLiteral).
		Allow().AllowHosts().Deny().DenyHosts().Operations()
	results, err := c.adm.DescribeACLs(ctx, builder)
	if err != nil {
		return nil, fmt.Errorf("describe topic acls: %w", err)
	}
	var out []cluster.AclBinding
	for _, r := range results {
		if r.Err != nil {
			return nil, r.Err
		}
		for _, acl := range r.Described {
			out = append(out, cluster.AclBinding{
				Principal:    acl.Principal,
				Host:         acl.Host,
				ResourceName: topic,
				ResourceType: aclResourceTypeToContract(acl.Type),
				PatternType:  aclPatternTypeToContract(acl.Pattern),
				Operation:    aclOperationToContract(acl.Operation),
				Permission:   aclPermissionToContract(acl.Permission),
			})
		}
	}
	return out, nil
}

// ActiveProducers reports topic's active (in-flight transactional)
// producers, per partition. The TopicsSet is built with no explicit
// partitions (kadm.TopicsSet.Add(topic) with zero varargs): kadm's
// DescribeProducers treats that as "all partitions of this topic" and
// resolves them itself via a Metadata call (txn.go's EmptyTopics handling),
// so this method doesn't need to know the topic's partition count upfront.
func (p *Pool) ActiveProducers(ctx context.Context, def cluster.Definition, topic string) ([]cluster.ProducerState, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	var ts kadm.TopicsSet
	ts.Add(topic)
	described, err := c.adm.DescribeProducers(ctx, ts)
	if err != nil {
		return nil, fmt.Errorf("describe active producers: %w", err)
	}
	dt, ok := described[topic]
	if !ok {
		return nil, nil
	}
	var out []cluster.ProducerState
	for _, part := range dt.Partitions.Sorted() {
		if part.Err != nil {
			return nil, part.Err
		}
		for _, prod := range part.ActiveProducers.Sorted() {
			out = append(out, cluster.ProducerState{
				Partition:                     part.Partition,
				ProducerID:                    prod.ProducerID,
				ProducerEpoch:                 int32(prod.ProducerEpoch),
				LastSequence:                  prod.LastSequence,
				LastTimestamp:                 prod.LastTimestamp,
				CoordinatorEpoch:              prod.CoordinatorEpoch,
				CurrentTransactionStartOffset: prod.CurrentTxnStartOffset,
			})
		}
	}
	return out, nil
}

// CreateTopic creates one topic per spec via kadm.CreateTopics (this repo
// standardizes on the plural form throughout, same as DeleteTopic below,
// even for a single topic — kadm.CreateTopic is a thin single-topic wrapper
// around it, no behavioural difference). spec.Partitions/ReplicationFactor
// pass straight through as -1 when the caller wants the cluster's own
// default (Kafka's own create-topic sentinel, valid against a 2.4+ broker).
func (p *Pool) CreateTopic(ctx context.Context, def cluster.Definition, spec cluster.TopicSpec) error {
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	var configs map[string]*string
	if len(spec.Configs) > 0 {
		configs = make(map[string]*string, len(spec.Configs))
		for k, v := range spec.Configs {
			v := v // 避免多次迭代共享同一变量地址
			configs[k] = &v
		}
	}
	resps, err := c.adm.CreateTopics(ctx, spec.Partitions, spec.ReplicationFactor, configs, spec.Name)
	if err != nil {
		return fmt.Errorf("create topic: %w", err)
	}
	return resps.Error()
}

// DeleteTopic deletes topic via kadm.DeleteTopics. Callers
// (app.TopicService.Delete) own the TOPIC_DELETION feature-flag check
// (RuntimeState.TopicDeletionEnabled) — this method has no such awareness
// and always attempts the delete.
func (p *Pool) DeleteTopic(ctx context.Context, def cluster.Definition, topic string) error {
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	resps, err := c.adm.DeleteTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("delete topic: %w", err)
	}
	return resps.Error()
}

// AlterTopicConfig incrementally alters a single topic config key via
// kadm.AlterTopicConfigs (IncrementalAlterConfigs) — never
// kadm.AlterTopicConfigsState, which replaces the topic's *entire* config
// state and would silently drop every other dynamic key (ADR-0003 §6.1,
// the same trap AlterBrokerConfig above already avoids for brokers).
// value == "" means "unset this key" (kadm.DeleteConfig, reverting to
// whatever lower-priority source then applies — e.g. a cluster default),
// rather than "set it to a literal empty string": TopicService.UpdateConfigs'
// diff (configOps) is this method's only caller for the unset case, and no
// real-world dynamic topic config (cleanup.policy, retention.ms,
// compression.type, ...) has a legitimate empty-string value, so the two
// meanings never actually collide in practice.
func (p *Pool) AlterTopicConfig(ctx context.Context, def cluster.Definition, topic, name, value string) error {
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	op := kadm.AlterConfig{Op: kadm.SetConfig, Name: name, Value: &value}
	if value == "" {
		op = kadm.AlterConfig{Op: kadm.DeleteConfig, Name: name}
	}
	resps, err := c.adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{op}, topic)
	if err != nil {
		return fmt.Errorf("alter topic config: %w", err)
	}
	// AlterConfigsResponses has no built-in Error() helper (unlike
	// Create/DeleteTopicResponses above) — same manual first-error loop as
	// AlterBrokerConfig (state.go), which faces the identical gap.
	for _, r := range resps {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// CreatePartitions sets topic's *total* partition count to total via kadm's
// UpdatePartitions — deliberately not kadm.CreatePartitions, despite the
// matching name: kadm.CreatePartitions takes an *incremental* add count
// ("adding N partitions to each topic"), while UpdatePartitions takes the
// *final* total ("setting the final partition count to N") — the contract's
// PartitionsIncrease.totalPartitionsCount is the latter semantics
// (capability_audit_test.go registers both real kadm methods so this choice
// isn't lost to a future reader who greps only for "CreatePartitions").
func (p *Pool) CreatePartitions(ctx context.Context, def cluster.Definition, topic string, total int32) error {
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	resps, err := c.adm.UpdatePartitions(ctx, int(total), topic)
	if err != nil {
		return fmt.Errorf("create partitions: %w", err)
	}
	return resps.Error()
}

// AlterPartitionAssignments reassigns topic's replicas per partition via
// kadm.AlterPartitionAssignmentsReq.Assign (topic -> partition -> ordered
// broker list). Iterates resps.Sorted() rather than resps.Error() /
// resps.Each() for the same reason MoveReplicaLogDir does above: the
// underlying type is a nested map, so bare map iteration order is
// nondeterministic — Sorted() gives a reproducible "first error" pick.
func (p *Pool) AlterPartitionAssignments(ctx context.Context, def cluster.Definition, topic string, assignment map[int32][]int32) error {
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	var req kadm.AlterPartitionAssignmentsReq
	for partition, brokers := range assignment {
		req.Assign(topic, partition, brokers)
	}
	resps, err := c.adm.AlterPartitionAssignments(ctx, req)
	if err != nil {
		return fmt.Errorf("alter partition assignments: %w", err)
	}
	for _, r := range resps.Sorted() {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// aclResourceTypeToContract maps kmsg's ACLResourceType.String() output onto
// the contract's KafkaAclResourceType enum text (same "translate the driver
// vocabulary explicitly, don't just pass its String() through" pattern as
// handlers_broker.go's sourceToGenerated). Confirmed against
// kmsg@v1.13.1 generated.go's String() (ANY/TOPIC/GROUP/CLUSTER/
// TRANSACTIONAL_ID/DELEGATION_TOKEN/USER): every value but ANY has a direct
// contract equivalent. TopicAcls only ever requests Topic-type ACLs (via
// kadm.NewACLs().Topics(topic)), so ANY should never actually reach here —
// it (and anything else unrecognized) falls to UNKNOWN rather than leaking
// a string the contract doesn't declare.
func aclResourceTypeToContract(t kmsg.ACLResourceType) string {
	switch t.String() {
	case "TOPIC":
		return "TOPIC"
	case "GROUP":
		return "GROUP"
	case "CLUSTER":
		return "CLUSTER"
	case "TRANSACTIONAL_ID":
		return "TRANSACTIONAL_ID"
	case "DELEGATION_TOKEN":
		return "DELEGATION_TOKEN"
	case "USER":
		return "USER"
	default:
		return "UNKNOWN"
	}
}

// aclPatternTypeToContract maps kmsg's ACLResourcePatternType.String()
// output onto the contract's KafkaAclNamePatternType enum text. The contract
// only declares LITERAL/MATCH/PREFIXED (no ANY/UNKNOWN) — TopicAcls filters
// with ACLPatternLiteral, so a real described ACL will only ever be LITERAL
// or PREFIXED (a broker-stored ACL is never itself pattern "ANY"; that value
// only exists as a filter/request shape). MATCH and anything else fall to
// "UNKNOWN": harmless (the generated field is a plain string type, not
// server-validated against the enum), and it's a clear signal something
// unexpected came back rather than a silently-wrong PREFIXED/LITERAL guess.
func aclPatternTypeToContract(pt kadm.ACLPattern) string {
	switch pt.String() {
	case "LITERAL":
		return "LITERAL"
	case "PREFIXED":
		return "PREFIXED"
	case "MATCH":
		return "MATCH"
	default:
		return "UNKNOWN"
	}
}

// aclOperationToContract maps kmsg's ACLOperation.String() output onto the
// contract's KafkaAclOperation enum text. Confirmed against kmsg@v1.13.1
// generated.go's String(): every value but ANY (ALL/READ/WRITE/CREATE/
// DELETE/ALTER/DESCRIBE/CLUSTER_ACTION/DESCRIBE_CONFIGS/ALTER_CONFIGS/
// IDEMPOTENT_WRITE/CREATE_TOKENS/DESCRIBE_TOKENS) has a direct, identically-
// spelled contract equivalent. ANY is filter-only (never a real ACL's own
// operation) and falls to UNKNOWN, same as every unrecognized value.
func aclOperationToContract(op kadm.ACLOperation) string {
	switch op.String() {
	case "ALL":
		return "ALL"
	case "ALTER":
		return "ALTER"
	case "ALTER_CONFIGS":
		return "ALTER_CONFIGS"
	case "CLUSTER_ACTION":
		return "CLUSTER_ACTION"
	case "CREATE":
		return "CREATE"
	case "CREATE_TOKENS":
		return "CREATE_TOKENS"
	case "DELETE":
		return "DELETE"
	case "DESCRIBE":
		return "DESCRIBE"
	case "DESCRIBE_CONFIGS":
		return "DESCRIBE_CONFIGS"
	case "DESCRIBE_TOKENS":
		return "DESCRIBE_TOKENS"
	case "IDEMPOTENT_WRITE":
		return "IDEMPOTENT_WRITE"
	case "READ":
		return "READ"
	case "WRITE":
		return "WRITE"
	default:
		return "UNKNOWN"
	}
}

// aclPermissionToContract maps kmsg's ACLPermissionType.String() output onto
// the contract's KafkaAclPermission enum text (ALLOW/DENY only — no
// ANY/UNKNOWN declared). ANY is filter-only (never a real ACL's own
// permission); anything unrecognized falls to "UNKNOWN" — not a declared
// contract value, but harmless for the same reason as
// aclPatternTypeToContract's fallback (plain string field, no server-side
// enum validation), and a real described ACL will always be ALLOW or DENY.
func aclPermissionToContract(perm kmsg.ACLPermissionType) string {
	switch perm.String() {
	case "ALLOW":
		return "ALLOW"
	case "DENY":
		return "DENY"
	default:
		return "UNKNOWN"
	}
}
