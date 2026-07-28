package kafka

// groups.go implements cluster.GroupAdminPort (P1b Task 6: consumer group
// list/describe/reset/delete) against the pooled kadm client — same
// clientFor + kadm call(s) + map onto domain types shape as topics.go, no
// caching.
//
// DescribeGroup/GroupsForTopic deliberately use the *classic* DescribeGroups
// (already audited in capability_audit_test.go), never the also-audited
// DescribeConsumerGroups: that method is kadm's KIP-848 "next generation"
// consumer-group-protocol equivalent (its own doc comment: "specifically for
// consumer groups using the new consumer group protocol"), which fails or
// returns nothing for groups using the classic protocol — the overwhelming
// majority of real deployments, and what T7's testcontainers-backed
// integration fixtures create. See capability_audit_test.go's Task 6 note
// and ADR-0003 §6 item 6 for the full writeup of this trap.
//
// DescribeGroup's Offsets are deliberately scoped to *committed* partitions
// only (from FetchOffsets) — a partition a group's members are currently
// assigned to consume but have never committed against is not surfaced as a
// GroupOffset{Committed: -1} entry today, even though the domain type models
// that case. Widening this to the union of assigned-and-committed partitions
// was considered and deferred: it needs no new kadm capability (DescribedGroup
// already exposes AssignedPartitions()) but adds real complexity that has no
// live-cluster test coverage in this task (T6 is unit-tests-only; T7 owns
// integration behaviour) — see this task's report for the full rationale.

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

type groupAdmin interface {
	ListGroups(context.Context, ...string) (kadm.ListedGroups, error)
	DescribeGroups(context.Context, ...string) (kadm.DescribedGroups, error)
	FetchOffsets(context.Context, string) (kadm.OffsetResponses, error)
	FetchManyOffsets(context.Context, ...string) kadm.FetchOffsetsResponses
	ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error)
}

const topicGroupOffsetBatchSize = 32

// groupStateToGenerated maps kadm's raw DescribedGroup/ListedGroup.State
// string (Kafka's own o.a.k.common.ConsumerGroupState.toString() spelling —
// per kadm's own doc comment: "the state this group is in (Empty, Dead,
// Stable, etc.)") onto the contract's ConsumerGroupState enum spelling.
// Unrecognized/empty input falls to "UNKNOWN" rather than leaking a raw
// string the contract doesn't declare — same defensive default as
// aclResourceTypeToContract et al. (topics.go).
func groupStateToGenerated(raw string) string {
	switch raw {
	case "Empty":
		return "EMPTY"
	case "Stable":
		return "STABLE"
	case "Dead":
		return "DEAD"
	case "PreparingRebalance":
		return "PREPARING_REBALANCE"
	case "CompletingRebalance":
		return "COMPLETING_REBALANCE"
	default:
		return "UNKNOWN"
	}
}

// ListGroups reports every group's identity/state/coordinator only — see
// cluster.GroupAdminPort's doc comment for why members/offsets are
// deliberately excluded at this level.
func (p *Pool) ListGroups(ctx context.Context, def cluster.Definition) ([]cluster.GroupState, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	listed, err := c.adm.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	out := make([]cluster.GroupState, 0, len(listed))
	for _, g := range listed.Sorted() {
		out = append(out, cluster.GroupState{
			ID:            g.Group,
			State:         groupStateToGenerated(g.State),
			CoordinatorID: g.Coordinator,
		})
	}
	return out, nil
}

// DescribeGroup reports one group's full detail: members (with partition
// assignments) plus, for every topic-partition it has committed offsets
// against, the committed/end offset pair.
func (p *Pool) DescribeGroup(ctx context.Context, def cluster.Definition, id string) (cluster.GroupState, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return cluster.GroupState{}, err
	}
	return describeOneGroup(ctx, c.adm, id)
}

// GroupsForTopic reports every group that has committed offsets against topic.
// Kafka has no reverse topic -> group index, so the implementation first
// fetches every listed group's committed offsets in one batched read, then
// performs the full per-group describe only for the matching group IDs.
func (p *Pool) GroupsForTopic(ctx context.Context, def cluster.Definition, topic string) ([]cluster.GroupState, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	return groupsForTopic(ctx, c.adm, topic)
}

func groupsForTopic(ctx context.Context, adm groupAdmin, topic string) ([]cluster.GroupState, error) {
	listed, err := adm.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	names := listed.Groups() // sorted []string, every group name
	var out []cluster.GroupState
	for start := 0; start < len(names); start += topicGroupOffsetBatchSize {
		end := min(start+topicGroupOffsetBatchSize, len(names))
		batch := names[start:end]
		fetched := adm.FetchManyOffsets(ctx, batch...)
		for _, name := range batch {
			response, ok := fetched[name]
			if !ok || response.Err != nil || !offsetResponsesTouchTopic(response.Fetched, topic) {
				continue
			}
			gs, err := describeOneGroup(ctx, adm, name)
			if err != nil {
				continue // a single group failing to describe doesn't fail the whole topic-scoped query
			}
			if groupTouchesTopic(gs, topic) {
				out = append(out, gs)
			}
		}
	}
	return out, nil
}

func offsetResponsesTouchTopic(offsets kadm.OffsetResponses, topic string) bool {
	for _, offset := range offsets[topic] {
		if offset.Err == nil {
			return true
		}
	}
	return false
}

// groupTouchesTopic reports whether gs (a detail-level GroupState, from
// describeOneGroup) has a committed offset against topic.
func groupTouchesTopic(gs cluster.GroupState, topic string) bool {
	for _, o := range gs.Offsets {
		if o.Topic == topic {
			return true
		}
	}
	return false
}

// describeOneGroup is DescribeGroup/GroupsForTopic's shared core: classic
// DescribeGroups (members/assignor/coordinator/state) + FetchOffsets
// (committed) + ListEndOffsets (the topics FetchOffsets found, for the lag
// denominator), assembled into one GroupState. ctx/adm are threaded through
// rather than closing over a Pool receiver so GroupsForTopic can call this
// once per group against the same already-resolved *kadm.Client without a
// second clientFor round trip.
func describeOneGroup(ctx context.Context, adm groupAdmin, id string) (cluster.GroupState, error) {
	described, err := adm.DescribeGroups(ctx, id)
	if err != nil {
		return cluster.GroupState{}, fmt.Errorf("describe consumer group: %w", err)
	}
	dg, err := described.On(id, nil)
	if err != nil {
		return cluster.GroupState{}, fmt.Errorf("describe consumer group: %w", err)
	}
	if dg.Err != nil {
		return cluster.GroupState{}, fmt.Errorf("describe consumer group: %w", dg.Err)
	}

	gs := cluster.GroupState{
		ID:            dg.Group,
		State:         groupStateToGenerated(dg.State),
		Coordinator:   dg.Coordinator.Host,
		CoordinatorID: dg.Coordinator.NodeID,
		Protocol:      dg.Protocol,
		Members:       membersFromDescribed(dg.Members),
	}

	fetched, err := adm.FetchOffsets(ctx, id)
	if err != nil {
		return cluster.GroupState{}, fmt.Errorf("fetch consumer group offsets: %w", err)
	}
	topics := fetched.Partitions().Topics() // topics this group has committed offsets against
	var ends kadm.ListedOffsets
	if len(topics) > 0 {
		if ends, err = adm.ListEndOffsets(ctx, topics...); err != nil {
			return cluster.GroupState{}, fmt.Errorf("list end offsets: %w", err)
		}
	}
	for _, o := range fetched.Sorted() {
		if o.Err != nil {
			continue // a per-partition fetch error (e.g. topic deleted after the group committed against it) degrades to "skip", not a whole-describe failure
		}
		end := int64(-1)
		if lo, ok := ends.Lookup(o.Topic, o.Partition); ok {
			end = lo.Offset
		}
		gs.Offsets = append(gs.Offsets, cluster.GroupOffset{
			Topic: o.Topic, Partition: o.Partition, Committed: o.At, End: end,
		})
	}
	return gs, nil
}

// membersFromDescribed maps kadm's DescribedGroupMember slice onto domain
// GroupMember. Assignments is only populated when a member's Assigned
// metadata is the classic "consumer" protocol shape (AsConsumer) — a
// "connect" or otherwise unrecognized protocol member degrades to no
// assignments, the same "no data source for this case" convention used
// elsewhere in this package.
func membersFromDescribed(ms []kadm.DescribedGroupMember) []cluster.GroupMember {
	out := make([]cluster.GroupMember, 0, len(ms))
	for _, m := range ms {
		gm := cluster.GroupMember{MemberID: m.MemberID, ClientID: m.ClientID, Host: m.ClientHost}
		if assign, ok := m.Assigned.AsConsumer(); ok {
			for _, t := range assign.Topics {
				gm.Assignments = append(gm.Assignments, cluster.TopicPartitions{Topic: t.Topic, Partitions: t.Partitions})
			}
		}
		out = append(out, gm)
	}
	return out
}

// ResetOffsets resets group id's committed offsets for spec.Topic per
// spec's reset strategy, via kadm's CommitOffsets (the same manual-commit
// entry point ordinary consumers use to checkpoint progress — there is no
// separate "admin reset" RPC in the Kafka protocol, this simply commits the
// caller-chosen offsets on the group's behalf).
func (p *Pool) ResetOffsets(ctx context.Context, def cluster.Definition, id string, spec cluster.ResetSpec) error {
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	offsets, err := resetOffsetsFor(ctx, c.adm, spec)
	if err != nil {
		return err
	}
	resps, err := c.adm.CommitOffsets(ctx, id, offsets)
	if err != nil {
		return fmt.Errorf("reset consumer group offsets: %w", err)
	}
	return resps.Error()
}

// resetOffsetsFor builds the kadm.Offsets CommitOffsets should apply, per
// spec's reset strategy. The unknown-ResetType branch is checked first and
// needs no network call at all — the contract's ConsumerGroupOffsetsReset.
// resetType is a plain string at the wire level (no server-side enum
// enforcement outside test-time contract validation), so a defensive
// rejection here is this method's own responsibility, not something the
// generated request-decode step catches for us.
func resetOffsetsFor(ctx context.Context, adm *kadm.Client, spec cluster.ResetSpec) (kadm.Offsets, error) {
	switch spec.ResetType {
	case "EARLIEST":
		lo, err := adm.ListStartOffsets(ctx, spec.Topic)
		if err != nil {
			return nil, fmt.Errorf("list start offsets: %w", err)
		}
		return filterOffsetsByPartitions(lo.Offsets(), spec.Topic, spec.Partitions), nil
	case "LATEST":
		lo, err := adm.ListEndOffsets(ctx, spec.Topic)
		if err != nil {
			return nil, fmt.Errorf("list end offsets: %w", err)
		}
		return filterOffsetsByPartitions(lo.Offsets(), spec.Topic, spec.Partitions), nil
	case "TIMESTAMP":
		lo, err := adm.ListOffsetsAfterMilli(ctx, spec.Timestamp, spec.Topic)
		if err != nil {
			return nil, fmt.Errorf("list offsets after timestamp: %w", err)
		}
		return filterOffsetsByPartitions(lo.Offsets(), spec.Topic, spec.Partitions), nil
	case "OFFSET":
		offsets := make(kadm.Offsets)
		for partition, at := range spec.PartitionsOffsets {
			offsets.Add(kadm.Offset{Topic: spec.Topic, Partition: partition, At: at})
		}
		return offsets, nil
	default:
		return nil, fmt.Errorf("reset consumer group offsets: unknown reset type %q", spec.ResetType)
	}
}

// filterOffsetsByPartitions narrows all (built from a single-topic
// List*Offsets call, so it only ever holds spec.Topic's own partitions) down
// to just partitions' partition numbers for topic — or returns all
// unchanged when partitions is empty ("every partition of Topic", the
// contract's own empty-partitions convention for resetConsumerGroupOffsets).
func filterOffsetsByPartitions(all kadm.Offsets, topic string, partitions []int32) kadm.Offsets {
	if len(partitions) == 0 {
		return all
	}
	want := make(map[int32]bool, len(partitions))
	for _, p := range partitions {
		want[p] = true
	}
	out := make(kadm.Offsets)
	for _, o := range all[topic] {
		if want[o.Partition] {
			out.Add(o)
		}
	}
	return out
}

// DeleteGroup deletes consumer group id entirely via kadm's singular
// DeleteGroup (a thin per-group wrapper around DeleteGroups whose second
// return value already is that group's own per-group error — see its doc
// comment — so there is no separate map lookup needed here).
func (p *Pool) DeleteGroup(ctx context.Context, def cluster.Definition, id string) error {
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	if _, err := c.adm.DeleteGroup(ctx, id); err != nil {
		return fmt.Errorf("delete consumer group: %w", err)
	}
	return nil
}

// DeleteGroupOffsets deletes group id's committed offsets for topic only,
// via kadm's DeleteOffsets (the OffsetDelete API, Kafka v2.4+). The
// partition set to delete is derived from the group's own current
// FetchOffsets response (filtered to topic) rather than the topic's full
// partition count: DeleteOffsets' underlying OffsetDeleteRequest only
// touches partitions actually named in the request, so naming zero
// partitions for topic (kadm.TopicsSet.Add(topic) with no partitions "still
// creates the topic" entry, per its own doc comment, but with an empty
// partition set) would silently delete nothing — the group's own committed
// partitions for topic are the only correct source for "which partitions
// does this group actually have offsets for on this topic".
func (p *Pool) DeleteGroupOffsets(ctx context.Context, def cluster.Definition, id, topic string) error {
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	fetched, err := c.adm.FetchOffsets(ctx, id)
	if err != nil {
		return fmt.Errorf("fetch offsets for delete: %w", err)
	}
	var ts kadm.TopicsSet
	for _, o := range fetched.Sorted() {
		if o.Topic != topic {
			continue
		}
		ts.Add(o.Topic, o.Partition)
	}
	if len(ts) == 0 {
		return nil // nothing committed for this topic: idempotent no-op, mirrors kadm.DeleteOffsets' own "len(s)==0 -> no-op" behaviour
	}
	resps, err := c.adm.DeleteOffsets(ctx, id, ts)
	if err != nil {
		return fmt.Errorf("delete consumer group offsets: %w", err)
	}
	return resps.Error()
}
