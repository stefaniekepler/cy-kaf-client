package cluster

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// GroupPageQuery is getConsumerGroupsPage'/getConsumerGroupsCsv's query
// params, already contract-enum-shaped (OrderBy/SortOrder/States hold the
// exact ConsumerGroupOrdering/SortOrder/ConsumerGroupState strings the
// generated param types carry — the api handler casts them straight across,
// no translation table needed here, same convention as TopicListQuery).
// Page/PerPage default to 1/25 when zero, matching TopicListQuery's own
// convention (the contract declares no default for either param).
//
// Fts (the contract's "full text search" flag) has no equivalent field here:
// this repo doesn't implement a separate fuzzy/ngram search mode for
// consumer groups (parity matrix's exemption #7 already covers the general
// "no Lucene-equivalent full-text engine" gap) — Search is always a plain
// case-insensitive substring match regardless of what the caller passes for
// Fts, so the api handler simply never reads that param into this struct.
type GroupPageQuery struct {
	Page, PerPage int
	Search        string
	OrderBy       string
	SortOrder     string
	States        []string
}

// GroupPage is one filtered/sorted/paginated page of a cluster's consumer
// group list.
type GroupPage struct {
	Groups    []cluster.GroupState
	PageCount int
}

const (
	defaultGroupPage    = 1
	defaultGroupPerPage = 25
)

// GroupService performs consumer-group-scoped operations (P1b Task 6's
// full 8-endpoint surface). Unlike TopicService, this has no StateCache
// dependency: nothing in these 8 endpoints reads a periodically-refreshed
// cache. Page is itself a live GroupAdminPort.ListGroups call (deliberately
// cheap by design — see GroupAdminPort's doc comment); every other method is
// inherently a live, per-request operation (single-group describe, reset,
// delete). Adding an unused *StateCache field here would just be dead
// weight (see this task's report for the fuller rationale on why this
// deliberately doesn't mirror TopicService's constructor shape 1:1).
type GroupService struct {
	res  *Resolver
	port cluster.GroupAdminPort
}

func NewGroupService(res *Resolver, port cluster.GroupAdminPort) *GroupService {
	return &GroupService{res: res, port: port}
}

// Page filters/sorts/paginates a live GroupAdminPort.ListGroups call through
// the pure filterSortPageGroups.
func (s *GroupService) Page(ctx context.Context, name string, q GroupPageQuery) (GroupPage, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return GroupPage{}, err
	}
	all, err := s.port.ListGroups(ctx, def)
	if err != nil {
		return GroupPage{}, err
	}
	return filterSortPageGroups(all, q), nil
}

// Get resolves name then describes one group's full detail
// (GroupAdminPort.DescribeGroup, live on every call).
func (s *GroupService) Get(ctx context.Context, name, id string) (cluster.GroupState, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return cluster.GroupState{}, err
	}
	return s.port.DescribeGroup(ctx, def, id)
}

// Lag resolves name once, then describes each of ids individually
// (GroupAdminPort.DescribeGroup, the same detail-level call Get uses) —
// skipping (not failing the whole request over) any individual id that
// fails to describe, e.g. a group deleted between the caller listing ids
// and this call landing, or simply a caller-supplied id that never existed.
// This mirrors getConsumerGroupsLag's real-world usage (a dashboard batch-
// fetching lag for a set of currently-visible groups): one stale/bad id
// shouldn't blank out lag data for every other group in the same request. A
// completely unknown cluster still fails the whole call (checked once, up
// front, via Lookup) — that's the one dimension where this repo's normal
// "unknown cluster -> distinct error" rule still applies unchanged.
func (s *GroupService) Lag(ctx context.Context, name string, ids []string) ([]cluster.GroupState, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return nil, err
	}
	out := make([]cluster.GroupState, 0, len(ids))
	for _, id := range ids {
		gs, err := s.port.DescribeGroup(ctx, def, id)
		if err != nil {
			continue
		}
		out = append(out, gs)
	}
	return out, nil
}

// ForTopic resolves name then reports every group with committed offsets
// against topic (GroupAdminPort.GroupsForTopic, live on every call).
func (s *GroupService) ForTopic(ctx context.Context, name, topic string) ([]cluster.GroupState, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return nil, err
	}
	return s.port.GroupsForTopic(ctx, def, topic)
}

// ErrGroupNotInactive is Reset's sentinel for a target group whose current
// state (as of a fresh DescribeGroup call, done as Reset's own pre-check) is
// neither EMPTY nor DEAD -- i.e. it still has active members or is mid-
// rebalance. P1b final-review verdict② (2026-07 final full-branch review
// against upstream kafka-ui v1.5.0): upstream's own
// OffsetsResetService.checkGroupCondition runs this exact check *before*
// ever calling the broker, rejecting with a precise, actionable message
// ("...but group is in STABLE state"). This repo's Reset previously had no
// such pre-check and simply passed any broker-side rejection through
// (T7's integration test confirms the real broker rejects an admin-side
// reset against an active group with UNKNOWN_MEMBER_ID) -- which two-bucket
// errorResponse law then turned into an uninformative fixed-text 500,
// exactly the "the app is broken" false signal a user stops-forgot-to-stop-
// their-consumer sees in the single most common failure path for this
// endpoint. handlers_group.go maps this sentinel to a 400, the same "known
// error sentinel -> specific status code" shape as
// ErrTopicDeletionDisabled/ErrInvalidReplicationFactor (topic.go).
var ErrGroupNotInactive = errors.New("consumer group must be inactive (EMPTY/DEAD) to reset offsets")

// Reset resolves name, then pre-checks id's current state (a fresh
// GroupAdminPort.DescribeGroup call -- the same describe-with-state method
// Get uses) before resetting its offsets (GroupAdminPort.ResetOffsets): only
// EMPTY/DEAD are inactive enough to proceed (see ErrGroupNotInactive's doc
// comment for why). Any other state short-circuits with
// ErrGroupNotInactive, embedding the actual state in the error message
// (upstream-style) -- ResetOffsets is never called in that case, so a
// broker that would reject the reset anyway is never even asked. The
// describe call itself failing (a genuine backend error, not "the group is
// merely active") is propagated as-is, same as every other pass-through
// method in this file -- there is no special-casing of "group id doesn't
// exist" here, describe's own existing behavior for that case (whatever it
// is) is preserved unchanged.
func (s *GroupService) Reset(ctx context.Context, name, id string, spec cluster.ResetSpec) error {
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	gs, err := s.port.DescribeGroup(ctx, def, id)
	if err != nil {
		return err
	}
	if gs.State != "EMPTY" && gs.State != "DEAD" {
		return fmt.Errorf("%w: group is in %s state", ErrGroupNotInactive, gs.State)
	}
	return s.port.ResetOffsets(ctx, def, id, spec)
}

// Delete resolves name then deletes group id entirely
// (GroupAdminPort.DeleteGroup).
func (s *GroupService) Delete(ctx context.Context, name, id string) error {
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	return s.port.DeleteGroup(ctx, def, id)
}

// DeleteOffsets resolves name then deletes group id's committed offsets for
// topic only (GroupAdminPort.DeleteGroupOffsets).
func (s *GroupService) DeleteOffsets(ctx context.Context, name, id, topic string) error {
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	return s.port.DeleteGroupOffsets(ctx, def, id, topic)
}

// filterSortPageGroups is Page's pure core: filter (states + case-
// insensitive ID search-contains), sort (by q.OrderBy/q.SortOrder), then
// paginate (q.Page/q.PerPage, both defaulted and clamped) — same shape as
// topic.go's filterSortPage, split out so it's unit-testable without a
// Resolver/port at all.
func filterSortPageGroups(all []cluster.GroupState, q GroupPageQuery) GroupPage {
	perPage := q.PerPage
	if perPage <= 0 {
		perPage = defaultGroupPerPage
	}
	page := q.Page
	if page <= 0 {
		page = defaultGroupPage
	}

	wantStates := make(map[string]bool, len(q.States))
	for _, st := range q.States {
		wantStates[st] = true
	}

	filtered := make([]cluster.GroupState, 0, len(all))
	search := strings.ToLower(q.Search)
	for _, g := range all {
		if len(wantStates) > 0 && !wantStates[g.State] {
			continue
		}
		if search != "" && !strings.Contains(strings.ToLower(g.ID), search) {
			continue
		}
		filtered = append(filtered, g)
	}

	// MEMBERS/TOPIC_NUM/MESSAGES_BEHIND primary values are equal here: the
	// ListGroups data source leaves those fields zero-value (no enrich), unlike
	// upstream's describe-then-sort. The deterministic ID tie-break below still
	// fixes page membership; enriching those primary values remains the known
	// deferred P1c gap. See GroupAdminPort's doc comment and ADR-0005 §8.
	sortGroups(filtered, q.OrderBy, q.SortOrder)

	total := len(filtered)
	// PageCount semantics for an empty (post-filter) result: 0, not 1 — same
	// "ceil(0/perPage) taken literally" convention as filterSortPage
	// (topic.go), matching the vendored frontend's own pageCount fallback.
	pageCount := 0
	if total > 0 {
		pageCount = (total + perPage - 1) / perPage
		if page > pageCount {
			page = pageCount
		}
	}

	start := (page - 1) * perPage
	if start < 0 || start > total {
		start = total
	}
	end := start + perPage
	if end > total {
		end = total
	}
	return GroupPage{Groups: filtered[start:end], PageCount: pageCount}
}

// sortGroups sorts gs in place by orderBy (one of the contract's
// ConsumerGroupOrdering values: NAME/MEMBERS/STATE/MESSAGES_BEHIND/
// TOPIC_NUM), ascending unless sortOrder is exactly "DESC". An empty/
// unrecognized orderBy falls back to NAME ascending, same convention as
// sortTopics (topic.go).
//
// MEMBERS/TOPIC_NUM/MESSAGES_BEHIND are computed from Members/Offsets/Lag()
// — fields GroupAdminPort.ListGroups deliberately leaves empty at list
// level (see its doc comment), so these three primary orderings degrade to
// the ID tie-break when sorting a plain ListGroups result; they become
// meaningful once Page's input carries detail-level data. Equal primary
// values always use ID ascending, including under DESC, so page membership
// is independent of source order.
func sortGroups(gs []cluster.GroupState, orderBy, sortOrder string) {
	compare := func(i, j int) int {
		switch orderBy {
		case "MEMBERS":
			return cmp.Compare(len(gs[i].Members), len(gs[j].Members))
		case "STATE":
			return cmp.Compare(gs[i].State, gs[j].State)
		case "MESSAGES_BEHIND":
			return cmp.Compare(gs[i].Lag(), gs[j].Lag())
		case "TOPIC_NUM":
			return cmp.Compare(gs[i].TopicCount(), gs[j].TopicCount())
		default: // "NAME" and anything unrecognized
			return cmp.Compare(gs[i].ID, gs[j].ID)
		}
	}
	sort.SliceStable(gs, func(i, j int) bool {
		comparison := compare(i, j)
		if comparison == 0 {
			return gs[i].ID < gs[j].ID
		}
		if sortOrder == "DESC" {
			return comparison > 0
		}
		return comparison < 0
	})
}
