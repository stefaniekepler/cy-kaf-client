package api

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// GroupServicer is the narrow interface api's consumer-groups handlers
// consume; app.GroupService satisfies it (same "resolve name -> Definition,
// delegate" shape as TopicServicer above).
type GroupServicer interface {
	Page(ctx context.Context, name string, q appcluster.GroupPageQuery) (appcluster.GroupPage, error)
	Get(ctx context.Context, name, id string) (cluster.GroupState, error)
	Lag(ctx context.Context, name string, ids []string) ([]cluster.GroupState, error)
	ForTopic(ctx context.Context, name, topic string) ([]cluster.GroupState, error)
	Reset(ctx context.Context, name, id string, spec cluster.ResetSpec) error
	Delete(ctx context.Context, name, id string) error
	DeleteOffsets(ctx context.Context, name, id, topic string) error
}

// consumerGroupDetails is GetConsumerGroup's actual wire shape: the
// contract's ConsumerGroupDetails schema (allOf ConsumerGroup + a sibling
// "partitions" property). oapi-codegen's generated
// generated.ConsumerGroupDetails is a bare type ALIAS to generated.
// ConsumerGroup (models.gen.go) — this discriminator+allOf+sibling-property
// combination silently drops the "partitions" property; confirmed by
// grepping models.gen.go for "ConsumerGroupTopicPartition", which is defined
// but never referenced anywhere else in the generated file. The vendored
// frontend's OWN generated client (frontend/src/generated-sources/models/
// ConsumerGroupDetails.ts) still reads a top-level "partitions" sibling key
// unconditionally, so GetConsumerGroup must still emit it. This local
// wrapper embeds generated.ConsumerGroup (an anonymous embed: encoding/json
// promotes/flattens an embedded struct's exported fields into the same JSON
// object when the embedded field itself carries no json tag) and adds the
// field the generator dropped back. kin-openapi's contract validation
// doesn't care how the Go value was built, only the wire JSON bytes, so this
// satisfies both the real schema and the frontend's actual expectations.
type consumerGroupDetails struct {
	generated.ConsumerGroup
	Partitions *[]generated.ConsumerGroupTopicPartition `json:"partitions,omitempty"`
}

// groupCsvRow is getConsumerGroupsCsv's row shape. That endpoint's response
// schema is a bare `type: string` (CSV text, no structural contract to
// match — unlike the JSON list endpoint), so this is free to pick whatever
// columns are useful; it deliberately flattens Coordinator (a nested Broker
// object in the JSON shape) into a plain CoordinatorId column rather than
// reusing generated.ConsumerGroup directly: rowsToCsv's reflection-based
// csvCell only knows how to render scalar kinds, and a nested struct pointer
// would otherwise render as Go's default %v-formatted struct dump (pointer
// addresses and all) instead of a clean CSV cell.
type groupCsvRow struct {
	GroupId           string  `json:"groupId"`
	State             string  `json:"state,omitempty"`
	Members           *int32  `json:"members,omitempty"`
	Topics            *int32  `json:"topics,omitempty"`
	PartitionAssignor *string `json:"partitionAssignor,omitempty"`
	CoordinatorId     *int32  `json:"coordinatorId,omitempty"`
	ConsumerLag       *int64  `json:"consumerLag,omitempty"`
}

// GetConsumerGroupsPage serves /api/clusters/{clusterName}/consumer-groups/paged:
// the paged/filtered/sorted consumer group list (GroupService.Page ->
// filterSortPageGroups does the actual work; this handler only translates
// params in and the result out — same shape as GetTopics).
func (s *apiServer) GetConsumerGroupsPage(w http.ResponseWriter, r *http.Request, clusterName string, params generated.GetConsumerGroupsPageParams) {
	page, err := s.deps.Groups.Page(r.Context(), clusterName, groupPageQueryFrom(params))
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetConsumerGroupsPage", "failed to list consumer groups", err)
		return
	}
	rows := make([]generated.ConsumerGroup, 0, len(page.Groups))
	for _, gs := range page.Groups {
		rows = append(rows, groupToGenerated(gs))
	}
	writeJSON(w, http.StatusOK, generated.ConsumerGroupsPageResponse{
		ConsumerGroups: &rows,
		PageCount:      ptr(int32(page.PageCount)),
	})
}

// GetConsumerGroupsCsv serves /api/clusters/{clusterName}/consumer-groups/csv:
// every consumer group (no page/perPage — CSV export means "all of it", the
// contract declares no paging params here), rendered as CSV via rowsToCsv.
func (s *apiServer) GetConsumerGroupsCsv(w http.ResponseWriter, r *http.Request, clusterName string, params generated.GetConsumerGroupsCsvParams) {
	page, err := s.deps.Groups.Page(r.Context(), clusterName, groupPageQueryFromCsv(params))
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetConsumerGroupsCsv", "failed to list consumer groups", err)
		return
	}
	body, err := rowsToCsv(groupsToCsvRows(page.Groups))
	if err != nil {
		// Unreachable today (groupsToCsvRows always returns a []groupCsvRow,
		// which rowsToCsv always accepts) — guarded anyway, same defensive
		// shape as GetTopicsCsv/GetBrokersCsv.
		serverError(w, "GetConsumerGroupsCsv", "failed to render consumer groups csv", err)
		return
	}
	w.Header().Set("Content-Type", "text/csv")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body))
}

// baseGroupPageQuery maps the four query params GetConsumerGroupsPage and
// GetConsumerGroupsCsv share onto an appcluster.GroupPageQuery — same split
// as baseTopicListQuery (handlers_topic.go). The contract's "fts" (full-text
// search) flag has no destination field here: see GroupPageQuery's doc
// comment for why Search is always a plain substring match regardless.
func baseGroupPageQuery(search *string, orderBy *generated.ConsumerGroupOrdering, sortOrder *generated.SortOrder, state *[]generated.ConsumerGroupState) appcluster.GroupPageQuery {
	var q appcluster.GroupPageQuery
	if search != nil {
		q.Search = *search
	}
	if orderBy != nil {
		q.OrderBy = string(*orderBy)
	}
	if sortOrder != nil {
		q.SortOrder = string(*sortOrder)
	}
	if state != nil {
		q.States = make([]string, 0, len(*state))
		for _, st := range *state {
			q.States = append(q.States, string(st))
		}
	}
	return q
}

func groupPageQueryFrom(p generated.GetConsumerGroupsPageParams) appcluster.GroupPageQuery {
	q := baseGroupPageQuery(p.Search, p.OrderBy, p.SortOrder, p.State)
	if p.Page != nil {
		q.Page = int(*p.Page)
	}
	if p.PerPage != nil {
		q.PerPage = int(*p.PerPage)
	}
	return q
}

// groupPageQueryFromCsv sets PerPage to a value no real consumer-group list
// will ever reach, so filterSortPageGroups' single default page (Page
// defaults to 1) always contains every row — same convention as
// topicListQueryFromCsv.
func groupPageQueryFromCsv(p generated.GetConsumerGroupsCsvParams) appcluster.GroupPageQuery {
	q := baseGroupPageQuery(p.Search, p.OrderBy, p.SortOrder, p.State)
	q.PerPage = math.MaxInt32
	return q
}

// GetConsumerGroup serves /api/clusters/{clusterName}/consumer-groups/{id}:
// one group's full detail (GroupService.Get -> GroupAdminPort.DescribeGroup,
// live on every call).
func (s *apiServer) GetConsumerGroup(w http.ResponseWriter, r *http.Request, clusterName, id string) {
	gs, err := s.deps.Groups.Get(r.Context(), clusterName, id)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetConsumerGroup", "failed to describe consumer group", err)
		return
	}
	writeJSON(w, http.StatusOK, groupDetailsToGenerated(gs))
}

// DeleteConsumerGroup serves DELETE /api/clusters/{clusterName}/consumer-groups/{id}:
// 204 on success, no body.
func (s *apiServer) DeleteConsumerGroup(w http.ResponseWriter, r *http.Request, clusterName, id string) {
	if err := s.deps.Groups.Delete(r.Context(), clusterName, id); err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "DeleteConsumerGroup", "failed to delete consumer group", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ResetConsumerGroupOffsets serves POST
// /api/clusters/{clusterName}/consumer-groups/{id}/offsets: resets id's
// committed offsets per the request body's ConsumerGroupOffsetsReset shape.
// The contract declares only 204 — the 400 below (like the existing 404/500)
// is "over-contract" (P1b final-review verdict②; see
// appcluster.ErrGroupNotInactive's doc comment): id must be EMPTY/DEAD
// (checked by GroupService.Reset's own pre-check, before any broker call) or
// this reports a precise 400 instead of letting an active group's reset
// attempt fall through to serverError's generic 500.
func (s *apiServer) ResetConsumerGroupOffsets(w http.ResponseWriter, r *http.Request, clusterName, id string) {
	var body generated.ConsumerGroupOffsetsReset
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid request body"))
		return
	}
	if err := s.deps.Groups.Reset(r.Context(), clusterName, id, resetSpecFromGenerated(body)); err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		if errors.Is(err, appcluster.ErrGroupNotInactive) {
			writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "consumer group must be inactive (EMPTY/DEAD) to reset offsets"))
			return
		}
		serverError(w, "ResetConsumerGroupOffsets", "failed to reset consumer group offsets", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// resetSpecFromGenerated maps the contract's ConsumerGroupOffsetsReset
// request body onto a domain ResetSpec: ResetType casts straight across
// (both are the contract's enum spelling), Partitions/ResetToTimestamp
// unwrap directly, and PartitionsOffsets flattens the []PartitionOffset pair
// array into a partition->offset map (a pair with a nil Offset is skipped —
// the contract marks PartitionOffset.offset optional, but a reset request
// with no target offset for a partition has nothing to apply for it).
func resetSpecFromGenerated(b generated.ConsumerGroupOffsetsReset) cluster.ResetSpec {
	spec := cluster.ResetSpec{Topic: b.Topic, ResetType: string(b.ResetType)}
	if b.Partitions != nil {
		spec.Partitions = *b.Partitions
	}
	if b.ResetToTimestamp != nil {
		spec.Timestamp = *b.ResetToTimestamp
	}
	if b.PartitionsOffsets != nil {
		spec.PartitionsOffsets = make(map[int32]int64, len(*b.PartitionsOffsets))
		for _, po := range *b.PartitionsOffsets {
			if po.Offset != nil {
				spec.PartitionsOffsets[po.Partition] = *po.Offset
			}
		}
	}
	return spec
}

// DeleteConsumerGroupOffsets serves DELETE
// /api/clusters/{clusterName}/consumer-groups/{id}/topics/{topicName}: 204
// on success, no body.
func (s *apiServer) DeleteConsumerGroupOffsets(w http.ResponseWriter, r *http.Request, clusterName, id, topicName string) {
	if err := s.deps.Groups.DeleteOffsets(r.Context(), clusterName, id, topicName); err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "DeleteConsumerGroupOffsets", "failed to delete consumer group offsets", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetTopicConsumerGroups serves
// /api/clusters/{clusterName}/topics/{topicName}/consumer-groups: every
// group with committed offsets against topicName (GroupService.ForTopic).
// The contract declares a plain []ConsumerGroup response here (not Details),
// so this uses the Topic-specific base mapper, never groupDetailsToGenerated.
func (s *apiServer) GetTopicConsumerGroups(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	groups, err := s.deps.Groups.ForTopic(r.Context(), clusterName, topicName)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetTopicConsumerGroups", "failed to list topic consumer groups", err)
		return
	}
	out := make([]generated.ConsumerGroup, 0, len(groups))
	for _, gs := range groups {
		out = append(out, topicGroupToGenerated(gs, topicName))
	}
	writeJSON(w, http.StatusOK, out)
}

// GetConsumerGroupsLag serves /api/clusters/{clusterName}/consumer-groups/lag:
// per-group (and, when includePartitions is set, per-partition) lag for the
// requested ids (GroupService.Lag, which itself skips any individual id
// that fails to describe rather than failing the whole batch — see its doc
// comment). lastUpdate is accepted (the contract declares it) but unused:
// this repo always computes lag live on every call, it has no cached/
// throttled lag store for that param to gate against.
func (s *apiServer) GetConsumerGroupsLag(w http.ResponseWriter, r *http.Request, clusterName string, params generated.GetConsumerGroupsLagParams) {
	groups, err := s.deps.Groups.Lag(r.Context(), clusterName, params.Ids)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetConsumerGroupsLag", "failed to compute consumer group lag", err)
		return
	}
	includePartitions := params.IncludePartitions != nil && *params.IncludePartitions
	out := generated.ConsumerGroupsLagResponse{
		UpdateTimestamp: time.Now().UnixMilli(),
		ConsumerGroups:  make(map[string]generated.ConsumerGroupLag, len(groups)),
	}
	for _, gs := range groups {
		out.ConsumerGroups[gs.ID] = groupLagToGenerated(gs, includePartitions)
	}
	writeJSON(w, http.StatusOK, out)
}

// --- domain -> contract mapping helpers ---

// groupHasAnyCommit reports whether gs has at least one offset entry with a
// real commit (Committed >= 0) — the gate for whether ConsumerLag should be
// a real number or omitted entirely (contract: "null if consumer group has
// no offsets committed"). Checking every entry rather than just len(Offsets)
// > 0 stays correct even if a future widening of DescribeGroup starts
// surfacing assigned-but-uncommitted partitions (Committed == -1 entries)
// alongside committed ones — see groups.go's doc comment on that deferred
// widening.
func groupHasAnyCommit(gs cluster.GroupState) bool {
	for _, o := range gs.Offsets {
		if o.Committed >= 0 {
			return true
		}
	}
	return false
}

// groupToGenerated maps one domain GroupState onto the contract's
// ConsumerGroup shape (the list/page/csv/topic-consumer-groups row shape).
// Inherit is set to "ConsumerGroup" (the schema's own name — there is no
// explicit discriminator mapping entry for the base/non-details variant, only
// for "details"; see groupDetailsToGenerated). Members/Topics/
// PartitionAssignor/ConsumerLag are all "known-only" (nil/omitted unless
// there is real data): list-level GroupState (from ListGroups) always has
// them at their Go zero value (empty Members/Offsets, empty Protocol — see
// GroupAdminPort's doc comment), and a real consumer group's counts/lag are
// never legitimately reported as a bare 0 when they're simply unknown/
// unfetched at this call site. Coordinator is always populated: every real
// consumer group has a resolvable coordinator broker, list-level or not
// (only its Host string may be unknown, hence the conditional there).
func groupToGenerated(gs cluster.GroupState) generated.ConsumerGroup {
	cg := generated.ConsumerGroup{GroupId: gs.ID, Inherit: "ConsumerGroup"}
	if gs.State != "" {
		st := generated.ConsumerGroupState(gs.State)
		cg.State = &st
	}
	if len(gs.Members) > 0 {
		cg.Members = ptr(int32(len(gs.Members)))
	}
	if n := gs.TopicCount(); n > 0 {
		cg.Topics = ptr(int32(n))
	}
	if gs.Protocol != "" {
		cg.PartitionAssignor = ptr(gs.Protocol)
	}
	coordinator := generated.Broker{Id: gs.CoordinatorID}
	if gs.Coordinator != "" {
		coordinator.Host = ptr(gs.Coordinator)
	}
	cg.Coordinator = &coordinator
	if groupHasAnyCommit(gs) {
		cg.ConsumerLag = ptr(gs.Lag())
	}
	return cg
}

// topicGroupToGenerated projects a group onto one Topic response row. It
// preserves group identity and state fields while deriving members, topics,
// and lag solely from the requested Topic without mutating gs.
func topicGroupToGenerated(gs cluster.GroupState, topic string) generated.ConsumerGroup {
	cg := groupToGenerated(gs)
	var active int32
	for _, member := range gs.Members {
		if memberAssignedToTopic(member, topic) {
			active++
		}
	}
	cg.Members = ptr(active)
	cg.Topics = ptr(int32(1))
	cg.ConsumerLag = nil
	var lag int64
	known := false
	for _, offset := range gs.Offsets {
		if offset.Topic != topic || offset.Committed < 0 {
			continue
		}
		known = true
		if delta := offset.End - offset.Committed; delta > 0 {
			lag += delta
		}
	}
	if known {
		cg.ConsumerLag = ptr(lag)
	}
	return cg
}

// memberAssignedToTopic reports whether member owns any partition of topic.
// A member with several partitions is counted once by topicGroupToGenerated.
func memberAssignedToTopic(member cluster.GroupMember, topic string) bool {
	for _, assignment := range member.Assignments {
		if assignment.Topic == topic {
			return true
		}
	}
	return false
}

// groupDetailsToGenerated maps one domain GroupState onto GetConsumerGroup's
// actual wire shape (see consumerGroupDetails' doc comment for why that's a
// local wrapper, not the bare generated.ConsumerGroupDetails alias):
// groupToGenerated's same fields, Inherit overridden to "details" (the
// contract's discriminator mapping key for this schema), plus a per-
// partition breakdown built from gs.Offsets.
func groupDetailsToGenerated(gs cluster.GroupState) consumerGroupDetails {
	cg := groupToGenerated(gs)
	cg.Inherit = "details"
	d := consumerGroupDetails{ConsumerGroup: cg}
	if len(gs.Offsets) > 0 {
		parts := make([]generated.ConsumerGroupTopicPartition, 0, len(gs.Offsets))
		for _, o := range gs.Offsets {
			parts = append(parts, groupOffsetToGenerated(gs.Members, o))
		}
		d.Partitions = &parts
	}
	return d
}

// groupOffsetToGenerated maps one domain GroupOffset onto the contract's
// ConsumerGroupTopicPartition shape. CurrentOffset/ConsumerLag are both
// omitted (not a misleading literal 0/-1) when Committed < 0 (no commit for
// this partition) — the same "known-only" rule groupToGenerated's
// ConsumerLag uses at the whole-group level. ConsumerId/Host identify the
// member (if any) currently assigned to this exact (topic, partition), found
// via memberFor; an Empty-state group (no members) or a partition nobody is
// currently assigned leaves both unset.
func groupOffsetToGenerated(members []cluster.GroupMember, o cluster.GroupOffset) generated.ConsumerGroupTopicPartition {
	p := generated.ConsumerGroupTopicPartition{Topic: o.Topic, Partition: o.Partition}
	if o.End >= 0 {
		p.EndOffset = ptr(o.End)
	}
	if o.Committed >= 0 {
		p.CurrentOffset = ptr(o.Committed)
		if o.End >= 0 {
			lag := o.End - o.Committed
			if lag < 0 {
				lag = 0
			}
			p.ConsumerLag = ptr(lag)
		}
	}
	if m := memberFor(members, o.Topic, o.Partition); m != nil {
		p.ConsumerId = ptr(m.MemberID)
		if m.Host != "" {
			p.Host = ptr(m.Host)
		}
	}
	return p
}

// memberFor returns the member (if any) among members currently assigned to
// consume (topic, partition), or nil if no member's Assignments cover it
// (e.g. the group is Empty, or that partition simply isn't assigned to
// anyone right now).
func memberFor(members []cluster.GroupMember, topic string, partition int32) *cluster.GroupMember {
	for i := range members {
		for _, tp := range members[i].Assignments {
			if tp.Topic != topic {
				continue
			}
			for _, p := range tp.Partitions {
				if p == partition {
					return &members[i]
				}
			}
		}
	}
	return nil
}

// groupLagToGenerated maps one domain GroupState onto the contract's
// ConsumerGroupLag shape for the getConsumerGroupsLag endpoint: Lag is the
// whole-group total (GroupState.Lag()), Topics sums each topic's own lag
// (only over partitions with a real commit — same "Committed<0 contributes
// 0" rule as Lag() itself), and TopicPartitions (a per-topic, per-partition
// breakdown) is built only when includePartitions is requested — the
// contract makes that entire nested structure optional, and it's extra work
// callers that only want the summary shouldn't have to receive.
func groupLagToGenerated(gs cluster.GroupState, includePartitions bool) generated.ConsumerGroupLag {
	topics := map[string]int64{}
	var byTopic map[string]map[string]int64
	if includePartitions {
		byTopic = map[string]map[string]int64{}
	}
	for _, o := range gs.Offsets {
		if o.Committed < 0 || o.End < 0 {
			continue // no commit, or end offset unknown: nothing to attribute to this partition's lag
		}
		lag := o.End - o.Committed
		if lag < 0 {
			lag = 0
		}
		topics[o.Topic] += lag
		if includePartitions {
			if byTopic[o.Topic] == nil {
				byTopic[o.Topic] = map[string]int64{}
			}
			byTopic[o.Topic][strconv.Itoa(int(o.Partition))] = lag
		}
	}
	out := generated.ConsumerGroupLag{Lag: gs.Lag(), Topics: topics}
	if includePartitions && len(byTopic) > 0 {
		tp := make(map[string]generated.ConsumerGroupTopicLag, len(byTopic))
		for topic, parts := range byTopic {
			tp[topic] = generated.ConsumerGroupTopicLag{Partitions: &parts}
		}
		out.TopicPartitions = &tp
	}
	return out
}

// groupsToCsvRows maps domain GroupState entries onto getConsumerGroupsCsv's
// flat row shape (groupCsvRow) — same "known-only" field population as
// groupToGenerated, just without the nested Broker/Coordinator object.
func groupsToCsvRows(gss []cluster.GroupState) []groupCsvRow {
	out := make([]groupCsvRow, 0, len(gss))
	for _, gs := range gss {
		row := groupCsvRow{GroupId: gs.ID, State: gs.State, CoordinatorId: ptr(gs.CoordinatorID)}
		if len(gs.Members) > 0 {
			row.Members = ptr(int32(len(gs.Members)))
		}
		if n := gs.TopicCount(); n > 0 {
			row.Topics = ptr(int32(n))
		}
		if gs.Protocol != "" {
			row.PartitionAssignor = ptr(gs.Protocol)
		}
		if groupHasAnyCommit(gs) {
			row.ConsumerLag = ptr(gs.Lag())
		}
		out = append(out, row)
	}
	return out
}
