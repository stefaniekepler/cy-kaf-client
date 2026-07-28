package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// TopicListQuery is getTopics'/getTopicsCsv's query params, already
// contract-enum-shaped (OrderBy/SortOrder hold the exact
// TopicColumnsToSort/SortOrder strings the generated param types carry —
// the api handler casts them straight across, no translation table needed
// here). Page/PerPage default to 1/25 when zero (the contract declares no
// default for either — confirmed by grepping contract/openapi.yaml's getTopics
// parameters — so the 1/25 convention is this app's own choice, matching
// upstream kafka-ui's default page size).
type TopicListQuery struct {
	Page, PerPage int
	Search        string
	ShowInternal  bool
	OrderBy       string
	SortOrder     string
}

// TopicPage is one filtered/sorted/paginated page of a cluster's topic list.
type TopicPage struct {
	Topics    []cluster.TopicState
	PageCount int
}

const (
	defaultTopicPage    = 1
	defaultTopicPerPage = 25
)

// ErrTopicDeletionDisabled is returned by Delete (and by Recreate, which
// deletes internally) when the target cluster's TOPIC_DELETION feature is
// off (RuntimeState.TopicDeletionEnabled == false, scraped from the live
// cluster's delete.topic.enable broker config). Upstream kafka-ui hides the
// delete entry point in the UI when this feature is off; the API side still
// needs its own defensive check since a client can call the endpoint
// directly regardless of what the UI shows. handlers_topic.go maps this to
// a fixed-text 403, the same "known error sentinel -> specific status code"
// shape as ErrUnknownCluster -> 404.
var ErrTopicDeletionDisabled = errors.New("topic deletion is disabled for this cluster")

// defaultTopicPollInterval is how often Recreate polls for the deleted
// topic to disappear before re-creating it, in production. Tests inject a
// much shorter interval via WithPollInterval so the poll loop doesn't slow
// the suite down.
const defaultTopicPollInterval = 200 * time.Millisecond

// maxTopicPollAttempts bounds Recreate's poll-until-gone loop: if the topic
// still appears to exist after this many probes, Recreate proceeds to
// CreateTopic anyway rather than looping forever — a stuck poll would
// otherwise hang the request indefinitely on a slow/degraded cluster. A
// premature proceed just means CreateTopic itself may fail with "topic
// already exists", which surfaces as an ordinary backend error.
const maxTopicPollAttempts = 50

// TopicService performs topic-scoped operations (P1b Task 4's read surface
// plus Task 5's write surface): List and Details read the periodically-
// refreshed StateCache (like GetBrokers/GetClusterStats do today), while
// everything else goes straight through cluster.TopicAdminPort to the live
// cluster on every call — the same BrokerService shape (Resolver + a narrow
// domain port), not more methods hung off StateCache.
type TopicService struct {
	res          *Resolver
	states       *StateCache
	port         cluster.TopicAdminPort
	pollInterval time.Duration
}

// TopicServiceOption customizes NewTopicService's construction — currently
// only WithPollInterval, added for Recreate's poll-until-gone step to be
// test-fast without needing a real StateScraper/clock injection everywhere.
type TopicServiceOption func(*TopicService)

// WithPollInterval overrides Recreate's poll-until-gone interval (default
// defaultTopicPollInterval). Tests use a 1ms interval so the poll loop
// completes in well under a second.
func WithPollInterval(d time.Duration) TopicServiceOption {
	return func(s *TopicService) { s.pollInterval = d }
}

func NewTopicService(res *Resolver, states *StateCache, port cluster.TopicAdminPort, opts ...TopicServiceOption) *TopicService {
	s := &TopicService{res: res, states: states, port: port, pollInterval: defaultTopicPollInterval}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// List filters/sorts/paginates the cached topic list for name through the
// pure filterSortPage. Like GetBrokers/GetClusterStats, this collapses
// "unknown cluster name" and "known cluster, first scrape not done yet"
// into the same signal (states.Get's bool return) — both report
// ErrUnknownCluster, matching every other cache-backed read in this package
// (there's no live probe fallback here, same as those).
func (s *TopicService) List(ctx context.Context, name string, q TopicListQuery) (TopicPage, error) {
	st, ok := s.states.Get(ctx, name)
	if !ok {
		return TopicPage{}, fmt.Errorf("%w: %q", ErrUnknownCluster, name)
	}
	return filterSortPage(st.Topics, q), nil
}

// Details resolves name (Lookup, so an unknown cluster fails fast without a
// wasted network round trip), reads the cached TopicState the same way List
// does, and layers on a live TopicConfigs call (Details needs it for
// contract fields List doesn't, e.g. CleanUpPolicy, which only configs can
// answer). A nonexistent topic isn't special-cased at this layer: the live
// TopicConfigs call itself fails against a real cluster (kadm surfaces
// kerr.UnknownTopicOrPartition), which propagates as a plain error — the api
// layer's uniform "known cluster, backend call failed => 500" rule then
// applies, exactly like Configs/Acls/ActiveProducers below (CLAUDE.md's
// errorResponse 铁律: this repo deliberately has only two buckets, unknown
// cluster => 404 and everything else => 500, no third "unknown resource
// within a known cluster" category). If TopicConfigs succeeds but the topic
// isn't (yet) in the cached snapshot — a race with the periodic scrape,
// since the topic demonstrably exists — this degrades to a
// name-only TopicState paired with the real configs, rather than an error.
func (s *TopicService) Details(ctx context.Context, name, topic string) (cluster.TopicState, []cluster.ConfigEntry, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return cluster.TopicState{}, nil, err
	}
	st, ok := s.states.Get(ctx, name)
	if !ok {
		return cluster.TopicState{}, nil, fmt.Errorf("%w: %q", ErrUnknownCluster, name)
	}
	cfgs, err := s.port.TopicConfigs(ctx, def, topic)
	if err != nil {
		return cluster.TopicState{}, nil, err
	}
	for _, ts := range st.Topics {
		if ts.Name == topic {
			return ts, cfgs, nil
		}
	}
	return cluster.TopicState{Name: topic}, cfgs, nil
}

// Configs, Acls and ActiveProducers are pure Lookup+port passthroughs —
// same shape as BrokerService's methods (no caching: every call is a fresh
// describe against the live client pool).

func (s *TopicService) Configs(ctx context.Context, name, topic string) ([]cluster.ConfigEntry, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return nil, err
	}
	return s.port.TopicConfigs(ctx, def, topic)
}

func (s *TopicService) Acls(ctx context.Context, name, topic string) ([]cluster.AclBinding, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return nil, err
	}
	return s.port.TopicAcls(ctx, def, topic)
}

func (s *TopicService) ActiveProducers(ctx context.Context, name, topic string) ([]cluster.ProducerState, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return nil, err
	}
	return s.port.ActiveProducers(ctx, def, topic)
}

// Connectors is P1b Task 8a's empty getTopicConnectors stub (see
// docs/superpowers/plans/2026-07-04-p1b-topics-groups.md's 2026-07-04
// revision): resolves name (Lookup) so an unknown cluster still fails fast
// with ErrUnknownCluster, matching every other topic method's
// unknown-cluster signal, but there's nothing further to do — unlike
// Configs/Acls/ActiveProducers above, TopicAdminPort has no Kafka
// Connect-backed data source at all yet (a real query is P2's scope). topic
// is accepted for signature symmetry with those methods (and because a
// real P2 implementation will need it) but goes unused today.
func (s *TopicService) Connectors(_ context.Context, name, _ string) error {
	_, err := s.res.Lookup(name)
	return err
}

// --- P1b Task 5: write surface ---

// refreshAfterWrite forces a synchronous StateCache refresh for name after a
// successful write operation (op is just a label for the warning below),
// so the write is immediately visible to List/Details instead of waiting up
// to StateCache's periodic refreshEvery (30s in production) — the
// read-your-writes fix for the gap P1b Task 8b's e2e re-run exposed (POST
// /topics et al. wrote straight to the live cluster while GET /topics/GET
// /topics/{t} only ever read the periodically-refreshed cache). Synchronous,
// not a goroutine: the point is that the cache is already updated by the
// time this write method returns, so the very next read (e.g. the list call
// an e2e test or a real UI makes right after a create) sees it — a
// fire-and-forget async refresh would just move the race, not close it.
//
// Refresh failing doesn't change the write's own (already-successful)
// result: Refresh only returns an error when the cluster name is unknown to
// the Resolver, and every caller below has already done its own successful
// Resolver.Lookup(name) earlier in the same method — so this branch is near
// unreachable in practice. It's still handled defensively (log and swallow,
// same style as state.go refreshOne's own slog.Warn) rather than ignored
// outright. A genuine scrape failure (as opposed to an unknown-name lookup
// failure) is a different case entirely: refreshOne itself still writes that
// failure into the cache and logs its own Warn — it never surfaces through
// this return value at all.
//
// op label convention (for the Warn line only): pass the actual port method
// the caller invoked — CreateTopic/DeleteTopic/CreatePartitions/
// AlterPartitionAssignments. Recreate and Clone both bottom out in
// s.port.CreateTopic as well, but pass synthesized "RecreateTopic"/
// "CloneTopic" labels instead, so a warning names the caller's operation
// rather than colliding with Create's own "CreateTopic". A future 7th write
// method should follow the same rule: the real port method name, or a
// synthesized TopicService-method label when it shares a port call with an
// existing writer.
func (s *TopicService) refreshAfterWrite(ctx context.Context, name, op string) {
	if _, err := s.states.Refresh(ctx, name); err != nil {
		slog.Warn("post-write cache refresh failed", "op", op, "cluster", name, "err", err)
	}
}

// Create creates a topic per spec (createTopic's contract-facing shape:
// name/partitions/replicationFactor/initial configs). The returned
// TopicState is a best-effort synthesis from spec's own values
// (synthesizeTopicState) rather than a fresh live describe: right after
// creation the periodic StateCache scrape almost certainly hasn't picked
// the new topic up yet, and there's no dedicated live "describe one topic's
// partitions/replicas" port method in this task's scope — TopicConfigs only
// reports config entries, not partition/replica placement.
func (s *TopicService) Create(ctx context.Context, name string, spec cluster.TopicSpec) (cluster.TopicState, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return cluster.TopicState{}, err
	}
	if err := s.port.CreateTopic(ctx, def, spec); err != nil {
		return cluster.TopicState{}, err
	}
	s.refreshAfterWrite(ctx, name, "CreateTopic")
	return synthesizeTopicState(spec), nil
}

// synthesizeTopicState builds a best-effort TopicState directly from a
// TopicSpec's own known values, for Create/Recreate/Clone's response shape
// when re-describing the just-written topic live isn't available (see
// Create's doc comment). Partitions is a placeholder-length slice (no
// per-partition replica data — the response type these feed,
// generated.Topic via handlers_topic.go's topicToGenerated, only ever needs
// counts/sums, never per-partition detail) purely so its length reports
// spec.Partitions accurately; a -1 "cluster default" leaves it empty (0
// reported) — an honest "unknown" rather than a wrong guess.
//
// INVARIANT (relied on by handlers_topic.go's topicMessagesCount, P1b Task
// 8c): the placeholder partitions are left at their zero-value Start/EndOffset
// (0/0), so topicToGenerated computes CreateTopic's messagesCount as exactly 0
// — the correct "brand-new empty topic" count. Do not populate these offsets
// with a guess in future changes; a real just-created topic has no messages.
func synthesizeTopicState(spec cluster.TopicSpec) cluster.TopicState {
	ts := cluster.TopicState{Name: spec.Name}
	if spec.ReplicationFactor > 0 {
		ts.ReplicationFactor = int(spec.ReplicationFactor)
	}
	if spec.Partitions > 0 {
		ts.Partitions = make([]cluster.PartitionState, spec.Partitions)
	}
	return ts
}

// Delete deletes topic, first checking the cluster's TOPIC_DELETION feature
// flag (RuntimeState.TopicDeletionEnabled) — ErrTopicDeletionDisabled if
// it's off. A cluster that's configured but hasn't completed its first
// scrape yet (states.Get reports ok=false) defaults to "deletion allowed",
// mirroring FetchState's own "can't prove it's disabled -> default enabled"
// convention — Lookup, not states.Get, is this method's actual
// unknown-cluster-name gate.
func (s *TopicService) Delete(ctx context.Context, name, topic string) error {
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	if st, ok := s.states.Get(ctx, name); ok && !st.TopicDeletionEnabled {
		return ErrTopicDeletionDisabled
	}
	if err := s.port.DeleteTopic(ctx, def, topic); err != nil {
		return err
	}
	s.refreshAfterWrite(ctx, name, "DeleteTopic")
	return nil
}

// topicStateFromCache resolves name's cached RuntimeState and returns
// topic's cached entry, or a name-only zero-value TopicState if the topic
// isn't in it yet (the same "known cluster, topic not yet in the periodic
// scrape" degrade Details establishes) — shared by write methods that don't
// change the topic's shape and so can serve their response from the cache
// instead of paying for another live call.
func (s *TopicService) topicStateFromCache(ctx context.Context, name, topic string) (cluster.TopicState, error) {
	st, ok := s.states.Get(ctx, name)
	if !ok {
		return cluster.TopicState{}, fmt.Errorf("%w: %q", ErrUnknownCluster, name)
	}
	for _, ts := range st.Topics {
		if ts.Name == topic {
			return ts, nil
		}
	}
	return cluster.TopicState{Name: topic}, nil
}

// UpdateConfigs reconciles topic's dynamic configs to exactly the desired
// full set (contract's TopicUpdate.configs — see configOps' doc comment for
// why this is full-set-replace-by-diff, not a partial patch) via configOps,
// applying each resulting op through AlterTopicConfig — which only ever
// issues kadm's incremental AlterTopicConfigs, never a full-state replace
// (ADR-0003 §6.1's soul constraint). Returns topic's current cached shape:
// Update never touches partition/replica placement, only configs, so the
// existing cache entry (not a fresh describe) is the right response source.
func (s *TopicService) UpdateConfigs(ctx context.Context, name, topic string, desired map[string]string) (cluster.TopicState, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return cluster.TopicState{}, err
	}
	current, err := s.port.TopicConfigs(ctx, def, topic)
	if err != nil {
		return cluster.TopicState{}, err
	}
	for _, op := range configOps(current, desired) {
		value := op.Value
		if op.Unset {
			value = "" // AlterTopicConfig's own delete sentinel — see its doc comment
		}
		// KNOWN COLLISION: if op is a Set (not Unset) whose Value is itself ""
		// (a caller explicitly desiring an empty-string config value),
		// AlterTopicConfig can't tell that apart from the Unset case above —
		// value == "" always means "delete this key" to it. An intended
		// set-to-empty would silently become an unset instead. Left as a
		// documented limitation per AlterTopicConfig's own doc comment
		// (infra/kafka/topics.go): its value == "" sentinel is locked, and no
		// real dynamic topic config has a legitimate empty-string value today.
		if err := s.port.AlterTopicConfig(ctx, def, topic, op.Name, value); err != nil {
			return cluster.TopicState{}, err
		}
	}
	return s.topicStateFromCache(ctx, name, topic)
}

// Recreate wipes topic's data while preserving its shape: read the current
// TopicState + dynamic configs, delete (through Delete, so the same
// TOPIC_DELETION gate/error applies), poll until the topic has actually
// disappeared (awaitTopicGone), then re-create it with the same partitions/
// replication factor/dynamic configs. Returns the pre-delete TopicState: by
// design the re-created topic has the identical shape, and re-describing
// the brand new topic live has the same cache-not-scraped-yet limitation
// Create's doc comment explains.
func (s *TopicService) Recreate(ctx context.Context, name, topic string) (cluster.TopicState, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return cluster.TopicState{}, err
	}
	ts, cfgs, err := s.Details(ctx, name, topic)
	if err != nil {
		return cluster.TopicState{}, err
	}
	spec := cluster.TopicSpec{
		Name:              topic,
		Partitions:        int32(len(ts.Partitions)),
		ReplicationFactor: int16(ts.ReplicationFactor),
		Configs:           dynamicConfigsMap(cfgs),
	}
	if err := s.Delete(ctx, name, topic); err != nil {
		return cluster.TopicState{}, err
	}
	s.awaitTopicGone(ctx, def, topic)
	if err := s.port.CreateTopic(ctx, def, spec); err != nil {
		return cluster.TopicState{}, err
	}
	s.refreshAfterWrite(ctx, name, "RecreateTopic")
	// The re-created topic is empty: return the pre-delete shape but with
	// zeroed partition offsets, so the response's messagesCount reads 0 (see
	// emptyOffsetPartitions — a fresh slice, never an in-place edit of the
	// cache-aliased ts.Partitions).
	ts.Partitions = emptyOffsetPartitions(ts.Partitions)
	return ts, nil
}

// awaitTopicGone polls TopicConfigs for topic every s.pollInterval, up to
// maxTopicPollAttempts times, returning as soon as it errors (treated as
// "the topic no longer exists" — the only signal available here without
// widening TopicAdminPort just for this one internal step: app can't
// type-assert a specific kadm/kerr error without violating its
// domain-plus-stdlib-only dependency rule). A stuck poll — the topic never
// appears to disappear within the attempt budget — just proceeds to
// CreateTopic anyway rather than hanging forever; see maxTopicPollAttempts'
// doc comment.
func (s *TopicService) awaitTopicGone(ctx context.Context, def cluster.Definition, topic string) {
	for i := 0; i < maxTopicPollAttempts; i++ {
		if _, err := s.port.TopicConfigs(ctx, def, topic); err != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.pollInterval):
		}
	}
}

// dynamicConfigsMap extracts the DYNAMIC_TOPIC_CONFIG-sourced entries from
// cfgs into a plain name->value map — Recreate/Clone use this so the
// re-created/cloned topic keeps only what was explicitly customized
// (static/default-sourced entries naturally reapply on the fresh topic;
// there's nothing to carry over for those).
func dynamicConfigsMap(cfgs []cluster.ConfigEntry) map[string]string {
	out := map[string]string{}
	for _, c := range cfgs {
		if c.Source == "DYNAMIC_TOPIC_CONFIG" {
			out[c.Name] = c.Value
		}
	}
	return out
}

// emptyOffsetPartitions returns a fresh slice mirroring ps's per-partition
// placement (ID/Leader/Replicas/ISR preserved, so handlers_topic.go's
// topicToGenerated keeps computing accurate replica/ISR tallies) but with
// every partition's Start/EndOffset reset to 0 — the offset state a
// just-recreated / just-cloned (hence empty) topic actually has. Recreate and
// Clone build their response Partitions through this so the response's
// messagesCount (Σ EndOffset-StartOffset, P1b Task 8c) reads 0, matching
// CreateTopic, instead of echoing the pre-delete / source topic's stale
// offsets (a "confidently wrong" value, worse than the pre-8c omission).
//
// A FRESH slice is mandatory, never an in-place edit: the TopicState these
// come from is TopicService.Details' return, whose Partitions slice aliases
// the StateCache's own backing array (Details returns a cached entry by value,
// but a slice field still shares its store) — zeroing offsets in place would
// silently corrupt every later cache read of that topic. Copying into a new
// array leaves everything the cache still points at untouched (the shared
// Replicas/ISR sub-slices are only ever read downstream, never mutated).
func emptyOffsetPartitions(ps []cluster.PartitionState) []cluster.PartitionState {
	if ps == nil {
		return nil
	}
	out := make([]cluster.PartitionState, len(ps))
	for i, p := range ps {
		p.StartOffset, p.EndOffset = 0, 0
		out[i] = p
	}
	return out
}

// Clone creates newTopic with the same shape (partitions/replication
// factor) and dynamic configs as an existing sourceTopic (read live via
// Details), then CreateTopic under the new name. The returned TopicState
// mirrors the source's tallies (same partition count/replication factor,
// hence the same Replicas/InSyncReplicas sums topicToGenerated computes)
// under the new name — an approximation of the freshly-created topic's real
// placement, same rationale as Create's doc comment.
func (s *TopicService) Clone(ctx context.Context, name, sourceTopic, newTopic string) (cluster.TopicState, error) {
	def, err := s.res.Lookup(name)
	if err != nil {
		return cluster.TopicState{}, err
	}
	ts, cfgs, err := s.Details(ctx, name, sourceTopic)
	if err != nil {
		return cluster.TopicState{}, err
	}
	spec := cluster.TopicSpec{
		Name:              newTopic,
		Partitions:        int32(len(ts.Partitions)),
		ReplicationFactor: int16(ts.ReplicationFactor),
		Configs:           dynamicConfigsMap(cfgs),
	}
	if err := s.port.CreateTopic(ctx, def, spec); err != nil {
		return cluster.TopicState{}, err
	}
	s.refreshAfterWrite(ctx, name, "CloneTopic")
	// The clone is a brand-new empty topic: mirror the source's shape under the
	// new name but with zeroed partition offsets, so the response's
	// messagesCount reads 0 (see emptyOffsetPartitions — a fresh slice, never
	// an in-place edit of the cache-aliased source ts.Partitions).
	out := ts
	out.Name = newTopic
	out.Partitions = emptyOffsetPartitions(ts.Partitions)
	return out, nil
}

// IncreasePartitions sets topic's total partition count to total (the
// contract's PartitionsIncrease.totalPartitionsCount — a target total, not
// an incremental add; see cluster.TopicAdminPort.CreatePartitions' doc
// comment for the kadm-level naming trap this avoids).
func (s *TopicService) IncreasePartitions(ctx context.Context, name, topic string, total int32) error {
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	if err := s.port.CreatePartitions(ctx, def, topic, total); err != nil {
		return err
	}
	s.refreshAfterWrite(ctx, name, "CreatePartitions")
	return nil
}

// ErrInvalidReplicationFactor is reassignForFactor's/
// ChangeReplicationFactor's sentinel for a target factor outside the
// achievable range (< 1, or more replicas than there are brokers to hold
// them) — handlers_topic.go maps this to a 400 (the contract declares one
// for changeReplicationFactor), distinguishing it from a genuine backend
// failure (500).
var ErrInvalidReplicationFactor = errors.New("invalid target replication factor")

// ChangeReplicationFactor reassigns topic's partitions to target's
// replication factor via reassignForFactor's deterministic algorithm (see
// its doc comment), using the cached RuntimeState for topic's current
// per-partition placement and the cluster's broker ID list.
func (s *TopicService) ChangeReplicationFactor(ctx context.Context, name, topic string, target int16) error {
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	st, ok := s.states.Get(ctx, name)
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownCluster, name)
	}
	var current []cluster.PartitionState
	found := false
	for _, ts := range st.Topics {
		if ts.Name == topic {
			current, found = ts.Partitions, true
			break
		}
	}
	if !found {
		return fmt.Errorf("topic not found in cached cluster state: %q", topic)
	}
	brokers := make([]int32, 0, len(st.Brokers))
	for _, b := range st.Brokers {
		brokers = append(brokers, b.ID)
	}
	assignment, err := reassignForFactor(current, brokers, target)
	if err != nil {
		return err
	}
	if err := s.port.AlterPartitionAssignments(ctx, def, topic, assignment); err != nil {
		return err
	}
	s.refreshAfterWrite(ctx, name, "AlterPartitionAssignments")
	return nil
}

// filterSortPage is List's pure core: filter (internal-topic exclusion +
// case-insensitive search-contains), sort (by q.OrderBy/q.SortOrder), then
// paginate (q.Page/q.PerPage, both defaulted and clamped). Split out from
// List so it's unit-testable without a StateCache/Resolver/port at all.
func filterSortPage(all []cluster.TopicState, q TopicListQuery) TopicPage {
	perPage := q.PerPage
	if perPage <= 0 {
		perPage = defaultTopicPerPage
	}
	page := q.Page
	if page <= 0 {
		page = defaultTopicPage
	}

	filtered := make([]cluster.TopicState, 0, len(all))
	search := strings.ToLower(q.Search)
	for _, t := range all {
		if !q.ShowInternal && t.Internal {
			continue
		}
		if search != "" && !strings.Contains(strings.ToLower(t.Name), search) {
			continue
		}
		filtered = append(filtered, t)
	}

	sortTopics(filtered, q.OrderBy, q.SortOrder)

	total := len(filtered)
	// PageCount semantics for an empty (post-filter) result: 0, not 1. This
	// is ceil(0/perPage) taken literally — "zero pages of results" — and
	// matches the vendored frontend's own fallback (TopicTable.tsx:
	// `const pageCount = data?.pageCount || 0;`), which already treats a
	// missing/0 pageCount as the router-of-record for "nothing to page
	// through", so there's no upstream requirement forcing a 1-not-0 lie.
	pageCount := 0
	if total > 0 {
		pageCount = (total + perPage - 1) / perPage
		if page > pageCount {
			page = pageCount // clamp an out-of-range page to the last real page
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
	return TopicPage{Topics: filtered[start:end], PageCount: pageCount}
}

// sortTopics sorts ts in place by orderBy (one of the contract's
// TopicColumnsToSort values), ascending unless sortOrder is exactly "DESC".
// Only the four columns this app can actually compute from cluster.TopicState
// are handled directly (NAME/TOTAL_PARTITIONS/REPLICATION_FACTOR/SIZE).
// MESSAGES_COUNT uses TopicState.MessagesCount so unknown counts stay last
// in both directions. Equal computed values use NAME ascending as a stable
// pagination tie-break even when the primary order is descending.
// OUT_OF_SYNC_REPLICAS and unrecognized values fall back to NAME ordering
// rather than erroring.
func messagesCountLess(a, b cluster.TopicState, desc bool) bool {
	ac, ak := a.MessagesCount()
	bc, bk := b.MessagesCount()
	if ak != bk {
		return ak
	}
	if ak && ac != bc {
		if desc {
			return ac > bc
		}
		return ac < bc
	}
	return a.Name < b.Name
}

func sortTopics(ts []cluster.TopicState, orderBy, sortOrder string) {
	if orderBy == "MESSAGES_COUNT" {
		desc := sortOrder == "DESC"
		sort.SliceStable(ts, func(i, j int) bool {
			return messagesCountLess(ts[i], ts[j], desc)
		})
		return
	}

	desc := sortOrder == "DESC"
	if orderBy == "NAME" ||
		(orderBy != "TOTAL_PARTITIONS" && orderBy != "REPLICATION_FACTOR" && orderBy != "SIZE") {
		sort.SliceStable(ts, func(i, j int) bool {
			if desc {
				return ts[i].Name > ts[j].Name
			}
			return ts[i].Name < ts[j].Name
		})
		return
	}

	sort.SliceStable(ts, func(i, j int) bool {
		comparison := 0
		switch orderBy {
		case "TOTAL_PARTITIONS":
			comparison = compareInt(len(ts[i].Partitions), len(ts[j].Partitions))
		case "REPLICATION_FACTOR":
			comparison = compareInt(ts[i].ReplicationFactor, ts[j].ReplicationFactor)
		case "SIZE":
			comparison = compareInt64(ts[i].SegmentSize, ts[j].SegmentSize)
		}
		if comparison == 0 {
			return ts[i].Name < ts[j].Name
		}
		if desc {
			return comparison > 0
		}
		return comparison < 0
	})
}

func compareInt(left, right int) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareInt64(left, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

// reassignForFactor is ChangeReplicationFactor's pure algorithmic core: given
// a topic's current per-partition placement, the cluster's broker ID list,
// and a target replication factor, it produces the new partition->ordered-
// broker-list assignment ChangeReplicationFactor hands to
// AlterPartitionAssignments. It never touches the live cluster — every
// case is a deterministic, in-memory transformation, which is what makes it
// independently table-testable (topic_internal_test.go) without a port/
// StateCache/Resolver at all.
//
// Per-partition, not per-topic: each partition's own current replica count
// decides whether it grows, shrinks, or is a no-op — this handles the (rare
// but possible) case of a topic whose partitions don't all share the same
// current replication factor, and it's no more code than assuming
// uniformity.
//
//   - target < 1, or target > len(brokers): error (can't spread more
//     replicas than there are brokers to hold them, and a topic needs at
//     least 1 replica per partition).
//   - target == a partition's current replica count: no-op, that
//     partition's existing assignment is copied through unchanged.
//   - target < current: shrink. The partition's Leader always survives
//     (moved to the front of the kept list) even if it wasn't first in
//     Replicas — losing the leader on a replication-factor decrease would be
//     a much more disruptive change than this operation is meant to be.
//     The remaining slots are filled from the existing Replicas in their
//     original order (skipping the leader, already placed).
//   - target > current: grow. New replicas are picked from the brokers this
//     partition doesn't already hold (never a duplicate), round-robining
//     through that per-partition eligible list using a rotation counter that
//     keeps advancing across partitions (not reset per partition) — this is
//     what spreads the added load evenly across the whole broker set
//     instead of every partition picking the same "first eligible" broker.
func reassignForFactor(current []cluster.PartitionState, brokers []int32, target int16) (map[int32][]int32, error) {
	if target < 1 {
		return nil, fmt.Errorf("%w: must be >= 1, got %d", ErrInvalidReplicationFactor, target)
	}
	if int(target) > len(brokers) {
		return nil, fmt.Errorf("%w: %d exceeds broker count %d", ErrInvalidReplicationFactor, target, len(brokers))
	}

	brokerIDs := append([]int32(nil), brokers...)
	sort.Slice(brokerIDs, func(i, j int) bool { return brokerIDs[i] < brokerIDs[j] })

	parts := append([]cluster.PartitionState(nil), current...)
	sort.Slice(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })

	out := make(map[int32][]int32, len(parts))
	rotation := 0
	for _, p := range parts {
		switch currentRF := len(p.Replicas); {
		case int(target) == currentRF:
			out[p.ID] = append([]int32(nil), p.Replicas...)
		case int(target) < currentRF:
			out[p.ID] = shrinkReplicas(p, int(target))
		default:
			added := int(target) - currentRF
			out[p.ID] = growReplicas(p, brokerIDs, added, rotation)
			rotation += added
		}
	}
	return out, nil
}

// shrinkReplicas keeps p.Leader first, then fills the remaining target-1
// slots from p.Replicas in their original order (skipping the leader, which
// is already placed) until target entries are reached. When the partition
// has no current leader (Leader < 0 — e.g. offline/under-replicated), there
// is nothing valid to place first: broker ID -1 would end up in the
// AlterPartitionAssignments request and a real cluster rejects that. In that
// case this falls back to just the first target entries from p.Replicas in
// their original order, same as if the (nonexistent) leader had already been
// skipped.
func shrinkReplicas(p cluster.PartitionState, target int) []int32 {
	out := make([]int32, 0, target)
	if p.Leader >= 0 {
		out = append(out, p.Leader)
	}
	for _, r := range p.Replicas {
		if len(out) >= target {
			break
		}
		if r == p.Leader {
			continue
		}
		out = append(out, r)
	}
	return out
}

// growReplicas appends `added` new replicas to p's existing Replicas, picked
// from brokerIDs \ p.Replicas (never a broker p already holds), round-
// robining through that per-partition eligible list starting at `rotation`
// — the caller advances rotation by `added` between partitions so
// consecutive partitions don't all start their pick at the same eligible
// broker (see reassignForFactor's doc comment).
func growReplicas(p cluster.PartitionState, brokerIDs []int32, added, rotation int) []int32 {
	held := make(map[int32]bool, len(p.Replicas))
	for _, r := range p.Replicas {
		held[r] = true
	}
	eligible := make([]int32, 0, len(brokerIDs)-len(held))
	for _, b := range brokerIDs {
		if !held[b] {
			eligible = append(eligible, b)
		}
	}
	out := append([]int32(nil), p.Replicas...)
	for i := 0; i < added && len(eligible) > 0; i++ {
		out = append(out, eligible[(rotation+i)%len(eligible)])
	}
	return out
}

// configOp is one incremental change configOps computes: Set (apply Value)
// or Unset (revert to whatever lower-priority source then applies, e.g. a
// cluster default — Value is unused/zero for this case).
type configOp struct {
	Name  string
	Value string
	Unset bool
}

// configOps diffs a topic's current configuration against a desired full
// config map (updateTopic's contract semantics: TopicUpdate.configs is the
// complete desired dynamic-config set, mirroring upstream kafka-ui's
// mergeWithNonDefaultProperties) and returns the incremental Set/Unset
// operations needed to reconcile them — never a full replace (ADR-0003
// §6.1's soul constraint; TopicService.UpdateConfigs applies these one at a
// time through AlterTopicConfig, which itself only ever issues kadm's
// incremental AlterTopicConfigs). Three states per key:
//   - present in desired, value differs from (or key absent from) current:
//     Set.
//   - present in both with the same value: no-op, omitted from the result.
//   - a current DYNAMIC_TOPIC_CONFIG entry whose name is absent from
//     desired: Unset. Only DYNAMIC_TOPIC_CONFIG-sourced entries qualify here
//     — static/default-sourced entries were never dynamically overridden in
//     the first place, so there's nothing to revert.
//
// Returned in a deterministic (name-sorted) order so callers/tests don't
// depend on map iteration order.
func configOps(current []cluster.ConfigEntry, desired map[string]string) []configOp {
	byName := make(map[string]cluster.ConfigEntry, len(current))
	for _, c := range current {
		byName[c.Name] = c
	}
	var ops []configOp
	for name, value := range desired {
		if cur, ok := byName[name]; !ok || cur.Value != value {
			ops = append(ops, configOp{Name: name, Value: value})
		}
	}
	for _, c := range current {
		if c.Source != "DYNAMIC_TOPIC_CONFIG" {
			continue
		}
		if _, ok := desired[c.Name]; !ok {
			ops = append(ops, configOp{Name: c.Name, Unset: true})
		}
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].Name < ops[j].Name })
	return ops
}
