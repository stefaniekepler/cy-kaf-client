package cluster_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// errBoom is a shared sentinel for tests asserting a backend error
// propagates unchanged (as opposed to ErrUnknownCluster).
var errBoom = errors.New("boom")

// fakeTopicAdmin implements cluster.TopicAdminPort for TopicService tests —
// same call-recording shape as broker_test.go's fakeBrokerAdmin.
type fakeTopicAdmin struct {
	configsCalls atomic.Int32
	lastCfgDef   cluster.Definition
	lastCfgTopic string
	configs      []cluster.ConfigEntry
	configsErr   error

	aclsCalls   atomic.Int32
	lastAclsDef cluster.Definition
	acls        []cluster.AclBinding
	aclsErr     error

	producersCalls   atomic.Int32
	lastProducersDef cluster.Definition
	producers        []cluster.ProducerState
	producersErr     error

	createCalls atomic.Int32
	lastCreate  cluster.TopicSpec
	createErr   error

	deleteCalls     atomic.Int32
	lastDeleteDef   cluster.Definition
	lastDeleteTopic string
	deleteErr       error
	// deleteGoneAfter, when > 0, makes TopicConfigs (used by
	// TopicService.awaitTopicGone's polling probe) return an error starting
	// from the deleteGoneAfter-th call — simulating "the topic has actually
	// disappeared" for Recreate's poll-until-gone step, without needing a
	// live cluster. 0 means TopicConfigs never fails on its own account.
	deleteGoneAfter int32

	alterConfigCalls atomic.Int32
	lastAlterConfig  []alterConfigCall
	alterConfigErr   error

	createPartitionsCalls atomic.Int32
	lastCreatePartitions  createPartitionsCall
	createPartitionsErr   error

	alterAssignmentsCalls atomic.Int32
	lastAssignment        map[int32][]int32
	alterAssignmentsErr   error
}

type alterConfigCall struct {
	Topic, Name, Value string
}

type createPartitionsCall struct {
	Topic string
	Total int32
}

func (f *fakeTopicAdmin) TopicConfigs(_ context.Context, def cluster.Definition, topic string) ([]cluster.ConfigEntry, error) {
	n := f.configsCalls.Add(1)
	f.lastCfgDef = def
	f.lastCfgTopic = topic
	if f.deleteGoneAfter > 0 && n >= f.deleteGoneAfter {
		return nil, errors.New("unknown topic or partition")
	}
	return f.configs, f.configsErr
}

func (f *fakeTopicAdmin) TopicAcls(_ context.Context, def cluster.Definition, _ string) ([]cluster.AclBinding, error) {
	f.aclsCalls.Add(1)
	f.lastAclsDef = def
	return f.acls, f.aclsErr
}

func (f *fakeTopicAdmin) ActiveProducers(_ context.Context, def cluster.Definition, _ string) ([]cluster.ProducerState, error) {
	f.producersCalls.Add(1)
	f.lastProducersDef = def
	return f.producers, f.producersErr
}

func (f *fakeTopicAdmin) CreateTopic(_ context.Context, _ cluster.Definition, spec cluster.TopicSpec) error {
	f.createCalls.Add(1)
	f.lastCreate = spec
	return f.createErr
}

func (f *fakeTopicAdmin) DeleteTopic(_ context.Context, def cluster.Definition, topic string) error {
	f.deleteCalls.Add(1)
	f.lastDeleteDef = def
	f.lastDeleteTopic = topic
	return f.deleteErr
}

func (f *fakeTopicAdmin) AlterTopicConfig(_ context.Context, _ cluster.Definition, topic, name, value string) error {
	f.alterConfigCalls.Add(1)
	f.lastAlterConfig = append(f.lastAlterConfig, alterConfigCall{Topic: topic, Name: name, Value: value})
	return f.alterConfigErr
}

func (f *fakeTopicAdmin) CreatePartitions(_ context.Context, _ cluster.Definition, topic string, total int32) error {
	f.createPartitionsCalls.Add(1)
	f.lastCreatePartitions = createPartitionsCall{Topic: topic, Total: total}
	return f.createPartitionsErr
}

func (f *fakeTopicAdmin) AlterPartitionAssignments(_ context.Context, _ cluster.Definition, _ string, assignment map[int32][]int32) error {
	f.alterAssignmentsCalls.Add(1)
	f.lastAssignment = assignment
	return f.alterAssignmentsErr
}

// newTopicServiceForTest builds a TopicService whose StateCache is seeded
// synchronously via SetForTest (state_internal_test.go's test-only escape
// hatch, exported so cluster_test can reach it too) — no background
// scraping goroutine, no polling required. Recreate's poll-until-gone step
// uses a 1ms interval (appcluster.WithPollInterval) so tests exercising it
// stay fast without needing a real StateScraper. Thin wrapper around
// newTopicServiceForRefreshTest that drops the *fakeState scraper handle —
// every test in this file except the write-path cache refresh tests below
// has no need to reach into it.
func newTopicServiceForTest(def cluster.Definition, port cluster.TopicAdminPort) (*appcluster.TopicService, *appcluster.StateCache) {
	svc, states, _ := newTopicServiceForRefreshTest(def, port)
	return svc, states
}

// newTopicServiceForRefreshTest is newTopicServiceForTest's sibling for the
// write-path cache refresh tests (Task 8b, below): those need to control
// what a write method's post-success forced Refresh sees as "the cluster's
// current truth" (fakeState.custom/useCustom) and to count how many extra
// scrapes each write triggers (fakeState.calls) — both only reachable
// through the *fakeState scraper itself, which newTopicServiceForTest above
// builds but doesn't expose.
func newTopicServiceForRefreshTest(def cluster.Definition, port cluster.TopicAdminPort) (*appcluster.TopicService, *appcluster.StateCache, *fakeState) {
	res := appcluster.NewResolver([]cluster.Definition{def})
	fs := &fakeState{}
	states := appcluster.NewStateCache(res, fs, fs, time.Hour)
	svc := appcluster.NewTopicService(res, states, port, appcluster.WithPollInterval(time.Millisecond))
	return svc, states, fs
}

func TestTopicServiceListUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	_, err := svc.List(context.Background(), "nope", appcluster.TopicListQuery{})
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestTopicServiceListDelegatesToFilterSortPageForKnownCluster(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	svc, states := newTopicServiceForTest(def, &fakeTopicAdmin{})
	states.SetForTest("prod", cluster.RuntimeState{
		Definition: def,
		Topics: []cluster.TopicState{
			{Name: "b"}, {Name: "a"},
		},
	})
	page, err := svc.List(context.Background(), "prod", appcluster.TopicListQuery{})
	require.NoError(t, err)
	require.Len(t, page.Topics, 2)
	require.Equal(t, "a", page.Topics[0].Name) // NAME asc default, proves filterSortPage ran
	require.Equal(t, "b", page.Topics[1].Name)
	require.Equal(t, 1, page.PageCount)
}

func TestTopicServiceListUsesNameAscendingTieBreakBeforePagination(t *testing.T) {
	type metricTopic struct {
		name  string
		value int
	}
	firstOrder := []metricTopic{
		{name: "beta", value: 1},
		{name: "echo", value: 3},
		{name: "delta", value: 2},
		{name: "alpha", value: 1},
		{name: "foxtrot", value: 3},
		{name: "charlie", value: 2},
	}
	secondOrder := []metricTopic{
		{name: "charlie", value: 2},
		{name: "foxtrot", value: 3},
		{name: "alpha", value: 1},
		{name: "delta", value: 2},
		{name: "echo", value: 3},
		{name: "beta", value: 1},
	}

	for _, orderBy := range []string{
		"TOTAL_PARTITIONS",
		"REPLICATION_FACTOR",
		"SIZE",
		"MESSAGES_COUNT",
	} {
		t.Run(orderBy, func(t *testing.T) {
			toTopicStates := func(input []metricTopic) []cluster.TopicState {
				topics := make([]cluster.TopicState, len(input))
				for index, item := range input {
					topic := cluster.TopicState{Name: item.name}
					switch orderBy {
					case "TOTAL_PARTITIONS":
						topic.Partitions = make([]cluster.PartitionState, item.value)
					case "REPLICATION_FACTOR":
						topic.ReplicationFactor = item.value
					case "SIZE":
						topic.SegmentSize = int64(item.value)
					case "MESSAGES_COUNT":
						topic.Partitions = []cluster.PartitionState{{
							ID: 0, StartOffset: 0, EndOffset: int64(item.value),
						}}
					}
					topics[index] = topic
				}
				return topics
			}
			listNames := func(input []metricTopic, sortOrder string, pageNumber int) []string {
				def := cluster.Definition{Name: "prod"}
				svc, states := newTopicServiceForTest(def, &fakeTopicAdmin{})
				states.SetForTest("prod", cluster.RuntimeState{
					Definition: def,
					Topics:     toTopicStates(input),
				})
				page, err := svc.List(context.Background(), "prod", appcluster.TopicListQuery{
					OrderBy: orderBy, SortOrder: sortOrder, Page: pageNumber, PerPage: 2,
				})
				require.NoError(t, err)
				require.Equal(t, 3, page.PageCount)
				names := make([]string, len(page.Topics))
				for index := range page.Topics {
					names[index] = page.Topics[index].Name
				}
				return names
			}

			for _, test := range []struct {
				sortOrder string
				page      int
				want      []string
			}{
				{sortOrder: "ASC", page: 1, want: []string{"alpha", "beta"}},
				{sortOrder: "ASC", page: 2, want: []string{"charlie", "delta"}},
				{sortOrder: "DESC", page: 1, want: []string{"echo", "foxtrot"}},
				{sortOrder: "DESC", page: 2, want: []string{"charlie", "delta"}},
			} {
				fromFirst := listNames(firstOrder, test.sortOrder, test.page)
				fromSecond := listNames(secondOrder, test.sortOrder, test.page)
				require.Equal(t, test.want, fromFirst)
				require.Equal(t, fromFirst, fromSecond)
			}
		})
	}
}

func TestTopicServiceDetailsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	_, _, err := svc.Details(context.Background(), "nope", "t1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestTopicServiceDetailsCombinesCachedStateAndLiveConfigs(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{configs: []cluster.ConfigEntry{{Name: "cleanup.policy", Value: "delete"}}}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{
		Definition: def,
		Topics:     []cluster.TopicState{{Name: "t1", ReplicationFactor: 3}},
	})
	ts, cfgs, err := svc.Details(context.Background(), "prod", "t1")
	require.NoError(t, err)
	require.Equal(t, "t1", ts.Name)
	require.Equal(t, 3, ts.ReplicationFactor)
	require.Equal(t, []cluster.ConfigEntry{{Name: "cleanup.policy", Value: "delete"}}, cfgs)
	require.Equal(t, def, port.lastCfgDef)
	require.Equal(t, "t1", port.lastCfgTopic)
}

// TestTopicServiceDetailsDegradesWhenTopicNotYetCached covers the race with
// the periodic scrape documented on TopicService.Details: the live configs
// call succeeds (the topic demonstrably exists) but the cached snapshot
// doesn't have it yet — Details must still return the real configs alongside
// a name-only TopicState, not an error.
func TestTopicServiceDetailsDegradesWhenTopicNotYetCached(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{configs: []cluster.ConfigEntry{{Name: "retention.ms", Value: "60000"}}}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def}) // no Topics yet
	ts, cfgs, err := svc.Details(context.Background(), "prod", "brand-new-topic")
	require.NoError(t, err)
	require.Equal(t, cluster.TopicState{Name: "brand-new-topic"}, ts)
	require.Equal(t, []cluster.ConfigEntry{{Name: "retention.ms", Value: "60000"}}, cfgs)
}

func TestTopicServiceDetailsPropagatesConfigsBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{configsErr: errBoom}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def})
	_, _, err := svc.Details(context.Background(), "prod", "t1")
	require.ErrorIs(t, err, errBoom)
}

func TestTopicServiceConfigsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	_, err := svc.Configs(context.Background(), "nope", "t1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestTopicServiceConfigsDelegatesToPortForKnownCluster(t *testing.T) {
	def := cluster.Definition{Name: "prod", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"a:1"}}}
	want := []cluster.ConfigEntry{{Name: "retention.ms", Value: "60000"}}
	port := &fakeTopicAdmin{configs: want}
	svc, _ := newTopicServiceForTest(def, port)
	got, err := svc.Configs(context.Background(), "prod", "t1")
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, def, port.lastCfgDef)
	require.Equal(t, "t1", port.lastCfgTopic)
	require.Equal(t, int32(1), port.configsCalls.Load())
}

func TestTopicServiceAclsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	_, err := svc.Acls(context.Background(), "nope", "t1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestTopicServiceAclsDelegatesToPortForKnownCluster(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	want := []cluster.AclBinding{{Principal: "User:alice", Host: "*", ResourceName: "t1",
		ResourceType: "TOPIC", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"}}
	port := &fakeTopicAdmin{acls: want}
	svc, _ := newTopicServiceForTest(def, port)
	got, err := svc.Acls(context.Background(), "prod", "t1")
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, def, port.lastAclsDef)
	require.Equal(t, int32(1), port.aclsCalls.Load())
}

func TestTopicServiceActiveProducersUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	_, err := svc.ActiveProducers(context.Background(), "nope", "t1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestTopicServiceActiveProducersDelegatesToPortForKnownCluster(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	want := []cluster.ProducerState{{Partition: 0, ProducerID: 42}}
	port := &fakeTopicAdmin{producers: want}
	svc, _ := newTopicServiceForTest(def, port)
	got, err := svc.ActiveProducers(context.Background(), "prod", "t1")
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, def, port.lastProducersDef)
	require.Equal(t, int32(1), port.producersCalls.Load())
}

// --- Connectors (P1b Task 8a: empty getTopicConnectors stub) ---

func TestTopicServiceConnectorsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	err := svc.Connectors(context.Background(), "nope", "t1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

// TestTopicServiceConnectorsKnownClusterReturnsNoError covers the only other
// case Connectors has: a known cluster always succeeds with no error and no
// payload — there's no Kafka Connect-backed data source behind it yet (P1b
// Task 8a's empty stub; a real query is P2's scope), so unlike Acls/
// ActiveProducers above there's no port to delegate to or assert a call
// against.
func TestTopicServiceConnectorsKnownClusterReturnsNoError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	svc, _ := newTopicServiceForTest(def, &fakeTopicAdmin{})
	err := svc.Connectors(context.Background(), "prod", "t1")
	require.NoError(t, err)
}

// --- Create ---

func TestTopicServiceCreateUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	_, err := svc.Create(context.Background(), "nope", cluster.TopicSpec{Name: "t1"})
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestTopicServiceCreateDelegatesToPortAndSynthesizesResponse(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, _ := newTopicServiceForTest(def, port)
	spec := cluster.TopicSpec{Name: "t1", Partitions: 3, ReplicationFactor: 2, Configs: map[string]string{"retention.ms": "1000"}}
	got, err := svc.Create(context.Background(), "prod", spec)
	require.NoError(t, err)
	require.Equal(t, spec, port.lastCreate)
	require.Equal(t, int32(1), port.createCalls.Load())
	require.Equal(t, "t1", got.Name)
	require.Equal(t, 2, got.ReplicationFactor)
	require.Len(t, got.Partitions, 3)
}

func TestTopicServiceCreateWithDefaultsLeavesUnknownFieldsZero(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	svc, _ := newTopicServiceForTest(def, &fakeTopicAdmin{})
	got, err := svc.Create(context.Background(), "prod", cluster.TopicSpec{Name: "t1", Partitions: -1, ReplicationFactor: -1})
	require.NoError(t, err)
	require.Equal(t, cluster.TopicState{Name: "t1"}, got)
}

func TestTopicServiceCreatePropagatesBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{createErr: errBoom}
	svc, _ := newTopicServiceForTest(def, port)
	_, err := svc.Create(context.Background(), "prod", cluster.TopicSpec{Name: "t1"})
	require.ErrorIs(t, err, errBoom)
}

// --- Delete ---

func TestTopicServiceDeleteUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	err := svc.Delete(context.Background(), "nope", "t1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestTopicServiceDeleteCallsPortWhenFeatureEnabled(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def, TopicDeletionEnabled: true})
	err := svc.Delete(context.Background(), "prod", "t1")
	require.NoError(t, err)
	require.Equal(t, int32(1), port.deleteCalls.Load())
	require.Equal(t, "t1", port.lastDeleteTopic)
}

func TestTopicServiceDeleteReturnsErrTopicDeletionDisabledWhenFeatureOff(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def, TopicDeletionEnabled: false})
	err := svc.Delete(context.Background(), "prod", "t1")
	require.ErrorIs(t, err, appcluster.ErrTopicDeletionDisabled)
	require.Equal(t, int32(0), port.deleteCalls.Load()) // 守卫拦在 port 调用之前
}

func TestTopicServiceDeleteAllowsWhenClusterNotYetScraped(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, _ := newTopicServiceForTest(def, port) // 未 SetForTest：states.Get 返回 ok=false
	err := svc.Delete(context.Background(), "prod", "t1")
	require.NoError(t, err)
	require.Equal(t, int32(1), port.deleteCalls.Load())
}

func TestTopicServiceDeletePropagatesBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{deleteErr: errBoom}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def, TopicDeletionEnabled: true})
	err := svc.Delete(context.Background(), "prod", "t1")
	require.ErrorIs(t, err, errBoom)
}

// --- UpdateConfigs ---

func TestTopicServiceUpdateConfigsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	_, err := svc.UpdateConfigs(context.Background(), "nope", "t1", nil)
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestTopicServiceUpdateConfigsAppliesIncrementalDiff(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{configs: []cluster.ConfigEntry{
		{Name: "cleanup.policy", Value: "delete", Source: "DYNAMIC_TOPIC_CONFIG"}, // absent from desired -> unset
		{Name: "retention.ms", Value: "60000", Source: "DYNAMIC_TOPIC_CONFIG"},    // unchanged
	}}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def,
		Topics: []cluster.TopicState{{Name: "t1", ReplicationFactor: 2}}})

	got, err := svc.UpdateConfigs(context.Background(), "prod", "t1", map[string]string{
		"retention.ms":      "60000",
		"max.message.bytes": "2000000",
	})
	require.NoError(t, err)
	require.Equal(t, "t1", got.Name)
	require.Equal(t, 2, got.ReplicationFactor) // 来自缓存，未变

	// configOps 的三态：unset(cleanup.policy) + set(max.message.bytes)，按名排序。
	require.Equal(t, []alterConfigCall{
		{Topic: "t1", Name: "cleanup.policy", Value: ""},
		{Topic: "t1", Name: "max.message.bytes", Value: "2000000"},
	}, port.lastAlterConfig)
}

func TestTopicServiceUpdateConfigsPropagatesConfigsReadError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{configsErr: errBoom}
	svc, _ := newTopicServiceForTest(def, port)
	_, err := svc.UpdateConfigs(context.Background(), "prod", "t1", nil)
	require.ErrorIs(t, err, errBoom)
}

func TestTopicServiceUpdateConfigsPropagatesAlterError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{
		configs:        []cluster.ConfigEntry{{Name: "retention.ms", Value: "1", Source: "DYNAMIC_TOPIC_CONFIG"}},
		alterConfigErr: errBoom,
	}
	svc, _ := newTopicServiceForTest(def, port)
	_, err := svc.UpdateConfigs(context.Background(), "prod", "t1", map[string]string{"retention.ms": "2"})
	require.ErrorIs(t, err, errBoom)
}

// --- Recreate ---

func recreateFixtureConfigs() []cluster.ConfigEntry {
	return []cluster.ConfigEntry{
		{Name: "retention.ms", Value: "1000", Source: "DYNAMIC_TOPIC_CONFIG"},
		{Name: "cleanup.policy", Value: "delete", Source: "DEFAULT_CONFIG"}, // 非 dynamic：不进 recreate spec
	}
}

func TestTopicServiceRecreateUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	_, err := svc.Recreate(context.Background(), "nope", "t1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestTopicServiceRecreateDeletesPollsThenRecreatesSameSpec(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{configs: recreateFixtureConfigs(), deleteGoneAfter: 3}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def, TopicDeletionEnabled: true,
		Topics: []cluster.TopicState{{Name: "t1", ReplicationFactor: 2, Partitions: []cluster.PartitionState{
			{ID: 0, Leader: 1, Replicas: []int32{1, 2}},
			{ID: 1, Leader: 2, Replicas: []int32{1, 2}},
			{ID: 2, Leader: 1, Replicas: []int32{1, 2}},
		}}}})

	got, err := svc.Recreate(context.Background(), "prod", "t1")
	require.NoError(t, err)
	require.Equal(t, "t1", got.Name)
	require.Equal(t, 2, got.ReplicationFactor)
	require.Len(t, got.Partitions, 3)

	require.Equal(t, int32(1), port.deleteCalls.Load())
	require.Equal(t, "t1", port.lastDeleteTopic)
	require.Equal(t, int32(1), port.createCalls.Load())
	require.Equal(t, cluster.TopicSpec{
		Name: "t1", Partitions: 3, ReplicationFactor: 2,
		Configs: map[string]string{"retention.ms": "1000"},
	}, port.lastCreate)
	require.GreaterOrEqual(t, port.configsCalls.Load(), int32(3)) // 预读 1 次 + 至少两次轮询
}

// TestTopicServiceRecreateReturnsEmptyOffsetsWithoutPollutingCache locks in
// the fix for a P1b Task 8c review finding: Recreate returns the *pre-delete*
// TopicState (read via Details before the wipe), whose partition offsets are
// the old topic's — but the re-created topic is empty, so its response's
// messagesCount (Σ EndOffset-StartOffset in topicToGenerated) must read 0, not
// the stale pre-delete count. The offsets in the returned ts must therefore be
// zeroed, while ID/Leader/Replicas/ISR stay intact so the replica/ISR tallies
// remain correct (matching the re-created topic's identical placement).
//
// Crucially this must NOT be done in place: Details' returned Partitions slice
// aliases the StateCache's own backing array, so zeroing offsets there would
// corrupt the cache. The test holds a direct reference to the exact partitions
// slice it seeds and asserts, after Recreate, that its offsets are STILL the
// original non-zero values — a naive in-place fix would flip them to 0 here
// (caught), while the pre-fix code returns the stale non-zero offsets in got
// (caught by the got-side assertion). Only a fresh-slice fix passes both.
func TestTopicServiceRecreateReturnsEmptyOffsetsWithoutPollutingCache(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	// Non-zero offsets on the seeded (pre-delete) topic: the bug returns these
	// verbatim; the fix must return 0/0 instead.
	seedPartitions := []cluster.PartitionState{
		{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}, StartOffset: 5, EndOffset: 100},
		{ID: 1, Leader: 2, Replicas: []int32{1, 2}, ISR: []int32{1}, StartOffset: 0, EndOffset: 50},
	}
	port := &fakeTopicAdmin{configs: recreateFixtureConfigs(), deleteGoneAfter: 2}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def, TopicDeletionEnabled: true,
		Topics: []cluster.TopicState{{Name: "t1", ReplicationFactor: 2, Partitions: seedPartitions}}})

	got, err := svc.Recreate(context.Background(), "prod", "t1")
	require.NoError(t, err)
	require.Len(t, got.Partitions, 2)
	for i, p := range got.Partitions {
		require.Zerof(t, p.StartOffset, "returned partition %d StartOffset must be 0 (empty re-created topic)", i)
		require.Zerof(t, p.EndOffset, "returned partition %d EndOffset must be 0 (empty re-created topic)", i)
	}
	// Placement preserved so the replica/ISR tallies stay correct.
	require.Equal(t, int32(1), got.Partitions[0].Leader)
	require.Equal(t, []int32{1, 2}, got.Partitions[0].Replicas)
	require.Equal(t, []int32{1, 2}, got.Partitions[0].ISR)
	// No aliasing pollution: the exact slice we seeded still carries its
	// original non-zero offsets (a fresh-slice fix leaves it untouched; an
	// in-place zeroing would have corrupted it — and with it the cache).
	require.Equal(t, int64(5), seedPartitions[0].StartOffset, "cache-aliased seed slice must not be mutated in place")
	require.Equal(t, int64(100), seedPartitions[0].EndOffset, "cache-aliased seed slice must not be mutated in place")
	require.Equal(t, int64(50), seedPartitions[1].EndOffset, "cache-aliased seed slice must not be mutated in place")
}

func TestTopicServiceRecreateReturnsErrTopicDeletionDisabledWhenFeatureOff(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{configs: recreateFixtureConfigs()}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def, TopicDeletionEnabled: false,
		Topics: []cluster.TopicState{{Name: "t1", ReplicationFactor: 1}}})

	_, err := svc.Recreate(context.Background(), "prod", "t1")
	require.ErrorIs(t, err, appcluster.ErrTopicDeletionDisabled)
	require.Equal(t, int32(0), port.deleteCalls.Load())
	require.Equal(t, int32(0), port.createCalls.Load())
}

func TestTopicServiceRecreatePropagatesCreateBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{configs: recreateFixtureConfigs(), deleteGoneAfter: 2, createErr: errBoom}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def, TopicDeletionEnabled: true,
		Topics: []cluster.TopicState{{Name: "t1", ReplicationFactor: 1}}})

	_, err := svc.Recreate(context.Background(), "prod", "t1")
	require.ErrorIs(t, err, errBoom)
	require.Equal(t, int32(1), port.deleteCalls.Load())
}

// --- Clone ---

func TestTopicServiceCloneUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	_, err := svc.Clone(context.Background(), "nope", "src", "dst")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestTopicServiceCloneCreatesNewTopicWithSourceShape(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	srcPartitions := []cluster.PartitionState{
		{ID: 0, Leader: 1, Replicas: []int32{1, 2, 3}},
		{ID: 1, Leader: 2, Replicas: []int32{1, 2, 3}},
	}
	port := &fakeTopicAdmin{configs: recreateFixtureConfigs()}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def,
		Topics: []cluster.TopicState{{Name: "src", ReplicationFactor: 3, Partitions: srcPartitions}}})

	got, err := svc.Clone(context.Background(), "prod", "src", "dst")
	require.NoError(t, err)
	require.Equal(t, "dst", got.Name)
	require.Equal(t, 3, got.ReplicationFactor)
	require.Len(t, got.Partitions, 2)
	require.Equal(t, cluster.TopicSpec{
		Name: "dst", Partitions: 2, ReplicationFactor: 3,
		Configs: map[string]string{"retention.ms": "1000"},
	}, port.lastCreate)
}

// TestTopicServiceCloneReturnsEmptyOffsetsWithoutPollutingCache is Clone's
// counterpart to the Recreate test above (same P1b Task 8c review finding):
// Clone returns the *source* topic's TopicState renamed, whose partition
// offsets are the source's — but the clone is a brand-new empty topic, so its
// response's messagesCount must read 0, not the source's count. Offsets zeroed,
// placement (ID/Leader/Replicas/ISR) preserved, and — because Details' slice
// aliases the cache — done via a fresh slice, never in place. Same dual
// assertion: got carries zeroed offsets, the seeded source slice keeps its
// original non-zero offsets.
func TestTopicServiceCloneReturnsEmptyOffsetsWithoutPollutingCache(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	seedPartitions := []cluster.PartitionState{
		{ID: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}, StartOffset: 10, EndOffset: 210},
		{ID: 1, Leader: 2, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}, StartOffset: 0, EndOffset: 90},
	}
	port := &fakeTopicAdmin{configs: recreateFixtureConfigs()}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def,
		Topics: []cluster.TopicState{{Name: "src", ReplicationFactor: 3, Partitions: seedPartitions}}})

	got, err := svc.Clone(context.Background(), "prod", "src", "dst")
	require.NoError(t, err)
	require.Equal(t, "dst", got.Name)
	require.Len(t, got.Partitions, 2)
	for i, p := range got.Partitions {
		require.Zerof(t, p.StartOffset, "returned partition %d StartOffset must be 0 (empty clone)", i)
		require.Zerof(t, p.EndOffset, "returned partition %d EndOffset must be 0 (empty clone)", i)
	}
	require.Equal(t, int32(1), got.Partitions[0].Leader)
	require.Equal(t, []int32{1, 2, 3}, got.Partitions[0].Replicas)
	// No aliasing pollution of the cache-aliased source slice.
	require.Equal(t, int64(10), seedPartitions[0].StartOffset, "cache-aliased seed slice must not be mutated in place")
	require.Equal(t, int64(210), seedPartitions[0].EndOffset, "cache-aliased seed slice must not be mutated in place")
	require.Equal(t, int64(90), seedPartitions[1].EndOffset, "cache-aliased seed slice must not be mutated in place")
}

func TestTopicServiceClonePropagatesCreateBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{createErr: errBoom}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def,
		Topics: []cluster.TopicState{{Name: "src", ReplicationFactor: 1}}})
	_, err := svc.Clone(context.Background(), "prod", "src", "dst")
	require.ErrorIs(t, err, errBoom)
}

// --- IncreasePartitions ---

func TestTopicServiceIncreasePartitionsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	err := svc.IncreasePartitions(context.Background(), "nope", "t1", 6)
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestTopicServiceIncreasePartitionsDelegatesToPort(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, _ := newTopicServiceForTest(def, port)
	err := svc.IncreasePartitions(context.Background(), "prod", "t1", 6)
	require.NoError(t, err)
	require.Equal(t, createPartitionsCall{Topic: "t1", Total: 6}, port.lastCreatePartitions)
}

func TestTopicServiceIncreasePartitionsPropagatesBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{createPartitionsErr: errBoom}
	svc, _ := newTopicServiceForTest(def, port)
	err := svc.IncreasePartitions(context.Background(), "prod", "t1", 6)
	require.ErrorIs(t, err, errBoom)
}

// --- ChangeReplicationFactor ---

func TestTopicServiceChangeReplicationFactorUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc, _ := newTopicServiceForTest(cluster.Definition{Name: "prod"}, &fakeTopicAdmin{})
	err := svc.ChangeReplicationFactor(context.Background(), "nope", "t1", 3)
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestTopicServiceChangeReplicationFactorDelegatesReassignment(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def,
		Brokers: []cluster.BrokerInfo{{ID: 1}, {ID: 2}, {ID: 3}},
		Topics: []cluster.TopicState{{Name: "t1", Partitions: []cluster.PartitionState{
			{ID: 0, Leader: 1, Replicas: []int32{1, 2}},
		}}},
	})
	err := svc.ChangeReplicationFactor(context.Background(), "prod", "t1", 3)
	require.NoError(t, err)
	require.Equal(t, map[int32][]int32{0: {1, 2, 3}}, port.lastAssignment)
}

func TestTopicServiceChangeReplicationFactorInvalidTargetIsErrInvalidReplicationFactor(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def,
		Brokers: []cluster.BrokerInfo{{ID: 1}},
		Topics:  []cluster.TopicState{{Name: "t1", Partitions: []cluster.PartitionState{{ID: 0, Leader: 1, Replicas: []int32{1}}}}},
	})
	err := svc.ChangeReplicationFactor(context.Background(), "prod", "t1", 0)
	require.ErrorIs(t, err, appcluster.ErrInvalidReplicationFactor)
	require.Equal(t, int32(0), port.alterAssignmentsCalls.Load())
}

func TestTopicServiceChangeReplicationFactorPropagatesBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{alterAssignmentsErr: errBoom}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def,
		Brokers: []cluster.BrokerInfo{{ID: 1}, {ID: 2}},
		Topics:  []cluster.TopicState{{Name: "t1", Partitions: []cluster.PartitionState{{ID: 0, Leader: 1, Replicas: []int32{1}}}}},
	})
	err := svc.ChangeReplicationFactor(context.Background(), "prod", "t1", 2)
	require.ErrorIs(t, err, errBoom)
}

func TestTopicServiceChangeReplicationFactorTopicNotInCacheIsError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, states := newTopicServiceForTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def, Brokers: []cluster.BrokerInfo{{ID: 1}}})
	err := svc.ChangeReplicationFactor(context.Background(), "prod", "missing", 1)
	require.Error(t, err)
	require.NotErrorIs(t, err, appcluster.ErrUnknownCluster)
	require.Equal(t, int32(0), port.alterAssignmentsCalls.Load())
}

// --- write-path cache refresh (Task 8b: read-your-writes) ---
//
// T8's e2e re-run exposed a real, independent bug: every write method above
// went straight to the live cluster (cluster.TopicAdminPort), while List/
// Details only ever read the periodically-refreshed (30s) StateCache — so a
// topic just created/deleted/altered could take up to 30s to show up (or
// disappear) in the list/details response, racing e2e's own assertion
// timeout. The fix: each write method now forces a synchronous
// StateCache.Refresh (via the shared refreshAfterWrite helper) once its own
// port call succeeds. These tests prove the fix actually closes the gap for
// the read path — not just that Refresh gets called somewhere — using
// newTopicServiceForRefreshTest's controllable fakeState scraper as a stand-
// in for "the live cluster's current truth" a forced Refresh would observe.

// topicNames extracts just the Name field from a TopicState slice, for
// order-agnostic Contains/NotContains assertions below.
func topicNames(ts []cluster.TopicState) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = t.Name
	}
	return out
}

// TestTopicServiceCreateRefreshesCacheSoListImmediatelySeesNewTopic is this
// task's primary evidence for Create: before the fix, List would keep
// serving the stale pre-create snapshot (states.SetForTest below) until the
// next periodic scrape — up to 30s in production. It also folds in the
// scrape-count aux assertion for Create (one of the "6 write methods" the
// aux tests below cover for the rest).
func TestTopicServiceCreateRefreshesCacheSoListImmediatelySeesNewTopic(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, states, fs := newTopicServiceForRefreshTest(def, port)
	// 写前：cache 是上一轮周期性扫描的旧快照，不含新 topic。
	//
	// 新鲜度守卫提醒：这里写前快照与下面 fs.custom 的写后"真相"都留零值
	// RefreshedAt，且必须保持一致（都零值，或都显式对齐）。refreshAfterWrite
	// 触发的 refreshOne 有新鲜度守卫（state.go:66: 仅当
	// cur.RefreshedAt.After(st.RefreshedAt) 才拒写），零值 After 零值为 false
	// 故放行、刷新落地、List 读到新真相。若"为求真实"单给这个写前快照塞一个
	// 更晚的 RefreshedAt，守卫会误拒写后刷新、List 仍读旧快照——测试假红，
	// 而非真实回归。同一约定见下面 Delete 主证。
	states.SetForTest("prod", cluster.RuntimeState{Definition: def,
		Topics: []cluster.TopicState{{Name: "existing"}}})
	// 集群"真相"：Create 生效后集群上已经有新 topic 了（下一次 FetchState 该看到它）。
	fs.useCustom = true
	fs.custom = cluster.RuntimeState{Topics: []cluster.TopicState{
		{Name: "existing"}, {Name: "new-topic", ReplicationFactor: 1},
	}}
	before := fs.calls.Load()

	_, err := svc.Create(context.Background(), "prod", cluster.TopicSpec{Name: "new-topic", Partitions: 1, ReplicationFactor: 1})
	require.NoError(t, err)
	require.Equal(t, before+1, fs.calls.Load()) // 恰好一次额外 scrape

	page, err := svc.List(context.Background(), "prod", appcluster.TopicListQuery{})
	require.NoError(t, err)
	require.Contains(t, topicNames(page.Topics), "new-topic") // 读己所写：List 不必等 30s 周期刷新
}

// TestTopicServiceCreateFailureDoesNotRefresh is this task's failure-path
// evidence for Create: a failed write must not trigger a refresh (there's
// nothing new for the cache to learn, and refreshing on every failed attempt
// would just be a wasted scrape).
func TestTopicServiceCreateFailureDoesNotRefresh(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{createErr: errBoom}
	svc, states, fs := newTopicServiceForRefreshTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def})
	before := fs.calls.Load()

	_, err := svc.Create(context.Background(), "prod", cluster.TopicSpec{Name: "t1"})
	require.ErrorIs(t, err, errBoom)
	require.Equal(t, before, fs.calls.Load())
}

// TestTopicServiceDeleteRefreshesCacheSoListImmediatelyDropsDeletedTopic is
// this task's primary evidence for Delete, symmetric to Create's above: the
// deleted topic must disappear from List right away, not after the next
// periodic scrape.
func TestTopicServiceDeleteRefreshesCacheSoListImmediatelyDropsDeletedTopic(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, states, fs := newTopicServiceForRefreshTest(def, port)
	// 写前快照与下面 fs.custom 的 RefreshedAt 须保持一致（均零值），否则
	// state.go:66 新鲜度守卫会误拒写后刷新、测试假红——详见上面 Create 主证。
	states.SetForTest("prod", cluster.RuntimeState{Definition: def, TopicDeletionEnabled: true,
		Topics: []cluster.TopicState{{Name: "t1"}, {Name: "keep"}}})
	fs.useCustom = true
	fs.custom = cluster.RuntimeState{TopicDeletionEnabled: true,
		Topics: []cluster.TopicState{{Name: "keep"}}}
	before := fs.calls.Load()

	err := svc.Delete(context.Background(), "prod", "t1")
	require.NoError(t, err)
	require.Equal(t, before+1, fs.calls.Load())

	page, err := svc.List(context.Background(), "prod", appcluster.TopicListQuery{})
	require.NoError(t, err)
	names := topicNames(page.Topics)
	require.NotContains(t, names, "t1")
	require.Contains(t, names, "keep")
}

// TestTopicServiceDeleteFailureDoesNotRefresh is Delete's failure-path
// counterpart to TestTopicServiceCreateFailureDoesNotRefresh above.
func TestTopicServiceDeleteFailureDoesNotRefresh(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{deleteErr: errBoom}
	svc, states, fs := newTopicServiceForRefreshTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def, TopicDeletionEnabled: true})
	before := fs.calls.Load()

	err := svc.Delete(context.Background(), "prod", "t1")
	require.ErrorIs(t, err, errBoom)
	require.Equal(t, before, fs.calls.Load())
}

// --- the remaining write methods: aux evidence (scrape count only — the
// mechanism, not List's downstream behavior, which Create/Delete above
// already prove end to end) ---

func TestTopicServiceCloneRefreshesCacheAfterSuccess(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, states, fs := newTopicServiceForRefreshTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def,
		Topics: []cluster.TopicState{{Name: "src", ReplicationFactor: 1}}})
	before := fs.calls.Load()

	_, err := svc.Clone(context.Background(), "prod", "src", "dst")
	require.NoError(t, err)
	require.Equal(t, before+1, fs.calls.Load())
}

func TestTopicServiceCloneFailureDoesNotRefresh(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{createErr: errBoom}
	svc, states, fs := newTopicServiceForRefreshTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def,
		Topics: []cluster.TopicState{{Name: "src", ReplicationFactor: 1}}})
	before := fs.calls.Load()

	_, err := svc.Clone(context.Background(), "prod", "src", "dst")
	require.ErrorIs(t, err, errBoom)
	require.Equal(t, before, fs.calls.Load())
}

func TestTopicServiceIncreasePartitionsRefreshesCacheAfterSuccess(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, states, fs := newTopicServiceForRefreshTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def})
	before := fs.calls.Load()

	err := svc.IncreasePartitions(context.Background(), "prod", "t1", 6)
	require.NoError(t, err)
	require.Equal(t, before+1, fs.calls.Load())
}

func TestTopicServiceIncreasePartitionsFailureDoesNotRefresh(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{createPartitionsErr: errBoom}
	svc, states, fs := newTopicServiceForRefreshTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def})
	before := fs.calls.Load()

	err := svc.IncreasePartitions(context.Background(), "prod", "t1", 6)
	require.ErrorIs(t, err, errBoom)
	require.Equal(t, before, fs.calls.Load())
}

func TestTopicServiceChangeReplicationFactorRefreshesCacheAfterSuccess(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{}
	svc, states, fs := newTopicServiceForRefreshTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def,
		Brokers: []cluster.BrokerInfo{{ID: 1}, {ID: 2}, {ID: 3}},
		Topics: []cluster.TopicState{{Name: "t1", Partitions: []cluster.PartitionState{
			{ID: 0, Leader: 1, Replicas: []int32{1, 2}},
		}}},
	})
	before := fs.calls.Load()

	err := svc.ChangeReplicationFactor(context.Background(), "prod", "t1", 3)
	require.NoError(t, err)
	require.Equal(t, before+1, fs.calls.Load())
}

func TestTopicServiceChangeReplicationFactorFailureDoesNotRefresh(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{alterAssignmentsErr: errBoom}
	svc, states, fs := newTopicServiceForRefreshTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def,
		Brokers: []cluster.BrokerInfo{{ID: 1}, {ID: 2}},
		Topics:  []cluster.TopicState{{Name: "t1", Partitions: []cluster.PartitionState{{ID: 0, Leader: 1, Replicas: []int32{1}}}}},
	})
	before := fs.calls.Load()

	err := svc.ChangeReplicationFactor(context.Background(), "prod", "t1", 2)
	require.ErrorIs(t, err, errBoom)
	require.Equal(t, before, fs.calls.Load())
}

// TestTopicServiceRecreateRefreshesCacheTwice covers Recreate's documented
// double-refresh: it calls s.Delete internally (one refresh) and then does
// its own final refresh after re-creating — two extra scrapes, not one. This
// is accepted as correctness-first (see Recreate's doc comment in topic.go);
// the point of this test is only to lock in that the count really is 2 (not
// 1, which would mean one of the two paths silently lost its refresh, and
// not 3+, which would mean an unintended extra scrape crept in).
func TestTopicServiceRecreateRefreshesCacheTwice(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{configs: recreateFixtureConfigs(), deleteGoneAfter: 2}
	svc, states, fs := newTopicServiceForRefreshTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def, TopicDeletionEnabled: true,
		Topics: []cluster.TopicState{{Name: "t1", ReplicationFactor: 1}}})
	before := fs.calls.Load()

	_, err := svc.Recreate(context.Background(), "prod", "t1")
	require.NoError(t, err)
	require.Equal(t, before+2, fs.calls.Load())
}

// TestTopicServiceRecreateRefreshesOnceWhenRecreateCreateFails locks in the
// scrape count for Recreate's chained-failure path: its internal s.Delete
// succeeds here (refreshAfterWrite fires, +1 scrape), but after
// awaitTopicGone the final s.port.CreateTopic fails, so Recreate returns the
// error *before* reaching its own end-of-method refreshAfterWrite. Net delta
// is therefore exactly +1 (from the internal Delete's refresh), not 0 and
// not 2 — and that +1 is correct, not a leak: the internal Delete really did
// remove the topic, so the cache *should* reflect the deletion even though
// the re-create half then failed. Without this test a future Recreate
// refactor could silently drop that refresh (delta 0 — cache would keep
// serving a topic that's actually gone) or add a spurious one (delta 2).
// Port setup mirrors TestTopicServiceRecreatePropagatesCreateBackendError.
func TestTopicServiceRecreateRefreshesOnceWhenRecreateCreateFails(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := &fakeTopicAdmin{configs: recreateFixtureConfigs(), deleteGoneAfter: 2, createErr: errBoom}
	svc, states, fs := newTopicServiceForRefreshTest(def, port)
	states.SetForTest("prod", cluster.RuntimeState{Definition: def, TopicDeletionEnabled: true,
		Topics: []cluster.TopicState{{Name: "t1", ReplicationFactor: 1}}})
	before := fs.calls.Load()

	_, err := svc.Recreate(context.Background(), "prod", "t1")
	require.ErrorIs(t, err, errBoom)
	require.Equal(t, before+1, fs.calls.Load()) // 仅内部 Delete 那次刷新；末尾刷新在 CreateTopic 失败 return 之前未到达
}
