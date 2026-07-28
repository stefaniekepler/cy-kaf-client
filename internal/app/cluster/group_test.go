package cluster_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// fakeGroupAdmin implements cluster.GroupAdminPort for GroupService tests —
// same call-recording shape as topic_test.go's fakeTopicAdmin: per-method
// canned result/error plus atomic call counters and last-args fields.
type fakeGroupAdmin struct {
	listCalls atomic.Int32
	lastList  cluster.Definition
	list      []cluster.GroupState
	listErr   error

	describeCalls atomic.Int32
	lastDescribe  []string // every id DescribeGroup was called with, in call order
	describeByID  map[string]cluster.GroupState
	describeErr   map[string]error // per-id error; absent id with no describeErr and not in describeByID -> generic errBoom

	forTopicCalls atomic.Int32
	lastTopic     string
	forTopic      []cluster.GroupState
	forTopicErr   error

	resetCalls    atomic.Int32
	lastResetID   string
	lastResetSpec cluster.ResetSpec
	resetErr      error

	deleteCalls  atomic.Int32
	lastDeleteID string
	deleteErr    error

	deleteOffsetsCalls atomic.Int32
	lastDeleteOffsets  [2]string // [id, topic]
	deleteOffsetsErr   error
}

func newFakeGroupAdmin() *fakeGroupAdmin {
	return &fakeGroupAdmin{describeByID: map[string]cluster.GroupState{}, describeErr: map[string]error{}}
}

func (f *fakeGroupAdmin) ListGroups(_ context.Context, def cluster.Definition) ([]cluster.GroupState, error) {
	f.listCalls.Add(1)
	f.lastList = def
	return f.list, f.listErr
}

func (f *fakeGroupAdmin) DescribeGroup(_ context.Context, _ cluster.Definition, id string) (cluster.GroupState, error) {
	f.describeCalls.Add(1)
	f.lastDescribe = append(f.lastDescribe, id)
	if err, ok := f.describeErr[id]; ok {
		return cluster.GroupState{}, err
	}
	gs, ok := f.describeByID[id]
	if !ok {
		return cluster.GroupState{}, errBoom
	}
	return gs, nil
}

func (f *fakeGroupAdmin) GroupsForTopic(_ context.Context, _ cluster.Definition, topic string) ([]cluster.GroupState, error) {
	f.forTopicCalls.Add(1)
	f.lastTopic = topic
	return f.forTopic, f.forTopicErr
}

func (f *fakeGroupAdmin) ResetOffsets(_ context.Context, _ cluster.Definition, id string, spec cluster.ResetSpec) error {
	f.resetCalls.Add(1)
	f.lastResetID = id
	f.lastResetSpec = spec
	return f.resetErr
}

func (f *fakeGroupAdmin) DeleteGroup(_ context.Context, _ cluster.Definition, id string) error {
	f.deleteCalls.Add(1)
	f.lastDeleteID = id
	return f.deleteErr
}

func (f *fakeGroupAdmin) DeleteGroupOffsets(_ context.Context, _ cluster.Definition, id, topic string) error {
	f.deleteOffsetsCalls.Add(1)
	f.lastDeleteOffsets = [2]string{id, topic}
	return f.deleteOffsetsErr
}

func newGroupServiceForTest(def cluster.Definition, port cluster.GroupAdminPort) *appcluster.GroupService {
	res := appcluster.NewResolver([]cluster.Definition{def})
	return appcluster.NewGroupService(res, port)
}

// --- Page ---

func TestGroupServicePageUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newGroupServiceForTest(cluster.Definition{Name: "prod"}, newFakeGroupAdmin())
	_, err := svc.Page(context.Background(), "nope", appcluster.GroupPageQuery{})
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestGroupServicePageDelegatesToFilterSortPageForKnownCluster(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	port.list = []cluster.GroupState{{ID: "b"}, {ID: "a"}}
	svc := newGroupServiceForTest(def, port)

	page, err := svc.Page(context.Background(), "prod", appcluster.GroupPageQuery{})
	require.NoError(t, err)
	require.Len(t, page.Groups, 2)
	require.Equal(t, "a", page.Groups[0].ID) // NAME asc default, proves filterSortPageGroups ran
	require.Equal(t, "b", page.Groups[1].ID)
	require.Equal(t, 1, page.PageCount)
	require.Equal(t, def, port.lastList)
	require.Equal(t, int32(1), port.listCalls.Load())
}

func TestGroupServicePagePropagatesBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	port.listErr = errBoom
	svc := newGroupServiceForTest(def, port)
	_, err := svc.Page(context.Background(), "prod", appcluster.GroupPageQuery{})
	require.ErrorIs(t, err, errBoom)
}

// --- Get ---

func TestGroupServiceGetUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newGroupServiceForTest(cluster.Definition{Name: "prod"}, newFakeGroupAdmin())
	_, err := svc.Get(context.Background(), "nope", "g1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestGroupServiceGetDelegatesToPortForKnownCluster(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	want := cluster.GroupState{ID: "g1", State: "STABLE"}
	port.describeByID["g1"] = want
	svc := newGroupServiceForTest(def, port)

	got, err := svc.Get(context.Background(), "prod", "g1")
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, []string{"g1"}, port.lastDescribe)
}

func TestGroupServiceGetPropagatesBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	port.describeErr["g1"] = errBoom
	svc := newGroupServiceForTest(def, port)
	_, err := svc.Get(context.Background(), "prod", "g1")
	require.ErrorIs(t, err, errBoom)
}

// --- Lag ---

func TestGroupServiceLagUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newGroupServiceForTest(cluster.Definition{Name: "prod"}, newFakeGroupAdmin())
	_, err := svc.Lag(context.Background(), "nope", []string{"g1"})
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestGroupServiceLagDescribesEveryID(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	port.describeByID["g1"] = cluster.GroupState{ID: "g1"}
	port.describeByID["g2"] = cluster.GroupState{ID: "g2"}
	svc := newGroupServiceForTest(def, port)

	got, err := svc.Lag(context.Background(), "prod", []string{"g1", "g2"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, []string{"g1", "g2"}, port.lastDescribe)
}

// TestGroupServiceLagSkipsFailingIDsRatherThanFailingWholeBatch locks the
// deliberate per-id-skip policy documented on GroupService.Lag: one bad id
// (never existed / deleted mid-flight) must not blank out the other,
// perfectly describable ids in the same request.
func TestGroupServiceLagSkipsFailingIDsRatherThanFailingWholeBatch(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	port.describeByID["good"] = cluster.GroupState{ID: "good"}
	port.describeErr["bad"] = errBoom
	svc := newGroupServiceForTest(def, port)

	got, err := svc.Lag(context.Background(), "prod", []string{"good", "bad"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "good", got[0].ID)
}

// --- ForTopic ---

func TestGroupServiceForTopicUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newGroupServiceForTest(cluster.Definition{Name: "prod"}, newFakeGroupAdmin())
	_, err := svc.ForTopic(context.Background(), "nope", "t1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestGroupServiceForTopicDelegatesToPortForKnownCluster(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	want := []cluster.GroupState{{ID: "g1"}}
	port.forTopic = want
	svc := newGroupServiceForTest(def, port)

	got, err := svc.ForTopic(context.Background(), "prod", "t1")
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, "t1", port.lastTopic)
	require.Equal(t, int32(1), port.forTopicCalls.Load())
}

func TestGroupServiceForTopicPropagatesBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	port.forTopicErr = errBoom
	svc := newGroupServiceForTest(def, port)
	_, err := svc.ForTopic(context.Background(), "prod", "t1")
	require.ErrorIs(t, err, errBoom)
}

// --- Reset ---

// TestGroupServiceResetUnknownClusterIsErrUnknownCluster also locks that an
// unknown cluster name fails via Resolver.Lookup *before* the new
// describe-based state pre-check (Task 8e) ever runs: describeCalls must
// stay at 0.
func TestGroupServiceResetUnknownClusterIsErrUnknownCluster(t *testing.T) {
	port := newFakeGroupAdmin()
	svc := newGroupServiceForTest(cluster.Definition{Name: "prod"}, port)
	err := svc.Reset(context.Background(), "nope", "g1", cluster.ResetSpec{Topic: "t1", ResetType: "EARLIEST"})
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
	require.Equal(t, int32(0), port.describeCalls.Load(), "unknown cluster must fail via Lookup before any describe call")
}

func TestGroupServiceResetDelegatesToPortForKnownCluster(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	// Task 8e's pre-check describes id first: EMPTY is inactive, so Reset
	// must proceed to ResetOffsets exactly as before.
	port.describeByID["g1"] = cluster.GroupState{ID: "g1", State: "EMPTY"}
	svc := newGroupServiceForTest(def, port)
	spec := cluster.ResetSpec{Topic: "t1", ResetType: "OFFSET", PartitionsOffsets: map[int32]int64{0: 5}}

	err := svc.Reset(context.Background(), "prod", "g1", spec)
	require.NoError(t, err)
	require.Equal(t, "g1", port.lastResetID)
	require.Equal(t, spec, port.lastResetSpec)
	require.Equal(t, int32(1), port.resetCalls.Load())
}

// TestGroupServiceResetPropagatesResetOffsetsBackendError locks that a
// genuine ResetOffsets failure -- the describe pre-check itself having
// succeeded with an inactive (EMPTY) state -- still propagates as an
// ordinary backend error. Distinct from
// TestGroupServiceResetPropagatesDescribeBackendError below, which covers
// the describe call itself failing (a different failure point, now that
// Reset makes two port calls instead of one).
func TestGroupServiceResetPropagatesResetOffsetsBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	port.describeByID["g1"] = cluster.GroupState{ID: "g1", State: "EMPTY"}
	port.resetErr = errBoom
	svc := newGroupServiceForTest(def, port)
	err := svc.Reset(context.Background(), "prod", "g1", cluster.ResetSpec{Topic: "t1", ResetType: "EARLIEST"})
	require.ErrorIs(t, err, errBoom)
	require.Equal(t, int32(1), port.resetCalls.Load())
}

// TestGroupServiceResetPropagatesDescribeBackendError locks that a genuine
// describe-call failure (e.g. a real kadm error, or a group id that no
// longer exists) propagates as an ordinary backend error -- never
// misclassified as ErrGroupNotInactive -- and that ResetOffsets is never
// called in this case either (there's no state to have judged inactive).
func TestGroupServiceResetPropagatesDescribeBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	port.describeErr["g1"] = errBoom
	svc := newGroupServiceForTest(def, port)
	err := svc.Reset(context.Background(), "prod", "g1", cluster.ResetSpec{Topic: "t1", ResetType: "EARLIEST"})
	require.ErrorIs(t, err, errBoom)
	require.NotErrorIs(t, err, appcluster.ErrGroupNotInactive)
	require.Equal(t, int32(0), port.resetCalls.Load())
}

// TestGroupServiceResetGroupStateGate is the soul test for Task 8e (P1b
// final-review verdict②, mirroring upstream kafka-ui's
// OffsetsResetService.checkGroupCondition): Reset must pre-check id's
// current state via DescribeGroup *before* ever calling ResetOffsets. Only
// EMPTY/DEAD (the two states upstream treats as inactive) proceed; every
// other state -- both rebalance states and a defensive "unrecognized"
// catch-all -- is rejected with ErrGroupNotInactive (message embedding the
// actual state, upstream-style). The critical assertion on the rejected
// side is resetCalls == 0: this proves the short-circuit happens *before*
// ever touching the broker/port, not merely that the overall call happens
// to fail (the "never reached" shape 8c's CreateTopic tests already
// established for TOPIC_DELETION).
func TestGroupServiceResetGroupStateGate(t *testing.T) {
	cases := []struct {
		name         string
		state        string
		wantInactive bool
	}{
		{"EMPTY proceeds", "EMPTY", false},
		{"DEAD proceeds", "DEAD", false},
		{"STABLE rejected", "STABLE", true},
		{"PREPARING_REBALANCE rejected", "PREPARING_REBALANCE", true},
		{"COMPLETING_REBALANCE rejected", "COMPLETING_REBALANCE", true},
		{"UNKNOWN rejected", "UNKNOWN", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def := cluster.Definition{Name: "prod"}
			port := newFakeGroupAdmin()
			port.describeByID["g1"] = cluster.GroupState{ID: "g1", State: tc.state}
			svc := newGroupServiceForTest(def, port)

			err := svc.Reset(context.Background(), "prod", "g1", cluster.ResetSpec{Topic: "t1", ResetType: "EARLIEST"})
			if tc.wantInactive {
				require.ErrorIs(t, err, appcluster.ErrGroupNotInactive)
				require.Contains(t, err.Error(), tc.state)
				require.Equal(t, int32(0), port.resetCalls.Load(), "ResetOffsets must never be called for a non-inactive group")
			} else {
				require.NoError(t, err)
				require.Equal(t, int32(1), port.resetCalls.Load())
			}
		})
	}
}

// --- Delete ---

func TestGroupServiceDeleteUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newGroupServiceForTest(cluster.Definition{Name: "prod"}, newFakeGroupAdmin())
	err := svc.Delete(context.Background(), "nope", "g1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestGroupServiceDeleteDelegatesToPortForKnownCluster(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	svc := newGroupServiceForTest(def, port)

	err := svc.Delete(context.Background(), "prod", "g1")
	require.NoError(t, err)
	require.Equal(t, "g1", port.lastDeleteID)
	require.Equal(t, int32(1), port.deleteCalls.Load())
}

func TestGroupServiceDeletePropagatesBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	port.deleteErr = errBoom
	svc := newGroupServiceForTest(def, port)
	err := svc.Delete(context.Background(), "prod", "g1")
	require.ErrorIs(t, err, errBoom)
}

// --- DeleteOffsets ---

func TestGroupServiceDeleteOffsetsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newGroupServiceForTest(cluster.Definition{Name: "prod"}, newFakeGroupAdmin())
	err := svc.DeleteOffsets(context.Background(), "nope", "g1", "t1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestGroupServiceDeleteOffsetsDelegatesToPortForKnownCluster(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	svc := newGroupServiceForTest(def, port)

	err := svc.DeleteOffsets(context.Background(), "prod", "g1", "t1")
	require.NoError(t, err)
	require.Equal(t, [2]string{"g1", "t1"}, port.lastDeleteOffsets)
	require.Equal(t, int32(1), port.deleteOffsetsCalls.Load())
}

func TestGroupServiceDeleteOffsetsPropagatesBackendError(t *testing.T) {
	def := cluster.Definition{Name: "prod"}
	port := newFakeGroupAdmin()
	port.deleteOffsetsErr = errBoom
	svc := newGroupServiceForTest(def, port)
	err := svc.DeleteOffsets(context.Background(), "prod", "g1", "t1")
	require.ErrorIs(t, err, errBoom)
}
