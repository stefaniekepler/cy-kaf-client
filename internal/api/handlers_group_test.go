package api_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// fakeGroupServicer implements api.GroupServicer for the groups handler
// tests — same "error map wins, else result-map presence = known cluster"
// convention, keyed by cluster name only, as handlers_topic_test.go's
// fakeTopicServicer.
type fakeGroupServicer struct {
	listPages     map[string]appcluster.GroupPage
	listErr       map[string]error
	lastPageQuery map[string]appcluster.GroupPageQuery

	getResult map[string]cluster.GroupState
	getErr    map[string]error

	lagResult  map[string][]cluster.GroupState
	lagErr     map[string]error
	lastLagIDs map[string][]string

	forTopicResult map[string][]cluster.GroupState
	forTopicErr    map[string]error

	resetErr      map[string]error
	resetKnown    map[string]bool
	lastResetID   map[string]string
	lastResetSpec map[string]cluster.ResetSpec

	deleteErr    map[string]error
	deleteKnown  map[string]bool
	lastDeleteID map[string]string

	deleteOffsetsErr   map[string]error
	deleteOffsetsKnown map[string]bool
	lastDeleteOffsets  map[string][2]string // [id, topic]
}

func newFakeGroupServicer() *fakeGroupServicer {
	return &fakeGroupServicer{
		listPages: map[string]appcluster.GroupPage{}, listErr: map[string]error{}, lastPageQuery: map[string]appcluster.GroupPageQuery{},
		getResult: map[string]cluster.GroupState{}, getErr: map[string]error{},
		lagResult: map[string][]cluster.GroupState{}, lagErr: map[string]error{}, lastLagIDs: map[string][]string{},
		forTopicResult: map[string][]cluster.GroupState{}, forTopicErr: map[string]error{},
		resetErr: map[string]error{}, resetKnown: map[string]bool{}, lastResetID: map[string]string{}, lastResetSpec: map[string]cluster.ResetSpec{},
		deleteErr: map[string]error{}, deleteKnown: map[string]bool{}, lastDeleteID: map[string]string{},
		deleteOffsetsErr: map[string]error{}, deleteOffsetsKnown: map[string]bool{}, lastDeleteOffsets: map[string][2]string{},
	}
}

func (f *fakeGroupServicer) Page(_ context.Context, name string, q appcluster.GroupPageQuery) (appcluster.GroupPage, error) {
	f.lastPageQuery[name] = q
	if err, ok := f.listErr[name]; ok {
		return appcluster.GroupPage{}, err
	}
	page, ok := f.listPages[name]
	if !ok {
		return appcluster.GroupPage{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return page, nil
}

func (f *fakeGroupServicer) Get(_ context.Context, name, _ string) (cluster.GroupState, error) {
	if err, ok := f.getErr[name]; ok {
		return cluster.GroupState{}, err
	}
	gs, ok := f.getResult[name]
	if !ok {
		return cluster.GroupState{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return gs, nil
}

func (f *fakeGroupServicer) Lag(_ context.Context, name string, ids []string) ([]cluster.GroupState, error) {
	f.lastLagIDs[name] = ids
	if err, ok := f.lagErr[name]; ok {
		return nil, err
	}
	gss, ok := f.lagResult[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return gss, nil
}

func (f *fakeGroupServicer) ForTopic(_ context.Context, name, _ string) ([]cluster.GroupState, error) {
	if err, ok := f.forTopicErr[name]; ok {
		return nil, err
	}
	gss, ok := f.forTopicResult[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return gss, nil
}

func (f *fakeGroupServicer) Reset(_ context.Context, name, id string, spec cluster.ResetSpec) error {
	f.lastResetID[name] = id
	f.lastResetSpec[name] = spec
	if err, ok := f.resetErr[name]; ok {
		return err
	}
	if !f.resetKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return nil
}

func (f *fakeGroupServicer) Delete(_ context.Context, name, id string) error {
	f.lastDeleteID[name] = id
	if err, ok := f.deleteErr[name]; ok {
		return err
	}
	if !f.deleteKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return nil
}

func (f *fakeGroupServicer) DeleteOffsets(_ context.Context, name, id, topic string) error {
	f.lastDeleteOffsets[name] = [2]string{id, topic}
	if err, ok := f.deleteOffsetsErr[name]; ok {
		return err
	}
	if !f.deleteOffsetsKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return nil
}

// withGroups wires fg as Deps.Groups — same pattern as withTopics.
func withGroups(fg *fakeGroupServicer) testServerOption {
	return func(d *api.Deps) { d.Groups = fg }
}

// sampleGroupState is a full detail-level fixture (as DescribeGroup/
// GroupsForTopic would produce): one member assigned to both of a topic's
// two partitions, one partition fully caught up (lag 0) and one 10 behind.
func sampleGroupState() cluster.GroupState {
	return cluster.GroupState{
		ID: "orders-consumer", State: "STABLE",
		Coordinator: "broker-1.internal", CoordinatorID: 1,
		Protocol: "range",
		Members: []cluster.GroupMember{
			{MemberID: "m1", ClientID: "c1", Host: "10.0.0.5",
				Assignments: []cluster.TopicPartitions{{Topic: "orders", Partitions: []int32{0, 1}}}},
		},
		Offsets: []cluster.GroupOffset{
			{Topic: "orders", Partition: 0, Committed: 90, End: 100},
			{Topic: "orders", Partition: 1, Committed: 100, End: 100},
		},
	}
}

// topicProjectionGroupState has three group members across two topics, with
// only one member assigned to orders. It catches the regression where the
// topic consumer-groups endpoint reports whole-group counts and lag.
func topicProjectionGroupState() cluster.GroupState {
	return cluster.GroupState{
		ID: "charge_join_meeting", State: "STABLE",
		Coordinator: "broker-1.internal", CoordinatorID: 1,
		Protocol: "range",
		Members: []cluster.GroupMember{
			{MemberID: "orders-member", Assignments: []cluster.TopicPartitions{{Topic: "orders", Partitions: []int32{0}}}},
			{MemberID: "payments-member", Assignments: []cluster.TopicPartitions{{Topic: "payments", Partitions: []int32{0}}}},
			{MemberID: "unassigned-member"},
		},
		Offsets: []cluster.GroupOffset{
			{Topic: "orders", Partition: 0, Committed: 90, End: 100},
			{Topic: "payments", Partition: 0, Committed: 0, End: 90},
		},
	}
}

// --- GetConsumerGroupsPage ---

func TestGetConsumerGroupsPage(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.listPages["prod"] = appcluster.GroupPage{Groups: []cluster.GroupState{sampleGroupState()}, PageCount: 2}
	srv := newTestServer(withGroups(fg))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv,
		"/api/clusters/prod/consumer-groups/paged?page=2&perPage=10&search=order&orderBy=NAME&sortOrder=DESC&state=STABLE,DEAD", &got)
	require.Equal(t, 200, code)
	require.Equal(t, float64(2), got["pageCount"])
	groups, ok := got["consumerGroups"].([]any)
	require.True(t, ok)
	require.Len(t, groups, 1)
	g0, ok := groups[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "orders-consumer", g0["groupId"])
	require.Equal(t, "STABLE", g0["state"])
	require.Equal(t, float64(1), g0["members"])
	require.Equal(t, float64(1), g0["topics"])
	require.Equal(t, "range", g0["partitionAssignor"])
	require.Equal(t, float64(10), g0["consumerLag"]) // 10 + 0
	coord, ok := g0["coordinator"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(1), coord["id"])
	require.Equal(t, "broker-1.internal", coord["host"])
	require.NotContains(t, g0, "partitions") // list view never carries per-partition detail
	validateAgainstContract(t, req, code, hdr, body)

	require.Equal(t, appcluster.GroupPageQuery{
		Page: 2, PerPage: 10, Search: "order", OrderBy: "NAME", SortOrder: "DESC", States: []string{"STABLE", "DEAD"},
	}, fg.lastPageQuery["prod"])
}

// TestGetConsumerGroupsPageOmitsCountsAtListLevel covers ListGroups'
// deliberately-thin list-level shape (only ID/State/CoordinatorID; no
// Members/Offsets/Protocol — see cluster.GroupAdminPort's doc comment):
// members/topics/partitionAssignor/consumerLag must all be omitted, not a
// misleading literal 0/false.
func TestGetConsumerGroupsPageOmitsCountsAtListLevel(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.listPages["prod"] = appcluster.GroupPage{
		Groups:    []cluster.GroupState{{ID: "bare-group", State: "EMPTY", CoordinatorID: 2}},
		PageCount: 1,
	}
	srv := newTestServer(withGroups(fg))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/consumer-groups/paged", &got)
	require.Equal(t, 200, code)
	groups := got["consumerGroups"].([]any)
	require.Len(t, groups, 1)
	g0 := groups[0].(map[string]any)
	require.Equal(t, "bare-group", g0["groupId"])
	require.NotContains(t, g0, "members")
	require.NotContains(t, g0, "topics")
	require.NotContains(t, g0, "partitionAssignor")
	require.NotContains(t, g0, "consumerLag")
	coord := g0["coordinator"].(map[string]any)
	require.Equal(t, float64(2), coord["id"])
	require.NotContains(t, coord, "host") // Coordinator host string is "" at list level
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConsumerGroupsPageUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withGroups(newFakeGroupServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/consumer-groups/paged", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConsumerGroupsPageBackendFailureIs500(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.listErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withGroups(fg))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/consumer-groups/paged", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to list consumer groups", "kadm boom")
}

// --- GetConsumerGroupsCsv ---

func TestGetConsumerGroupsCsv(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.listPages["prod"] = appcluster.GroupPage{Groups: []cluster.GroupState{sampleGroupState()}, PageCount: 1}
	srv := newTestServer(withGroups(fg))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/clusters/prod/consumer-groups/csv", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, 200, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/csv")
	b, _ := io.ReadAll(resp.Body)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	require.Len(t, lines, 2) // header + 1 data row
	require.Contains(t, lines[0], "groupId")
	validateAgainstContract(t, req, resp.StatusCode, resp.Header, b)
}

func TestGetConsumerGroupsCsvUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withGroups(newFakeGroupServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/consumer-groups/csv", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConsumerGroupsCsvBackendFailureIs500(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.listErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withGroups(fg))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/consumer-groups/csv", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to list consumer groups", "kadm boom")
}

// --- GetConsumerGroup ---

// TestGetConsumerGroup locks the ConsumerGroupDetails wire shape: oapi-
// codegen's generated.ConsumerGroupDetails is a bare type alias to
// generated.ConsumerGroup (the discriminator+allOf+sibling-property
// combination drops the schema's own "partitions" property — confirmed by
// grepping models.gen.go), yet the vendored frontend's OWN generated client
// (frontend/src/generated-sources/models/ConsumerGroupDetails.ts) still
// expects a top-level "partitions" sibling key. handlers_group.go works
// around the generator gap with a local wrapper struct; this test is the
// end-to-end lock that the wire JSON actually carries both "inherit":
// "details" and a populated "partitions" array, not just what the bare
// generated Go type could hold.
func TestGetConsumerGroup(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.getResult["prod"] = sampleGroupState()
	srv := newTestServer(withGroups(fg))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/consumer-groups/orders-consumer", &got)
	require.Equal(t, 200, code)
	require.Equal(t, "orders-consumer", got["groupId"])
	require.Equal(t, "details", got["inherit"])
	require.Equal(t, float64(10), got["consumerLag"])
	parts, ok := got["partitions"].([]any)
	require.True(t, ok)
	require.Len(t, parts, 2)
	byPartition := map[float64]map[string]any{}
	for _, raw := range parts {
		p := raw.(map[string]any)
		byPartition[p["partition"].(float64)] = p
	}
	p0 := byPartition[0]
	require.Equal(t, "orders", p0["topic"])
	require.Equal(t, float64(90), p0["currentOffset"])
	require.Equal(t, float64(100), p0["endOffset"])
	require.Equal(t, float64(10), p0["consumerLag"])
	require.Equal(t, "m1", p0["consumerId"])
	require.Equal(t, "10.0.0.5", p0["host"])
	p1 := byPartition[1]
	require.Equal(t, float64(0), p1["consumerLag"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConsumerGroupUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withGroups(newFakeGroupServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/consumer-groups/g1", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConsumerGroupBackendFailureIs500(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.getErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withGroups(fg))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/consumer-groups/g1", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to describe consumer group", "kadm boom")
}

// --- DeleteConsumerGroup ---

func TestDeleteConsumerGroup(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.deleteKnown["prod"] = true
	srv := newTestServer(withGroups(fg))
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/consumer-groups/g1", nil)
	require.Equal(t, 204, code)
	require.Equal(t, "g1", fg.lastDeleteID["prod"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestDeleteConsumerGroupReadOnlyIs403(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.deleteKnown["prod"] = true
	srv := newTestServer(withGroups(fg), withReadOnly("prod"))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/consumer-groups/g1", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fg.lastDeleteID["prod"])
}

func TestDeleteConsumerGroupUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withGroups(newFakeGroupServicer()))
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/nope/consumer-groups/g1", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestDeleteConsumerGroupBackendFailureIs500(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.deleteErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withGroups(fg))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/consumer-groups/g1", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to delete consumer group", "kadm boom")
}

// --- ResetConsumerGroupOffsets ---

// TestResetConsumerGroupOffsetsMapsAllFourResetTypes covers
// resetSpecFromGenerated's three (four, counting OFFSET/TIMESTAMP as
// separate shapes) request-body branches, asserting the fake-recorded
// ResetSpec the handler built matches the request body exactly for each of
// the contract's four ConsumerGroupOffsetsResetType values.
func TestResetConsumerGroupOffsetsMapsAllFourResetTypes(t *testing.T) {
	cases := []struct {
		name string
		body string
		want cluster.ResetSpec
	}{
		{
			name: "EARLIEST with explicit partitions",
			body: `{"topic":"orders","resetType":"EARLIEST","partitions":[0,1]}`,
			want: cluster.ResetSpec{Topic: "orders", ResetType: "EARLIEST", Partitions: []int32{0, 1}},
		},
		{
			name: "LATEST with no partitions (every partition)",
			body: `{"topic":"orders","resetType":"LATEST"}`,
			want: cluster.ResetSpec{Topic: "orders", ResetType: "LATEST"},
		},
		{
			name: "OFFSET with explicit partition->offset pairs",
			body: `{"topic":"orders","resetType":"OFFSET","partitionsOffsets":[{"partition":0,"offset":5},{"partition":1,"offset":10}]}`,
			want: cluster.ResetSpec{Topic: "orders", ResetType: "OFFSET", PartitionsOffsets: map[int32]int64{0: 5, 1: 10}},
		},
		{
			name: "TIMESTAMP with resetToTimestamp",
			body: `{"topic":"orders","resetType":"TIMESTAMP","resetToTimestamp":1700000000000}`,
			want: cluster.ResetSpec{Topic: "orders", ResetType: "TIMESTAMP", Timestamp: 1700000000000},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := newFakeGroupServicer()
			fg.resetKnown["prod"] = true
			srv := newTestServer(withGroups(fg))
			defer srv.Close()

			req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/consumer-groups/g1/offsets", tc.body)
			require.Equal(t, 204, code)
			require.Equal(t, "g1", fg.lastResetID["prod"])
			require.Equal(t, tc.want, fg.lastResetSpec["prod"])
			validateAgainstContract(t, req, code, hdr, body)
		})
	}
}

func TestResetConsumerGroupOffsetsInvalidBodyIs400(t *testing.T) {
	srv := newTestServer(withGroups(newFakeGroupServicer()))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/consumer-groups/g1/offsets", `{`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
}

func TestResetConsumerGroupOffsetsReadOnlyIs403(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.resetKnown["prod"] = true
	srv := newTestServer(withGroups(fg), withReadOnly("prod"))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/consumer-groups/g1/offsets",
		`{"topic":"orders","resetType":"EARLIEST"}`)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fg.lastResetID["prod"])
}

func TestResetConsumerGroupOffsetsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withGroups(newFakeGroupServicer()))
	defer srv.Close()
	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/nope/consumer-groups/g1/offsets",
		`{"topic":"orders","resetType":"EARLIEST"}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestResetConsumerGroupOffsetsBackendFailureIs500(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.resetErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withGroups(fg))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/consumer-groups/g1/offsets",
		`{"topic":"orders","resetType":"EARLIEST"}`)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to reset consumer group offsets", "kadm boom")
}

// TestResetConsumerGroupOffsetsGroupNotInactiveIs400 locks P1b final-review
// verdict②: appcluster.ErrGroupNotInactive (GroupService.Reset's new
// describe-based pre-check sentinel, returned when the target group's state
// is neither EMPTY nor DEAD) maps to 400, not the generic 500
// TestResetConsumerGroupOffsetsBackendFailureIs500 above covers -- an active
// group is a precise, actionable client error ("stop your consumers first"),
// not a backend failure. Same "skip contract ValidateResponse" convention
// as that 500 test (contract declares only 204 for this operation -- see
// ResetConsumerGroupOffsets' doc comment in handlers_group.go -- so neither
// undeclared status has a schema worth asserting against).
func TestResetConsumerGroupOffsetsGroupNotInactiveIs400(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.resetErr["prod"] = fmt.Errorf("%w: group is in STABLE state", appcluster.ErrGroupNotInactive)
	srv := newTestServer(withGroups(fg))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/consumer-groups/g1/offsets",
		`{"topic":"orders","resetType":"EARLIEST"}`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "consumer group must be inactive (EMPTY/DEAD) to reset offsets", "")
}

// --- DeleteConsumerGroupOffsets ---

func TestDeleteConsumerGroupOffsets(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.deleteOffsetsKnown["prod"] = true
	srv := newTestServer(withGroups(fg))
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/consumer-groups/g1/topics/orders", nil)
	require.Equal(t, 204, code)
	require.Equal(t, [2]string{"g1", "orders"}, fg.lastDeleteOffsets["prod"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestDeleteConsumerGroupOffsetsReadOnlyIs403(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.deleteOffsetsKnown["prod"] = true
	srv := newTestServer(withGroups(fg), withReadOnly("prod"))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/consumer-groups/g1/topics/orders", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Zero(t, fg.lastDeleteOffsets["prod"])
}

func TestDeleteConsumerGroupOffsetsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withGroups(newFakeGroupServicer()))
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/nope/consumer-groups/g1/topics/orders", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestDeleteConsumerGroupOffsetsBackendFailureIs500(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.deleteOffsetsErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withGroups(fg))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/consumer-groups/g1/topics/orders", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to delete consumer group offsets", "kadm boom")
}

// --- GetTopicConsumerGroups ---

func TestGetTopicConsumerGroups(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.forTopicResult["prod"] = []cluster.GroupState{topicProjectionGroupState()}
	srv := newTestServer(withGroups(fg))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/topics/orders/consumer-groups", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 1)
	require.Equal(t, "charge_join_meeting", got[0]["groupId"])
	require.Equal(t, float64(1), got[0]["members"])
	require.Equal(t, float64(1), got[0]["topics"])
	require.Equal(t, float64(10), got[0]["consumerLag"])
	require.NotContains(t, got[0], "partitions") // this operation's response schema is []ConsumerGroup, not Details
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConsumerGroupsPageKeepsWholeGroupCounts(t *testing.T) {
	fg := newFakeGroupServicer()
	fixture := topicProjectionGroupState()
	fg.forTopicResult["prod"] = []cluster.GroupState{fixture}
	fg.listPages["prod"] = appcluster.GroupPage{Groups: []cluster.GroupState{fixture}, PageCount: 1}
	srv := newTestServer(withGroups(fg))
	defer srv.Close()

	// First render the topic projection; it must not alter the shared fixture
	// subsequently used by the whole-group page.
	_, topicCode, _, _ := getJSON(t, srv, "/api/clusters/prod/topics/orders/consumer-groups", nil)
	require.Equal(t, 200, topicCode)

	var got map[string]any
	_, code, _, _ := getJSON(t, srv, "/api/clusters/prod/consumer-groups/paged", &got)
	require.Equal(t, 200, code)
	groups := got["consumerGroups"].([]any)
	require.Len(t, groups, 1)
	group := groups[0].(map[string]any)
	require.Equal(t, float64(3), group["members"])
	require.Equal(t, float64(2), group["topics"])
	require.Equal(t, float64(100), group["consumerLag"])
}

func TestGetTopicConsumerGroupsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withGroups(newFakeGroupServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/topics/orders/consumer-groups", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetTopicConsumerGroupsBackendFailureIs500(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.forTopicErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withGroups(fg))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/topics/orders/consumer-groups", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to list topic consumer groups", "kadm boom")
}

// --- GetConsumerGroupsLag ---

func TestGetConsumerGroupsLag(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.lagResult["prod"] = []cluster.GroupState{sampleGroupState()}
	srv := newTestServer(withGroups(fg))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/consumer-groups/lag?ids=orders-consumer", &got)
	require.Equal(t, 200, code)
	require.NotZero(t, got["updateTimestamp"])
	groups, ok := got["consumerGroups"].(map[string]any)
	require.True(t, ok)
	gl, ok := groups["orders-consumer"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(10), gl["lag"])
	topics, ok := gl["topics"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(10), topics["orders"])
	require.NotContains(t, gl, "topicPartitions") // includePartitions not requested
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, []string{"orders-consumer"}, fg.lastLagIDs["prod"])
}

func TestGetConsumerGroupsLagIncludesPartitionsWhenRequested(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.lagResult["prod"] = []cluster.GroupState{sampleGroupState()}
	srv := newTestServer(withGroups(fg))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/consumer-groups/lag?ids=orders-consumer&includePartitions=true", &got)
	require.Equal(t, 200, code)
	groups := got["consumerGroups"].(map[string]any)
	gl := groups["orders-consumer"].(map[string]any)
	tp, ok := gl["topicPartitions"].(map[string]any)
	require.True(t, ok)
	ordersTP, ok := tp["orders"].(map[string]any)
	require.True(t, ok)
	parts, ok := ordersTP["partitions"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(10), parts["0"])
	require.Equal(t, float64(0), parts["1"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConsumerGroupsLagUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withGroups(newFakeGroupServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/consumer-groups/lag?ids=g1", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConsumerGroupsLagBackendFailureIs500(t *testing.T) {
	fg := newFakeGroupServicer()
	fg.lagErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withGroups(fg))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/consumer-groups/lag?ids=g1", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to compute consumer group lag", "kadm boom")
}
