package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// This file is package cluster (white-box), not cluster_test: filterSortPageGroups
// and sortGroups are unexported pure functions — same rationale as
// topic_internal_test.go's identical split for filterSortPage/sortTopics.

func membersOfCount(n int) []cluster.GroupMember {
	ms := make([]cluster.GroupMember, n)
	for i := range ms {
		ms[i] = cluster.GroupMember{MemberID: string(rune('a' + i))}
	}
	return ms
}

func offsetsForTopics(topics ...string) []cluster.GroupOffset {
	os := make([]cluster.GroupOffset, len(topics))
	for i, t := range topics {
		os[i] = cluster.GroupOffset{Topic: t, Committed: 0, End: 0}
	}
	return os
}

func TestFilterSortPageGroups(t *testing.T) {
	cases := []struct {
		name      string
		in        []cluster.GroupState
		query     GroupPageQuery
		wantIDs   []string
		wantCount int
	}{
		{
			name:      "empty input yields an empty page and zero pageCount",
			in:        nil,
			query:     GroupPageQuery{},
			wantIDs:   []string{},
			wantCount: 0,
		},
		{
			name: "search is case-insensitive contains on ID",
			in: []cluster.GroupState{
				{ID: "OrdersConsumer"}, {ID: "payments-worker"}, {ID: "orders-eu"},
			},
			query:     GroupPageQuery{Search: "ORDER"},
			wantIDs:   []string{"OrdersConsumer", "orders-eu"}, // NAME asc default, byte-wise: 'O' < 'o'
			wantCount: 1,
		},
		{
			name: "States filter keeps only matching states",
			in: []cluster.GroupState{
				{ID: "a", State: "STABLE"}, {ID: "b", State: "DEAD"}, {ID: "c", State: "STABLE"},
			},
			query:     GroupPageQuery{States: []string{"STABLE"}},
			wantIDs:   []string{"a", "c"},
			wantCount: 1,
		},
		{
			name: "States filter with multiple values is an OR",
			in: []cluster.GroupState{
				{ID: "a", State: "STABLE"}, {ID: "b", State: "DEAD"}, {ID: "c", State: "EMPTY"},
			},
			query:     GroupPageQuery{States: []string{"STABLE", "EMPTY"}},
			wantIDs:   []string{"a", "c"},
			wantCount: 1,
		},
		{
			name: "orderBy NAME ascending (default)",
			in: []cluster.GroupState{
				{ID: "charlie"}, {ID: "alpha"}, {ID: "bravo"},
			},
			query:     GroupPageQuery{OrderBy: "NAME"},
			wantIDs:   []string{"alpha", "bravo", "charlie"},
			wantCount: 1,
		},
		{
			name: "orderBy MEMBERS ascending",
			in: []cluster.GroupState{
				{ID: "three", Members: membersOfCount(3)},
				{ID: "one", Members: membersOfCount(1)},
				{ID: "two", Members: membersOfCount(2)},
			},
			query:     GroupPageQuery{OrderBy: "MEMBERS"},
			wantIDs:   []string{"one", "two", "three"},
			wantCount: 1,
		},
		{
			name: "orderBy STATE ascending (lexical on the contract-spelled string)",
			in: []cluster.GroupState{
				{ID: "s", State: "STABLE"}, {ID: "d", State: "DEAD"}, {ID: "e", State: "EMPTY"},
			},
			query:     GroupPageQuery{OrderBy: "STATE"},
			wantIDs:   []string{"d", "e", "s"}, // DEAD < EMPTY < STABLE
			wantCount: 1,
		},
		{
			name: "orderBy TOPIC_NUM ascending",
			in: []cluster.GroupState{
				{ID: "two-topics", Offsets: offsetsForTopics("t1", "t2")},
				{ID: "one-topic", Offsets: offsetsForTopics("t1")},
			},
			query:     GroupPageQuery{OrderBy: "TOPIC_NUM"},
			wantIDs:   []string{"one-topic", "two-topics"},
			wantCount: 1,
		},
		{
			name: "orderBy MESSAGES_BEHIND ascending (total Lag())",
			in: []cluster.GroupState{
				{ID: "high-lag", Offsets: []cluster.GroupOffset{{Topic: "t1", Committed: 0, End: 100}}},
				{ID: "low-lag", Offsets: []cluster.GroupOffset{{Topic: "t1", Committed: 90, End: 100}}},
			},
			query:     GroupPageQuery{OrderBy: "MESSAGES_BEHIND"},
			wantIDs:   []string{"low-lag", "high-lag"},
			wantCount: 1,
		},
		{
			name: "sortOrder DESC reverses the ordering",
			in: []cluster.GroupState{
				{ID: "alpha"}, {ID: "bravo"}, {ID: "charlie"},
			},
			query:     GroupPageQuery{OrderBy: "NAME", SortOrder: "DESC"},
			wantIDs:   []string{"charlie", "bravo", "alpha"},
			wantCount: 1,
		},
		{
			name: "unrecognized orderBy falls back to NAME ascending",
			in: []cluster.GroupState{
				{ID: "b"}, {ID: "a"},
			},
			query:     GroupPageQuery{OrderBy: "NOT_A_REAL_COLUMN"},
			wantIDs:   []string{"a", "b"},
			wantCount: 1,
		},
		{
			name: "pagination slice boundary: page 2 of perPage 2 over 5 items",
			in: []cluster.GroupState{
				{ID: "g1"}, {ID: "g2"}, {ID: "g3"}, {ID: "g4"}, {ID: "g5"},
			},
			query:     GroupPageQuery{Page: 2, PerPage: 2},
			wantIDs:   []string{"g3", "g4"},
			wantCount: 3, // ceil(5/2)
		},
		{
			name: "out-of-range page clamps to the last real page",
			in: []cluster.GroupState{
				{ID: "g1"}, {ID: "g2"}, {ID: "g3"},
			},
			query:     GroupPageQuery{Page: 99, PerPage: 2},
			wantIDs:   []string{"g3"},
			wantCount: 2, // ceil(3/2)
		},
		{
			name: "zero Page/PerPage default to 1/25",
			in: []cluster.GroupState{
				{ID: "only"},
			},
			query:     GroupPageQuery{},
			wantIDs:   []string{"only"},
			wantCount: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := filterSortPageGroups(tc.in, tc.query)
			ids := make([]string, len(got.Groups))
			for i, g := range got.Groups {
				ids[i] = g.ID
			}
			require.Equal(t, tc.wantIDs, ids)
			require.Equal(t, tc.wantCount, got.PageCount)
		})
	}
}

func TestFilterSortPageGroupsUsesNameAscendingTieBreakBeforePagination(t *testing.T) {
	tests := []struct {
		orderBy string
		groups  []cluster.GroupState
	}{
		{
			orderBy: "MEMBERS",
			groups: []cluster.GroupState{
				{ID: "charlie", Members: membersOfCount(1)},
				{ID: "alpha", Members: membersOfCount(1)},
				{ID: "bravo", Members: membersOfCount(1)},
			},
		},
		{
			orderBy: "STATE",
			groups: []cluster.GroupState{
				{ID: "charlie", State: "STABLE"},
				{ID: "alpha", State: "STABLE"},
				{ID: "bravo", State: "STABLE"},
			},
		},
		{
			orderBy: "MESSAGES_BEHIND",
			groups: []cluster.GroupState{
				{ID: "charlie", Offsets: []cluster.GroupOffset{{Topic: "t", Committed: 0, End: 1}}},
				{ID: "alpha", Offsets: []cluster.GroupOffset{{Topic: "t", Committed: 0, End: 1}}},
				{ID: "bravo", Offsets: []cluster.GroupOffset{{Topic: "t", Committed: 0, End: 1}}},
			},
		},
		{
			orderBy: "TOPIC_NUM",
			groups: []cluster.GroupState{
				{ID: "charlie", Offsets: offsetsForTopics("t")},
				{ID: "alpha", Offsets: offsetsForTopics("t")},
				{ID: "bravo", Offsets: offsetsForTopics("t")},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.orderBy, func(t *testing.T) {
			page := filterSortPageGroups(test.groups, GroupPageQuery{
				Page: 1, PerPage: 2, OrderBy: test.orderBy, SortOrder: "DESC",
			})
			require.Equal(t, []cluster.GroupState{test.groups[1], test.groups[2]}, page.Groups)
			require.Equal(t, 2, page.PageCount)
		})
	}
}
