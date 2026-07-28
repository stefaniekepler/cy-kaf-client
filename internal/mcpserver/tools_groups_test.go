package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

type recordingGroupApp struct {
	mu sync.Mutex

	page        appcluster.GroupPage
	pages       map[int]appcluster.GroupPage
	maxPageSize int
	group       domaincluster.GroupState
	lag         []domaincluster.GroupState
	forTopic    []domaincluster.GroupState
	pageErr     error
	getErr      error
	lagErr      error
	topicErr    error
	resetErr    error
	deleteErr   error
	offsetErr   error

	calls      []string
	queries    []appcluster.GroupPageQuery
	lagIDs     [][]string
	resetSpecs []domaincluster.ResetSpec
}

func (f *recordingGroupApp) Page(_ context.Context, clusterName string, query appcluster.GroupPageQuery) (appcluster.GroupPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "groups.page("+clusterName+")")
	f.queries = append(f.queries, cloneGroupPageQuery(query))
	if f.maxPageSize > 0 && query.PerPage > f.maxPageSize {
		return appcluster.GroupPage{}, fmt.Errorf(
			"requested %d rows, maximum is %d",
			query.PerPage,
			f.maxPageSize,
		)
	}
	if f.pages != nil {
		return cloneGroupPage(f.pages[query.Page]), f.pageErr
	}
	return cloneGroupPage(f.page), f.pageErr
}

func (f *recordingGroupApp) Get(_ context.Context, clusterName, id string) (domaincluster.GroupState, error) {
	f.record("groups.get(" + clusterName + "," + id + ")")
	return f.group, f.getErr
}

func (f *recordingGroupApp) Lag(_ context.Context, clusterName string, ids []string) ([]domaincluster.GroupState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "groups.lag("+clusterName+")")
	f.lagIDs = append(f.lagIDs, append([]string(nil), ids...))
	return append([]domaincluster.GroupState(nil), f.lag...), f.lagErr
}

func (f *recordingGroupApp) ForTopic(_ context.Context, clusterName, topicName string) ([]domaincluster.GroupState, error) {
	f.record("groups.topic(" + clusterName + "," + topicName + ")")
	return append([]domaincluster.GroupState(nil), f.forTopic...), f.topicErr
}

func (f *recordingGroupApp) Reset(_ context.Context, clusterName, id string, spec domaincluster.ResetSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "groups.reset("+clusterName+","+id+")")
	f.resetSpecs = append(f.resetSpecs, cloneResetSpec(spec))
	return f.resetErr
}

func (f *recordingGroupApp) Delete(_ context.Context, clusterName, id string) error {
	f.record("groups.delete(" + clusterName + "," + id + ")")
	return f.deleteErr
}

func (f *recordingGroupApp) DeleteOffsets(_ context.Context, clusterName, id, topicName string) error {
	f.record("groups.deleteOffsets(" + clusterName + "," + id + "," + topicName + ")")
	return f.offsetErr
}

func (f *recordingGroupApp) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *recordingGroupApp) snapshot() (calls []string, queries []appcluster.GroupPageQuery, lagIDs [][]string, specs []domaincluster.ResetSpec) {
	f.mu.Lock()
	defer f.mu.Unlock()
	calls = append([]string(nil), f.calls...)
	for _, query := range f.queries {
		queries = append(queries, cloneGroupPageQuery(query))
	}
	for _, ids := range f.lagIDs {
		lagIDs = append(lagIDs, append([]string(nil), ids...))
	}
	for _, spec := range f.resetSpecs {
		specs = append(specs, cloneResetSpec(spec))
	}
	return calls, queries, lagIDs, specs
}

func TestConsumerGroupToolsDelegateAllEightOperationsThroughApplicationPort(t *testing.T) {
	tests := []struct {
		name       string
		input      map[string]any
		wantCall   string
		wantAccess AccessClass
		wantJSON   string
		dynamicLag bool
	}{
		{
			name: "getConsumerGroup",
			input: map[string]any{
				"clusterName": "prod",
				"id":          "group-a",
			},
			wantCall:   "groups.get(prod,group-a)",
			wantAccess: AccessReadOnly,
			wantJSON: `{
				"consumerLag":20,
				"coordinator":{"host":"broker-7.example.test","id":7},
				"groupId":"group-a",
				"inherit":"details",
				"members":1,
				"partitionAssignor":"range",
				"state":"STABLE",
				"topics":2,
				"partitions":[
					{"consumerId":"member-a","consumerLag":10,"currentOffset":90,"endOffset":100,"host":"192.0.2.5","partition":0,"topic":"orders"},
					{"consumerId":"member-a","consumerLag":0,"currentOffset":100,"endOffset":100,"host":"192.0.2.5","partition":1,"topic":"orders"},
					{"consumerLag":10,"currentOffset":40,"endOffset":50,"partition":0,"topic":"payments"}
				]
			}`,
		},
		{
			name: "getConsumerGroupsLag",
			input: map[string]any{
				"clusterName": "prod",
				"query": map[string]any{
					"ids":               []any{"group-a"},
					"includePartitions": true,
				},
			},
			wantCall:   "groups.lag(prod)",
			wantAccess: AccessReadOnly,
			dynamicLag: true,
		},
		{
			name: "getTopicConsumerGroups",
			input: map[string]any{
				"clusterName": "prod",
				"topicName":   "orders",
			},
			wantCall:   "groups.topic(prod,orders)",
			wantAccess: AccessReadOnly,
			wantJSON: `{"result":[{
				"consumerLag":10,
				"coordinator":{"host":"broker-7.example.test","id":7},
				"groupId":"group-a",
				"inherit":"ConsumerGroup",
				"members":1,
				"partitionAssignor":"range",
				"state":"STABLE",
				"topics":1
			}]}`,
		},
		{
			name:       "getConsumerGroupsPage",
			input:      map[string]any{"clusterName": "prod"},
			wantCall:   "groups.page(prod)",
			wantAccess: AccessReadOnly,
			wantJSON: `{
				"items":[{
					"consumerLag":20,
					"coordinator":{"host":"broker-7.example.test","id":7},
					"groupId":"group-a",
					"inherit":"ConsumerGroup",
					"members":1,
					"partitionAssignor":"range",
					"state":"STABLE",
					"topics":2
				}],
				"page":1,
				"pageCount":1,
				"truncated":false
			}`,
		},
		{
			name:       "getConsumerGroupsCsv",
			input:      map[string]any{"clusterName": "prod"},
			wantCall:   "groups.page(prod)",
			wantAccess: AccessReadOnly,
			wantJSON: `{
				"result":"groupId,state,members,topics,partitionAssignor,coordinatorId,consumerLag\ngroup-a,STABLE,1,2,range,7,20\n"
			}`,
		},
		{
			name: "deleteConsumerGroup",
			input: map[string]any{
				"clusterName": "prod",
				"id":          "group-a",
			},
			wantCall:   "groups.delete(prod,group-a)",
			wantAccess: AccessWrite,
			wantJSON:   `{"result":null}`,
		},
		{
			name: "deleteConsumerGroupOffsets",
			input: map[string]any{
				"clusterName": "prod",
				"id":          "group-a",
				"topicName":   "orders",
			},
			wantCall:   "groups.deleteOffsets(prod,group-a,orders)",
			wantAccess: AccessWrite,
			wantJSON:   `{"result":null}`,
		},
		{
			name: "resetConsumerGroupOffsets",
			input: map[string]any{
				"clusterName": "prod",
				"id":          "group-a",
				"body": map[string]any{
					"topic":     "orders",
					"resetType": "OFFSET",
					"partitionsOffsets": []any{
						map[string]any{"partition": float64(0), "offset": float64(5)},
						map[string]any{"partition": float64(1), "offset": float64(10)},
					},
				},
			},
			wantCall:   "groups.reset(prod,group-a)",
			wantAccess: AccessWrite,
			wantJSON:   `{"result":null}`,
		},
	}
	require.Len(t, tests, 8)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingGroupApp()
			executor, _ := newGroupExecutor(t, app, &recordingReadOnlyResolver{}, true)
			spec := requireCatalogSpec(t, test.name)
			require.Equal(t, test.wantAccess, spec.Meta.Access)

			result, err := newSDKSession(t, spec, executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{Name: test.name, Arguments: test.input},
			)
			require.NoError(t, err)
			require.False(t, result.IsError, callToolText(t, result))
			if test.dynamicLag {
				requireExactConsumerGroupLagResult(t, result)
			} else {
				require.JSONEq(t, test.wantJSON, callToolText(t, result))
				requireStructuredJSONEq(t, test.wantJSON, result.StructuredContent)
			}
			calls, _, _, _ := app.snapshot()
			require.Equal(t, []string{test.wantCall}, calls)
			require.NotContains(t, callToolText(t, result), "client-secret-marker")
		})
	}
}

func TestConsumerGroupToolsKeepExactlyFiveReadsDefaultVisible(t *testing.T) {
	app := newRecordingGroupApp()
	executor, store := newGroupExecutor(t, app, &recordingReadOnlyResolver{}, false)
	session := newSDKCatalogSession(t, VisibleCatalog(*mustLoadPolicy(t, store)), executor)

	listed, err := session.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 51)
	for _, name := range []string{
		"getConsumerGroup",
		"getConsumerGroupsLag",
		"getTopicConsumerGroups",
		"getConsumerGroupsPage",
		"getConsumerGroupsCsv",
	} {
		require.NotNil(t, findTool(t, listed.Tools, name))
	}
	for _, name := range []string{
		"deleteConsumerGroup",
		"deleteConsumerGroupOffsets",
		"resetConsumerGroupOffsets",
	} {
		require.Nil(t, findToolOrNil(listed.Tools, name))
	}
}

func TestConsumerGroupToolsPaginationDefaultsBoundsAndMetadata(t *testing.T) {
	t.Run("defaults and metadata", func(t *testing.T) {
		app := newRecordingGroupApp()
		app.page = appcluster.GroupPage{
			Groups: []domaincluster.GroupState{
				{ID: "zeta", State: "STABLE", CoordinatorID: 1},
				{ID: "alpha", State: "EMPTY", CoordinatorID: 2},
			},
			PageCount: 3,
		}
		result := callGroupTool(t, app, "getConsumerGroupsPage", map[string]any{
			"clusterName": "prod",
		}, false)

		_, queries, _, _ := app.snapshot()
		require.Equal(t, []appcluster.GroupPageQuery{{Page: 1, PerPage: 25}}, queries)
		want := `{
			"items":[
				{"coordinator":{"id":2},"groupId":"alpha","inherit":"ConsumerGroup","state":"EMPTY"},
				{"coordinator":{"id":1},"groupId":"zeta","inherit":"ConsumerGroup","state":"STABLE"}
			],
			"page":1,
			"pageCount":3,
			"truncated":true
		}`
		require.JSONEq(t, want, callToolText(t, result))
		requireStructuredJSONEq(t, want, result.StructuredContent)
	})

	t.Run("empty result normalizes requested page to one", func(t *testing.T) {
		app := newRecordingGroupApp()
		app.page = appcluster.GroupPage{}
		result := callGroupTool(t, app, "getConsumerGroupsPage", map[string]any{
			"clusterName": "prod",
			"query":       map[string]any{"page": float64(7)},
		}, false)
		want := `{"items":[],"page":1,"pageCount":0,"truncated":false}`
		require.JSONEq(t, want, callToolText(t, result))
		requireStructuredJSONEq(t, want, result.StructuredContent)
	})

	t.Run("out of range page clamps to last real page", func(t *testing.T) {
		app := newRecordingGroupApp()
		app.page = appcluster.GroupPage{
			Groups:    []domaincluster.GroupState{{ID: "last", CoordinatorID: 1}},
			PageCount: 2,
		}
		result := callGroupTool(t, app, "getConsumerGroupsPage", map[string]any{
			"clusterName": "prod",
			"query":       map[string]any{"page": float64(7)},
		}, false)
		want := `{
			"items":[{"coordinator":{"id":1},"groupId":"last","inherit":"ConsumerGroup"}],
			"page":2,
			"pageCount":2,
			"truncated":false
		}`
		require.JSONEq(t, want, callToolText(t, result))
		requireStructuredJSONEq(t, want, result.StructuredContent)
	})

	for _, perPage := range []float64{0, 101} {
		t.Run(fmt.Sprintf("reject perPage %.0f", perPage), func(t *testing.T) {
			app := newRecordingGroupApp()
			result := callGroupToolError(t, app, "getConsumerGroupsPage", map[string]any{
				"clusterName": "prod",
				"query":       map[string]any{"perPage": perPage},
			}, false)
			require.Equal(t, "invalid_request", callToolText(t, result))
			calls, _, _, _ := app.snapshot()
			require.Empty(t, calls)
		})
	}

	for _, query := range []map[string]any{
		{"page": float64(0)},
		{"orderBy": "NOT_REAL"},
		{"sortOrder": "NOT_REAL"},
		{"state": []any{"NOT_REAL"}},
	} {
		t.Run(fmt.Sprintf("reject invalid query %v", query), func(t *testing.T) {
			app := newRecordingGroupApp()
			result := callGroupToolError(t, app, "getConsumerGroupsPage", map[string]any{
				"clusterName": "prod",
				"query":       query,
			}, false)
			require.Equal(t, "invalid_request", callToolText(t, result))
			calls, _, _, _ := app.snapshot()
			require.Empty(t, calls)
		})
	}

	t.Run("accept complete bounded query", func(t *testing.T) {
		app := newRecordingGroupApp()
		callGroupTool(t, app, "getConsumerGroupsPage", map[string]any{
			"clusterName": "prod",
			"query": map[string]any{
				"page":      float64(2),
				"perPage":   float64(100),
				"search":    "worker",
				"orderBy":   "STATE",
				"sortOrder": "DESC",
				"state":     []any{"STABLE", "EMPTY"},
				"fts":       true,
			},
		}, false)
		_, queries, _, _ := app.snapshot()
		require.Equal(t, []appcluster.GroupPageQuery{{
			Page: 2, PerPage: 100, Search: "worker", OrderBy: "STATE",
			SortOrder: "DESC", States: []string{"STABLE", "EMPTY"},
		}}, queries)
	})
}

func TestConsumerGroupToolsLagValidatesCapsAndStableDeduplicatesIDs(t *testing.T) {
	t.Run("stable dedupe and exact summary", func(t *testing.T) {
		app := newRecordingGroupApp()
		app.lag = nil
		result := callGroupTool(t, app, "getConsumerGroupsLag", map[string]any{
			"clusterName": "prod",
			"query": map[string]any{
				"ids":        []any{"group-b", "group-a", "group-b"},
				"lastUpdate": float64(123),
			},
		}, false)
		_, _, lagIDs, _ := app.snapshot()
		require.Equal(t, [][]string{{"group-b", "group-a"}}, lagIDs)

		var output generated.ConsumerGroupsLagResponse
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &output))
		require.NotZero(t, output.UpdateTimestamp)
		require.Empty(t, output.ConsumerGroups)
		require.NotContains(t, callToolText(t, result), "topicPartitions")
	})

	t.Run("accept exactly one hundred IDs", func(t *testing.T) {
		app := newRecordingGroupApp()
		ids := make([]any, 100)
		for index := range ids {
			ids[index] = fmt.Sprintf("group-%03d", index)
		}
		callGroupTool(t, app, "getConsumerGroupsLag", map[string]any{
			"clusterName": "prod",
			"query":       map[string]any{"ids": ids},
		}, false)
		_, _, lagIDs, _ := app.snapshot()
		require.Len(t, lagIDs[0], 100)
	})

	for _, test := range []struct {
		name string
		ids  []any
	}{
		{name: "empty", ids: []any{}},
		{name: "blank", ids: []any{" "}},
		{name: "too many", ids: groupIDValues(101)},
	} {
		t.Run("reject "+test.name, func(t *testing.T) {
			app := newRecordingGroupApp()
			result := callGroupToolError(t, app, "getConsumerGroupsLag", map[string]any{
				"clusterName": "prod",
				"query":       map[string]any{"ids": test.ids},
			}, false)
			require.Equal(t, "invalid_request", callToolText(t, result))
			calls, _, _, _ := app.snapshot()
			require.Empty(t, calls)
		})
	}
}

func TestConsumerGroupToolsCsvIsIndependentBoundedAndDeterministic(t *testing.T) {
	app := newRecordingGroupApp()
	app.maxPageSize = maxMCPGroupPerPage
	app.pages = make(map[int]appcluster.GroupPage)
	for pageNumber := 1; pageNumber <= 6; pageNumber++ {
		groups := make([]domaincluster.GroupState, maxMCPGroupPerPage)
		for index := range groups {
			id := pageNumber*maxMCPGroupPerPage - index - 1
			groups[index] = domaincluster.GroupState{
				ID:            fmt.Sprintf("group-%03d", id),
				State:         "STABLE",
				CoordinatorID: 7,
			}
		}
		app.pages[pageNumber] = appcluster.GroupPage{
			Groups: groups, PageCount: 6,
		}
	}

	result := callGroupTool(t, app, "getConsumerGroupsCsv", map[string]any{
		"clusterName": "prod",
		"query": map[string]any{
			"page":      float64(7),
			"perPage":   float64(1),
			"orderBy":   "STATE",
			"sortOrder": "DESC",
			"search":    "group",
			"state":     []any{"STABLE"},
		},
	}, false)
	var envelope struct {
		Result string `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &envelope))

	_, queries, _, _ := app.snapshot()
	require.Len(t, queries, 5)
	for index, query := range queries {
		require.Equal(t, appcluster.GroupPageQuery{
			Page: index + 1, PerPage: maxMCPGroupPerPage, Search: "group",
			OrderBy: "STATE", SortOrder: "DESC", States: []string{"STABLE"},
		}, query)
	}

	lines := strings.Split(strings.TrimSpace(envelope.Result), "\n")
	require.Len(t, lines, maxListItems+1)
	require.Equal(t, "groupId,state,members,topics,partitionAssignor,coordinatorId,consumerLag", lines[0])
	for index := 0; index < maxListItems; index++ {
		require.Equal(t, fmt.Sprintf("group-%03d,STABLE,,,,7,", index), lines[index+1])
	}
	require.NotContains(t, envelope.Result, "group-500")

	t.Run("empty page ends an inconsistent zero-page-count response", func(t *testing.T) {
		app := newRecordingGroupApp()
		app.maxPageSize = maxMCPGroupPerPage
		groups := make([]domaincluster.GroupState, maxMCPGroupPerPage)
		for index := range groups {
			groups[index] = domaincluster.GroupState{
				ID: fmt.Sprintf("group-%03d", index), State: "STABLE", CoordinatorID: 7,
			}
		}
		app.pages = map[int]appcluster.GroupPage{
			1: {
				Groups: groups, PageCount: 0,
			},
		}
		result := callGroupTool(t, app, "getConsumerGroupsCsv", map[string]any{
			"clusterName": "prod",
		}, false)
		var envelope struct {
			Result string `json:"result"`
		}
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &envelope))
		require.Contains(t, envelope.Result, "group-000")
		require.Contains(t, envelope.Result, "group-099")
		_, queries, _, _ := app.snapshot()
		require.Len(t, queries, 2)
		require.Equal(t, 1, queries[0].Page)
		require.Equal(t, maxMCPGroupPerPage, queries[0].PerPage)
		require.Equal(t, 2, queries[1].Page)
		require.Equal(t, maxMCPGroupPerPage, queries[1].PerPage)
	})

	t.Run("underreported page count does not silently truncate later rows", func(t *testing.T) {
		app := newRecordingGroupApp()
		app.maxPageSize = maxMCPGroupPerPage
		firstPage := make([]domaincluster.GroupState, maxMCPGroupPerPage)
		for index := range firstPage {
			firstPage[index] = domaincluster.GroupState{
				ID: fmt.Sprintf("group-%03d", index), State: "STABLE", CoordinatorID: 7,
			}
		}
		app.pages = map[int]appcluster.GroupPage{
			1: {Groups: firstPage, PageCount: 1},
			2: {
				Groups: []domaincluster.GroupState{
					{ID: "group-100", State: "STABLE", CoordinatorID: 7},
				},
				PageCount: 1,
			},
		}

		result := callGroupTool(t, app, "getConsumerGroupsCsv", map[string]any{
			"clusterName": "prod",
		}, false)
		var envelope struct {
			Result string `json:"result"`
		}
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &envelope))
		lines := strings.Split(strings.TrimSpace(envelope.Result), "\n")
		require.Len(t, lines, 102)
		require.Equal(t, "group-100,STABLE,,,,7,", lines[101])
		_, queries, _, _ := app.snapshot()
		require.Len(t, queries, 2)
	})

	t.Run("encoded CSV bytes stay within the shared one MiB limit", func(t *testing.T) {
		app := newRecordingGroupApp()
		app.maxPageSize = maxMCPGroupPerPage
		app.pages = make(map[int]appcluster.GroupPage)
		for pageNumber := 1; pageNumber <= 5; pageNumber++ {
			groups := make([]domaincluster.GroupState, maxMCPGroupPerPage)
			for index := range groups {
				groups[index] = domaincluster.GroupState{
					ID: fmt.Sprintf(
						"%03d-%s",
						(pageNumber-1)*maxMCPGroupPerPage+index,
						strings.Repeat("g", maxClusterBrokerNameBytes-4),
					),
					State:         strings.Repeat("s", maxClusterBrokerNameBytes),
					Protocol:      strings.Repeat("p", maxClusterBrokerNameBytes),
					CoordinatorID: int32(index),
				}
			}
			app.pages[pageNumber] = appcluster.GroupPage{
				Groups: groups, PageCount: 5,
			}
		}
		result := callGroupToolError(t, app, "getConsumerGroupsCsv", map[string]any{
			"clusterName": "prod",
		}, false)
		require.Equal(t, "result_too_large", callToolText(t, result))
	})
}

func TestConsumerGroupToolsResetConvertsEveryStrategy(t *testing.T) {
	tests := []struct {
		name string
		body map[string]any
		want domaincluster.ResetSpec
	}{
		{
			name: "EARLIEST",
			body: map[string]any{
				"topic": "orders", "resetType": "EARLIEST",
				"partitions": []any{float64(0), float64(2)},
			},
			want: domaincluster.ResetSpec{
				Topic: "orders", ResetType: "EARLIEST", Partitions: []int32{0, 2},
			},
		},
		{
			name: "LATEST",
			body: map[string]any{"topic": "orders", "resetType": "LATEST"},
			want: domaincluster.ResetSpec{Topic: "orders", ResetType: "LATEST"},
		},
		{
			name: "OFFSET",
			body: map[string]any{
				"topic": "orders", "resetType": "OFFSET",
				"partitionsOffsets": []any{
					map[string]any{"partition": float64(1), "offset": float64(10)},
					map[string]any{"partition": float64(0), "offset": float64(5)},
				},
			},
			want: domaincluster.ResetSpec{
				Topic: "orders", ResetType: "OFFSET",
				PartitionsOffsets: map[int32]int64{0: 5, 1: 10},
			},
		},
		{
			name: "TIMESTAMP",
			body: map[string]any{
				"topic": "orders", "resetType": "TIMESTAMP",
				"partitions":       []any{float64(0)},
				"resetToTimestamp": float64(1700000000000),
			},
			want: domaincluster.ResetSpec{
				Topic: "orders", ResetType: "TIMESTAMP",
				Partitions: []int32{0}, Timestamp: 1700000000000,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingGroupApp()
			result := callGroupTool(t, app, "resetConsumerGroupOffsets", map[string]any{
				"clusterName": "prod",
				"id":          "group-a",
				"body":        test.body,
			}, true)
			require.JSONEq(t, `{"result":null}`, callToolText(t, result))
			requireStructuredJSONEq(t, `{"result":null}`, result.StructuredContent)
			_, _, _, specs := app.snapshot()
			require.Equal(t, []domaincluster.ResetSpec{test.want}, specs)
		})
	}
}

func TestConsumerGroupToolsResetDistinguishesOmittedAndExplicitEmptyPartitions(t *testing.T) {
	// Raw argument maps pass through the official MCP SDK schema decoder here,
	// proving that omission and an explicit [] survive as distinct body states.
	tests := []struct {
		name      string
		resetType string
		timestamp bool
	}{
		{name: "EARLIEST", resetType: "EARLIEST"},
		{name: "LATEST", resetType: "LATEST"},
		{name: "TIMESTAMP", resetType: "TIMESTAMP", timestamp: true},
	}

	for _, test := range tests {
		t.Run(test.name+" omitted means all partitions", func(t *testing.T) {
			app := newRecordingGroupApp()
			body := map[string]any{
				"topic": "orders", "resetType": test.resetType,
			}
			if test.timestamp {
				body["resetToTimestamp"] = float64(1700000000000)
			}
			result := callGroupTool(t, app, "resetConsumerGroupOffsets", map[string]any{
				"clusterName": "prod",
				"id":          "group-a",
				"body":        body,
			}, true)
			require.JSONEq(t, `{"result":null}`, callToolText(t, result))
			_, _, _, specs := app.snapshot()
			require.Len(t, specs, 1)
			require.Nil(t, specs[0].Partitions)
		})

		t.Run(test.name+" explicit empty is rejected by the adapter", func(t *testing.T) {
			app := newRecordingGroupApp()
			body := map[string]any{
				"topic": "orders", "resetType": test.resetType,
				"partitions": []any{},
			}
			if test.timestamp {
				body["resetToTimestamp"] = float64(1700000000000)
			}
			result := callGroupToolError(t, app, "resetConsumerGroupOffsets", map[string]any{
				"clusterName": "prod",
				"id":          "group-a",
				"body":        body,
			}, true)
			require.Equal(t, "invalid_request", callToolText(t, result))
			calls, _, _, _ := app.snapshot()
			require.Empty(t, calls)
		})
	}
}

func TestConsumerGroupToolsRejectInvalidResetBeforeApplicationPort(t *testing.T) {
	tooManyPartitions := make([]any, maxListItems+1)
	tooManyOffsets := make([]any, maxListItems+1)
	for index := range tooManyPartitions {
		tooManyPartitions[index] = float64(index)
		tooManyOffsets[index] = map[string]any{
			"partition": float64(index),
			"offset":    float64(index),
		}
	}
	tests := []struct {
		name string
		id   string
		body map[string]any
	}{
		{name: "unknown enum", id: "g", body: map[string]any{"topic": "orders", "resetType": "NOPE"}},
		{name: "blank group", id: " ", body: map[string]any{"topic": "orders", "resetType": "LATEST"}},
		{name: "invalid topic", id: "g", body: map[string]any{"topic": "bad topic", "resetType": "LATEST"}},
		{name: "negative partition", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "EARLIEST", "partitions": []any{float64(-1)},
		}},
		{name: "duplicate partition", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "LATEST", "partitions": []any{float64(1), float64(1)},
		}},
		{name: "too many partitions", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "EARLIEST", "partitions": tooManyPartitions,
		}},
		{name: "too many partition offsets", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "OFFSET", "partitionsOffsets": tooManyOffsets,
		}},
		{name: "earliest with timestamp", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "EARLIEST", "resetToTimestamp": float64(1),
		}},
		{name: "latest with offsets", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "LATEST",
			"partitionsOffsets": []any{map[string]any{"partition": float64(0), "offset": float64(1)}},
		}},
		{name: "offset missing offsets", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "OFFSET",
		}},
		{name: "offset empty offsets", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "OFFSET", "partitionsOffsets": []any{},
		}},
		{name: "offset with partitions", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "OFFSET", "partitions": []any{float64(0)},
			"partitionsOffsets": []any{map[string]any{"partition": float64(0), "offset": float64(1)}},
		}},
		{name: "offset missing value", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "OFFSET",
			"partitionsOffsets": []any{map[string]any{"partition": float64(0)}},
		}},
		{name: "offset negative partition", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "OFFSET",
			"partitionsOffsets": []any{map[string]any{"partition": float64(-1), "offset": float64(1)}},
		}},
		{name: "offset negative value", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "OFFSET",
			"partitionsOffsets": []any{map[string]any{"partition": float64(0), "offset": float64(-1)}},
		}},
		{name: "offset duplicate partition", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "OFFSET",
			"partitionsOffsets": []any{
				map[string]any{"partition": float64(0), "offset": float64(1)},
				map[string]any{"partition": float64(0), "offset": float64(2)},
			},
		}},
		{name: "timestamp missing value", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "TIMESTAMP",
		}},
		{name: "timestamp negative", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "TIMESTAMP", "resetToTimestamp": float64(-1),
		}},
		{name: "timestamp with offsets", id: "g", body: map[string]any{
			"topic": "orders", "resetType": "TIMESTAMP", "resetToTimestamp": float64(1),
			"partitionsOffsets": []any{map[string]any{"partition": float64(0), "offset": float64(1)}},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingGroupApp()
			result := callGroupToolError(t, app, "resetConsumerGroupOffsets", map[string]any{
				"clusterName": "prod",
				"id":          test.id,
				"body":        test.body,
			}, true)
			require.Equal(t, "invalid_request", callToolText(t, result))
			calls, _, _, _ := app.snapshot()
			require.Empty(t, calls)
		})
	}
}

func TestConsumerGroupToolsRejectUnboundedNestedGroupResults(t *testing.T) {
	app := newRecordingGroupApp()
	app.group.Offsets = make([]domaincluster.GroupOffset, maxListItems+1)
	for index := range app.group.Offsets {
		app.group.Offsets[index] = domaincluster.GroupOffset{
			Topic: "orders", Partition: int32(index), Committed: 0, End: 1,
		}
	}
	result := callGroupToolError(t, app, "getConsumerGroup", map[string]any{
		"clusterName": "prod",
		"id":          "group-a",
	}, false)
	require.Equal(t, "result_too_large", callToolText(t, result))
	calls, _, _, _ := app.snapshot()
	require.Equal(t, []string{"groups.get(prod,group-a)"}, calls)
}

func TestConsumerGroupToolsWritesUseReadOnlyAndStalePolicyGates(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
	}{
		{
			name: "deleteConsumerGroup",
			input: map[string]any{
				"clusterName": "prod", "id": "group-a",
			},
		},
		{
			name: "deleteConsumerGroupOffsets",
			input: map[string]any{
				"clusterName": "prod", "id": "group-a", "topicName": "orders",
			},
		},
		{
			name: "resetConsumerGroupOffsets",
			input: map[string]any{
				"clusterName": "prod", "id": "group-a",
				"body": map[string]any{"topic": "orders", "resetType": "LATEST"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name+" read-only cluster", func(t *testing.T) {
			app := newRecordingGroupApp()
			resolver := &recordingReadOnlyResolver{readOnly: true}
			executor, _ := newGroupExecutor(t, app, resolver, true)
			result, err := newSDKSession(t, requireCatalogSpec(t, test.name), executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{Name: test.name, Arguments: test.input},
			)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "cluster_read_only", callToolText(t, result))
			require.Equal(t, []string{"prod"}, resolver.recordedClusters())
			calls, _, _, _ := app.snapshot()
			require.Empty(t, calls)
		})

		t.Run(test.name+" stale policy", func(t *testing.T) {
			app := newRecordingGroupApp()
			executor, store := newGroupExecutor(t, app, &recordingReadOnlyResolver{}, true)
			session := newSDKSession(t, requireCatalogSpec(t, test.name), executor)

			first, err := session.CallTool(context.Background(), &mcp.CallToolParams{
				Name: test.name, Arguments: test.input,
			})
			require.NoError(t, err)
			require.False(t, first.IsError, callToolText(t, first))

			require.NoError(t, store.Save(context.Background(), *policy(true, false)))
			second, err := session.CallTool(context.Background(), &mcp.CallToolParams{
				Name: test.name, Arguments: test.input,
			})
			require.NoError(t, err)
			require.True(t, second.IsError)
			require.Equal(t, "writes_disabled", callToolText(t, second))
			calls, _, _, _ := app.snapshot()
			require.Len(t, calls, 1)
		})
	}
}

func TestConsumerGroupToolsValidateNamesAndMapErrorsThroughCentralCodes(t *testing.T) {
	t.Run("invalid names do not call application port", func(t *testing.T) {
		tests := []struct {
			name  string
			input map[string]any
		}{
			{name: "getConsumerGroup", input: map[string]any{"clusterName": "prod", "id": " "}},
			{name: "getTopicConsumerGroups", input: map[string]any{"clusterName": "prod", "topicName": "bad topic"}},
			{name: "deleteConsumerGroup", input: map[string]any{"clusterName": "prod", "id": " "}},
			{name: "deleteConsumerGroupOffsets", input: map[string]any{
				"clusterName": "prod", "id": "g", "topicName": "bad topic",
			}},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				app := newRecordingGroupApp()
				result := callGroupToolError(t, app, test.name, test.input, true)
				require.Equal(t, "invalid_request", callToolText(t, result))
				calls, _, _, _ := app.snapshot()
				require.Empty(t, calls)
			})
		}
	})

	t.Run("unknown cluster stays distinct", func(t *testing.T) {
		app := newRecordingGroupApp()
		app.getErr = appcluster.ErrUnknownCluster
		result := callGroupToolError(t, app, "getConsumerGroup", map[string]any{
			"clusterName": "missing", "id": "group-a",
		}, false)
		require.Equal(t, "cluster_not_found", callToolText(t, result))
	})

	t.Run("raw group error is not exposed", func(t *testing.T) {
		app := newRecordingGroupApp()
		app.getErr = errors.New("SASL password=credential-marker group missing")
		result := callGroupToolError(t, app, "getConsumerGroup", map[string]any{
			"clusterName": "prod", "id": "missing",
		}, false)
		require.Equal(t, "operation_failed", callToolText(t, result))
		require.NotContains(t, callToolText(t, result), "credential-marker")
	})

	t.Run("not configured dependency is safe operation failure", func(t *testing.T) {
		executor, _ := newGroupExecutor(t, nil, &recordingReadOnlyResolver{}, false)
		result, err := newSDKSession(t, requireCatalogSpec(t, "getConsumerGroup"), executor).CallTool(
			context.Background(),
			&mcp.CallToolParams{Name: "getConsumerGroup", Arguments: map[string]any{
				"clusterName": "prod", "id": "group-a",
			}},
		)
		require.NoError(t, err)
		require.True(t, result.IsError)
		require.Equal(t, "operation_failed", callToolText(t, result))
	})
}

func requireExactConsumerGroupLagResult(t *testing.T, result *mcp.CallToolResult) {
	t.Helper()
	var output generated.ConsumerGroupsLagResponse
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &output))
	require.NotZero(t, output.UpdateTimestamp)
	want := fmt.Sprintf(`{
		"consumerGroups":{
			"group-a":{
				"lag":20,
				"topics":{"orders":10,"payments":10},
				"topicPartitions":{
					"orders":{"partitions":{"0":10,"1":0}},
					"payments":{"partitions":{"0":10}}
				}
			}
		},
		"updateTimestamp":%d
	}`, output.UpdateTimestamp)
	require.JSONEq(t, want, callToolText(t, result))
	requireStructuredJSONEq(t, want, result.StructuredContent)
}

func newRecordingGroupApp() *recordingGroupApp {
	fixture := consumerGroupFixture()
	return &recordingGroupApp{
		page:     appcluster.GroupPage{Groups: []domaincluster.GroupState{fixture}, PageCount: 1},
		group:    fixture,
		lag:      []domaincluster.GroupState{fixture},
		forTopic: []domaincluster.GroupState{fixture},
	}
}

func consumerGroupFixture() domaincluster.GroupState {
	return domaincluster.GroupState{
		ID:            "group-a",
		State:         "STABLE",
		Coordinator:   "broker-7.example.test",
		CoordinatorID: 7,
		Protocol:      "range",
		Members: []domaincluster.GroupMember{{
			MemberID: "member-a",
			ClientID: "client-secret-marker",
			Host:     "192.0.2.5",
			Assignments: []domaincluster.TopicPartitions{{
				Topic: "orders", Partitions: []int32{0, 1},
			}},
		}},
		Offsets: []domaincluster.GroupOffset{
			{Topic: "payments", Partition: 0, Committed: 40, End: 50},
			{Topic: "orders", Partition: 1, Committed: 100, End: 100},
			{Topic: "orders", Partition: 0, Committed: 90, End: 100},
		},
	}
}

func newGroupExecutor(
	t *testing.T,
	app GroupServicer,
	resolver *recordingReadOnlyResolver,
	allowWrites bool,
) (*Executor, *mcppolicy.Store) {
	t.Helper()
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, allowWrites)))
	executor, err := NewExecutor(Dependencies{
		Groups:     app,
		Policy:     store,
		IsReadOnly: resolver.Resolve,
	})
	require.NoError(t, err)
	return executor, store
}

func callGroupTool(
	t *testing.T,
	app *recordingGroupApp,
	name string,
	input map[string]any,
	allowWrites bool,
) *mcp.CallToolResult {
	t.Helper()
	executor, _ := newGroupExecutor(t, app, &recordingReadOnlyResolver{}, allowWrites)
	result, err := newSDKSession(t, requireCatalogSpec(t, name), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{Name: name, Arguments: input},
	)
	require.NoError(t, err)
	require.False(t, result.IsError, callToolText(t, result))
	return result
}

func callGroupToolError(
	t *testing.T,
	app *recordingGroupApp,
	name string,
	input map[string]any,
	allowWrites bool,
) *mcp.CallToolResult {
	t.Helper()
	executor, _ := newGroupExecutor(t, app, &recordingReadOnlyResolver{}, allowWrites)
	result, err := newSDKSession(t, requireCatalogSpec(t, name), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{Name: name, Arguments: input},
	)
	require.NoError(t, err)
	require.True(t, result.IsError)
	return result
}

func groupIDValues(count int) []any {
	values := make([]any, count)
	for index := range values {
		values[index] = fmt.Sprintf("group-%03d", index)
	}
	return values
}

func cloneGroupPageQuery(query appcluster.GroupPageQuery) appcluster.GroupPageQuery {
	query.States = append([]string(nil), query.States...)
	return query
}

func cloneGroupPage(page appcluster.GroupPage) appcluster.GroupPage {
	page.Groups = append([]domaincluster.GroupState(nil), page.Groups...)
	return page
}

func cloneResetSpec(spec domaincluster.ResetSpec) domaincluster.ResetSpec {
	spec.Partitions = append([]int32(nil), spec.Partitions...)
	if spec.PartitionsOffsets != nil {
		source := spec.PartitionsOffsets
		spec.PartitionsOffsets = make(map[int32]int64, len(source))
		for partition, offset := range source {
			spec.PartitionsOffsets[partition] = offset
		}
	}
	return spec
}

var _ GroupServicer = (*recordingGroupApp)(nil)
