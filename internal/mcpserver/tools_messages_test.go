package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

type recordingMessageApp struct {
	mu sync.Mutex

	browseFn func(context.Context, appcluster.BrowseSpec, func(appcluster.BrowseEvent) error) error

	browseCluster string
	browseTopic   string
	browseSpec    appcluster.BrowseSpec
	browseCalls   int

	sendCluster string
	sendTopic   string
	sendSpec    appcluster.SendSpec
	sendCalls   int
	sendErr     error

	deleteCluster    string
	deleteTopic      string
	deletePartitions []int32
	deleteCalls      int
	deleteErr        error
}

type collidingBrowseError struct{}

func (collidingBrowseError) Error() string {
	return "real browse failure"
}

func (collidingBrowseError) Is(error) bool {
	return true
}

type cyclicBrowseError struct{}

func (*cyclicBrowseError) Error() string {
	return "cyclic browse failure"
}

func (err *cyclicBrowseError) Unwrap() error {
	return err
}

func (f *recordingMessageApp) Browse(
	ctx context.Context,
	clusterName string,
	topicName string,
	spec appcluster.BrowseSpec,
	emit func(appcluster.BrowseEvent) error,
) error {
	f.mu.Lock()
	f.browseCluster = clusterName
	f.browseTopic = topicName
	f.browseSpec = spec
	f.browseCalls++
	fn := f.browseFn
	f.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(ctx, spec, emit)
}

func (f *recordingMessageApp) Send(
	_ context.Context,
	clusterName string,
	topicName string,
	spec appcluster.SendSpec,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendCluster = clusterName
	f.sendTopic = topicName
	f.sendSpec = spec
	f.sendCalls++
	return f.sendErr
}

func (f *recordingMessageApp) Delete(
	_ context.Context,
	clusterName string,
	topicName string,
	partitions []int32,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCluster = clusterName
	f.deleteTopic = topicName
	f.deletePartitions = append([]int32(nil), partitions...)
	f.deleteCalls++
	return f.deleteErr
}

func (f *recordingMessageApp) snapshot() recordingMessageApp {
	f.mu.Lock()
	defer f.mu.Unlock()
	return recordingMessageApp{
		browseCluster:    f.browseCluster,
		browseTopic:      f.browseTopic,
		browseSpec:       f.browseSpec,
		browseCalls:      f.browseCalls,
		sendCluster:      f.sendCluster,
		sendTopic:        f.sendTopic,
		sendSpec:         f.sendSpec,
		sendCalls:        f.sendCalls,
		deleteCluster:    f.deleteCluster,
		deleteTopic:      f.deleteTopic,
		deletePartitions: append([]int32(nil), f.deletePartitions...),
		deleteCalls:      f.deleteCalls,
	}
}

type recordingSerdeApp struct {
	mu sync.Mutex

	suggestion serde.Suggestion
	err        error

	clusterName string
	topicName   string
	use         serde.Usage
	calls       int
}

func (f *recordingSerdeApp) Suggest(
	_ context.Context,
	clusterName string,
	topicName string,
	use serde.Usage,
) (serde.Suggestion, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clusterName = clusterName
	f.topicName = topicName
	f.use = use
	f.calls++
	return f.suggestion, f.err
}

type recordingSmartFilterApp struct {
	mu sync.Mutex

	registerID  string
	registerErr error
	testMatched bool
	testEvalErr string
	testErr     error

	registerCode  string
	registerCalls int
	testInput     appcluster.SmartFilterTest
	testCalls     int
}

func (f *recordingSmartFilterApp) Register(code string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registerCode = code
	f.registerCalls++
	return f.registerID, f.registerErr
}

func (f *recordingSmartFilterApp) Test(input appcluster.SmartFilterTest) (bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.testInput = input
	f.testCalls++
	return f.testMatched, f.testEvalErr, f.testErr
}

func TestMessageToolsCatalogContainsExactlySixCurrentOperationsAndKeepsSmartFiltersReadOnly(t *testing.T) {
	names := []string{
		"executeSmartFilterTest",
		"getTopicMessagesV2",
		"getSerdes",
		"registerFilter",
		"deleteTopicMessages",
		"sendTopicMessages",
	}
	for _, name := range names {
		require.Equal(t, "Messages", requireCatalogSpec(t, name).Meta.Subsystem)
	}
	require.NotContains(t, catalogNames(), "getTopicMessages")

	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, false)))
	executor, err := NewExecutor(Dependencies{
		Policy:     store,
		IsReadOnly: func(string) bool { return true },
	})
	require.NoError(t, err)
	session := newSDKCatalogSession(t, VisibleCatalog(*mustLoadPolicy(t, store)), executor)
	listed, err := session.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 51)
	for _, name := range []string{"executeSmartFilterTest", "registerFilter"} {
		tool := findTool(t, listed.Tools, name)
		require.True(t, tool.Annotations.ReadOnlyHint)
	}
	require.Nil(t, findToolOrNil(listed.Tools, "deleteTopicMessages"))
	require.Nil(t, findToolOrNil(listed.Tools, "sendTopicMessages"))
}

func TestMessageToolsBrowseMapsV2QueryAndReturnsExactContractFields(t *testing.T) {
	app := &recordingMessageApp{}
	app.browseFn = func(_ context.Context, _ appcluster.BrowseSpec, emit func(appcluster.BrowseEvent) error) error {
		if err := emit(appcluster.BrowseEvent{
			Kind: appcluster.EventMessage,
			Message: &appcluster.DecodedMessage{
				Partition:     3,
				Offset:        42,
				TimestampMs:   1700000000123,
				TimestampType: "CREATE_TIME",
				Key:           "order-42",
				Value:         `{"status":"paid"}`,
				Headers:       map[string]string{"trace-id": "abc"},
				KeySize:       8,
				ValueSize:     17,
				HeadersSize:   11,
				KeySerde:      "String",
				ValueSerde:    "Json",
			},
		}); err != nil {
			return err
		}
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone, CursorID: "cursor-next"})
	}
	executor, _, _ := newMessageExecutor(t, app, nil, nil, true, false)

	result := callMessageTool(t, executor, "getTopicMessagesV2", map[string]any{
		"clusterName": "prod",
		"topicName":   "orders",
		"query": map[string]any{
			"mode":          "TO_TIMESTAMP",
			"partitions":    []any{float64(3), float64(4)},
			"limit":         float64(25),
			"stringFilter":  "paid",
			"smartFilterId": "filter-1",
			"offset":        float64(41),
			"timestamp":     float64(1700000000000),
			"keySerde":      "String",
			"valueSerde":    "Json",
			"cursor":        "cursor-before",
		},
	})
	want := `{
		"items":[{
			"partition":3,
			"offset":42,
			"timestamp":"2023-11-14T22:13:20.123Z",
			"timestampType":"CREATE_TIME",
			"key":"order-42",
			"headers":{"trace-id":"abc"},
			"value":"{\"status\":\"paid\"}",
			"keySize":8,
			"valueSize":17,
			"headersSize":11,
			"keySerde":"String",
			"valueSerde":"Json"
		}],
		"truncated":false,
		"cursor":"cursor-next"
	}`
	require.JSONEq(t, want, callToolText(t, result))
	requireStructuredJSONEq(t, want, result.StructuredContent)

	got := app.snapshot()
	require.Equal(t, "prod", got.browseCluster)
	require.Equal(t, "orders", got.browseTopic)
	require.Equal(t, appcluster.BrowseSpec{
		Mode:          appcluster.ModeToTimestamp,
		Partitions:    []int32{3, 4},
		Limit:         25,
		Offset:        41,
		TimestampMs:   1700000000000,
		StringFilter:  "paid",
		SmartFilterID: "filter-1",
		KeySerde:      "String",
		ValueSerde:    "Json",
		Cursor:        "cursor-before",
	}, got.browseSpec)
}

func TestMessageToolsBrowseCapsOneHundredMessagesWithoutRedactingOrLoggingEnvelope(t *testing.T) {
	const (
		messageKey   = "password-shaped-message-key"
		messageValue = "message-value-marker"
		headerValue  = "header-value-marker"
	)
	var callbackCalls int
	reachedDone := false
	app := &recordingMessageApp{}
	app.browseFn = func(_ context.Context, _ appcluster.BrowseSpec, emit func(appcluster.BrowseEvent) error) error {
		for index := 0; index < 150; index++ {
			callbackCalls++
			if err := emit(appcluster.BrowseEvent{
				Kind:     appcluster.EventMessage,
				CursorID: fmt.Sprintf("cursor-%d", index+1),
				Message: &appcluster.DecodedMessage{
					Partition:   int32(index % 3),
					Offset:      int64(index),
					TimestampMs: int64(index),
					Key:         messageKey,
					Value:       messageValue,
					Headers: map[string]string{
						"password":             headerValue,
						"basic.auth.user.info": headerValue,
						"api.key":              headerValue,
						"authorization":        headerValue,
					},
				},
			}); err != nil {
				return err
			}
		}
		reachedDone = true
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone, CursorID: "cursor-150"})
	}

	var logs bytes.Buffer
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(originalLogger) })

	executor, _, _ := newMessageExecutor(t, app, nil, nil, false, false)
	result := callMessageTool(t, executor, "getTopicMessagesV2", map[string]any{
		"clusterName": "prod",
		"topicName":   "orders",
	})

	var output struct {
		Items []struct {
			Key     string            `json:"key"`
			Value   string            `json:"value"`
			Headers map[string]string `json:"headers"`
		} `json:"items"`
		Truncated bool   `json:"truncated"`
		Cursor    string `json:"cursor"`
	}
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &output))
	require.Len(t, output.Items, maxMessageItems)
	require.True(t, output.Truncated)
	require.Equal(t, "cursor-100", output.Cursor)
	require.Equal(t, maxMessageItems+1, callbackCalls)
	require.False(t, reachedDone)
	require.Equal(t, messageKey, output.Items[0].Key)
	require.Equal(t, messageValue, output.Items[0].Value)
	require.Equal(t, headerValue, output.Items[0].Headers["password"])
	require.Equal(t, headerValue, output.Items[0].Headers["basic.auth.user.info"])
	require.Equal(t, headerValue, output.Items[0].Headers["api.key"])
	require.Equal(t, headerValue, output.Items[0].Headers["authorization"])
	require.NotContains(t, callToolText(t, result), redactedValue)
	require.NotContains(t, logs.String(), messageKey)
	require.NotContains(t, logs.String(), messageValue)
	require.NotContains(t, logs.String(), headerValue)
}

func TestMessageToolsBrowseCapsAggregateResultAtOneMiBAndStopsAppending(t *testing.T) {
	app := &recordingMessageApp{}
	large := strings.Repeat("v", 700<<10)
	app.browseFn = func(_ context.Context, _ appcluster.BrowseSpec, emit func(appcluster.BrowseEvent) error) error {
		for index := 0; index < 3; index++ {
			if err := emit(appcluster.BrowseEvent{
				Kind: appcluster.EventMessage,
				Message: &appcluster.DecodedMessage{
					Partition:   1,
					Offset:      int64(index),
					TimestampMs: 1,
					Key:         "ordinary-key",
					Value:       large,
				},
			}); err != nil {
				return err
			}
		}
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone, CursorID: "cursor-large"})
	}
	executor, _, _ := newMessageExecutor(t, app, nil, nil, false, false)
	result := callMessageTool(t, executor, "getTopicMessagesV2", map[string]any{
		"clusterName": "prod",
		"topicName":   "orders",
	})
	require.LessOrEqual(t, len(callToolText(t, result)), maxResultBytes)

	var output struct {
		Items     []generated.TopicMessage `json:"items"`
		Truncated bool                     `json:"truncated"`
		Cursor    string                   `json:"cursor"`
	}
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &output))
	require.Len(t, output.Items, 1)
	require.True(t, output.Truncated)
	require.Empty(t, output.Cursor, "DONE after discarded messages is not a safe resume point")
	require.Equal(t, large, *output.Items[0].Value)
}

func TestMessageToolsBrowseStopsAtEffectiveLimitWithLastAcceptedCursor(t *testing.T) {
	var callbackCalls int
	var callbackErr error
	reachedDone := false
	app := &recordingMessageApp{}
	app.browseFn = func(_ context.Context, spec appcluster.BrowseSpec, emit func(appcluster.BrowseEvent) error) error {
		require.Equal(t, 2, spec.Limit)
		for index := 0; index < 5; index++ {
			callbackCalls++
			callbackErr = emit(appcluster.BrowseEvent{
				Kind:     appcluster.EventMessage,
				CursorID: fmt.Sprintf("cursor-%d", index+1),
				Message: &appcluster.DecodedMessage{
					Offset:      int64(index),
					TimestampMs: int64(index),
					Value:       fmt.Sprintf("value-%d", index),
				},
			})
			if callbackErr != nil {
				return callbackErr
			}
		}
		reachedDone = true
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone, CursorID: "cursor-done"})
	}
	executor, _, _ := newMessageExecutor(t, app, nil, nil, false, false)
	result := callMessageTool(t, executor, "getTopicMessagesV2", map[string]any{
		"clusterName": "prod",
		"topicName":   "orders",
		"query":       map[string]any{"limit": float64(2)},
	})

	var output struct {
		Items     []generated.TopicMessage `json:"items"`
		Truncated bool                     `json:"truncated"`
		Cursor    string                   `json:"cursor"`
	}
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &output))
	require.Len(t, output.Items, 2)
	require.Equal(t, int64(0), output.Items[0].Offset)
	require.Equal(t, int64(1), output.Items[1].Offset)
	require.True(t, output.Truncated)
	require.Equal(t, "cursor-2", output.Cursor)
	require.Equal(t, 3, callbackCalls, "the first discarded message must stop collection")
	require.Error(t, callbackErr)
	require.False(t, reachedDone, "a DONE cursor after discarded messages is not safe")
}

func TestMessageToolsBrowseByteCapStopsWithLastAcceptedCursor(t *testing.T) {
	var callbackCalls int
	reachedDone := false
	app := &recordingMessageApp{}
	large := strings.Repeat("v", 700<<10)
	app.browseFn = func(_ context.Context, _ appcluster.BrowseSpec, emit func(appcluster.BrowseEvent) error) error {
		for index := 0; index < 3; index++ {
			callbackCalls++
			err := emit(appcluster.BrowseEvent{
				Kind:     appcluster.EventMessage,
				CursorID: fmt.Sprintf("cursor-%d", index+1),
				Message: &appcluster.DecodedMessage{
					Offset:      int64(index),
					TimestampMs: 1,
					Value:       large,
				},
			})
			if err != nil {
				return err
			}
		}
		reachedDone = true
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone, CursorID: "cursor-done"})
	}
	executor, _, _ := newMessageExecutor(t, app, nil, nil, false, false)
	result := callMessageTool(t, executor, "getTopicMessagesV2", map[string]any{
		"clusterName": "prod",
		"topicName":   "orders",
	})

	var output struct {
		Items     []generated.TopicMessage `json:"items"`
		Truncated bool                     `json:"truncated"`
		Cursor    string                   `json:"cursor"`
	}
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &output))
	require.Len(t, output.Items, 1)
	require.True(t, output.Truncated)
	require.Equal(t, "cursor-1", output.Cursor)
	require.Equal(t, 2, callbackCalls)
	require.False(t, reachedDone)
	require.LessOrEqual(t, len(callToolText(t, result)), maxResultBytes)
}

func TestMessageToolsBrowseNoFittingMessageRetainsInputCursor(t *testing.T) {
	var callbackCalls int
	app := &recordingMessageApp{}
	app.browseFn = func(_ context.Context, _ appcluster.BrowseSpec, emit func(appcluster.BrowseEvent) error) error {
		callbackCalls++
		return emit(appcluster.BrowseEvent{
			Kind:     appcluster.EventMessage,
			CursorID: "cursor-after-discarded",
			Message: &appcluster.DecodedMessage{
				Value: strings.Repeat("v", maxResultBytes),
			},
		})
	}
	executor, _, _ := newMessageExecutor(t, app, nil, nil, false, false)
	result := callMessageTool(t, executor, "getTopicMessagesV2", map[string]any{
		"clusterName": "prod",
		"topicName":   "orders",
		"query":       map[string]any{"cursor": "cursor-input"},
	})

	require.JSONEq(t, `{
		"items":[],
		"truncated":true,
		"cursor":"cursor-input"
	}`, callToolText(t, result))
	require.Equal(t, 1, callbackCalls)
}

func TestMessageToolsBrowseRejectsNonRoundTrippableAndEncodedOversizeCursors(t *testing.T) {
	invalidUTF8 := generated.GetTopicMessagesV2Params{}
	invalidCursor := string([]byte{0xff})
	invalidUTF8.Cursor = &invalidCursor
	_, err := messageBrowseSpec(&invalidUTF8, maxMessageItems)
	require.ErrorIs(t, err, errInvalidRequest)

	executor, _, _ := newMessageExecutor(t, &recordingMessageApp{}, nil, nil, false, false)
	encodedOversize := strings.Repeat(`"`, (maxMessageCursorBytes/2)+1)
	inputResult, err := newSDKSession(t, requireCatalogSpec(t, "getTopicMessagesV2"), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name: "getTopicMessagesV2",
			Arguments: map[string]any{
				"clusterName": "prod",
				"topicName":   "orders",
				"query":       map[string]any{"cursor": encodedOversize},
			},
		},
	)
	require.NoError(t, err)
	require.True(t, inputResult.IsError)
	require.Equal(t, "invalid_request", callToolText(t, inputResult))

	app := &recordingMessageApp{}
	app.browseFn = func(_ context.Context, _ appcluster.BrowseSpec, emit func(appcluster.BrowseEvent) error) error {
		return emit(appcluster.BrowseEvent{
			Kind:     appcluster.EventDone,
			CursorID: encodedOversize,
		})
	}
	outputExecutor, _, _ := newMessageExecutor(t, app, nil, nil, false, false)
	outputResult, err := newSDKSession(t, requireCatalogSpec(t, "getTopicMessagesV2"), outputExecutor).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name: "getTopicMessagesV2",
			Arguments: map[string]any{
				"clusterName": "prod",
				"topicName":   "orders",
			},
		},
	)
	require.NoError(t, err)
	require.True(t, outputResult.IsError)
	require.Equal(t, "result_too_large", callToolText(t, outputResult))
}

func TestMessageToolsBrowseDoesNotConfuseApplicationErrorsWithLocalStop(t *testing.T) {
	app := &recordingMessageApp{}
	app.browseFn = func(_ context.Context, _ appcluster.BrowseSpec, emit func(appcluster.BrowseEvent) error) error {
		require.NoError(t, emit(appcluster.BrowseEvent{
			Kind:     appcluster.EventMessage,
			CursorID: "cursor-1",
			Message:  &appcluster.DecodedMessage{Value: "accepted"},
		}))
		stopErr := emit(appcluster.BrowseEvent{
			Kind:     appcluster.EventMessage,
			CursorID: "cursor-2",
			Message:  &appcluster.DecodedMessage{Value: "discarded"},
		})
		require.Error(t, stopErr)
		return collidingBrowseError{}
	}
	executor, _, _ := newMessageExecutor(t, app, nil, nil, false, false)
	limit := int32(1)
	_, err := browseTopicMessages(
		context.Background(),
		executor,
		topicOptionalQueryInput[generated.GetTopicMessagesV2Params]{
			ClusterName: "prod",
			TopicName:   "orders",
			Query:       &generated.GetTopicMessagesV2Params{Limit: &limit},
		},
		maxMessageItems,
	)
	require.EqualError(t, err, "real browse failure")
}

func TestMessageToolsBrowseRejectsMixedLocalStopErrorTrees(t *testing.T) {
	realServiceErr := errors.New("real service failure marker")
	tests := []struct {
		name       string
		serviceErr func(context.Context, error) error
	}{
		{
			name: "joined local sentinel and real service error",
			serviceErr: func(_ context.Context, stopErr error) error {
				return errors.Join(stopErr, realServiceErr)
			},
		},
		{
			name: "wrapped join of local sentinel and real service error",
			serviceErr: func(_ context.Context, stopErr error) error {
				return fmt.Errorf("service wrapper: %w", errors.Join(
					fmt.Errorf("stop wrapper: %w", stopErr),
					fmt.Errorf("real wrapper: %w", realServiceErr),
				))
			},
		},
		{
			name: "joined local child cancellation and real service error",
			serviceErr: func(ctx context.Context, _ error) error {
				return errors.Join(ctx.Err(), realServiceErr)
			},
		},
		{
			name: "wrapped join of local child cancellation and real service error",
			serviceErr: func(ctx context.Context, _ error) error {
				return fmt.Errorf("service wrapper: %w", errors.Join(
					fmt.Errorf("cancel wrapper: %w", ctx.Err()),
					fmt.Errorf("real wrapper: %w", realServiceErr),
				))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := callBrowseWithLocalStopServiceError(t, tc.serviceErr)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "operation_failed", callToolText(t, result))
			require.NotContains(t, callToolText(t, result), "truncated")
			require.NotContains(t, callToolText(t, result), "real service failure marker")
		})
	}
}

func TestMessageToolsBrowseAcceptsOnlyPureLocalStopErrorTrees(t *testing.T) {
	tests := []struct {
		name       string
		serviceErr func(context.Context, error) error
	}{
		{
			name: "direct local sentinel",
			serviceErr: func(_ context.Context, stopErr error) error {
				return stopErr
			},
		},
		{
			name: "wrapped local sentinel",
			serviceErr: func(_ context.Context, stopErr error) error {
				return fmt.Errorf("service wrapper: %w", stopErr)
			},
		},
		{
			name: "direct local child cancellation",
			serviceErr: func(ctx context.Context, _ error) error {
				return ctx.Err()
			},
		},
		{
			name: "wrapped local child cancellation",
			serviceErr: func(ctx context.Context, _ error) error {
				return fmt.Errorf("service wrapper: %w", ctx.Err())
			},
		},
		{
			name: "joined local sentinel and local child cancellation",
			serviceErr: func(ctx context.Context, stopErr error) error {
				return errors.Join(
					fmt.Errorf("stop wrapper: %w", stopErr),
					fmt.Errorf("cancel wrapper: %w", ctx.Err()),
				)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := callBrowseWithLocalStopServiceError(t, tc.serviceErr)
			require.NoError(t, err)
			require.False(t, result.IsError)
			var output messageBrowseResult
			require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &output))
			require.Len(t, output.Items, 1)
			require.NotNil(t, output.Items[0].Value)
			require.Equal(t, "accepted", *output.Items[0].Value)
			require.True(t, output.Truncated)
			require.Equal(t, "cursor-1", output.Cursor)
		})
	}
}

func TestMessageToolsBrowseFailsClosedOnAbusiveLocalStopErrorTrees(t *testing.T) {
	tests := []struct {
		name       string
		serviceErr func(error) error
	}{
		{
			name: "cyclic error tree",
			serviceErr: func(stopErr error) error {
				return errors.Join(stopErr, &cyclicBrowseError{})
			},
		},
		{
			name: "over-depth wrapper chain",
			serviceErr: func(stopErr error) error {
				result := stopErr
				for range 40 {
					result = fmt.Errorf("service wrapper: %w", result)
				}
				return result
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := &recordingMessageApp{}
			app.browseFn = func(
				_ context.Context,
				_ appcluster.BrowseSpec,
				emit func(appcluster.BrowseEvent) error,
			) error {
				require.NoError(t, emit(appcluster.BrowseEvent{
					Kind:     appcluster.EventMessage,
					CursorID: "cursor-1",
					Message:  &appcluster.DecodedMessage{Value: "accepted"},
				}))
				stopErr := emit(appcluster.BrowseEvent{
					Kind:     appcluster.EventMessage,
					CursorID: "cursor-2",
					Message:  &appcluster.DecodedMessage{Value: "discarded"},
				})
				require.Error(t, stopErr)
				return tc.serviceErr(stopErr)
			}
			executor, _, _ := newMessageExecutor(t, app, nil, nil, false, false)
			limit := int32(1)
			_, err := browseTopicMessages(
				context.Background(),
				executor,
				topicOptionalQueryInput[generated.GetTopicMessagesV2Params]{
					ClusterName: "prod",
					TopicName:   "orders",
					Query:       &generated.GetTopicMessagesV2Params{Limit: &limit},
				},
				maxMessageItems,
			)
			require.True(t, err == errOperationFailed)
		})
	}
}

func callBrowseWithLocalStopServiceError(
	t *testing.T,
	serviceErr func(context.Context, error) error,
) (*mcp.CallToolResult, error) {
	t.Helper()
	app := &recordingMessageApp{}
	app.browseFn = func(
		ctx context.Context,
		_ appcluster.BrowseSpec,
		emit func(appcluster.BrowseEvent) error,
	) error {
		require.NoError(t, emit(appcluster.BrowseEvent{
			Kind:     appcluster.EventMessage,
			CursorID: "cursor-1",
			Message:  &appcluster.DecodedMessage{Value: "accepted"},
		}))
		stopErr := emit(appcluster.BrowseEvent{
			Kind:     appcluster.EventMessage,
			CursorID: "cursor-2",
			Message:  &appcluster.DecodedMessage{Value: "discarded"},
		})
		require.Error(t, stopErr)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		return serviceErr(ctx, stopErr)
	}
	executor, _, _ := newMessageExecutor(t, app, nil, nil, false, false)
	return newSDKSession(
		t,
		requireCatalogSpec(t, "getTopicMessagesV2"),
		executor,
	).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name: "getTopicMessagesV2",
			Arguments: map[string]any{
				"clusterName": "prod",
				"topicName":   "orders",
				"query":       map[string]any{"limit": float64(1)},
			},
		},
	)
}

func TestMessageToolsBrowseDoesNotSuppressParentCancellationAsLocalStop(t *testing.T) {
	parentCtx, cancelParent := context.WithCancel(context.Background())
	app := &recordingMessageApp{}
	app.browseFn = func(
		_ context.Context,
		_ appcluster.BrowseSpec,
		emit func(appcluster.BrowseEvent) error,
	) error {
		require.NoError(t, emit(appcluster.BrowseEvent{
			Kind:     appcluster.EventMessage,
			CursorID: "cursor-1",
			Message:  &appcluster.DecodedMessage{Value: "accepted"},
		}))
		stopErr := emit(appcluster.BrowseEvent{
			Kind:     appcluster.EventMessage,
			CursorID: "cursor-2",
			Message:  &appcluster.DecodedMessage{Value: "discarded"},
		})
		require.Error(t, stopErr)
		cancelParent()
		return stopErr
	}
	executor, _, _ := newMessageExecutor(t, app, nil, nil, false, false)
	limit := int32(1)
	_, err := browseTopicMessages(
		parentCtx,
		executor,
		topicOptionalQueryInput[generated.GetTopicMessagesV2Params]{
			ClusterName: "prod",
			TopicName:   "orders",
			Query:       &generated.GetTopicMessagesV2Params{Limit: &limit},
		},
		maxMessageItems,
	)
	require.ErrorIs(t, err, errMessageCollectionStop)
}

func TestMessageToolsBrowseCancellationStopsCollectionPromptly(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	var callbackErr error
	app := &recordingMessageApp{}
	app.browseFn = func(ctx context.Context, _ appcluster.BrowseSpec, emit func(appcluster.BrowseEvent) error) error {
		defer close(finished)
		if err := emit(appcluster.BrowseEvent{
			Kind:    appcluster.EventMessage,
			Message: &appcluster.DecodedMessage{Key: "first", Value: "first"},
		}); err != nil {
			return err
		}
		close(started)
		<-ctx.Done()
		callbackErr = emit(appcluster.BrowseEvent{
			Kind:    appcluster.EventMessage,
			Message: &appcluster.DecodedMessage{Key: "late", Value: "late"},
		})
		return callbackErr
	}
	executor, _, _ := newMessageExecutor(t, app, nil, nil, false, false)
	session := newSDKSession(t, requireCatalogSpec(t, "getTopicMessagesV2"), executor)
	ctx, cancel := context.WithCancel(context.Background())
	callDone := make(chan struct{})
	go func() {
		defer close(callDone)
		_, _ = session.CallTool(ctx, &mcp.CallToolParams{
			Name: "getTopicMessagesV2",
			Arguments: map[string]any{
				"clusterName": "prod",
				"topicName":   "orders",
			},
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("browse did not start")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("browse callback did not stop promptly after cancellation")
	}
	require.ErrorIs(t, callbackErr, context.Canceled)
	select {
	case <-callDone:
	case <-time.After(time.Second):
		t.Fatal("MCP call did not return promptly after cancellation")
	}
}

func TestMessageToolsSendAndDeleteConvertExactSpecsAndReturnStructuredNull(t *testing.T) {
	app := &recordingMessageApp{}
	executor, _, resolver := newMessageExecutor(t, app, nil, nil, true, false)

	send := callMessageTool(t, executor, "sendTopicMessages", map[string]any{
		"clusterName": "prod",
		"topicName":   "orders",
		"body": map[string]any{
			"partition":            float64(3),
			"key":                  "k",
			"value":                "v",
			"headers":              map[string]any{"trace-id": "abc"},
			"keySerde":             "String",
			"valueSerde":           "Json",
			"keySerdeProperties":   map[string]any{"schema": "key-v1"},
			"valueSerdeProperties": map[string]any{"schema": "value-v1"},
		},
	})
	require.JSONEq(t, `{"result":null}`, callToolText(t, send))
	requireStructuredJSONEq(t, `{"result":null}`, send.StructuredContent)

	deleted := callMessageTool(t, executor, "deleteTopicMessages", map[string]any{
		"clusterName": "prod",
		"topicName":   "orders",
		"query":       map[string]any{"partitions": []any{float64(3), float64(1)}},
	})
	require.JSONEq(t, `{"result":null}`, callToolText(t, deleted))
	requireStructuredJSONEq(t, `{"result":null}`, deleted.StructuredContent)

	got := app.snapshot()
	require.Equal(t, 1, got.sendCalls)
	require.Equal(t, "prod", got.sendCluster)
	require.Equal(t, "orders", got.sendTopic)
	require.Equal(t, int32(3), got.sendSpec.Partition)
	require.NotNil(t, got.sendSpec.Key)
	require.Equal(t, "k", *got.sendSpec.Key)
	require.NotNil(t, got.sendSpec.Value)
	require.Equal(t, "v", *got.sendSpec.Value)
	require.Equal(t, map[string]string{"trace-id": "abc"}, got.sendSpec.Headers)
	require.Equal(t, "String", got.sendSpec.KeySerde)
	require.Equal(t, "Json", got.sendSpec.ValueSerde)
	require.Equal(t, map[string]any{"schema": "key-v1"}, got.sendSpec.KeySerdeProps)
	require.Equal(t, map[string]any{"schema": "value-v1"}, got.sendSpec.ValueSerdeProps)
	require.Equal(t, []int32{3, 1}, got.deletePartitions)
	require.Equal(t, 1, got.deleteCalls)
	require.Equal(t, []string{"prod", "prod"}, resolver.clusters)
}

func TestMessageToolsWritesUseActualClusterReadOnlyAndStalePolicyGates(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
	}{
		{
			name: "sendTopicMessages",
			input: map[string]any{
				"clusterName": "prod",
				"topicName":   "orders",
				"body":        map[string]any{"partition": float64(0), "value": "v"},
			},
		},
		{
			name: "deleteTopicMessages",
			input: map[string]any{
				"clusterName": "prod",
				"topicName":   "orders",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name+" read-only", func(t *testing.T) {
			app := &recordingMessageApp{}
			executor, _, resolver := newMessageExecutor(t, app, nil, nil, true, true)
			result, err := newSDKSession(t, requireCatalogSpec(t, tt.name), executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{Name: tt.name, Arguments: tt.input},
			)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "cluster_read_only", callToolText(t, result))
			require.Equal(t, []string{"prod"}, resolver.clusters)
			got := app.snapshot()
			require.Zero(t, got.sendCalls)
			require.Zero(t, got.deleteCalls)
		})

		t.Run(tt.name+" stale policy", func(t *testing.T) {
			app := &recordingMessageApp{}
			executor, store, _ := newMessageExecutor(t, app, nil, nil, true, false)
			session := newSDKSession(t, requireCatalogSpec(t, tt.name), executor)
			first, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tt.name, Arguments: tt.input})
			require.NoError(t, err)
			require.False(t, first.IsError, callToolText(t, first))

			require.NoError(t, store.Save(context.Background(), *policy(true, false)))
			second, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tt.name, Arguments: tt.input})
			require.NoError(t, err)
			require.True(t, second.IsError)
			require.Equal(t, "writes_disabled", callToolText(t, second))
			got := app.snapshot()
			if tt.name == "sendTopicMessages" {
				require.Equal(t, 1, got.sendCalls)
			} else {
				require.Equal(t, 1, got.deleteCalls)
			}
		})
	}
}

func TestMessageToolsRejectOversizedSendAndInvalidDeleteBeforeService(t *testing.T) {
	sendCases := []struct {
		name string
		body map[string]any
	}{
		{name: "negative partition", body: map[string]any{"partition": float64(-1), "value": "v"}},
		{name: "oversized value", body: map[string]any{"partition": float64(0), "value": strings.Repeat("v", maxResultBytes+1)}},
		{name: "too many headers", body: map[string]any{"partition": float64(0), "headers": messageHeaders(maxMessageItems + 1)}},
		{name: "empty header name", body: map[string]any{"partition": float64(0), "headers": map[string]any{"": "v"}}},
		{name: "oversized aggregate", body: map[string]any{
			"partition": float64(0),
			"key":       strings.Repeat("k", (maxResultBytes/2)+1),
			"value":     strings.Repeat("v", (maxResultBytes/2)+1),
		}},
	}
	for _, tc := range sendCases {
		t.Run("send "+tc.name, func(t *testing.T) {
			app := &recordingMessageApp{}
			executor, _, _ := newMessageExecutor(t, app, nil, nil, true, false)
			result, err := newSDKSession(t, requireCatalogSpec(t, "sendTopicMessages"), executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{
					Name: "sendTopicMessages",
					Arguments: map[string]any{
						"clusterName": "prod",
						"topicName":   "orders",
						"body":        tc.body,
					},
				},
			)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Zero(t, app.snapshot().sendCalls)
		})
	}

	deleteCases := []struct {
		name       string
		partitions []any
	}{
		{name: "negative", partitions: []any{float64(-1)}},
		{name: "duplicate", partitions: []any{float64(1), float64(1)}},
		{name: "too many", partitions: int32Values(maxMessageItems + 1)},
	}
	for _, tc := range deleteCases {
		t.Run("delete "+tc.name, func(t *testing.T) {
			app := &recordingMessageApp{}
			executor, _, _ := newMessageExecutor(t, app, nil, nil, true, false)
			result, err := newSDKSession(t, requireCatalogSpec(t, "deleteTopicMessages"), executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{
					Name: "deleteTopicMessages",
					Arguments: map[string]any{
						"clusterName": "prod",
						"topicName":   "orders",
						"query":       map[string]any{"partitions": tc.partitions},
					},
				},
			)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Zero(t, app.snapshot().deleteCalls)
		})
	}
}

func TestSerdeToolsReturnExactSuggestionAndMapUsage(t *testing.T) {
	schema := `{"type":"string"}`
	app := &recordingSerdeApp{
		suggestion: serde.Suggestion{
			Key: []serde.Description{{
				Name:        "String",
				Description: "UTF-8",
				Preferred:   true,
				Schema:      &schema,
				Params: []serde.Param{{
					Name:          "encoding",
					VisibleName:   "Encoding",
					AllowedValues: []string{"UTF-8", "ASCII"},
				}},
			}},
			Value: []serde.Description{{
				Name:        "Hex",
				Description: "hex bytes",
				Preferred:   false,
			}},
		},
	}
	executor, _, _ := newMessageExecutor(t, nil, app, nil, false, false)
	result := callMessageTool(t, executor, "getSerdes", map[string]any{
		"clusterName": "prod",
		"topicName":   "orders",
		"query":       map[string]any{"use": "DESERIALIZE"},
	})
	want := `{
		"key":[{
			"name":"String",
			"description":"UTF-8",
			"preferred":true,
			"schema":"{\"type\":\"string\"}",
			"parameters":[{
				"name":"encoding",
				"visibleName":"Encoding",
				"allowedValues":["UTF-8","ASCII"]
			}]
		}],
		"value":[{
			"name":"Hex",
			"description":"hex bytes",
			"preferred":false
		}]
	}`
	require.JSONEq(t, want, callToolText(t, result))
	requireStructuredJSONEq(t, want, result.StructuredContent)
	require.Equal(t, "prod", app.clusterName)
	require.Equal(t, "orders", app.topicName)
	require.Equal(t, serde.UsageDeserialize, app.use)
	require.Equal(t, 1, app.calls)
}

func TestSerdeToolsRejectAdversarialNestedSuggestionBeforeBuildingResult(t *testing.T) {
	cumulativeCandidates := serde.Suggestion{
		Key:   make([]serde.Description, 300),
		Value: make([]serde.Description, 300),
	}
	for index := range cumulativeCandidates.Key {
		cumulativeCandidates.Key[index] = serde.Description{Name: fmt.Sprintf("key-%03d", index)}
	}
	for index := range cumulativeCandidates.Value {
		cumulativeCandidates.Value[index] = serde.Description{Name: fmt.Sprintf("value-%03d", index)}
	}

	allowedValues := make([]string, maxListItems-1)
	for index := range allowedValues {
		allowedValues[index] = fmt.Sprintf("allowed-%03d", index)
	}
	cumulativeNested := serde.Suggestion{
		Key: []serde.Description{{
			Name: "String",
			Params: []serde.Param{{
				Name:          "encoding",
				AllowedValues: allowedValues,
			}},
		}},
	}
	cumulativeBytes := serde.Suggestion{
		Key: make([]serde.Description, 17),
	}
	for index := range cumulativeBytes.Key {
		cumulativeBytes.Key[index] = serde.Description{
			Name:        fmt.Sprintf("serde-%02d", index),
			Description: strings.Repeat("d", 64<<10),
		}
	}

	tests := []struct {
		name       string
		suggestion serde.Suggestion
	}{
		{name: "candidate count is cumulative across key and value", suggestion: cumulativeCandidates},
		{name: "candidate parameter and allowed values share one item budget", suggestion: cumulativeNested},
		{name: "field bytes share one result budget", suggestion: cumulativeBytes},
		{
			name: "serde name has an explicit bound",
			suggestion: serde.Suggestion{Key: []serde.Description{{
				Name: strings.Repeat("n", maxMessageSerdeNameBytes+1),
			}}},
		},
		{
			name: "description has an explicit bound",
			suggestion: serde.Suggestion{Key: []serde.Description{{
				Name:        "String",
				Description: strings.Repeat("d", (64<<10)+1),
			}}},
		},
		{
			name: "schema has an explicit bound",
			suggestion: serde.Suggestion{Key: []serde.Description{{
				Name:   "String",
				Schema: pointerTo(strings.Repeat("s", (256<<10)+1)),
			}}},
		},
		{
			name: "parameter name has an explicit bound",
			suggestion: serde.Suggestion{Key: []serde.Description{{
				Name: "String",
				Params: []serde.Param{{
					Name: strings.Repeat("p", maxMessageSerdeNameBytes+1),
				}},
			}}},
		},
		{
			name: "visible parameter name has an explicit bound",
			suggestion: serde.Suggestion{Key: []serde.Description{{
				Name: "String",
				Params: []serde.Param{{
					Name:        "encoding",
					VisibleName: strings.Repeat("v", maxMessageSerdeNameBytes+1),
				}},
			}}},
		},
		{
			name: "allowed value has an explicit bound",
			suggestion: serde.Suggestion{Key: []serde.Description{{
				Name: "String",
				Params: []serde.Param{{
					Name:          "encoding",
					AllowedValues: []string{strings.Repeat("a", (64<<10)+1)},
				}},
			}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := &recordingSerdeApp{suggestion: tc.suggestion}
			executor, _, _ := newMessageExecutor(t, nil, app, nil, false, false)
			result, err := newSDKSession(t, requireCatalogSpec(t, "getSerdes"), executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{
					Name: "getSerdes",
					Arguments: map[string]any{
						"clusterName": "prod",
						"topicName":   "orders",
						"query":       map[string]any{"use": "DESERIALIZE"},
					},
				},
			)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "result_too_large", callToolText(t, result))
			require.Equal(t, 1, app.calls)
		})
	}
}

func TestSmartFilterToolsReturnExactResultsAndMapEverySampleField(t *testing.T) {
	app := &recordingSmartFilterApp{registerID: "deadbeef", testMatched: true}
	executor, _, resolver := newMessageExecutor(t, nil, nil, app, false, true)

	registered := callMessageTool(t, executor, "registerFilter", map[string]any{
		"clusterName": "prod",
		"topicName":   "orders",
		"body":        map[string]any{"filterCode": "record.value == 'paid'"},
	})
	require.JSONEq(t, `{"id":"deadbeef"}`, callToolText(t, registered))
	requireStructuredJSONEq(t, `{"id":"deadbeef"}`, registered.StructuredContent)

	tested := callMessageTool(t, executor, "executeSmartFilterTest", map[string]any{
		"body": map[string]any{
			"filterCode":  "record.value == 'paid'",
			"key":         "order-42",
			"value":       "paid",
			"headers":     map[string]any{"trace-id": "abc"},
			"partition":   float64(3),
			"offset":      float64(42),
			"timestampMs": float64(1700),
		},
	})
	require.JSONEq(t, `{"result":true}`, callToolText(t, tested))
	requireStructuredJSONEq(t, `{"result":true}`, tested.StructuredContent)

	require.Equal(t, "record.value == 'paid'", app.registerCode)
	require.Equal(t, appcluster.SmartFilterTest{
		FilterCode:  "record.value == 'paid'",
		Key:         "order-42",
		Value:       "paid",
		Headers:     map[string]string{"trace-id": "abc"},
		Partition:   3,
		Offset:      42,
		TimestampMs: 1700,
	}, app.testInput)
	require.Empty(t, resolver.clusters, "read-only smart filters must not hit the cluster write gate")
}

func TestSmartFilterToolsBoundPayloadsAndNeverReturnRawFilterRuntimeErrors(t *testing.T) {
	app := &recordingSmartFilterApp{
		registerErr: errors.New("compile failed for record.value == 'payload-marker'"),
		testErr:     errors.New("compile failed for record.value == 'payload-marker'"),
	}
	executor, _, _ := newMessageExecutor(t, nil, nil, app, false, false)

	register, err := newSDKSession(t, requireCatalogSpec(t, "registerFilter"), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name: "registerFilter",
			Arguments: map[string]any{
				"clusterName": "prod",
				"topicName":   "orders",
				"body":        map[string]any{"filterCode": "record.value == 'payload-marker'"},
			},
		},
	)
	require.NoError(t, err)
	require.True(t, register.IsError)
	require.Equal(t, "invalid_request", callToolText(t, register))
	require.NotContains(t, callToolText(t, register), "payload-marker")

	tested, err := newSDKSession(t, requireCatalogSpec(t, "executeSmartFilterTest"), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name: "executeSmartFilterTest",
			Arguments: map[string]any{
				"body": map[string]any{"filterCode": "record.value == 'payload-marker'"},
			},
		},
	)
	require.NoError(t, err)
	require.True(t, tested.IsError)
	require.Equal(t, "invalid_request", callToolText(t, tested))
	require.NotContains(t, callToolText(t, tested), "payload-marker")

	app.testErr = nil
	app.testEvalErr = "evaluation failed for record.value == 'payload-marker'"
	evaluated, err := newSDKSession(t, requireCatalogSpec(t, "executeSmartFilterTest"), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name: "executeSmartFilterTest",
			Arguments: map[string]any{
				"body": map[string]any{"filterCode": "record.value == 'payload-marker'"},
			},
		},
	)
	require.NoError(t, err)
	require.True(t, evaluated.IsError)
	require.Equal(t, "invalid_request", callToolText(t, evaluated))
	require.NotContains(t, callToolText(t, evaluated), "payload-marker")

	oversized, err := newSDKSession(t, requireCatalogSpec(t, "executeSmartFilterTest"), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name: "executeSmartFilterTest",
			Arguments: map[string]any{
				"body": map[string]any{
					"filterCode": strings.Repeat("x", maxResultBytes+1),
				},
			},
		},
	)
	require.NoError(t, err)
	require.True(t, oversized.IsError)
	require.Equal(t, "invalid_request", callToolText(t, oversized))
	require.Equal(t, 2, app.testCalls)
}

func TestMessageToolsExposeContractShapedTypedSchemas(t *testing.T) {
	tools := listSDKCatalogTools(t)
	tests := []struct {
		name            string
		properties      []string
		required        []string
		bodyProperties  []string
		bodyRequired    []string
		queryProperties []string
		queryRequired   []string
	}{
		{
			name:       "getTopicMessagesV2",
			properties: []string{"clusterName", "query", "topicName"},
			required:   []string{"clusterName", "topicName"},
			queryProperties: []string{
				"cursor", "keySerde", "limit", "mode", "offset", "partitions",
				"smartFilterId", "stringFilter", "timestamp", "valueSerde",
			},
		},
		{
			name:            "getSerdes",
			properties:      []string{"clusterName", "query", "topicName"},
			required:        []string{"clusterName", "query", "topicName"},
			queryProperties: []string{"use"},
			queryRequired:   []string{"use"},
		},
		{
			name:           "registerFilter",
			properties:     []string{"body", "clusterName", "topicName"},
			required:       []string{"body", "clusterName", "topicName"},
			bodyProperties: []string{"filterCode"},
		},
		{
			name:       "sendTopicMessages",
			properties: []string{"body", "clusterName", "topicName"},
			required:   []string{"body", "clusterName", "topicName"},
			bodyProperties: []string{
				"headers", "key", "keySerde", "keySerdeProperties", "partition",
				"value", "valueSerde", "valueSerdeProperties",
			},
			bodyRequired: []string{"partition"},
		},
		{
			name:            "deleteTopicMessages",
			properties:      []string{"clusterName", "query", "topicName"},
			required:        []string{"clusterName", "topicName"},
			queryProperties: []string{"partitions"},
		},
		{
			name:           "executeSmartFilterTest",
			properties:     []string{"body"},
			required:       []string{"body"},
			bodyProperties: []string{"filterCode", "headers", "key", "offset", "partition", "timestampMs", "value"},
			bodyRequired:   []string{"filterCode"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema := requireSchemaObject(t, findTool(t, tools, tc.name).InputSchema)
			require.ElementsMatch(t, tc.properties, schemaPropertyNames(t, schema))
			require.ElementsMatch(t, tc.required, schemaRequired(t, schema))
			if len(tc.bodyProperties) > 0 {
				body := requireSchemaObject(t, schemaProperty(t, schema, "body"))
				require.ElementsMatch(t, tc.bodyProperties, schemaPropertyNames(t, body))
				require.ElementsMatch(t, tc.bodyRequired, schemaRequired(t, body))
			}
			if len(tc.queryProperties) > 0 {
				query := requireSchemaObject(t, schemaProperty(t, schema, "query"))
				require.ElementsMatch(t, tc.queryProperties, schemaPropertyNames(t, query))
				require.ElementsMatch(t, tc.queryRequired, schemaRequired(t, query))
			}
		})
	}
}

type recordingMessageReadOnlyResolver struct {
	mu       sync.Mutex
	readOnly bool
	clusters []string
}

func (r *recordingMessageReadOnlyResolver) Resolve(clusterName string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clusters = append(r.clusters, clusterName)
	return r.readOnly
}

func newMessageExecutor(
	t *testing.T,
	messages MessageServicer,
	serdes SerdeServicer,
	filters SmartFilterServicer,
	allowWrites bool,
	readOnly bool,
) (*Executor, *mcppolicy.Store, *recordingMessageReadOnlyResolver) {
	t.Helper()
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, allowWrites)))
	resolver := &recordingMessageReadOnlyResolver{readOnly: readOnly}
	executor, err := NewExecutor(Dependencies{
		Messages:     messages,
		Serdes:       serdes,
		SmartFilters: filters,
		Policy:       store,
		IsReadOnly:   resolver.Resolve,
	})
	require.NoError(t, err)
	return executor, store, resolver
}

func callMessageTool(t *testing.T, executor *Executor, name string, input map[string]any) *mcp.CallToolResult {
	t.Helper()
	result, err := newSDKSession(t, requireCatalogSpec(t, name), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{Name: name, Arguments: input},
	)
	require.NoError(t, err)
	require.False(t, result.IsError, callToolText(t, result))
	return result
}

func catalogNames() []string {
	names := make([]string, 0, len(Catalog()))
	for _, spec := range Catalog() {
		names = append(names, spec.Meta.Name)
	}
	return names
}

func messageHeaders(count int) map[string]any {
	headers := make(map[string]any, count)
	for index := 0; index < count; index++ {
		headers[string(rune('a'+index%26))+strings.Repeat("x", index/26)] = "value"
	}
	return headers
}
