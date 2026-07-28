package cluster_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// fakeSerdeProvider implements serde.Provider for SerdeService tests: records
// the (def, topic, use) it was last called with and returns a canned
// Suggestion -- same call-recording shape as fakeGroupAdmin/fakeTopicAdmin.
type fakeSerdeProvider struct {
	calls     int
	lastDef   cluster.Definition
	lastTopic string
	lastUse   serde.Usage
	result    serde.Suggestion
}

func (f *fakeSerdeProvider) Suggest(def cluster.Definition, topic string, use serde.Usage) serde.Suggestion {
	f.calls++
	f.lastDef = def
	f.lastTopic = topic
	f.lastUse = use
	return f.result
}

func (f *fakeSerdeProvider) Lookup(_ cluster.Definition, _ string) (serde.Serde, bool) {
	return nil, false
}

func newSerdeServiceForTest(def cluster.Definition, provider serde.Provider) *appcluster.SerdeService {
	res := appcluster.NewResolver([]cluster.Definition{def})
	return appcluster.NewSerdeService(res, provider)
}

func TestSerdeServiceSuggestUnknownClusterIsErrUnknownCluster(t *testing.T) {
	svc := newSerdeServiceForTest(cluster.Definition{Name: "prod"}, &fakeSerdeProvider{})
	_, err := svc.Suggest(context.Background(), "nope", "orders", serde.UsageDeserialize)
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

// TestSerdeServiceSuggestDelegatesToProviderForKnownCluster locks the
// "resolve name -> Definition, delegate straight through" shape: the
// resolved Definition, the topic and the use both reach the fake Provider
// unchanged, and its canned Suggestion is returned unchanged too.
func TestSerdeServiceSuggestDelegatesToProviderForKnownCluster(t *testing.T) {
	def := cluster.Definition{Name: "prod", DefaultKeySerde: "Hex"}
	want := serde.Suggestion{Value: []serde.Description{{Name: "String", Preferred: true}}}
	provider := &fakeSerdeProvider{result: want}
	svc := newSerdeServiceForTest(def, provider)

	got, err := svc.Suggest(context.Background(), "prod", "orders", serde.UsageSerialize)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, 1, provider.calls)
	require.Equal(t, def, provider.lastDef)
	require.Equal(t, "orders", provider.lastTopic)
	require.Equal(t, serde.UsageSerialize, provider.lastUse)
}
