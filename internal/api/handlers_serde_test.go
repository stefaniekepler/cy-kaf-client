package api_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// fakeSerdesServicer implements api.SerdesServicer for the GetSerdes handler
// tests — same "error map wins, else result-map presence = known cluster"
// convention as fakeGroupServicer/fakeTopicServicer, plus last-call recording
// for the "use param reaches the handler" test.
type fakeSerdesServicer struct {
	result map[string]serde.Suggestion
	err    map[string]error

	lastTopic string
	lastUse   serde.Usage
}

func newFakeSerdesServicer() *fakeSerdesServicer {
	return &fakeSerdesServicer{result: map[string]serde.Suggestion{}, err: map[string]error{}}
}

func (f *fakeSerdesServicer) Suggest(_ context.Context, name, topic string, use serde.Usage) (serde.Suggestion, error) {
	f.lastTopic = topic
	f.lastUse = use
	if err, ok := f.err[name]; ok {
		return serde.Suggestion{}, err
	}
	sg, ok := f.result[name]
	if !ok {
		return serde.Suggestion{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return sg, nil
}

// withSerdes wires fs as Deps.Serdes — same pattern as withGroups/withTopics.
func withSerdes(fs *fakeSerdesServicer) testServerOption {
	return func(d *api.Deps) { d.Serdes = fs }
}

// --- GetSerdes ---

// TestGetSerdes locks the 200 wire shape: key/value each map onto a
// []SerdeDescription, preferred flows through per-entry. This is the
// crash-blocker endpoint (brief: vendored frontend's useSerdes hook calls
// this unconditionally via useSuspenseQuery from both the Messages tab
// filters and the Produce side panel), so response contract-validity here is
// exactly what unblocks both.
func TestGetSerdes(t *testing.T) {
	fs := newFakeSerdesServicer()
	fs.result["prod"] = serde.Suggestion{
		Key: []serde.Description{
			{Name: "String", Description: "UTF-8 text, passed through unchanged", Preferred: false},
			{Name: "Hex", Description: "hex-encoded bytes", Preferred: true},
		},
		Value: []serde.Description{
			{Name: "String", Description: "UTF-8 text, passed through unchanged", Preferred: true},
		},
	}
	srv := newTestServer(withSerdes(fs))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/topics/orders/serdes?use=DESERIALIZE", &got)
	require.Equal(t, 200, code)
	key, ok := got["key"].([]any)
	require.True(t, ok)
	require.Len(t, key, 2)
	k1, ok := key[1].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Hex", k1["name"])
	require.Equal(t, true, k1["preferred"])
	value, ok := got["value"].([]any)
	require.True(t, ok)
	require.Len(t, value, 1)
	v0, ok := value[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "String", v0["name"])
	require.Equal(t, "UTF-8 text, passed through unchanged", v0["description"])
	require.Equal(t, true, v0["preferred"])
	validateResponseOnlyAgainstContract(t, req, code, hdr, body)
}

func TestGetSerdesUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withSerdes(newFakeSerdesServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/topics/orders/serdes?use=SERIALIZE", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetSerdesBackendFailureIs500(t *testing.T) {
	fs := newFakeSerdesServicer()
	fs.err["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withSerdes(fs))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/topics/orders/serdes?use=SERIALIZE", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to compute serde suggestions", "kadm boom")
}

// TestGetSerdesUsePassedThroughToServicer locks that the contract's required
// "use" query param (generated.GetSerdesParams.Use, bound by the generated
// wrapper) reaches SerdesServicer.Suggest correctly mapped onto the domain
// serde.Usage enum for both SERIALIZE and DESERIALIZE.
func TestGetSerdesUsePassedThroughToServicer(t *testing.T) {
	cases := []struct {
		query   string
		wantUse serde.Usage
	}{
		{"SERIALIZE", serde.UsageSerialize},
		{"DESERIALIZE", serde.UsageDeserialize},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			fs := newFakeSerdesServicer()
			fs.result["prod"] = serde.Suggestion{}
			srv := newTestServer(withSerdes(fs))
			defer srv.Close()

			_, code, _, _ := getJSON(t, srv, "/api/clusters/prod/topics/orders/serdes?use="+tc.query, nil)
			require.Equal(t, 200, code)
			require.Equal(t, tc.wantUse, fs.lastUse)
			require.Equal(t, "orders", fs.lastTopic)
		})
	}
}
