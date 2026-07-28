package api_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domainanalysis "github.com/cy-kaf/cy-kaf-client/internal/domain/analysis"
)

type fakeAnalysisServicer struct {
	analyzeErr error
	getErr     error
	cancelErr  error
	view       appcluster.AnalysisView
	found      bool
	analyzeN   int
	cancelN    int
}

func (f *fakeAnalysisServicer) Analyze(context.Context, string, string) error {
	f.analyzeN++
	return f.analyzeErr
}

func (f *fakeAnalysisServicer) Get(string, string) (appcluster.AnalysisView, bool, error) {
	return f.view, f.found, f.getErr
}

func (f *fakeAnalysisServicer) Cancel(context.Context, string, string) error {
	f.cancelN++
	return f.cancelErr
}

func withAnalysis(f *fakeAnalysisServicer) testServerOption {
	return func(d *api.Deps) { d.Analysis = f }
}

func TestAnalyzeTopic204(t *testing.T) {
	f := &fakeAnalysisServicer{}
	srv := newTestServer(withAnalysis(f))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/c1/topics/orders/analysis", "")
	require.Equal(t, http.StatusNoContent, code)
	require.Empty(t, body)
	require.Equal(t, 1, f.analyzeN)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestAnalyzeTopicMapsNotFoundAndBackendErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		msg  string
		code int
	}{
		{name: "unknown cluster", err: appcluster.ErrUnknownCluster, msg: "cluster not found", code: http.StatusNotFound},
		{name: "unknown topic", err: appcluster.ErrAnalysisTopicNotFound, msg: "topic not found", code: http.StatusNotFound},
		{name: "backend", err: errors.New("broker unavailable"), msg: "failed to analyze topic", code: http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeAnalysisServicer{analyzeErr: tc.err}
			srv := newTestServer(withAnalysis(f))
			defer srv.Close()
			req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/c1/topics/orders/analysis", "")
			require.Equal(t, tc.code, code)
			assertErrorEnvelope(t, body, tc.msg, "")
			validateAgainstContract(t, req, code, hdr, body)
		})
	}
}

func TestGetTopicAnalysisProgress200(t *testing.T) {
	f := &fakeAnalysisServicer{
		found: true,
		view: appcluster.AnalysisView{Progress: &appcluster.AnalysisProgress{
			StartedAt: 100, CompletenessPercent: 37.5, MsgsScanned: 3, BytesScanned: 99,
		}},
	}
	srv := newTestServer(withAnalysis(f))
	defer srv.Close()

	var out struct {
		Progress *struct {
			StartedAt           *int64   `json:"startedAt"`
			CompletenessPercent *float32 `json:"completenessPercent"`
			MsgsScanned         *int64   `json:"msgsScanned"`
			BytesScanned        *int64   `json:"bytesScanned"`
		} `json:"progress"`
		Result map[string]any `json:"result"`
	}
	req, code, hdr, body := doJSON(t, http.MethodGet, srv, "/api/clusters/c1/topics/orders/analysis", &out)
	require.Equal(t, http.StatusOK, code)
	require.NotNil(t, out.Progress)
	require.Equal(t, int64(100), *out.Progress.StartedAt)
	require.Equal(t, float32(37.5), *out.Progress.CompletenessPercent)
	require.Equal(t, int64(3), *out.Progress.MsgsScanned)
	require.Equal(t, int64(99), *out.Progress.BytesScanned)
	require.Nil(t, out.Result)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetTopicAnalysisResultMapsAllStats(t *testing.T) {
	partition := int32(2)
	f := &fakeAnalysisServicer{
		found: true,
		view: appcluster.AnalysisView{Result: &appcluster.AnalysisResult{
			StartedAt: 100, FinishedAt: 200,
			TotalStats: domainanalysis.Stats{
				HasData: true, TotalMsgs: 4, MinOffset: 3, MaxOffset: 9,
				MinTimestamp: 10, MaxTimestamp: 20, NullKeys: 1, NullValues: 2,
				ApproxUniqKeys: 3, ApproxUniqValues: 4,
				KeySize:         domainanalysis.SizeStats{Sum: 10, Min: 0, Max: 5, Avg: 2, Prctl50: 2, Prctl75: 3, Prctl95: 4, Prctl99: 5, Prctl999: 5},
				ValueSize:       domainanalysis.SizeStats{Sum: 20, Min: 0, Max: 8, Avg: 4, Prctl50: 4, Prctl75: 5, Prctl95: 6, Prctl99: 7, Prctl999: 8},
				HourlyMsgCounts: []domainanalysis.HourCount{{HourStart: 3_600_000, Count: 4}},
			},
			PartitionStats: []domainanalysis.Stats{{
				Partition: &partition, HasData: true, TotalMsgs: 4,
				KeySize: domainanalysis.SizeStats{Sum: 1}, ValueSize: domainanalysis.SizeStats{Sum: 2},
			}},
		}},
	}
	srv := newTestServer(withAnalysis(f))
	defer srv.Close()

	var out struct {
		Result struct {
			StartedAt      *int64         `json:"startedAt"`
			FinishedAt     *int64         `json:"finishedAt"`
			TotalStats     map[string]any `json:"totalStats"`
			PartitionStats []struct {
				Partition *int32 `json:"partition"`
				TotalMsgs *int64 `json:"totalMsgs"`
			} `json:"partitionStats"`
		} `json:"result"`
	}
	req, code, hdr, body := doJSON(t, http.MethodGet, srv, "/api/clusters/c1/topics/orders/analysis", &out)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, int64(100), *out.Result.StartedAt)
	require.Equal(t, int64(200), *out.Result.FinishedAt)
	require.Equal(t, float64(4), out.Result.TotalStats["totalMsgs"])
	require.Equal(t, float64(3), out.Result.TotalStats["approxUniqKeys"])
	require.Equal(t, float64(4), out.Result.TotalStats["approxUniqValues"])
	require.Equal(t, float64(10), out.Result.TotalStats["keySize"].(map[string]any)["sum"])
	require.Equal(t, float64(20), out.Result.TotalStats["valueSize"].(map[string]any)["sum"])
	require.Len(t, out.Result.TotalStats["hourlyMsgCounts"], 1)
	require.Len(t, out.Result.PartitionStats, 1)
	require.Equal(t, int32(2), *out.Result.PartitionStats[0].Partition)
	require.Equal(t, int64(4), *out.Result.PartitionStats[0].TotalMsgs)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetTopicAnalysisNotFoundAndBackendError(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    *fakeAnalysisServicer
		code int
		msg  string
	}{
		{name: "missing", f: &fakeAnalysisServicer{}, code: http.StatusNotFound, msg: "analysis not found"},
		{name: "unknown cluster", f: &fakeAnalysisServicer{getErr: appcluster.ErrUnknownCluster}, code: http.StatusNotFound, msg: "cluster not found"},
		{name: "backend", f: &fakeAnalysisServicer{getErr: errors.New("state failed")}, code: http.StatusInternalServerError, msg: "failed to get topic analysis"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestServer(withAnalysis(tc.f))
			defer srv.Close()
			req, code, hdr, body := doJSON(t, http.MethodGet, srv, "/api/clusters/c1/topics/orders/analysis", nil)
			require.Equal(t, tc.code, code)
			assertErrorEnvelope(t, body, tc.msg, "")
			validateAgainstContract(t, req, code, hdr, body)
		})
	}
}

func TestCancelTopicAnalysis204And404(t *testing.T) {
	f := &fakeAnalysisServicer{}
	srv := newTestServer(withAnalysis(f))
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/c1/topics/orders/analysis", nil)
	require.Equal(t, http.StatusNoContent, code)
	require.Empty(t, body)
	require.Equal(t, 1, f.cancelN)
	validateAgainstContract(t, req, code, hdr, body)

	f.cancelErr = appcluster.ErrUnknownCluster
	req, code, hdr, body = doJSON(t, http.MethodDelete, srv, "/api/clusters/c1/topics/orders/analysis", nil)
	require.Equal(t, http.StatusNotFound, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestAnalysisReadOnlyWhitelistAllowsPostAndDeleteButNotExtraPath(t *testing.T) {
	f := &fakeAnalysisServicer{}
	srv := newTestServer(withAnalysis(f), withReadOnly("ro"))
	defer srv.Close()
	_, code, _, _ := bodyJSON(t, http.MethodPost, srv, "/api/clusters/ro/topics/orders/analysis", "")
	require.Equal(t, http.StatusNoContent, code)
	_, code, _, _ = doJSON(t, http.MethodDelete, srv, "/api/clusters/ro/topics/orders/analysis", nil)
	require.Equal(t, http.StatusNoContent, code)
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/ro/topics/orders/analysis/extra", "")
	require.Equal(t, http.StatusForbidden, code)
	assertErrorEnvelope(t, body, "read-only mode", "")
	require.Equal(t, 1, f.analyzeN)
	require.Equal(t, 1, f.cancelN)
}
