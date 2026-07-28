package api

import (
	"errors"
	"net/http"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domainanalysis "github.com/cy-kaf/cy-kaf-client/internal/domain/analysis"
)

// AnalyzeTopic starts or restarts the in-memory whole-topic scan. The
// operation consumes records only, so readOnlyGuard allows it through its
// anchored whitelist before this handler is reached.
func (s *apiServer) AnalyzeTopic(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	if s.deps.Analysis == nil {
		serverError(w, "AnalyzeTopic", "failed to analyze topic", errors.New("analysis service unavailable"))
		return
	}
	if err := s.deps.Analysis.Analyze(r.Context(), clusterName, topicName); err != nil {
		switch {
		case errors.Is(err, appcluster.ErrUnknownCluster):
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
		case errors.Is(err, appcluster.ErrAnalysisTopicNotFound):
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "topic not found"))
		default:
			serverError(w, "AnalyzeTopic", "failed to analyze topic", err)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetTopicAnalysis returns either the live progress snapshot or the terminal
// result. A missing run is intentionally a 404 because the vendored UI uses
// that response to render its initial Start Analysis button.
func (s *apiServer) GetTopicAnalysis(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	if s.deps.Analysis == nil {
		serverError(w, "GetTopicAnalysis", "failed to get topic analysis", errors.New("analysis service unavailable"))
		return
	}
	view, found, err := s.deps.Analysis.Get(clusterName, topicName)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "GetTopicAnalysis", "failed to get topic analysis", err)
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "analysis not found"))
		return
	}
	writeJSON(w, http.StatusOK, analysisViewToGenerated(view))
}

// CancelTopicAnalysis removes the run from the service registry immediately;
// cancellation is idempotent for a known cluster and therefore always 204.
func (s *apiServer) CancelTopicAnalysis(w http.ResponseWriter, r *http.Request, clusterName, topicName string) {
	if s.deps.Analysis == nil {
		serverError(w, "CancelTopicAnalysis", "failed to cancel topic analysis", errors.New("analysis service unavailable"))
		return
	}
	if err := s.deps.Analysis.Cancel(r.Context(), clusterName, topicName); err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "CancelTopicAnalysis", "failed to cancel topic analysis", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func analysisViewToGenerated(view appcluster.AnalysisView) generated.TopicAnalysis {
	if view.Progress != nil {
		return generated.TopicAnalysis{Progress: ptr(generated.TopicAnalysisProgress{
			StartedAt:           ptr(view.Progress.StartedAt),
			CompletenessPercent: ptr(view.Progress.CompletenessPercent),
			MsgsScanned:         ptr(view.Progress.MsgsScanned),
			BytesScanned:        ptr(view.Progress.BytesScanned),
		})}
	}
	if view.Result != nil {
		return generated.TopicAnalysis{Result: ptr(analysisResultToGenerated(*view.Result))}
	}
	return generated.TopicAnalysis{}
}

func analysisResultToGenerated(result appcluster.AnalysisResult) generated.TopicAnalysisResult {
	partitions := make([]generated.TopicAnalysisStats, 0, len(result.PartitionStats))
	for _, stats := range result.PartitionStats {
		partitions = append(partitions, analysisStatsToGenerated(stats))
	}
	out := generated.TopicAnalysisResult{
		StartedAt:      ptr(result.StartedAt),
		FinishedAt:     ptr(result.FinishedAt),
		TotalStats:     ptr(analysisStatsToGenerated(result.TotalStats)),
		PartitionStats: ptr(partitions),
	}
	if result.Error != "" {
		out.Error = ptr(result.Error)
	}
	return out
}

func analysisStatsToGenerated(stats domainanalysis.Stats) generated.TopicAnalysisStats {
	out := generated.TopicAnalysisStats{}
	if stats.Partition != nil {
		out.Partition = ptr(*stats.Partition)
	}
	if !stats.HasData {
		return out
	}
	out.TotalMsgs = ptr(stats.TotalMsgs)
	out.MinOffset = ptr(stats.MinOffset)
	out.MaxOffset = ptr(stats.MaxOffset)
	out.MinTimestamp = ptr(stats.MinTimestamp)
	out.MaxTimestamp = ptr(stats.MaxTimestamp)
	out.NullKeys = ptr(stats.NullKeys)
	out.NullValues = ptr(stats.NullValues)
	out.ApproxUniqKeys = ptr(stats.ApproxUniqKeys)
	out.ApproxUniqValues = ptr(stats.ApproxUniqValues)
	out.KeySize = ptr(sizeStatsToGenerated(stats.KeySize))
	out.ValueSize = ptr(sizeStatsToGenerated(stats.ValueSize))
	if len(stats.HourlyMsgCounts) > 0 {
		hours := make([]struct {
			Count     *int64 `json:"count,omitempty"`
			HourStart *int64 `json:"hourStart,omitempty"`
		}, len(stats.HourlyMsgCounts))
		for i, hour := range stats.HourlyMsgCounts {
			hours[i].Count = ptr(hour.Count)
			hours[i].HourStart = ptr(hour.HourStart)
		}
		out.HourlyMsgCounts = ptr(hours)
	}
	return out
}

func sizeStatsToGenerated(stats domainanalysis.SizeStats) generated.TopicAnalysisSizeStats {
	return generated.TopicAnalysisSizeStats{
		Sum: ptr(stats.Sum), Min: ptr(stats.Min), Max: ptr(stats.Max), Avg: ptr(stats.Avg),
		Prctl50: ptr(stats.Prctl50), Prctl75: ptr(stats.Prctl75), Prctl95: ptr(stats.Prctl95),
		Prctl99: ptr(stats.Prctl99), Prctl999: ptr(stats.Prctl999),
	}
}

var _ AnalysisServicer = (*appcluster.AnalysisService)(nil)
