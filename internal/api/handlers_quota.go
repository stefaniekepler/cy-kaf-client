package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// QuotaServicer is the narrow interface api's client-quota handlers consume;
// *appcluster.QuotaService satisfies it.
type QuotaServicer interface {
	ListQuotas(ctx context.Context, name string) ([]cluster.ClientQuota, error)
	UpsertQuotas(ctx context.Context, name string, quota cluster.ClientQuota) error
}

// clientQuotaToGenerated maps a domain ClientQuota to the contract's
// ClientQuotas. Quotas are converted from domain float64 to contract float32
// key-by-key; empty dimensions and quotas are omitted.
func clientQuotaToGenerated(q cluster.ClientQuota) generated.ClientQuotas {
	out := generated.ClientQuotas{}
	if q.User != "" {
		out.User = &q.User
	}
	if q.ClientID != "" {
		out.ClientId = &q.ClientID
	}
	if q.IP != "" {
		out.Ip = &q.IP
	}
	if len(q.Quotas) > 0 {
		quotas := make(map[string]float32, len(q.Quotas))
		for name, value := range q.Quotas {
			quotas[name] = float32(value)
		}
		out.Quotas = &quotas
	}
	return out
}

// clientQuotaFromGenerated maps a decoded contract request to the domain.
// Quotas are converted from contract float32 to domain float64 key-by-key.
func clientQuotaFromGenerated(body generated.ClientQuotas) cluster.ClientQuota {
	quota := cluster.ClientQuota{Quotas: map[string]float64{}}
	if body.User != nil {
		quota.User = *body.User
	}
	if body.ClientId != nil {
		quota.ClientID = *body.ClientId
	}
	if body.Ip != nil {
		quota.IP = *body.Ip
	}
	if body.Quotas != nil {
		for name, value := range *body.Quotas {
			quota.Quotas[name] = float64(value)
		}
	}
	return quota
}

// ListQuotas serves GET /api/clusters/{clusterName}/clientquotas.
func (s *apiServer) ListQuotas(w http.ResponseWriter, r *http.Request, clusterName string) {
	quotas, err := s.deps.Quotas.ListQuotas(r.Context(), clusterName)
	if err != nil {
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "ListQuotas", "failed to list client quotas", err)
		return
	}
	out := make([]generated.ClientQuotas, 0, len(quotas))
	for _, quota := range quotas {
		out = append(out, clientQuotaToGenerated(quota))
	}
	writeJSON(w, http.StatusOK, out)
}

// UpsertClientQuotas serves POST /api/clusters/{clusterName}/clientquotas.
// It is a genuine write, so readOnlyGuard rejects it before this handler for a
// read-only cluster.
func (s *apiServer) UpsertClientQuotas(w http.ResponseWriter, r *http.Request, clusterName string) {
	body, ok := decodeStrictJSON[generated.ClientQuotas](w, r)
	if !ok {
		return
	}
	if !validQuotaEntityPointers(*body) {
		writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid quota request"))
		return
	}
	if err := s.deps.Quotas.UpsertQuotas(r.Context(), clusterName, clientQuotaFromGenerated(*body)); err != nil {
		if errors.Is(err, appcluster.ErrBadQuotaRequest) {
			writeJSON(w, http.StatusBadRequest, errorResponse(http.StatusBadRequest, "invalid quota request"))
			return
		}
		if errors.Is(err, appcluster.ErrUnknownCluster) {
			writeJSON(w, http.StatusNotFound, errorResponse(http.StatusNotFound, "cluster not found"))
			return
		}
		serverError(w, "UpsertClientQuotas", "failed to upsert client quotas", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func validQuotaEntityPointers(body generated.ClientQuotas) bool {
	present := false
	for _, dimension := range []*string{body.User, body.ClientId, body.Ip} {
		if dimension == nil {
			continue
		}
		present = true
		if strings.TrimSpace(*dimension) == "" {
			return false
		}
	}
	return present
}
