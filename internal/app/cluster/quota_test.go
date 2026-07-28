package cluster_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

type fakeQuotaPort struct {
	list     []cluster.ClientQuota
	upserted []cluster.ClientQuota
}

func (f *fakeQuotaPort) ListQuotas(_ context.Context, _ cluster.Definition) ([]cluster.ClientQuota, error) {
	return f.list, nil
}
func (f *fakeQuotaPort) UpsertQuotas(_ context.Context, _ cluster.Definition, q cluster.ClientQuota) error {
	f.upserted = append(f.upserted, q)
	return nil
}

func newQuotaService(t *testing.T, port cluster.QuotaPort) *appcluster.QuotaService {
	t.Helper()
	res := appcluster.NewResolver([]cluster.Definition{{Name: "c1"}})
	return appcluster.NewQuotaService(res, port)
}

func TestListQuotasForwards(t *testing.T) {
	f := &fakeQuotaPort{list: []cluster.ClientQuota{{User: "u", Quotas: map[string]float64{"producer_byte_rate": 1}}}}
	got, err := newQuotaService(t, f).ListQuotas(context.Background(), "c1")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "u", got[0].User)
}

func TestUpsertQuotasForwards(t *testing.T) {
	f := &fakeQuotaPort{}
	q := cluster.ClientQuota{User: "u", Quotas: map[string]float64{"producer_byte_rate": 1024}}
	require.NoError(t, newQuotaService(t, f).UpsertQuotas(context.Background(), "c1", q))
	require.Equal(t, []cluster.ClientQuota{q}, f.upserted)
}

func TestUpsertQuotasRejectsMissingEntityBeforePort(t *testing.T) {
	f := &fakeQuotaPort{}
	err := newQuotaService(t, f).UpsertQuotas(context.Background(), "c1", cluster.ClientQuota{
		Quotas: map[string]float64{},
	})
	require.ErrorIs(t, err, appcluster.ErrBadQuotaRequest)
	require.Empty(t, f.upserted)
}

func TestQuotaUnknownClusterIsErrUnknownCluster(t *testing.T) {
	_, err := newQuotaService(t, &fakeQuotaPort{}).ListQuotas(context.Background(), "nope")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}
