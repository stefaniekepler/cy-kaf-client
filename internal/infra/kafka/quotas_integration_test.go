//go:build integration

package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// TestQuotasRoundTripAgainstRealBroker is P2c Task 3's soul test:
// upsert -> list (read back) -> upsert (drop one key) -> list (Remove
// verified). Uses the same authorizer fixture (ANONYMOUS super-user may alter
// quotas); a plain broker would work too, but sharing the fixture keeps the
// suite uniform.
func TestQuotasRoundTripAgainstRealBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	brokers := startAuthorizerBroker(t, ctx)

	pool := NewPool()
	defer pool.Close()
	def := cluster.Definition{Name: "it-quotas", Conn: cluster.ConnectionSpec{BootstrapServers: brokers}}

	// upsert two limits for user=svc
	require.NoError(t, pool.UpsertQuotas(ctx, def, cluster.ClientQuota{
		User: "svc", Quotas: map[string]float64{"producer_byte_rate": 1048576, "consumer_byte_rate": 2097152},
	}))

	find := func(qs []cluster.ClientQuota) *cluster.ClientQuota {
		for i := range qs {
			if qs[i].User == "svc" && qs[i].ClientID == "" && qs[i].IP == "" {
				return &qs[i]
			}
		}
		return nil
	}

	require.Eventually(t, func() bool {
		qs, err := pool.ListQuotas(ctx, def)
		if err != nil {
			return false
		}
		q := find(qs)
		return q != nil && q.Quotas["producer_byte_rate"] == 1048576 && q.Quotas["consumer_byte_rate"] == 2097152
	}, 30*time.Second, time.Second, "both quota keys should be readable")

	// upsert with only producer_byte_rate -> consumer_byte_rate must be Removed
	require.NoError(t, pool.UpsertQuotas(ctx, def, cluster.ClientQuota{
		User: "svc", Quotas: map[string]float64{"producer_byte_rate": 1048576},
	}))
	require.Eventually(t, func() bool {
		qs, err := pool.ListQuotas(ctx, def)
		if err != nil {
			return false
		}
		q := find(qs)
		if q == nil {
			return false
		}
		_, stillHasConsumer := q.Quotas["consumer_byte_rate"]
		return q.Quotas["producer_byte_rate"] == 1048576 && !stillHasConsumer
	}, 30*time.Second, time.Second, "consumer_byte_rate should be removed by full-replace upsert")

	// The same Pool must serialize full replacements for one structural entity.
	// Repeat with a simultaneous start so the acceptance check is stable rather
	// than relying on a single scheduler interleaving.
	for round := 0; round < 5; round++ {
		wantA := map[string]float64{"producer_byte_rate": float64(1000 + round)}
		wantB := map[string]float64{"consumer_byte_rate": float64(2000 + round)}
		start := make(chan struct{})
		errCh := make(chan error, 2)
		var ready sync.WaitGroup
		ready.Add(2)
		for _, desired := range []map[string]float64{wantA, wantB} {
			desired := desired
			go func() {
				ready.Done()
				<-start
				errCh <- pool.UpsertQuotas(ctx, def, cluster.ClientQuota{User: "svc", Quotas: desired})
			}()
		}
		ready.Wait()
		close(start)
		require.NoError(t, <-errCh)
		require.NoError(t, <-errCh)

		qs, err := pool.ListQuotas(ctx, def)
		require.NoError(t, err)
		got := find(qs)
		require.NotNil(t, got)
		require.True(t, quotaValuesEqual(got.Quotas, wantA) || quotaValuesEqual(got.Quotas, wantB),
			"concurrent full replacements must finish as exact A or B, never their union: %#v", got.Quotas)
	}

	// An empty desired quota map is a valid identity-scoped delete, not a
	// default-entity request and not an unconditional no-op.
	require.NoError(t, pool.UpsertQuotas(ctx, def, cluster.ClientQuota{User: "svc", Quotas: map[string]float64{}}))
	qs, err := pool.ListQuotas(ctx, def)
	require.NoError(t, err)
	if got := find(qs); got != nil {
		require.Empty(t, got.Quotas)
	}
}
