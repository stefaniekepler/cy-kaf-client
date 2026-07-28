package analysis

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSizeHistogramQuantilesAndMerge(t *testing.T) {
	left, right := newSizeHistogram(), newSizeHistogram()
	left.observe(0)
	for i := int64(1); i <= 5000; i++ {
		left.observe(i)
	}
	for i := int64(5001); i <= 10000; i++ {
		right.observe(i)
	}
	require.Zero(t, newSizeHistogram().quantile(0.5))
	require.Zero(t, left.quantile(0))
	left.merge(right)
	for _, tc := range []struct {
		q, want float64
	}{
		{0.50, 5000},
		{0.75, 7500},
		{0.95, 9500},
		{0.99, 9900},
		{0.999, 9990},
	} {
		require.InDelta(t, tc.want, left.quantile(tc.q), tc.want*0.02)
	}
}

func TestSizeHistogramHandlesNegativeAndNilMerge(t *testing.T) {
	h := newSizeHistogram()
	h.observe(-10)
	require.Zero(t, h.quantile(0.5))
	h.merge(nil)
	require.Zero(t, h.quantile(0.5))
}
