package analysis

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHyperLogLogEstimateAndMerge(t *testing.T) {
	empty := newHyperLogLog()
	require.Zero(t, empty.estimate())
	empty.merge(nil)
	require.Zero(t, empty.estimate())

	left, right := newHyperLogLog(), newHyperLogLog()
	for i := 0; i < 7500; i++ {
		left.add([]byte(strconv.Itoa(i)))
	}
	for i := 5000; i < 10000; i++ {
		right.add([]byte(strconv.Itoa(i)))
	}
	require.InDelta(t, 7500, left.estimate(), 7500*0.03)
	left.merge(right)
	require.InDelta(t, 10000, left.estimate(), 10000*0.03)
}

func TestHyperLogLogIgnoresDuplicateInputs(t *testing.T) {
	h := newHyperLogLog()
	for i := 0; i < 100; i++ {
		h.add([]byte("same"))
	}
	require.InDelta(t, 1, h.estimate(), 1)
}
