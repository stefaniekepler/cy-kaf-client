package analysis

import (
	"math"
	"sort"
)

const sizeRelativeAccuracy = 0.01

var sizeHistogramGamma = (1 + sizeRelativeAccuracy) / (1 - sizeRelativeAccuracy)

type sizeHistogram struct {
	zeroCount int64
	total     int64
	buckets   map[int]int64
}

func newSizeHistogram() *sizeHistogram {
	return &sizeHistogram{buckets: make(map[int]int64)}
}

func (h *sizeHistogram) observe(value int64) {
	if h == nil {
		return
	}
	if h.buckets == nil {
		h.buckets = make(map[int]int64)
	}
	h.total++
	if value <= 0 {
		h.zeroCount++
		return
	}
	index := int(math.Ceil(math.Log(float64(value)) / math.Log(sizeHistogramGamma)))
	h.buckets[index]++
}

func (h *sizeHistogram) merge(other *sizeHistogram) {
	if h == nil || other == nil || h == other {
		return
	}
	if h.buckets == nil {
		h.buckets = make(map[int]int64)
	}
	h.zeroCount += other.zeroCount
	h.total += other.total
	for index, count := range other.buckets {
		h.buckets[index] += count
	}
}

func (h *sizeHistogram) quantile(q float64) int64 {
	if h == nil || h.total == 0 {
		return 0
	}
	if q < 0 {
		q = 0
	}
	rank := int64(math.Ceil(q * float64(h.total)))
	if rank < 1 {
		rank = 1
	}
	seen := int64(0)
	if h.zeroCount > 0 {
		seen = h.zeroCount
		if rank <= seen {
			return 0
		}
	}
	indexes := make([]int, 0, len(h.buckets))
	for index := range h.buckets {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		seen += h.buckets[index]
		if rank <= seen {
			representative := 2 * math.Pow(sizeHistogramGamma, float64(index)) / (sizeHistogramGamma + 1)
			if representative < 1 {
				return 1
			}
			if representative >= float64(math.MaxInt64) {
				return math.MaxInt64
			}
			return int64(math.Round(representative))
		}
	}
	// A rank above one is only possible for q > 1. Return the largest
	// represented value rather than silently returning a zero estimate.
	if len(indexes) == 0 {
		return 0
	}
	last := indexes[len(indexes)-1]
	representative := 2 * math.Pow(sizeHistogramGamma, float64(last)) / (sizeHistogramGamma + 1)
	if representative >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(math.Round(representative))
}
