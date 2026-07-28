package analysis

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccumulatorEmptySnapshot(t *testing.T) {
	got := NewAccumulator().Snapshot()
	require.False(t, got.Total.HasData)
	require.Nil(t, got.Total.Partition)
	require.Empty(t, got.Partitions)
}

func TestAccumulatorExactFieldsAndTotalMerge(t *testing.T) {
	a := NewAccumulator()
	a.Observe(0, 3, 3_600_001, []byte("same"), []byte("abc"))
	a.Observe(0, 4, 3_600_999, nil, []byte{})
	a.Observe(1, 8, 7_200_001, []byte("same"), nil)
	a.Observe(1, 9, 7_200_002, []byte("other"), []byte("12345"))

	got := a.Snapshot()
	require.True(t, got.Total.HasData)
	require.Nil(t, got.Total.Partition)
	require.Equal(t, int64(4), got.Total.TotalMsgs)
	require.Equal(t, int64(3), got.Total.MinOffset)
	require.Equal(t, int64(9), got.Total.MaxOffset)
	require.Equal(t, int64(3_600_001), got.Total.MinTimestamp)
	require.Equal(t, int64(7_200_002), got.Total.MaxTimestamp)
	require.Equal(t, int64(1), got.Total.NullKeys)
	require.Equal(t, int64(1), got.Total.NullValues)
	require.InDelta(t, 2, got.Total.ApproxUniqKeys, 1)
	require.InDelta(t, 2, got.Total.ApproxUniqValues, 1)
	require.Equal(t, int64(13), got.Total.KeySize.Sum)
	require.Equal(t, int64(0), got.Total.KeySize.Min)
	require.Equal(t, int64(5), got.Total.KeySize.Max)
	require.Equal(t, int64(3), got.Total.KeySize.Avg)
	require.Equal(t, int64(8), got.Total.ValueSize.Sum)
	require.Equal(t, int64(0), got.Total.ValueSize.Min)
	require.Equal(t, int64(5), got.Total.ValueSize.Max)
	require.Equal(t, int64(2), got.Total.ValueSize.Avg)
	require.Equal(t, []HourCount{{HourStart: 3_600_000, Count: 2}, {HourStart: 7_200_000, Count: 2}}, got.Total.HourlyMsgCounts)

	require.Equal(t, []int32{0, 1}, []int32{*got.Partitions[0].Partition, *got.Partitions[1].Partition})
	require.Equal(t, int64(2), got.Partitions[0].TotalMsgs)
	require.Equal(t, int64(3), got.Partitions[0].MinOffset)
	require.Equal(t, int64(4), got.Partitions[0].MaxOffset)
	require.Equal(t, int64(1), got.Partitions[0].NullKeys)
	require.Equal(t, int64(3), got.Partitions[0].ValueSize.Sum)
	require.Equal(t, int64(2), got.Partitions[1].TotalMsgs)
	require.Equal(t, int64(8), got.Partitions[1].MinOffset)
	require.Equal(t, int64(9), got.Partitions[1].MaxOffset)
	require.Equal(t, int64(1), got.Partitions[1].NullValues)
	require.Equal(t, int64(5), got.Partitions[1].ValueSize.Sum)
}

func TestAccumulatorNonNilEmptyBytesAreNotNull(t *testing.T) {
	a := NewAccumulator()
	a.Observe(2, 0, 0, []byte{}, []byte{})
	stats := a.Snapshot().Partitions[0]
	require.Equal(t, int64(0), stats.NullKeys)
	require.Equal(t, int64(0), stats.NullValues)
	require.Equal(t, int64(1), stats.TotalMsgs)
	require.Equal(t, int64(0), stats.KeySize.Min)
	require.Equal(t, int64(0), stats.ValueSize.Min)
}
