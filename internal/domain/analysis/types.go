package analysis

// HourCount is the number of records whose timestamp falls in one UTC
// millisecond hour bucket.
type HourCount struct {
	HourStart int64
	Count     int64
}

// SizeStats contains exact aggregate sizes and approximate percentile sizes.
// Min/Max/Avg/Sum are exact; the five Prctl fields come from a mergeable
// logarithmic histogram.
type SizeStats struct {
	Sum, Min, Max, Avg        int64
	Prctl50, Prctl75, Prctl95 int64
	Prctl99, Prctl999         int64
}

// Stats is the domain representation of one partition's statistics or the
// total statistics when Partition is nil. HasData distinguishes an empty
// topic/partition from a populated one so the API layer can omit optional
// contract fields for the empty case.
type Stats struct {
	Partition *int32
	HasData   bool

	TotalMsgs, MinOffset, MaxOffset, MinTimestamp, MaxTimestamp int64
	NullKeys, NullValues, ApproxUniqKeys, ApproxUniqValues      int64
	KeySize, ValueSize                                          SizeStats
	HourlyMsgCounts                                             []HourCount
}

// Snapshot is the immutable result of an accumulator pass. Partitions are
// sorted by partition id and contain only partitions with at least one
// observed record.
type Snapshot struct {
	Total      Stats
	Partitions []Stats
}
