package analysis

import "sort"

const analysisHourMillis int64 = 60 * 60 * 1000

// Accumulator collects exact counters plus mergeable approximate sketches for
// every partition observed in a topic scan.
type Accumulator struct {
	partitions map[int32]*partitionAccumulator
}

type partitionAccumulator struct {
	hasData      bool
	totalMsgs    int64
	minOffset    int64
	maxOffset    int64
	minTimestamp int64
	maxTimestamp int64
	nullKeys     int64
	nullValues   int64
	keyUnique    *hyperLogLog
	valueUnique  *hyperLogLog
	keySize      sizeAccumulator
	valueSize    sizeAccumulator
	hourly       map[int64]int64
}

type sizeAccumulator struct {
	count int64
	sum   int64
	min   int64
	max   int64
	hist  *sizeHistogram
}

func NewAccumulator() *Accumulator {
	return &Accumulator{partitions: make(map[int32]*partitionAccumulator)}
}

func (a *Accumulator) Observe(partition int32, offset, timestampMs int64, key, value []byte) {
	if a == nil {
		return
	}
	if a.partitions == nil {
		a.partitions = make(map[int32]*partitionAccumulator)
	}
	p := a.partitions[partition]
	if p == nil {
		p = newPartitionAccumulator()
		a.partitions[partition] = p
	}

	if !p.hasData {
		p.hasData = true
		p.minOffset, p.maxOffset = offset, offset
		p.minTimestamp, p.maxTimestamp = timestampMs, timestampMs
	} else {
		if offset < p.minOffset {
			p.minOffset = offset
		}
		if offset > p.maxOffset {
			p.maxOffset = offset
		}
		if timestampMs < p.minTimestamp {
			p.minTimestamp = timestampMs
		}
		if timestampMs > p.maxTimestamp {
			p.maxTimestamp = timestampMs
		}
	}

	p.totalMsgs++
	if key == nil {
		p.nullKeys++
	} else {
		p.keyUnique.add(key)
	}
	if value == nil {
		p.nullValues++
	} else {
		p.valueUnique.add(value)
	}
	p.keySize.observe(int64(len(key)))
	p.valueSize.observe(int64(len(value)))
	hour := floorHour(timestampMs)
	p.hourly[hour]++
}

func (a *Accumulator) Snapshot() Snapshot {
	if a == nil || len(a.partitions) == 0 {
		return Snapshot{}
	}
	ids := make([]int32, 0, len(a.partitions))
	for id := range a.partitions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	total := &partitionAccumulator{}
	partitions := make([]Stats, 0, len(ids))
	for _, id := range ids {
		p := a.partitions[id]
		if p == nil || !p.hasData {
			continue
		}
		total.merge(p)
		partition := id
		partitions = append(partitions, p.snapshot(&partition))
	}
	return Snapshot{Total: total.snapshot(nil), Partitions: partitions}
}

func newPartitionAccumulator() *partitionAccumulator {
	return &partitionAccumulator{
		keyUnique:   newHyperLogLog(),
		valueUnique: newHyperLogLog(),
		keySize:     newSizeAccumulator(),
		valueSize:   newSizeAccumulator(),
		hourly:      make(map[int64]int64),
	}
}

func newSizeAccumulator() sizeAccumulator {
	return sizeAccumulator{hist: newSizeHistogram()}
}

func (s *sizeAccumulator) observe(value int64) {
	if s.hist == nil {
		s.hist = newSizeHistogram()
	}
	if s.count == 0 {
		s.min, s.max = value, value
	} else {
		if value < s.min {
			s.min = value
		}
		if value > s.max {
			s.max = value
		}
	}
	s.count++
	s.sum += value
	s.hist.observe(value)
}

func (s *sizeAccumulator) merge(other *sizeAccumulator) {
	if s == nil || other == nil || other.count == 0 {
		return
	}
	if s.count == 0 {
		s.min, s.max = other.min, other.max
	}
	if other.min < s.min {
		s.min = other.min
	}
	if other.max > s.max {
		s.max = other.max
	}
	s.count += other.count
	s.sum += other.sum
	if s.hist == nil {
		s.hist = newSizeHistogram()
	}
	s.hist.merge(other.hist)
}

func (s *sizeAccumulator) snapshot() SizeStats {
	if s == nil || s.count == 0 {
		return SizeStats{}
	}
	return SizeStats{
		Sum: s.sum, Min: s.min, Max: s.max, Avg: s.sum / s.count,
		Prctl50: s.hist.quantile(0.50), Prctl75: s.hist.quantile(0.75),
		Prctl95: s.hist.quantile(0.95), Prctl99: s.hist.quantile(0.99),
		Prctl999: s.hist.quantile(0.999),
	}
}

func (p *partitionAccumulator) merge(other *partitionAccumulator) {
	if p == nil || other == nil || !other.hasData {
		return
	}
	if !p.hasData {
		p.hasData = true
		p.minOffset, p.maxOffset = other.minOffset, other.maxOffset
		p.minTimestamp, p.maxTimestamp = other.minTimestamp, other.maxTimestamp
		p.keyUnique = newHyperLogLog()
		p.valueUnique = newHyperLogLog()
		p.keySize = newSizeAccumulator()
		p.valueSize = newSizeAccumulator()
		p.hourly = make(map[int64]int64)
	} else {
		if other.minOffset < p.minOffset {
			p.minOffset = other.minOffset
		}
		if other.maxOffset > p.maxOffset {
			p.maxOffset = other.maxOffset
		}
		if other.minTimestamp < p.minTimestamp {
			p.minTimestamp = other.minTimestamp
		}
		if other.maxTimestamp > p.maxTimestamp {
			p.maxTimestamp = other.maxTimestamp
		}
	}
	p.totalMsgs += other.totalMsgs
	p.nullKeys += other.nullKeys
	p.nullValues += other.nullValues
	p.keyUnique.merge(other.keyUnique)
	p.valueUnique.merge(other.valueUnique)
	p.keySize.merge(&other.keySize)
	p.valueSize.merge(&other.valueSize)
	if p.hourly == nil {
		p.hourly = make(map[int64]int64)
	}
	for hour, count := range other.hourly {
		p.hourly[hour] += count
	}
}

func (p *partitionAccumulator) snapshot(partition *int32) Stats {
	if p == nil || !p.hasData {
		return Stats{Partition: partition}
	}
	return Stats{
		Partition:        partition,
		HasData:          true,
		TotalMsgs:        p.totalMsgs,
		MinOffset:        p.minOffset,
		MaxOffset:        p.maxOffset,
		MinTimestamp:     p.minTimestamp,
		MaxTimestamp:     p.maxTimestamp,
		NullKeys:         p.nullKeys,
		NullValues:       p.nullValues,
		ApproxUniqKeys:   p.keyUnique.estimate(),
		ApproxUniqValues: p.valueUnique.estimate(),
		KeySize:          p.keySize.snapshot(),
		ValueSize:        p.valueSize.snapshot(),
		HourlyMsgCounts:  sortedHours(p.hourly),
	}
}

func sortedHours(hourly map[int64]int64) []HourCount {
	if len(hourly) == 0 {
		return nil
	}
	keys := make([]int64, 0, len(hourly))
	for hour := range hourly {
		keys = append(keys, hour)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]HourCount, 0, len(keys))
	for _, hour := range keys {
		out = append(out, HourCount{HourStart: hour, Count: hourly[hour]})
	}
	return out
}

func floorHour(timestampMs int64) int64 {
	remainder := timestampMs % analysisHourMillis
	if remainder < 0 {
		remainder += analysisHourMillis
	}
	return timestampMs - remainder
}
