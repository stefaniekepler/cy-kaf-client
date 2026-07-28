package analysis

import (
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	hllPrecision = uint(14)
	hllRegisters = 1 << hllPrecision
)

// hyperLogLog is a fixed-size, mergeable HyperLogLog sketch. It is kept
// private because the public analysis value types should expose estimates,
// not an implementation-specific sketch representation.
type hyperLogLog struct {
	registers []uint8
}

func newHyperLogLog() *hyperLogLog {
	return &hyperLogLog{registers: make([]uint8, hllRegisters)}
}

func (h *hyperLogLog) add(value []byte) {
	if h == nil {
		return
	}
	if len(h.registers) != hllRegisters {
		h.registers = make([]uint8, hllRegisters)
	}
	hasher := fnv.New64a()
	_, _ = hasher.Write(value)
	x := avalanche64(hasher.Sum64())
	index := int(x >> (64 - hllPrecision))
	remaining := x << hllPrecision
	rank := bits.LeadingZeros64(remaining) + 1
	maxRank := int(64-hllPrecision) + 1
	if rank > maxRank {
		rank = maxRank
	}
	if uint8(rank) > h.registers[index] {
		h.registers[index] = uint8(rank)
	}
}

// avalanche64 makes the high bits used for the register index as well mixed
// as the lower bits of FNV-1a while retaining FNV as the deterministic input
// hash mandated by the design.
func avalanche64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (h *hyperLogLog) merge(other *hyperLogLog) {
	if h == nil || other == nil {
		return
	}
	if len(h.registers) != hllRegisters {
		h.registers = make([]uint8, hllRegisters)
	}
	limit := len(other.registers)
	if limit > hllRegisters {
		limit = hllRegisters
	}
	for i := 0; i < limit; i++ {
		if other.registers[i] > h.registers[i] {
			h.registers[i] = other.registers[i]
		}
	}
}

func (h *hyperLogLog) estimate() int64 {
	if h == nil || len(h.registers) == 0 {
		return 0
	}
	var inverseSum float64
	zeros := 0
	for _, register := range h.registers {
		inverseSum += math.Ldexp(1, -int(register))
		if register == 0 {
			zeros++
		}
	}
	if inverseSum == 0 {
		return math.MaxInt64
	}
	m := float64(hllRegisters)
	alpha := 0.7213 / (1 + 1.079/m)
	raw := alpha * m * m / inverseSum
	if raw <= 2.5*m && zeros > 0 {
		raw = m * math.Log(m/float64(zeros))
	}
	if raw >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(math.Round(raw))
}
