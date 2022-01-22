package collection

import (
	"sync/atomic"
	"time"
)

type OpStat struct {
	count       uint64
	duration    uint64
	minDuration uint64
	maxDuration uint64
}

func updateIfGreater(addr *uint64, new uint64) {
	for {
		old := atomic.LoadUint64(addr)
		if new > old {
			if !atomic.CompareAndSwapUint64(addr, old, new) {
				continue
			}
		}
		break
	}
}

func updateIfLess(addr *uint64, new uint64) {
	for {
		old := atomic.LoadUint64(addr)
		if new < old || old == 0 {
			if !atomic.CompareAndSwapUint64(addr, old, new) {
				continue
			}
		}
		break
	}
}

func (s *OpStat) record() func() {
	start := time.Now()
	return func() {
		duration := uint64(time.Since(start))
		atomic.AddUint64(&s.count, 1)
		atomic.AddUint64(&s.duration, duration)
		updateIfGreater(&s.maxDuration, duration)
		updateIfLess(&s.minDuration, duration)
	}
}

func (s *OpStat) Count() int64 {
	return int64(s.count)
}

func (s *OpStat) TotalDuration() time.Duration {
	return time.Duration(s.duration)
}

func (s *OpStat) MaxDuration() time.Duration {
	return time.Duration(s.maxDuration)
}

func (s *OpStat) MinDuration() time.Duration {
	return time.Duration(s.minDuration)
}

type CollectionStats struct {
	Get                OpStat
	Set                OpStat
	Delete             OpStat
	SetField           OpStat
	SetFields          OpStat
	Scan               OpStat
	ScanRange          OpStat
	SearchValues       OpStat
	SearchValuesRange  OpStat
	ScanGreaterOrEqual OpStat
	Within             OpStat
	Intersects         OpStat
	Nearby             OpStat
}
