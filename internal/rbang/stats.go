package rbang

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

func (s *OpStat) SetCount(value uint64) {
	atomic.StoreUint64(&s.count, value)
}

func (s *OpStat) IncCount(value uint64) {
	atomic.AddUint64(&s.count, value)
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

type RTreeStats struct {
	Split  OpStat
	Join   OpStat
	Height OpStat

	SplitEntries OpStat
	JoinEntries  OpStat
}
