package controller

import (
	"sync"
	"sync/atomic"
	"time"
)

type aint struct{ v int64 }

func (a *aint) add(d int) int {
	return int(atomic.AddInt64(&a.v, int64(d)))
}
func (a *aint) get() int {
	return int(atomic.LoadInt64(&a.v))
}
func (a *aint) set(i int) int {
	return int(atomic.SwapInt64(&a.v, int64(i)))
}

type abool struct{ v int64 }

func (a *abool) on() bool {
	return atomic.LoadInt64(&a.v) != 0
}
func (a *abool) set(t bool) bool {
	if t {
		return atomic.SwapInt64(&a.v, 1) != 0
	}
	return atomic.SwapInt64(&a.v, 0) != 0
}

type astring struct {
	mu sync.Mutex
	v  string
}

func (a *astring) get() string {
	a.mu.Lock()
	p := a.v
	a.mu.Unlock()
	return p
}
func (a *astring) set(s string) string {
	a.mu.Lock()
	p := a.v
	a.v = s
	a.mu.Unlock()
	return p
}

type atime struct {
	mu sync.Mutex
	v  time.Time
}

func (a *atime) get() time.Time {
	a.mu.Lock()
	p := a.v
	a.mu.Unlock()
	return p
}
func (a *atime) set(t time.Time) time.Time {
	a.mu.Lock()
	p := a.v
	a.v = t
	a.mu.Unlock()
	return p
}
