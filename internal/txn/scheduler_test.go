package txn

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestScheduler(t *testing.T) {
	readSlop := 100 * time.Millisecond
	writeSlop := 200 * time.Millisecond
	maxWriteWait := time.Second
	maxWriteDuration := time.Second / 3
	sched := NewScheduler(maxWriteWait, maxWriteDuration)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()

	var wg sync.WaitGroup

	deadlineFailures := []string{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				func() {
					start := time.Now()
					defer sched.Read()()
					elapsed := time.Now().Sub(start)
					if elapsed > maxWriteDuration+readSlop {
						deadlineFailures = append(deadlineFailures, fmt.Sprint("read took", elapsed, "expected", maxWriteDuration))
					}
				}()
			}
		}()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				func() {
					done, ts := sched.Scan()
					defer done()

					for j := 0; j < 100; j++ {
						time.Sleep(time.Second / 10)
						if ts.IsAborted() {
							return
						}
					}
				}()
			}
		}()
	}

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				func() {
					start := time.Now()
					defer sched.Write()()
					elapsed := time.Now().Sub(start)
					if elapsed > maxWriteWait+writeSlop {
						deadlineFailures = append(deadlineFailures, fmt.Sprint(" write took ", elapsed, " expected ", maxWriteWait))
					}
				}()
			}
		}()
	}

	wg.Wait()

	if len(deadlineFailures) > 0 {
		t.Error(deadlineFailures)
	}
}

func BenchmarkSchedulerUncontendedRead(b *testing.B) {
	sched := NewScheduler(1*time.Second, time.Second/3)
	for i := 0; i < b.N; i++ {
		sched.Read()()
	}
}

func BenchmarkRWMutexUncontendedRead(b *testing.B) {
	var mu sync.RWMutex
	for i := 0; i < b.N; i++ {
		mu.RLock()
		mu.RUnlock()
	}
}

func BenchmarkSchedulerUncontendedWrite(b *testing.B) {
	sched := NewScheduler(1*time.Second, time.Second/3)
	for i := 0; i < b.N; i++ {
		sched.Write()()
	}
}

func BenchmarkRWMutexUncontendedWrite(b *testing.B) {
	var mu sync.RWMutex
	for i := 0; i < b.N; i++ {
		mu.Lock()
		mu.Unlock()
	}
}

func BenchmarkSchedulerContended(b *testing.B) {
	var wg sync.WaitGroup
	sched := NewScheduler(1*time.Second, time.Second/3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			sched.Read()()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			sched.Write()()
		}
	}()

	wg.Wait()
}

func BenchmarkRWMutexContended(b *testing.B) {
	var wg sync.WaitGroup
	var mu sync.RWMutex

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			mu.RLock()
			mu.RUnlock()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			mu.Lock()
			mu.Unlock()
		}
	}()

	wg.Wait()
}
