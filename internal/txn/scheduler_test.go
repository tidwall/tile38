package txn

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestScheduler(t *testing.T) {
	readSlop := 100 * time.Millisecond
	writeSlop := 200 * time.Millisecond
	maxWriteDuration := time.Second / 3
	scanDuration := time.Second
	maxWriteWait := scanDuration * 2
	sched := NewScheduler(250*time.Millisecond, maxWriteDuration)

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
				retry:
					for {
						for j := 0; j < 10; j++ {
							time.Sleep(time.Second / 10)
							if ts.IsAborted() {
								ts.Retry()
								continue retry
							}
						}
						return
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

func TestStarvation(t *testing.T) {
	writesPerSec := 1
	writeTime := 1 * time.Millisecond
	writerCount := 50

	readsPerSec := 10
	readTime := 1 * time.Millisecond
	readerCount := 100

	scansPerSec := 2
	scanTime := 3 * time.Second
	scannerCount := 10

	runTime := 20 * time.Second

	var writeCount int32
	var writeWaitTime int64

	var readCount int32
	var readWaitTime int64

	var scanCount int32
	var scanWaitTime int64
	var scanRestarts int32

	var wg sync.WaitGroup
	sched := NewScheduler(200*time.Millisecond, 50*time.Millisecond)

	done := int32(0)

	writer := func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second / time.Duration(writesPerSec))
		for atomic.LoadInt32(&done) == 0 {
			func() {
				start := time.Now()
				done := sched.Write()
				defer done()
				wait := time.Now().Sub(start)
				atomic.AddInt64(&writeWaitTime, int64(wait/time.Millisecond))
				time.Sleep(writeTime)
				atomic.AddInt32(&writeCount, 1)
			}()
			<-ticker.C
		}
	}

	reader := func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second / time.Duration(readsPerSec))
		for atomic.LoadInt32(&done) == 0 {
			func() {
				start := time.Now()
				done := sched.Read()
				defer done()
				wait := time.Now().Sub(start)
				atomic.AddInt64(&readWaitTime, int64(wait/time.Millisecond))
				time.Sleep(readTime)
				atomic.AddInt32(&readCount, 1)
			}()
			<-ticker.C
		}
	}

	scanner := func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second / time.Duration(scansPerSec))
		for atomic.LoadInt32(&done) == 0 {
			func() {
				start := time.Now()
				cleanup, ts := sched.Scan()
				defer cleanup()
				wait := time.Now().Sub(start)
				deadline := time.Now().Add(scanTime)
				for {
					time.Sleep(readTime)
					if atomic.LoadInt32(&done) != 0 {
						return
					}
					if ts.IsAborted() {
						atomic.AddInt32(&scanRestarts, 1)
						deadline = time.Now().Add(scanTime)
						start = time.Now()
						ts.Retry()
						wait += time.Now().Sub(start)
					}
					if time.Now().After(deadline) {
						atomic.AddInt64(&scanWaitTime, int64(wait/time.Millisecond))
						atomic.AddInt32(&scanCount, 1)
						return
					}
				}
			}()
			<-ticker.C
		}
	}

	for i := 0; i < writerCount; i++ {
		wg.Add(1)
		go writer()
	}

	for i := 0; i < readerCount; i++ {
		wg.Add(1)
		go reader()
	}

	for i := 0; i < scannerCount; i++ {
		wg.Add(1)
		go scanner()
	}

	time.Sleep(runTime)
	atomic.StoreInt32(&done, 1)
	wg.Wait()

	t.Logf("write count: %v	avg_delay: %v expect: %v", writeCount, writeWaitTime/int64(writeCount), writesPerSec*writerCount*(int(runTime/time.Second)))
	t.Logf("read count: %v	avg_delay: %v", readCount, readWaitTime/int64(readCount))
	t.Logf("scan count: %v	avg_delay: %v	restarts: %v", scanCount, scanWaitTime/int64(scanCount), scanRestarts)
	if scanCount < 1 {
		t.Fail()
	}
}

func TestReaderLockout(t *testing.T) {
	var wg sync.WaitGroup

	sched := NewScheduler(200*time.Millisecond, 50*time.Millisecond)

	scanDone, ts := sched.Scan()
	for i := 0; i < 5; i++ {
		ts.Retry()
	}

	var done int32
	maxTime := time.Duration(0)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for atomic.LoadInt32(&done) == 0 {
			start := time.Now()
			sched.Read()()
			t := time.Now().Sub(start)
			if t > maxTime {
				maxTime = t
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		sched.Write()()
		atomic.StoreInt32(&done, 1)
	}()

	for {
		if ts.IsAborted() {
			scanDone()
			break
		}
		time.Sleep(1)
	}

	wg.Wait()
	if maxTime > time.Second {
		t.Fatal("read took too long:", maxTime)
	}
}
