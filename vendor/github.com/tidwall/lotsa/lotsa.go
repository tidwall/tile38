package lotsa

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"
)

// Output is used to print elased time and ops/sec
var Output io.Writer

// MemUsage is used to output the memory usage
var MemUsage bool

// Ops executed a number of operations over a multiple goroutines.
// count is the number of operations.
// threads is the number goroutines.
// op is the operation function
func Ops(count, threads int, op func(i, thread int)) {
	var start time.Time
	var wg sync.WaitGroup
	wg.Add(threads)
	var ms1 runtime.MemStats
	output := Output
	if output != nil {
		if MemUsage {
			runtime.GC()
			runtime.ReadMemStats(&ms1)
		}
		start = time.Now()
	}
	for i := 0; i < threads; i++ {
		s, e := count/threads*i, count/threads*(i+1)
		if i == threads-1 {
			e = count
		}
		go func(i, s, e int) {
			defer wg.Done()
			for j := s; j < e; j++ {
				op(j, i)
			}
		}(i, s, e)
	}
	wg.Wait()

	if output != nil {
		dur := time.Since(start)
		var alloc uint64
		if MemUsage {
			var ms2 runtime.MemStats
			runtime.ReadMemStats(&ms2)
			alloc = ms2.HeapAlloc - ms1.HeapAlloc
		}
		WriteOutput(output, count, threads, dur, alloc)
	}
}

func commaize(n int) string {
	s1, s2 := fmt.Sprintf("%d", n), ""
	for i, j := len(s1)-1, 0; i >= 0; i, j = i-1, j+1 {
		if j%3 == 0 && j != 0 {
			s2 = "," + s2
		}
		s2 = string(s1[i]) + s2
	}
	return s2
}

func memstr(alloc uint64) string {
	switch {
	case alloc <= 1024:
		return fmt.Sprintf("%d bytes", alloc)
	case alloc <= 1024*1024:
		return fmt.Sprintf("%.1f KB", float64(alloc)/1024)
	case alloc <= 1024*1024*1024:
		return fmt.Sprintf("%.1f MB", float64(alloc)/1024/1024)
	default:
		return fmt.Sprintf("%.1f GB", float64(alloc)/1024/1024/1024)
	}
}

// WriteOutput writes an output line to the specified writer
func WriteOutput(w io.Writer, count, threads int, elapsed time.Duration, alloc uint64) {
	var ss string
	if threads != 1 {
		ss = fmt.Sprintf("over %d threads ", threads)
	}
	var nsop int
	if count > 0 {
		nsop = int(elapsed / time.Duration(count))
	}
	var allocstr string
	if alloc > 0 {
		var bops uint64
		if count > 0 {
			bops = alloc / uint64(count)
		}
		allocstr = fmt.Sprintf(", %s, %d bytes/op", memstr(alloc), bops)
	}
	fmt.Fprintf(w, "%s ops %sin %.0fms, %s/sec, %d ns/op%s\n",
		commaize(count), ss, elapsed.Seconds()*1000,
		commaize(int(float64(count)/elapsed.Seconds())),
		nsop, allocstr,
	)
}
