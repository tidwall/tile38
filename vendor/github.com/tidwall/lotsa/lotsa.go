package lotsa

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Output is used to print elased time and ops/sec
var Output io.Writer

// Ops executed a number of operations over a multiple goroutines.
// count is the number of operations.
// threads is the number goroutines.
// op is the operation function
func Ops(count, threads int, op func(i, thread int)) {
	var start time.Time
	var wg sync.WaitGroup
	wg.Add(threads)
	output := Output
	if output != nil {
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
		var ss string
		if threads != 1 {
			ss = fmt.Sprintf("over %d threads ", threads)
		}
		fmt.Fprintf(output, "%s ops %sin %.0fms %s/sec\n",
			commaize(count), ss, dur.Seconds()*1000,
			commaize(int(float64(count)/dur.Seconds())))
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
