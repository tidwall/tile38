// +build linux darwin

package controller

import (
	"bytes"
	"fmt"
	"syscall"
)

func (c *Controller) writeInfoCPU(w *bytes.Buffer) {
	var selfRu syscall.Rusage
	var cRu syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &selfRu)
	syscall.Getrusage(syscall.RUSAGE_CHILDREN, &cRu)
	fmt.Fprintf(w,
		"used_cpu_sys:%.2f\r\n"+
			"used_cpu_user:%.2f\r\n"+
			"used_cpu_sys_children:%.2f\r\n"+
			"used_cpu_user_children:%.2f\r\n",
		float64(selfRu.Stime.Sec)+float64(selfRu.Stime.Usec/1000000),
		float64(selfRu.Utime.Sec)+float64(selfRu.Utime.Usec/1000000),
		float64(cRu.Stime.Sec)+float64(cRu.Stime.Usec/1000000),
		float64(cRu.Utime.Sec)+float64(cRu.Utime.Usec/1000000),
	)
}
