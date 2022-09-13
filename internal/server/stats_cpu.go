//go:build !linux && !darwin

package server

import (
	"bytes"
	"fmt"
)

func (s *Server) writeInfoCPU(w *bytes.Buffer) {
	fmt.Fprintf(w,
		"used_cpu_sys:%.2f\r\n"+
			"used_cpu_user:%.2f\r\n"+
			"used_cpu_sys_children:%.2f\r\n"+
			"used_cpu_user_children:%.2f\r\n",
		0.0, 0.0, 0.0, 0.0,
	)
}
