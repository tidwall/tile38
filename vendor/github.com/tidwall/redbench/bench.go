package redbench

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

func readResp(rd *bufio.Reader, n int) error {
	for i := 0; i < n; i++ {
		line, err := rd.ReadBytes('\n')
		if err != nil {
			return err
		}
		switch line[0] {
		default:
			return errors.New("invalid server response")
		case '+', ':':
		case '-':
		case '$':
			n, err := strconv.ParseInt(string(line[1:len(line)-2]), 10, 64)
			if err != nil {
				return err
			}
			if n >= 0 {
				if _, err = io.CopyN(ioutil.Discard, rd, n+2); err != nil {
					return err
				}
			}
		case '*':
			n, err := strconv.ParseInt(string(line[1:len(line)-2]), 10, 64)
			if err != nil {
				return err
			}
			readResp(rd, int(n))
		}
	}
	return nil
}

// Options represents various options used by the Bench() function.
type Options struct {
	Requests int
	Clients  int
	Pipeline int
	Quiet    bool
	CSV      bool
	Stdout   io.Writer
	Stderr   io.Writer
}

// DefaultsOptions are the default options used by the Bench() function.
var DefaultOptions = &Options{
	Requests: 100000,
	Clients:  50,
	Pipeline: 1,
	Quiet:    false,
	CSV:      false,
	Stdout:   os.Stdout,
	Stderr:   os.Stderr,
}

// Bench performs a benchmark on the server at the specified address.
func Bench(
	name string,
	addr string,
	opts *Options,
	prep func(conn net.Conn) bool,
	fill func(buf []byte) []byte,
) {
	if opts == nil {
		opts = DefaultOptions
	}
	if opts.Stderr == nil {
		opts.Stderr = ioutil.Discard
	}
	if opts.Stdout == nil {
		opts.Stdout = ioutil.Discard
	}
	var totalPayload uint64
	var count uint64
	var duration int64
	rpc := opts.Requests / opts.Clients
	rpcex := opts.Requests % opts.Clients
	var tstop int64
	remaining := int64(opts.Clients)
	errs := make([]error, opts.Clients)
	durs := make([][]time.Duration, opts.Clients)
	conns := make([]net.Conn, opts.Clients)

	// create all clients
	for i := 0; i < opts.Clients; i++ {
		crequests := rpc
		if i == opts.Clients-1 {
			crequests += rpcex
		}
		durs[i] = make([]time.Duration, crequests)
		for j := 0; j < len(durs[i]); j++ {
			durs[i][j] = -1
		}
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			if i == 0 {
				fmt.Fprintf(opts.Stderr, "%s\n", err.Error())
				return
			}
			errs[i] = err
		}
		if conn != nil && prep != nil {
			if !prep(conn) {
				conn.Close()
				conn = nil
			}
		}
		conns[i] = conn
	}

	tstart := time.Now()
	for i := 0; i < opts.Clients; i++ {
		crequests := rpc
		if i == opts.Clients-1 {
			crequests += rpcex
		}

		go func(conn net.Conn, client, crequests int) {
			defer func() {
				atomic.AddInt64(&remaining, -1)
			}()
			if conn == nil {
				return
			}
			err := func() error {
				var buf []byte
				rd := bufio.NewReader(conn)
				for i := 0; i < crequests; i += opts.Pipeline {
					n := opts.Pipeline
					if i+n > crequests {
						n = crequests - i
					}
					buf = buf[:0]
					for i := 0; i < n; i++ {
						buf = fill(buf)
					}
					atomic.AddUint64(&totalPayload, uint64(len(buf)))
					start := time.Now()
					_, err := conn.Write(buf)
					if err != nil {
						return err
					}
					if err := readResp(rd, n); err != nil {
						return err
					}
					stop := time.Since(start)
					for j := 0; j < n; j++ {
						durs[client][i+j] = stop / time.Duration(n)
					}
					atomic.AddInt64(&duration, int64(stop))
					atomic.AddUint64(&count, uint64(n))
					atomic.StoreInt64(&tstop, int64(time.Since(tstart)))
				}
				return nil
			}()
			if err != nil {
				errs[client] = err
			}
		}(conns[i], i, crequests)
	}
	var die bool
	for {
		remaining := int(atomic.LoadInt64(&remaining))        // active clients
		count := int(atomic.LoadUint64(&count))               // completed requests
		real := time.Duration(atomic.LoadInt64(&tstop))       // real duration
		totalPayload := int(atomic.LoadUint64(&totalPayload)) // size of all bytes sent
		more := remaining > 0
		var realrps float64
		if real > 0 {
			realrps = float64(count) / (float64(real) / float64(time.Second))
		}
		if !opts.CSV {
			fmt.Fprintf(opts.Stdout, "\r%s: %.2f", name, realrps)
			if more {
				fmt.Fprintf(opts.Stdout, "\r")
			} else if opts.Quiet {
				fmt.Fprintf(opts.Stdout, " requests per second\n")
			} else {
				fmt.Fprintf(opts.Stdout, "\r====== %s ======\n", name)
				fmt.Fprintf(opts.Stdout, "  %d requests completed in %.2f seconds\n", opts.Requests, float64(real)/float64(time.Second))
				fmt.Fprintf(opts.Stdout, "  %d parallel clients\n", opts.Clients)
				fmt.Fprintf(opts.Stdout, "  %d bytes payload\n", totalPayload/opts.Requests)
				fmt.Fprintf(opts.Stdout, "  keep alive: 1\n")
				fmt.Fprintf(opts.Stdout, "\n")
				var limit time.Duration
				var lastper float64
				for {
					limit += time.Millisecond
					var hits, count int
					for i := 0; i < len(durs); i++ {
						for j := 0; j < len(durs[i]); j++ {
							dur := durs[i][j]
							if dur == -1 {
								continue
							}
							if dur < limit {
								hits++
							}
							count++
						}
					}
					per := float64(hits) / float64(count)
					if math.Floor(per*10000) == math.Floor(lastper*10000) {
						continue
					}
					lastper = per
					fmt.Fprintf(opts.Stdout, "%.2f%% <= %d milliseconds\n", per*100, (limit-time.Millisecond)/time.Millisecond)
					if per == 1.0 {
						break
					}
				}
				fmt.Fprintf(opts.Stdout, "%.2f requests per second\n\n", realrps)
			}
		}
		if !more {
			if opts.CSV {
				fmt.Fprintf(opts.Stdout, "\"%s\",\"%.2f\"\n", name, realrps)
			}
			for _, err := range errs {
				if err != nil {
					fmt.Fprintf(opts.Stderr, "%s\n", err)
					die = true
					if count == 0 {
						break
					}
				}
			}
			break
		}
		time.Sleep(time.Second / 5)
	}

	// close clients
	for i := 0; i < len(conns); i++ {
		if conns[i] != nil {
			conns[i].Close()
		}
	}
	if die {
		os.Exit(1)
	}
}

// AppendCommand will append a Redis command to the byte slice and
// returns a modifed slice.
func AppendCommand(buf []byte, args ...string) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(arg)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}
