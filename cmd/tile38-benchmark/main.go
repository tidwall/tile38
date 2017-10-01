package main

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tidwall/redbench"
	"github.com/tidwall/tile38/core"
)

var (
	hostname = "127.0.0.1"
	port     = 9851
	clients  = 50
	requests = 100000
	quiet    = false
	pipeline = 1
	csv      = false
	json     = false
	tests    = "PING,SET,GET,SEARCH,EVAL"
	redis    = false
)

var addr string

func showHelp() bool {
	gitsha := ""
	if core.GitSHA == "" || core.GitSHA == "0000000" {
		gitsha = ""
	} else {
		gitsha = " (git:" + core.GitSHA + ")"
	}
	fmt.Fprintf(os.Stdout, "tile38-benchmark %s%s\n\n", core.Version, gitsha)
	fmt.Fprintf(os.Stdout, "Usage: tile38-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests>]\n")

	fmt.Fprintf(os.Stdout, " -h <hostname>      Server hostname (default: %s)\n", hostname)
	fmt.Fprintf(os.Stdout, " -p <port>          Server port (default: %d)\n", port)
	fmt.Fprintf(os.Stdout, " -c <clients>       Number of parallel connections (default %d)\n", clients)
	fmt.Fprintf(os.Stdout, " -n <requests>      Total number or requests (default %d)\n", requests)
	fmt.Fprintf(os.Stdout, " -q                 Quiet. Just show query/sec values\n")
	fmt.Fprintf(os.Stdout, " -P <numreq>        Pipeline <numreq> requests. Default 1 (no pipeline).\n")
	fmt.Fprintf(os.Stdout, " -t <tests>         Only run the comma separated list of tests. The test\n")
	fmt.Fprintf(os.Stdout, "                    names are the same as the ones produced as output.\n")
	fmt.Fprintf(os.Stdout, " --csv              Output in CSV format.\n")
	fmt.Fprintf(os.Stdout, " --json             Request JSON responses (default is RESP output)\n")
	fmt.Fprintf(os.Stdout, " --redis            Runs against a Redis server\n")
	fmt.Fprintf(os.Stdout, "\n")
	return false
}

func parseArgs() bool {
	defer func() {
		if v := recover(); v != nil {
			if v, ok := v.(string); ok && v == "bad arg" {
				showHelp()
			}
		}
	}()

	args := os.Args[1:]
	readArg := func(arg string) string {
		if len(args) == 0 {
			panic("bad arg")
		}
		var narg = args[0]
		args = args[1:]
		return narg
	}
	readIntArg := func(arg string) int {
		n, err := strconv.ParseUint(readArg(arg), 10, 64)
		if err != nil {
			panic("bad arg")
		}
		return int(n)
	}
	badArg := func(arg string) bool {
		fmt.Fprintf(os.Stderr, "Unrecognized option or bad number of args for: '%s'\n", arg)
		return false
	}

	for len(args) > 0 {
		arg := readArg("")
		if arg == "--help" || arg == "-?" {
			return showHelp()
		}
		if !strings.HasPrefix(arg, "-") {
			args = append([]string{arg}, args...)
			break
		}
		switch arg {
		default:
			return badArg(arg)
		case "-h":
			hostname = readArg(arg)
		case "-p":
			port = readIntArg(arg)
		case "-c":
			clients = readIntArg(arg)
			if clients <= 0 {
				clients = 1
			}
		case "-n":
			requests = readIntArg(arg)
			if requests <= 0 {
				requests = 0
			}
		case "-q":
			quiet = true
		case "-P":
			pipeline = readIntArg(arg)
			if pipeline <= 0 {
				pipeline = 1
			}
		case "-t":
			tests = readArg(arg)
		case "--csv":
			csv = true
		case "--json":
			json = true
		case "--redis":
			redis = true
		}
	}
	return true
}

func fillOpts() *redbench.Options {
	opts := *redbench.DefaultOptions
	opts.CSV = csv
	opts.Clients = clients
	opts.Pipeline = pipeline
	opts.Quiet = quiet
	opts.Requests = requests
	opts.Stderr = os.Stderr
	opts.Stdout = os.Stdout
	return &opts
}

func randPoint() (lat, lon float64) {
	return rand.Float64()*180 - 90, rand.Float64()*360 - 180
}

func randRect() (minlat, minlon, maxlat, maxlon float64) {
	for {
		minlat, minlon = randPoint()
		maxlat, maxlon = minlat+1, minlon+1
		if maxlat <= 180 && maxlon <= 180 {
			return
		}
	}
}
func prepFn(conn net.Conn) bool {
	if json {
		conn.Write([]byte("output json\r\n"))
		resp := make([]byte, 100)
		conn.Read(resp)
	}
	return true
}
func main() {
	rand.Seed(time.Now().UnixNano())
	if !parseArgs() {
		return
	}
	addr = fmt.Sprintf("%s:%d", hostname, port)
	for _, test := range strings.Split(tests, ",") {
		switch strings.ToUpper(strings.TrimSpace(test)) {
		case "PING":
			redbench.Bench("PING", addr, fillOpts(), prepFn,
				func(buf []byte) []byte {
					return redbench.AppendCommand(buf, "PING")
				},
			)
		case "GEOADD":
			//GEOADD key longitude latitude member
			if redis {
				var i int64
				redbench.Bench("GEOADD", addr, fillOpts(), prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						lat, lon := randPoint()
						return redbench.AppendCommand(buf, "GEOADD", "key:bench",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"id:"+strconv.FormatInt(i, 10),
						)
					},
				)
			}

		case "SET", "SET-POINT", "SET-RECT", "SET-STRING":
			if redis {
				redbench.Bench("SET", addr, fillOpts(), prepFn,
					func(buf []byte) []byte {
						return redbench.AppendCommand(buf, "SET", "key:__rand_int__", "xxx")
					},
				)
			} else {
				var i int64
				switch strings.ToUpper(strings.TrimSpace(test)) {
				case "SET", "SET-POINT":
					redbench.Bench("SET (point)", addr, fillOpts(), prepFn,
						func(buf []byte) []byte {
							i := atomic.AddInt64(&i, 1)
							lat, lon := randPoint()
							return redbench.AppendCommand(buf, "SET", "key:bench", "id:"+strconv.FormatInt(i, 10), "POINT",
								strconv.FormatFloat(lat, 'f', 5, 64),
								strconv.FormatFloat(lon, 'f', 5, 64),
							)
						},
					)
				}
				switch strings.ToUpper(strings.TrimSpace(test)) {
				case "SET", "SET-RECT":
					redbench.Bench("SET (rect)", addr, fillOpts(), prepFn,
						func(buf []byte) []byte {
							i := atomic.AddInt64(&i, 1)
							minlat, minlon, maxlat, maxlon := randRect()
							return redbench.AppendCommand(buf, "SET", "key:bench", "id:"+strconv.FormatInt(i, 10), "BOUNDS",
								strconv.FormatFloat(minlat, 'f', 5, 64),
								strconv.FormatFloat(minlon, 'f', 5, 64),
								strconv.FormatFloat(maxlat, 'f', 5, 64),
								strconv.FormatFloat(maxlon, 'f', 5, 64),
							)
						},
					)
				}
				switch strings.ToUpper(strings.TrimSpace(test)) {
				case "SET", "SET-STRING":
					redbench.Bench("SET (string)", addr, fillOpts(), prepFn,
						func(buf []byte) []byte {
							i := atomic.AddInt64(&i, 1)
							return redbench.AppendCommand(buf, "SET", "key:bench", "id:"+strconv.FormatInt(i, 10), "STRING", "xxx")
						},
					)
				}
			}
		case "GET":
			if redis {
				redbench.Bench("GET", addr, fillOpts(), prepFn,
					func(buf []byte) []byte {
						return redbench.AppendCommand(buf, "GET", "key:__rand_int__")
					},
				)
			} else {
				var i int64
				redbench.Bench("GET (point)", addr, fillOpts(), prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						return redbench.AppendCommand(buf, "GET", "key:bench", "id:"+strconv.FormatInt(i, 10), "POINT")
					},
				)
				redbench.Bench("GET (rect)", addr, fillOpts(), prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						return redbench.AppendCommand(buf, "GET", "key:bench", "id:"+strconv.FormatInt(i, 10), "BOUNDS")
					},
				)
				redbench.Bench("GET (string)", addr, fillOpts(), prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						return redbench.AppendCommand(buf, "GET", "key:bench", "id:"+strconv.FormatInt(i, 10), "OBJECT")
					},
				)
			}
		case "SEARCH":
			if !redis {
				redbench.Bench("SEARCH (nearby 1km)", addr, fillOpts(), prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf, "NEARBY", "key:bench", "COUNT", "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"1000")
					},
				)
				redbench.Bench("SEARCH (nearby 10km)", addr, fillOpts(), prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf, "NEARBY", "key:bench", "COUNT", "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"10000")
					},
				)
				redbench.Bench("SEARCH (nearby 100km)", addr, fillOpts(), prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf, "NEARBY", "key:bench", "COUNT", "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"100000")
					},
				)
			}
		case "EVAL":
			if !redis{
				var i int64
				redbench.Bench("EVAL (get point)", addr, fillOpts(), prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						script := fmt.Sprintf("return tile38.call('GET, 'KEYS[1]', 'id:%d', 'point')", i)
						return redbench.AppendCommand(buf, "EVAL", script, "1", "key:bench")
					},
				)
				redbench.Bench("EVAL (set point)", addr, fillOpts(), prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						lat, lon := randPoint()
						script := fmt.Sprintf(
							"return tile38.call('SET, 'KEYS[1]', 'id:%d', 'point', %.5f', '%.5f)",
							i, lat, lon)
						return redbench.AppendCommand(buf, "EVAL", script, "1", "key:bench")
					},
				)
			}
		}
	}
}
