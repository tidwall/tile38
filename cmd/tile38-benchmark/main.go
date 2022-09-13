package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/redbench"
	"github.com/tidwall/redcon"
	"github.com/tidwall/tile38/cmd/tile38-benchmark/az"
	"github.com/tidwall/tile38/core"
)

var (
	hostname = "127.0.0.1"
	port     = 9851
	auth     = ""
	clients  = 50
	requests = 100000
	quiet    = false
	pipeline = 1
	csv      = false
	json     = false
	allTests = "PING,SET,GET,INTERSECTS,WITHIN,NEARBY,EVAL"
	tests    = allTests
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
	fmt.Fprintf(os.Stdout, " -a <password>      Password for Tile38 Auth\n")
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
		case "-a":
			auth = readArg(arg)
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

func isValidRect(minlat, minlon, maxlat, maxlon float64) bool {
	return minlat > -90 && maxlat < 90 && minlon > -180 && maxlon < 180
}

func randRect(meters float64) (minlat, minlon, maxlat, maxlon float64) {
	for {
		lat, lon := randPoint()
		maxlat, _ = destinationPoint(lat, lon, meters, 0)
		_, maxlon = destinationPoint(lat, lon, meters, 90)
		minlat, _ = destinationPoint(lat, lon, meters, 180)
		_, minlon = destinationPoint(lat, lon, meters, 270)
		if isValidRect(minlat, minlon, maxlat, maxlon) {
			return
		}
	}
}

func prepFn(conn net.Conn) bool {
	var resp [64]byte
	conn.Write([]byte("CONFIG GET requirepass\r\n"))
	n, err := conn.Read(resp[:])
	if err != nil {
		log.Fatal(err)
	}
	if string(resp[:n]) == "-ERR authentication required\r\n" {
		if auth == "" {
			log.Fatal("invalid auth")
		} else {
			cmd := redcon.AppendArray(nil, 2)
			cmd = redcon.AppendBulkString(cmd, "AUTH")
			cmd = redcon.AppendBulkString(cmd, auth)
			conn.Write(cmd)
			n, err := conn.Read(resp[:])
			if err != nil || string(resp[:n]) != "+OK\r\n" {
				log.Fatal("invalid auth")
			}
		}
	} else if auth != "" {
		log.Fatal("invalid auth")
	}
	if json {
		conn.Write([]byte("output json\r\n"))
		conn.Read(make([]byte, 64))
	}
	return true
}
func main() {
	rand.Seed(time.Now().UnixNano())
	if !parseArgs() {
		return
	}
	opts := fillOpts()
	addr = fmt.Sprintf("%s:%d", hostname, port)

	testsArr := strings.Split(allTests, ",")
	var subtract bool
	var add bool
	for _, test := range strings.Split(tests, ",") {
		if strings.HasPrefix(test, "-") {
			if add {
				os.Stderr.Write([]byte("test flag cannot mix add and subtract\n"))
				os.Exit(1)
			}
			subtract = true
			for i := range testsArr {
				if strings.EqualFold(testsArr[i], test[1:]) {
					testsArr = append(testsArr[:i], testsArr[i+1:]...)
					break
				}
			}
		} else if subtract {
			add = true
			os.Stderr.Write([]byte("test flag cannot mix add and subtract\n"))
			os.Exit(1)
		}
	}
	if !subtract {
		testsArr = strings.Split(tests, ",")
	}

	for _, test := range testsArr {
		switch strings.ToUpper(strings.TrimSpace(test)) {
		case "PING":
			redbench.Bench("PING", addr, opts, prepFn,
				func(buf []byte) []byte {
					return redbench.AppendCommand(buf, "PING")
				},
			)
		case "GEOADD":
			//GEOADD key longitude latitude member
			if redis {
				var i int64
				redbench.Bench("GEOADD", addr, opts, prepFn,
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
				redbench.Bench("SET", addr, opts, prepFn,
					func(buf []byte) []byte {
						return redbench.AppendCommand(buf, "SET", "key:__rand_int__", "xxx")
					},
				)
			} else {
				var i int64
				switch strings.ToUpper(strings.TrimSpace(test)) {
				case "SET", "SET-POINT":
					redbench.Bench("SET (point)", addr, opts, prepFn,
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
					redbench.Bench("SET (rect)", addr, opts, prepFn,
						func(buf []byte) []byte {
							i := atomic.AddInt64(&i, 1)
							minlat, minlon, maxlat, maxlon := randRect(10000)
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
					redbench.Bench("SET (string)", addr, opts, prepFn,
						func(buf []byte) []byte {
							i := atomic.AddInt64(&i, 1)
							return redbench.AppendCommand(buf, "SET", "key:bench", "id:"+strconv.FormatInt(i, 10), "STRING", "xxx")
						},
					)
				}
			}
		case "GET":
			if redis {
				redbench.Bench("GET", addr, opts, prepFn,
					func(buf []byte) []byte {
						return redbench.AppendCommand(buf, "GET", "key:__rand_int__")
					},
				)
			} else {
				var i int64
				redbench.Bench("GET (point)", addr, opts, prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						return redbench.AppendCommand(buf, "GET", "key:bench", "id:"+strconv.FormatInt(i, 10), "POINT")
					},
				)
				redbench.Bench("GET (rect)", addr, opts, prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						return redbench.AppendCommand(buf, "GET", "key:bench", "id:"+strconv.FormatInt(i, 10), "BOUNDS")
					},
				)
				redbench.Bench("GET (string)", addr, opts, prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						return redbench.AppendCommand(buf, "GET", "key:bench", "id:"+strconv.FormatInt(i, 10), "OBJECT")
					},
				)
			}
		case "INTERSECTS",
			"INTERSECTS-BOUNDS", "INTERSECTS-BOUNDS-1000", "INTERSECTS-BOUNDS-10000", "INTERSECTS-BOUNDS-100000",
			"INTERSECTS-CIRCLE", "INTERSECTS-CIRCLE-1000", "INTERSECTS-CIRCLE-10000", "INTERSECTS-CIRCLE-100000",
			"INTERSECTS-AZ":
			if redis {
				break
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "INTERSECTS", "INTERSECTS-CIRCLE", "INTERSECTS-CIRCLE-1000":
				redbench.Bench("INTERSECTS (intersects-circle 1km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf,
							"INTERSECTS", "key:bench", "COUNT", "CIRCLE",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"1000")
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "INTERSECTS", "INTERSECTS-CIRCLE", "INTERSECTS-CIRCLE-10000":
				redbench.Bench("INTERSECTS (intersects-circle 10km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf,
							"INTERSECTS", "key:bench", "COUNT", "CIRCLE",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"10000")
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "INTERSECTS", "INTERSECTS-CIRCLE", "INTERSECTS-CIRCLE-100000":
				redbench.Bench("INTERSECTS (intersects-circle 100km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf,
							"INTERSECTS", "key:bench", "COUNT", "CIRCLE",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"100000")
					},
				)
			}
			// INTERSECTS-BOUNDS
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "INTERSECTS", "INTERSECTS-BOUNDS", "INTERSECTS-BOUNDS-1000":
				minlat, minlon, maxlat, maxlon := randRect(1000)
				redbench.Bench("INTERSECTS (intersects-bounds 1km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						return redbench.AppendCommand(buf,
							"INTERSECTS", "key:bench", "COUNT", "BOUNDS",
							strconv.FormatFloat(minlat, 'f', 5, 64),
							strconv.FormatFloat(minlon, 'f', 5, 64),
							strconv.FormatFloat(maxlat, 'f', 5, 64),
							strconv.FormatFloat(maxlon, 'f', 5, 64))
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "INTERSECTS", "INTERSECTS-BOUNDS", "INTERSECTS-BOUNDS-10000":
				minlat, minlon, maxlat, maxlon := randRect(10000)
				redbench.Bench("INTERSECTS (intersects-bounds 10km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						return redbench.AppendCommand(buf,
							"INTERSECTS", "key:bench", "COUNT", "BOUNDS",
							strconv.FormatFloat(minlat, 'f', 5, 64),
							strconv.FormatFloat(minlon, 'f', 5, 64),
							strconv.FormatFloat(maxlat, 'f', 5, 64),
							strconv.FormatFloat(maxlon, 'f', 5, 64))
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "INTERSECTS", "INTERSECTS-BOUNDS", "INTERSECTS-BOUNDS-100000":
				minlat, minlon, maxlat, maxlon := randRect(10000)
				redbench.Bench("INTERSECTS (intersects-bounds 100km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						return redbench.AppendCommand(buf,
							"INTERSECTS", "key:bench", "COUNT", "BOUNDS",
							strconv.FormatFloat(minlat, 'f', 5, 64),
							strconv.FormatFloat(minlon, 'f', 5, 64),
							strconv.FormatFloat(maxlat, 'f', 5, 64),
							strconv.FormatFloat(maxlon, 'f', 5, 64))
					},
				)
			}

			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "INTERSECTS", "INTERSECTS-AZ":
				var mu sync.Mutex
				var loaded bool
				redbench.Bench("INTERSECTS (intersects-az limit 5)", addr, opts, func(conn net.Conn) bool {
					func() {
						mu.Lock()
						defer mu.Unlock()
						if loaded {
							return
						}
						loaded = true
						p := make([]byte, 0xFF)
						conn.Write([]byte("GET keys:bench:geo az point\r\n"))
						n, err := conn.Read(p)
						if err != nil {
							panic(err)
						}
						if string(p[:n]) != "$-1\r\n" {
							return
						}
						args := []string{"SET", "key:bench:geo", "az", "object", az.JSON}
						out := redcon.AppendArray(nil, len(args))
						for _, arg := range args {
							out = redcon.AppendBulkString(out, arg)
						}
						conn.Write(out)
						n, err = conn.Read(p)
						if err != nil {
							panic(err)
						}
						if string(p[:n]) != "+OK\r\n" {
							panic("expected OK")
						}
					}()
					return prepFn(conn)
				},
					func(buf []byte) []byte {
						args := []string{"INTERSECTS", "key:bench", "LIMIT", "5",
							"COUNT", "GET", "key:bench:geo", "az"}
						return redbench.AppendCommand(buf, args...)
					},
				)
			}

		case "WITHIN",
			"WITHIN-RECT", "WITHIN-RECT-1000", "WITHIN-RECT-10000", "WITHIN-RECT-100000",
			"WITHIN-CIRCLE", "WITHIN-CIRCLE-1000", "WITHIN-CIRCLE-10000", "WITHIN-CIRCLE-100000":
			if redis {
				break
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "WITHIN", "WITHIN-CIRCLE", "WITHIN-CIRCLE-1000":
				redbench.Bench("WITHIN (within-circle 1km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf,
							"WITHIN", "key:bench", "COUNT", "CIRCLE",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"1000")
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "WITHIN", "WITHIN-CIRCLE", "WITHIN-CIRCLE-10000":
				redbench.Bench("WITHIN (within-circle 10km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf,
							"WITHIN", "key:bench", "COUNT", "CIRCLE",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"10000")
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "WITHIN", "WITHIN-CIRCLE", "WITHIN-CIRCLE-100000":
				redbench.Bench("WITHIN (within-circle 100km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf,
							"WITHIN", "key:bench", "COUNT", "CIRCLE",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"100000")
					},
				)
			}
			// WITHIN-BOUNDS
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "WITHIN", "WITHIN-BOUNDS", "WITHIN-BOUNDS-1000":
				minlat, minlon, maxlat, maxlon := randRect(1000)
				redbench.Bench("WITHIN (within-bounds 1km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						return redbench.AppendCommand(buf,
							"WITHIN", "key:bench", "COUNT", "BOUNDS",
							strconv.FormatFloat(minlat, 'f', 5, 64),
							strconv.FormatFloat(minlon, 'f', 5, 64),
							strconv.FormatFloat(maxlat, 'f', 5, 64),
							strconv.FormatFloat(maxlon, 'f', 5, 64))
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "WITHIN", "WITHIN-BOUNDS", "WITHIN-BOUNDS-10000":
				minlat, minlon, maxlat, maxlon := randRect(10000)
				redbench.Bench("WITHIN (within-bounds 10km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						return redbench.AppendCommand(buf,
							"WITHIN", "key:bench", "COUNT", "BOUNDS",
							strconv.FormatFloat(minlat, 'f', 5, 64),
							strconv.FormatFloat(minlon, 'f', 5, 64),
							strconv.FormatFloat(maxlat, 'f', 5, 64),
							strconv.FormatFloat(maxlon, 'f', 5, 64))
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "WITHIN", "WITHIN-BOUNDS", "WITHIN-BOUNDS-100000":
				minlat, minlon, maxlat, maxlon := randRect(10000)
				redbench.Bench("WITHIN (within-bounds 100km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						return redbench.AppendCommand(buf,
							"WITHIN", "key:bench", "COUNT", "BOUNDS",
							strconv.FormatFloat(minlat, 'f', 5, 64),
							strconv.FormatFloat(minlon, 'f', 5, 64),
							strconv.FormatFloat(maxlat, 'f', 5, 64),
							strconv.FormatFloat(maxlon, 'f', 5, 64))
					},
				)
			}
		case "NEARBY",
			"NEARBY-KNN", "NEARBY-KNN-1", "NEARBY-KNN-10", "NEARBY-KNN-100",
			"NEARBY-POINT", "NEARBY-POINT-1000", "NEARBY-POINT-10000", "NEARBY-POINT-100000":
			if redis {
				break
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "NEARBY", "NEARBY-KNN", "NEARBY-KNN-1":
				redbench.Bench("NEARBY (limit 1)", addr, opts, prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf,
							"NEARBY", "key:bench", "LIMIT", "1", "COUNT", "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
						)
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "NEARBY", "NEARBY-KNN", "NEARBY-KNN-10":
				redbench.Bench("NEARBY (limit 10)", addr, opts, prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf,
							"NEARBY", "key:bench", "LIMIT", "10", "COUNT", "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
						)
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "NEARBY", "NEARBY-KNN", "NEARBY-KNN-100":
				redbench.Bench("NEARBY (limit 100)", addr, opts, prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf,
							"NEARBY", "key:bench", "LIMIT", "100", "COUNT", "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
						)
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "NEARBY", "NEARBY-POINT", "NEARBY-POINT-1000":
				redbench.Bench("NEARBY (point 1km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf,
							"NEARBY", "key:bench", "COUNT", "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"1000",
						)
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "NEARBY", "NEARBY-POINT", "NEARBY-POINT-10000":
				redbench.Bench("NEARBY (point 10km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf,
							"NEARBY", "key:bench", "COUNT", "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"10000",
						)
					},
				)
			}
			switch strings.ToUpper(strings.TrimSpace(test)) {
			case "NEARBY", "NEARBY-POINT", "NEARBY-POINT-100000":
				redbench.Bench("NEARBY (point 100km)", addr, opts, prepFn,
					func(buf []byte) []byte {
						lat, lon := randPoint()
						return redbench.AppendCommand(buf,
							"NEARBY", "key:bench", "COUNT", "POINT",
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
							"100000",
						)
					},
				)
			}
		case "EVAL":
			if !redis {
				var i int64
				getScript := "return tile38.call('GET', KEYS[1], ARGV[1], 'point')"
				get4Script :=
					"local a = tile38.call('GET', KEYS[1], ARGV[1], 'point');" +
						"local b = tile38.call('GET', KEYS[1], ARGV[2], 'point');" +
						"local c = tile38.call('GET', KEYS[1], ARGV[3], 'point');" +
						"local d = tile38.call('GET', KEYS[1], ARGV[4], 'point');" +
						"return d"

				setScript := "return tile38.call('SET', KEYS[1], ARGV[1], 'point', ARGV[2], ARGV[3])"
				if !opts.Quiet {
					fmt.Println("Scripts to run:")
					fmt.Println("GET SCRIPT: " + getScript)
					fmt.Println("GET FOUR SCRIPT: " + get4Script)
					fmt.Println("SET SCRIPT: " + setScript)
				}

				redbench.Bench("EVAL (set point)", addr, opts, prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						lat, lon := randPoint()
						return redbench.AppendCommand(buf, "EVAL", setScript, "1",
							"key:bench",
							"id:"+strconv.FormatInt(i, 10),
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
						)
					},
				)
				redbench.Bench("EVALNA (set point)", addr, opts, prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						lat, lon := randPoint()
						return redbench.AppendCommand(buf, "EVALNA", setScript, "1",
							"key:bench",
							"id:"+strconv.FormatInt(i, 10),
							strconv.FormatFloat(lat, 'f', 5, 64),
							strconv.FormatFloat(lon, 'f', 5, 64),
						)
					},
				)
				redbench.Bench("EVALRO (get point)", addr, opts, prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						return redbench.AppendCommand(buf, "EVALRO", getScript, "1", "key:bench", "id:"+strconv.FormatInt(i, 10))
					},
				)
				redbench.Bench("EVALRO (get 4 points)", addr, opts, prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						return redbench.AppendCommand(buf, "EVALRO", get4Script, "1",
							"key:bench",
							"id:"+strconv.FormatInt(i, 10),
							"id:"+strconv.FormatInt(i+1, 10),
							"id:"+strconv.FormatInt(i+2, 10),
							"id:"+strconv.FormatInt(i+3, 10),
						)
					},
				)
				redbench.Bench("EVALNA (get point)", addr, opts, prepFn,
					func(buf []byte) []byte {
						i := atomic.AddInt64(&i, 1)
						return redbench.AppendCommand(buf, "EVALNA", getScript, "1", "key:bench", "id:"+strconv.FormatInt(i, 10))
					},
				)
			}
		}
	}
}

const earthRadius = 6371e3

func toRadians(deg float64) float64 { return deg * math.Pi / 180 }
func toDegrees(rad float64) float64 { return rad * 180 / math.Pi }

// destinationPoint return the destination from a point based on a distance and bearing.
func destinationPoint(lat, lon, meters, bearingDegrees float64) (destLat, destLon float64) {
	// see http://williams.best.vwh.net/avform.htm#LL
	δ := meters / earthRadius // angular distance in radians
	θ := toRadians(bearingDegrees)
	φ1 := toRadians(lat)
	λ1 := toRadians(lon)
	φ2 := math.Asin(math.Sin(φ1)*math.Cos(δ) + math.Cos(φ1)*math.Sin(δ)*math.Cos(θ))
	λ2 := λ1 + math.Atan2(math.Sin(θ)*math.Sin(δ)*math.Cos(φ1), math.Cos(δ)-math.Sin(φ1)*math.Sin(φ2))
	λ2 = math.Mod(λ2+3*math.Pi, 2*math.Pi) - math.Pi // normalise to -180..+180°
	return toDegrees(φ2), toDegrees(λ2)
}
