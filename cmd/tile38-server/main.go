package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/tidwall/gjson"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/internal/hservice"
	"github.com/tidwall/tile38/internal/log"
	"github.com/tidwall/tile38/internal/server"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// TODO: Set to false in 2.*
var httpTransport = true

////////////////////////////////////////////////////////////////////////////////
//
// Fire up a webhook test server by using the --webhook-http-consumer-port
// for example
//   $ ./tile38-server --webhook-http-consumer-port 9999
//
// The create hooks like such...
//   SETHOOK myhook http://localhost:9999/myhook NEARBY mykey FENCE POINT 33.5 -115.5 1000
//
////////////////////////////////////////////////////////////////////////////////
//
// Memory profiling - start the server with the -pprofport flag
//
//   $ ./tile38-server -pprofport 6060
//
// Then, at any point, from a different terminal execute:
//   $ go tool pprof -svg http://localhost:6060/debug/pprof/heap > out.svg
//
// Load the SVG into a web browser to visualize the memory usage
//
////////////////////////////////////////////////////////////////////////////////

type hserver struct{}

func (s *hserver) Send(ctx context.Context, in *hservice.MessageRequest) (*hservice.MessageReply, error) {
	return &hservice.MessageReply{Ok: true}, nil
}

func main() {
	gitsha := " (" + core.GitSHA + ")"
	if gitsha == " (0000000)" {
		gitsha = ""
	}
	versionLine := `tile38-server version: ` + core.Version + gitsha

	output := os.Stderr
	flag.Usage = func() {
		fmt.Fprintf(output,
			"%s", versionLine+`

Usage: tile38-server [-p port]

Basic Options:
  -h hostname : listening host
  -p port     : listening port (default: 9851)
  -d path     : data directory (default: data)
  -s socket   : listen on unix socket file
  -l encoding : set log encoding to json or text (default: text) 
  -o output   : auto set client output to json or resp (default: resp) 
  -q          : no logging. totally silent output
  -v          : enable verbose logging
  -vv         : enable very verbose logging

Advanced Options: 
  --pidfile path          : file that contains the pid
  --appendonly yes/no     : AOF persistence (default: yes)
  --appendfilename path   : AOF path (default: data/appendonly.aof)
  --queuefilename path    : Event queue path (default:data/queue.db)
  --http-transport yes/no : HTTP transport (default: yes)
  --protected-mode yes/no : protected mode (default: yes)
  --nohup                 : do not exit on SIGHUP
  --spinlock              : use a spinlock. For very write-heavy workloads

Developer Options:
  --dev                             : enable developer mode
  --webhook-http-consumer-port port : Start a test HTTP webhook server
  --webhook-grpc-consumer-port port : Start a test GRPC webhook server

`,
		)
	}

	if len(os.Args) == 3 && os.Args[1] == "--webhook-http-consumer-port" {
		log.SetOutput(os.Stderr)
		port, err := strconv.ParseUint(os.Args[2], 10, 16)
		if err != nil {
			log.Fatal(err)
		}
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				log.Fatal(err)
			}
			log.HTTPf("http: %s : %s", r.URL.Path, string(data))
		})
		log.Infof("webhook server http://localhost:%d/", port)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
			log.Fatal(err)
		}
		return
	}

	if len(os.Args) == 3 && os.Args[1] == "--webhook-grpc-consumer-port" {
		log.SetOutput(os.Stderr)
		port, err := strconv.ParseUint(os.Args[2], 10, 16)
		if err != nil {
			log.Fatal(err)
		}

		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			log.Fatal(err)
		}
		s := grpc.NewServer()
		hservice.RegisterHookServiceServer(s, &hserver{})
		log.Infof("webhook server grpc://localhost:%d/", port)
		if err := s.Serve(lis); err != nil {
			log.Fatal(err)
		}
		return
	}

	var (
		nohup               bool
		showEvioDisabled    bool
		showThreadsDisabled bool
		spinlock            bool
	)

	var (
		// use to be in core/options.go

		// DevMode puts application in to dev mode
		devMode = false

		// ShowDebugMessages allows for log.Debug to print to console.
		showDebugMessages = false

		// ProtectedMode forces Tile38 to default in protected mode.
		protectedMode = "no"

		// AppendOnly allows for disabling the appendonly file.
		appendOnly = true

		// AppendFileName allows for custom appendonly file path
		appendFileName = ""

		// QueueFileName allows for custom queue.db file path
		queueFileName = ""

		// ClientOutput for auto assigning the output for client.
		clientOutput = ""
	)

	// parse non standard args.
	nargs := []string{os.Args[0]}
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--help":
			output = os.Stdout
			flag.Usage()
			return
		case "--version":
			fmt.Fprintf(os.Stdout, "%s\n", versionLine)
			return
		case "--protected-mode", "-protected-mode":
			i++
			if i < len(os.Args) {
				switch strings.ToLower(os.Args[i]) {
				case "no":
					protectedMode = "no"
					continue
				case "yes":
					protectedMode = "yes"
					continue
				}
			}
			fmt.Fprintf(os.Stderr, "protected-mode must be 'yes' or 'no'\n")
			os.Exit(1)
		case "--dev", "-dev":
			devMode = true
			continue
		case "--nohup", "-nohup":
			nohup = true
			continue
		case "--spinlock", "-spinlock":
			spinlock = true
			continue
		case "--appendonly", "-appendonly":
			i++
			if i < len(os.Args) {
				switch strings.ToLower(os.Args[i]) {
				case "no":
					appendOnly = false
					continue
				case "yes":
					appendOnly = true
					continue
				}
			}
			fmt.Fprintf(os.Stderr, "appendonly must be 'yes' or 'no'\n")
			os.Exit(1)
		case "--appendfilename", "-appendfilename":
			i++
			if i == len(os.Args) || os.Args[i] == "" {
				fmt.Fprintf(os.Stderr, "appendfilename must have a value\n")
				os.Exit(1)
			}
			appendFileName = os.Args[i]
		case "--queuefilename", "-queuefilename":
			i++
			if i == len(os.Args) || os.Args[i] == "" {
				fmt.Fprintf(os.Stderr, "queuefilename must have a value\n")
				os.Exit(1)
			}
			queueFileName = os.Args[i]
		case "-o":
			i++
			if i < len(os.Args) {
				switch strings.ToLower(os.Args[i]) {
				case "resp", "json":
					clientOutput = strings.ToLower(os.Args[i])
					continue
				}
			}
			fmt.Fprintf(os.Stderr, "output must be 'resp' or 'json'\n")
			os.Exit(1)
		case "--http-transport", "-http-transport":
			i++
			if i < len(os.Args) {
				switch strings.ToLower(os.Args[i]) {
				case "1", "true", "yes":
					httpTransport = true
					continue
				case "0", "false", "no":
					httpTransport = false
					continue
				}
			}
			fmt.Fprintf(os.Stderr, "http-transport must be 'yes' or 'no'\n")
			os.Exit(1)
		case "--threads", "-threads":
			i++
			if i < len(os.Args) {
				_, err := strconv.ParseUint(os.Args[i], 10, 16)
				if err != nil {
					fmt.Fprintf(os.Stderr, "threads must be a valid number\n")
					os.Exit(1)
				}
				showThreadsDisabled = true
				continue
			}
			fmt.Fprintf(os.Stderr, "threads must be a valid number \n")
			os.Exit(1)
		case "--evio", "-evio":
			i++
			if i < len(os.Args) {
				switch strings.ToLower(os.Args[i]) {
				case "no", "yes":
					showEvioDisabled = true
					continue
				}
			}
			fmt.Fprintf(os.Stderr, "evio must be 'yes' or 'no'\n")
			os.Exit(1)
		default:
			nargs = append(nargs, os.Args[i])
		}
	}
	os.Args = nargs

	metricsAddr := flag.String("metrics-addr", "", "The listening addr for Prometheus metrics.")

	var (
		dir         string
		port        int
		host        string
		unixSocket  string
		verbose     bool
		veryVerbose bool
		logEncoding string
		quiet       bool
		pidfile     string
		cpuprofile  string
		memprofile  string
		pprofport   int
	)

	flag.IntVar(&port, "p", 9851, "The listening port")
	flag.StringVar(&pidfile, "pidfile", "", "A file that contains the pid")
	flag.StringVar(&host, "h", "", "The listening host")
	flag.StringVar(&unixSocket, "s", "", "Listen on a unix socket")
	flag.StringVar(&dir, "d", "data", "The data directory")
	flag.StringVar(&logEncoding, "l", "text", "The log encoding json or text (default: text)")
	flag.BoolVar(&verbose, "v", false, "Enable verbose logging")
	flag.BoolVar(&quiet, "q", false, "Quiet logging. Totally silent")
	flag.BoolVar(&veryVerbose, "vv", false, "Enable very verbose logging")
	flag.IntVar(&pprofport, "pprofport", 0, "pprofport http at port")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to `file`")
	flag.StringVar(&memprofile, "memprofile", "", "write memory profile to `file`")
	flag.Parse()

	if logEncoding == "json" {
		log.SetLogJSON(true)
		data, _ := os.ReadFile(filepath.Join(dir, "config"))
		if gjson.GetBytes(data, "logconfig.encoding").String() == "json" {
			c := gjson.GetBytes(data, "logconfig").String()
			log.Build(c)
		} else {
			log.Build("")
		}
	}

	var logw io.Writer = os.Stderr
	if quiet {
		logw = io.Discard
	}

	log.SetOutput(logw)

	if quiet {
		log.SetLevel(0)
	} else if veryVerbose {
		log.SetLevel(3)
	} else if verbose {
		log.SetLevel(2)
	} else {
		log.SetLevel(1)
	}

	showDebugMessages = veryVerbose

	hostd := ""
	if host != "" {
		hostd = "Addr: " + host + ", "
	}

	// pprof
	if cpuprofile != "" {
		log.Debugf("cpuprofile active")
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
	}
	if memprofile != "" {
		log.Debug("memprofile active")
	}

	var pprofcleanedup bool
	var pprofcleanupMu sync.Mutex
	pprofcleanup := func() {
		pprofcleanupMu.Lock()
		defer pprofcleanupMu.Unlock()
		if pprofcleanedup {
			return
		}
		// cleanup code
		if cpuprofile != "" {
			pprof.StopCPUProfile()
		}
		if memprofile != "" {
			f, err := os.Create(memprofile)
			if err != nil {
				log.Fatal("could not create memory profile: ", err)
			}
			runtime.GC() // get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Fatal("could not write memory profile: ", err)
			}
			f.Close()
		}
		pprofcleanedup = true
	}
	defer pprofcleanup()

	if pprofport != 0 {
		log.Debugf("pprof http at port %d", pprofport)
		go func() {
			log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", pprofport), nil))
		}()
	}

	if unixSocket != "" {
		port = 0
	}

	// pid file
	var pidferr error
	var pidcleanedup bool
	var pidcleanupMu sync.Mutex
	pidcleanup := func() {
		if pidfile != "" {
			pidcleanupMu.Lock()
			defer pidcleanupMu.Unlock()
			if pidcleanedup {
				return
			}
			// cleanup code
			if pidfile != "" {
				os.Remove(pidfile)
			}
			pidcleanedup = true
		}
	}
	defer pidcleanup()
	if pidfile != "" {
		os.WriteFile(pidfile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0666)
	}

	c := make(chan os.Signal, 1)
	shutdown := make(chan bool, 1)

	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for s := range c {
			if s == syscall.SIGHUP && nohup {
				continue
			}
			log.Warnf("signal: %v", s)
			pidcleanup()
			pprofcleanup()
			switch {
			default:
				os.Exit(-1)
			case s == syscall.SIGHUP:
				os.Exit(1)
			case s == syscall.SIGINT:
				os.Exit(2)
			case s == syscall.SIGQUIT:
				os.Exit(3)
			case s == syscall.SIGTERM:
				shutdown <- true
			}
		}
	}()

	var saddr string
	if unixSocket != "" {
		saddr = fmt.Sprintf("Socket: %s", unixSocket)
	} else {
		saddr = fmt.Sprintf("Port: %d", port)
	}

	if log.LogJSON() {
		log.Printf(`Tile38 %s%s %d bit (%s/%s) %s%s, PID: %d. Visit tile38.com/sponsor to support the project`,
			core.Version, gitsha, strconv.IntSize, runtime.GOARCH, runtime.GOOS, hostd, saddr, os.Getpid())
	} else {
		fmt.Fprintf(logw, `
   _____ _ _     ___ ___
  |_   _|_| |___|_  | . |  Tile38 %s%s %d bit (%s/%s)
    | | | | | -_|_  | . |  %s%s, PID: %d
    |_| |_|_|___|___|___|  tile38.com

`, core.Version, gitsha, strconv.IntSize, runtime.GOARCH, runtime.GOOS, hostd,
			saddr, os.Getpid())
	}

	if pidferr != nil {
		log.Warnf("pidfile: %v", pidferr)
	}
	if showEvioDisabled {
		log.Warnf("evio is not currently supported")
	}
	if showThreadsDisabled {
		log.Warnf("thread flag is deprecated use GOMAXPROCS to set number of threads instead")
	}
	opts := server.Options{
		Host:              host,
		Port:              port,
		Dir:               dir,
		UseHTTP:           httpTransport,
		MetricsAddr:       *metricsAddr,
		UnixSocketPath:    unixSocket,
		DevMode:           devMode,
		ShowDebugMessages: showDebugMessages,
		ProtectedMode:     protectedMode,
		AppendOnly:        appendOnly,
		AppendFileName:    appendFileName,
		QueueFileName:     queueFileName,
		Shutdown:          shutdown,
		Spinlock:          spinlock,
		ClientOutput:      clientOutput,
	}
	if err := server.Serve(opts); err != nil {
		log.Fatal(err)
	}
}
