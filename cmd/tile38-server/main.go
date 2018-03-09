package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/tidwall/tile38/controller"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/hservice"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	dir         string
	port        int
	host        string
	verbose     bool
	veryVerbose bool
	devMode     bool
	quiet       bool
	pidfile     string
)

// TODO: Set to false in 2.*
var httpTransport = true

// Fire up a webhook test server by using the --webhook-http-consumer-port
// for example
//   $ ./tile38-server --webhook-http-consumer-port 9999
//
// The create hooks like such...
//   SETHOOK myhook http://localhost:9999/myhook NEARBY mykey FENCE POINT 33.5 -115.5 1000

type hserver struct{}

func (s *hserver) Send(ctx context.Context, in *hservice.MessageRequest) (*hservice.MessageReply, error) {
	return &hservice.MessageReply{true}, nil
}

func main() {
	if len(os.Args) == 3 && os.Args[1] == "--webhook-http-consumer-port" {
		log.SetOutput(os.Stderr)
		port, err := strconv.ParseUint(os.Args[2], 10, 16)
		if err != nil {
			log.Fatal(err)
		}
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			data, err := ioutil.ReadAll(r.Body)
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

	// parse non standard args.
	nargs := []string{os.Args[0]}
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--protected-mode", "-protected-mode":
			i++
			if i < len(os.Args) {
				switch strings.ToLower(os.Args[i]) {
				case "no":
					core.ProtectedMode = "no"
				case "yes":
					core.ProtectedMode = "yes"
				}
				continue
			}
			fmt.Fprintf(os.Stderr, "protected-mode must be 'yes' or 'no'\n")
			os.Exit(1)
		case "--dev", "-dev":
			devMode = true
			continue
		case "--http-transport", "-http-transport":
			i++
			if i < len(os.Args) {
				switch strings.ToLower(os.Args[i]) {
				case "1", "true", "yes":
					httpTransport = true
				case "0", "false", "no":
					httpTransport = false
				}
				continue
			}
			fmt.Fprintf(os.Stderr, "http-transport must be 'yes' or 'no'\n")
			os.Exit(1)
		}
		nargs = append(nargs, os.Args[i])
	}
	os.Args = nargs

	flag.IntVar(&port, "p", 9851, "The listening port.")
	flag.StringVar(&pidfile, "pidfile", "", "A file that contains the pid")
	flag.StringVar(&host, "h", "", "The listening host.")
	flag.StringVar(&dir, "d", "data", "The data directory.")
	flag.BoolVar(&verbose, "v", false, "Enable verbose logging.")
	flag.BoolVar(&quiet, "q", false, "Quiet logging. Totally silent.")
	flag.BoolVar(&veryVerbose, "vv", false, "Enable very verbose logging.")
	flag.Parse()

	var logw io.Writer = os.Stderr
	if quiet {
		logw = ioutil.Discard
	}
	log.SetOutput(logw)
	if quiet {
		log.Level = 0
	} else if veryVerbose {
		log.Level = 3
	} else if verbose {
		log.Level = 2
	} else {
		log.Level = 1
	}
	core.DevMode = devMode
	core.ShowDebugMessages = veryVerbose

	hostd := ""
	if host != "" {
		hostd = "Addr: " + host + ", "
	}
	gitsha := " (" + core.GitSHA + ")"
	if gitsha == " (0000000)" {
		gitsha = ""
	}
	var pidferr error

	var cleanedup bool
	var cleanupMu sync.Mutex
	cleanup := func() {
		cleanupMu.Lock()
		defer cleanupMu.Unlock()
		if cleanedup {
			return
		}
		// cleanup code
		if pidfile != "" {
			os.Remove(pidfile)
		}
		cleanedup = true
	}
	defer cleanup()

	if pidfile != "" {
		pidferr := ioutil.WriteFile(pidfile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0666)
		if pidferr == nil {
		}
	}

	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		s := <-c
		log.Warnf("signal: %v", s)
		if pidfile != "" {
			cleanup()
		}
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
			os.Exit(0xf)
		}
	}()

	//  _____ _ _     ___ ___
	// |_   _|_| |___|_  | . |
	//   | | | | | -_|_  | . |
	//   |_| |_|_|___|___|___|
	fmt.Fprintf(logw, `
   _______ _______
  |       |       |
  |____   |   _   |   Tile38 %s%s %d bit (%s/%s)
  |       |       |   %sPort: %d, PID: %d
  |____   |   _   | 
  |       |       |   tile38.com
  |_______|_______| 
`+"\n", core.Version, gitsha, strconv.IntSize, runtime.GOARCH, runtime.GOOS, hostd, port, os.Getpid())
	if pidferr != nil {
		log.Warnf("pidfile: %v", pidferr)
	}
	if err := controller.ListenAndServe(host, port, dir, httpTransport); err != nil {
		log.Fatal(err)
	}
}
