package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/tidwall/tile38/controller"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/hservice"
)

var (
	dir         string
	port        int
	host        string
	verbose     bool
	veryVerbose bool
	devMode     bool
	quiet       bool
)

// TODO: Set to false in 2.*
var httpTransport bool = true

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
		log.Default = log.New(os.Stderr, &log.Config{})
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
		log.Default = log.New(os.Stderr, &log.Config{})
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
		case "--http-transport":
			i++
			if i < len(os.Args) {
				switch strings.ToLower(os.Args[i]) {
				case "1", "true":
					httpTransport = true
				case "0", "false":
					httpTransport = false
				}
				continue
			}
		}
		nargs = append(nargs, os.Args[i])
	}
	os.Args = nargs

	flag.IntVar(&port, "p", 9851, "The listening port.")
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
	log.Default = log.New(logw, &log.Config{
		HideDebug: !veryVerbose,
		HideWarn:  !(veryVerbose || verbose),
	})
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

	httpTransportEnabled := "Enabled"
	if !httpTransport {
		httpTransportEnabled = "Disabled"
	}

	//  _____ _ _     ___ ___
	// |_   _|_| |___|_  | . |
	//   | | | | | -_|_  | . |
	//   |_| |_|_|___|___|___|

	fmt.Fprintf(logw, `
   _______ _______
  |       |       |
  |____   |   _   |   Tile38 %s%s %d bit (%s/%s)
  |       |       |   %sPort: %d, PID: %d
  |____   |   _   |   HTTP & WebSocket transports: %s
  |       |       |
  |_______|_______|   tile38.com
`+"\n", core.Version, gitsha, strconv.IntSize, runtime.GOARCH, runtime.GOOS, hostd, port, os.Getpid(), httpTransportEnabled)

	fmt.Printf("%v\n", httpTransport)

	if err := controller.ListenAndServe(host, port, dir, httpTransport); err != nil {
		log.Fatal(err)
	}
}
