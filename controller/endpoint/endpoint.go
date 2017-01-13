package endpoint

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// EndpointProtocol is the type of protocol that the endpoint represents.
type EndpointProtocol string

const (
	HTTP   = EndpointProtocol("http")   // HTTP
	Disque = EndpointProtocol("disque") // Disque
	GRPC   = EndpointProtocol("grpc")   // GRPC
	Redis  = EndpointProtocol("redis")  // Redis
)

// Endpoint represents an endpoint.
type Endpoint struct {
	Protocol EndpointProtocol
	Original string
	GRPC     struct {
		Host string
		Port int
	}
	Disque struct {
		Host      string
		Port      int
		QueueName string
		Options   struct {
			Replicate int
		}
	}
	Redis struct {
		Host		string
		Port		int
		Channel	string
	}
}

type EndpointConn interface {
	Expired() bool
	Send(val string) error
}

type EndpointManager struct {
	mu    sync.RWMutex // this is intentionally exposed
	conns map[string]EndpointConn
}

func NewEndpointManager() *EndpointManager {
	epc := &EndpointManager{
		conns: make(map[string]EndpointConn),
	}
	go epc.Run()
	return epc
}
func (epc *EndpointManager) Run() {
	for {
		time.Sleep(time.Second)
		func() {
			epc.mu.Lock()
			defer epc.mu.Unlock()
			for endpoint, conn := range epc.conns {
				if conn.Expired() {
					delete(epc.conns, endpoint)
				}
			}
		}()
	}
}

// Get finds an endpoint based on its url. If the enpoint does not
// exist a new only is created.
func (epc *EndpointManager) Validate(url string) error {
	_, err := parseEndpoint(url)
	return err
}

func (epc *EndpointManager) Send(endpoint, val string) error {
	epc.mu.Lock()
	conn, ok := epc.conns[endpoint]
	if !ok || conn.Expired() {
		ep, err := parseEndpoint(endpoint)
		if err != nil {
			epc.mu.Unlock()
			return err
		}
		switch ep.Protocol {
		default:
			return errors.New("invalid protocol")
		case HTTP:
			conn = newHTTPEndpointConn(ep)
		case Disque:
			conn = newDisqueEndpointConn(ep)
		case GRPC:
			conn = newGRPCEndpointConn(ep)
		case Redis:
			conn = newRedisEndpointConn(ep)
		}
		epc.conns[endpoint] = conn
	}
	epc.mu.Unlock()
	return conn.Send(val)
}

func parseEndpoint(s string) (Endpoint, error) {
	var endpoint Endpoint
	endpoint.Original = s
	switch {
	default:
		return endpoint, errors.New("unknown scheme")
	case strings.HasPrefix(s, "http:"):
		endpoint.Protocol = HTTP
	case strings.HasPrefix(s, "https:"):
		endpoint.Protocol = HTTP
	case strings.HasPrefix(s, "disque:"):
		endpoint.Protocol = Disque
	case strings.HasPrefix(s, "grpc:"):
		endpoint.Protocol = GRPC
	case strings.HasPrefix(s, "redis:"):
		endpoint.Protocol = Redis
	}

	s = s[strings.Index(s, ":")+1:]
	if !strings.HasPrefix(s, "//") {
		return endpoint, errors.New("missing the two slashes")
	}

	sqp := strings.Split(s[2:], "?")
	sp := strings.Split(sqp[0], "/")
	s = sp[0]
	if s == "" {
		return endpoint, errors.New("missing host")
	}

	if endpoint.Protocol == GRPC {
		dp := strings.Split(s, ":")
		switch len(dp) {
		default:
			return endpoint, errors.New("invalid grpc url")
		case 1:
			endpoint.GRPC.Host = dp[0]
			endpoint.GRPC.Port = 80
		case 2:
			endpoint.GRPC.Host = dp[0]
			n, err := strconv.ParseUint(dp[1], 10, 16)
			if err != nil {
				return endpoint, errors.New("invalid grpc url")
			}
			endpoint.GRPC.Port = int(n)
		}
	}

	if endpoint.Protocol == Redis {
		dp := strings.Split(s, ":")
		switch len(dp) {
		default:
			return endpoint, errors.New("invalid redis url")
		case 1:
			endpoint.Redis.Host = dp[0]
			endpoint.Redis.Port = 6379
		case 2:
			endpoint.Redis.Host = dp[0]
			n, err := strconv.ParseUint(dp[1], 10, 16)
			if err != nil {
				return endpoint, errors.New("invalid redis url port")
			}
			endpoint.Redis.Port = int(n)
		}

		if len(sp) > 1 {
			var err error
			endpoint.Redis.Channel, err = url.QueryUnescape(sp[1])
			if err != nil {
				return endpoint, errors.New("invalid redis channel name")
			}
		}
	}

	if endpoint.Protocol == Disque {
		dp := strings.Split(s, ":")
		switch len(dp) {
		default:
			return endpoint, errors.New("invalid disque url")
		case 1:
			endpoint.Disque.Host = dp[0]
			endpoint.Disque.Port = 7711
		case 2:
			endpoint.Disque.Host = dp[0]
			n, err := strconv.ParseUint(dp[1], 10, 16)
			if err != nil {
				return endpoint, errors.New("invalid disque url")
			}
			endpoint.Disque.Port = int(n)
		}
		if len(sp) > 1 {
			var err error
			endpoint.Disque.QueueName, err = url.QueryUnescape(sp[1])
			if err != nil {
				return endpoint, errors.New("invalid disque queue name")
			}
		}
		if len(sqp) > 1 {
			m, err := url.ParseQuery(sqp[1])
			if err != nil {
				return endpoint, errors.New("invalid disque url")
			}
			for key, val := range m {
				if len(val) == 0 {
					continue
				}
				switch key {
				case "replicate":
					n, err := strconv.ParseUint(val[0], 10, 8)
					if err != nil {
						return endpoint, errors.New("invalid disque replicate value")
					}
					endpoint.Disque.Options.Replicate = int(n)
				}
			}
		}
		if endpoint.Disque.QueueName == "" {
			return endpoint, errors.New("missing disque queue name")
		}
	}

	return endpoint, nil
}
