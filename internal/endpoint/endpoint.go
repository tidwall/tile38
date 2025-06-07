package endpoint

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

var errExpired = errors.New("expired")

// Protocol is the type of protocol that the endpoint represents.
type Protocol string

const (
	// Local protocol
	Local = Protocol("local")
	// HTTP protocol
	HTTP = Protocol("http")
	// Disque protocol
	Disque = Protocol("disque")
	// GRPC protocol
	GRPC = Protocol("grpc")
	// Redis protocol
	Redis = Protocol("redis")
	// Kafka protocol
	Kafka = Protocol("kafka")
	// MQTT protocol
	MQTT = Protocol("mqtt")
	// AMQP protocol
	AMQP = Protocol("amqp")
	// SQS protocol
	SQS = Protocol("sqs")
	// Google Cloud Pubsub protocol
	PubSub = Protocol("pubsub")
	// NATS protocol
	NATS = Protocol("nats")
	// EventHub protocol
	EventHub = Protocol("sb")
)

// Endpoint represents an endpoint.
type Endpoint struct {
	Protocol Protocol
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
		Host    string
		Port    int
		Channel string
	}
	Kafka struct {
		Host       string
		Port       int
		TopicName  string
		Auth       string
		SSL        bool
		SASLSHA256 bool
		SASLSHA512 bool
		CACertFile string
		CertFile   string
		KeyFile    string
	}
	AMQP struct {
		URI          string
		SSL          bool
		QueueName    string
		RouteKey     string
		Type         string
		Durable      bool
		AutoDelete   bool
		Internal     bool
		NoWait       bool
		Mandatory    bool
		Immediate    bool
		DeliveryMode uint8
		Priority     uint8
	}
	MQTT struct {
		Host       string
		Port       int
		QueueName  string
		Qos        byte
		Retained   bool
		CACertFile string
		CertFile   string
		KeyFile    string
	}
	PubSub struct {
		Project  string
		Topic    string
		CredPath string
	}
	SQS struct {
		PlainURL    string
		QueueID     string
		Region      string
		CredPath    string
		CredProfile string
		QueueName   string
		CreateQueue bool
	}
	NATS struct {
		Host    string
		Port    int
		User    string
		Pass    string
		Topic   string
		Token   string
		TLS     bool
		TLSCert string
		TLSKey  string
		Secure  bool
		// Jetstream indicates publishing via jetstream acknowledgements.
		Jetstream          bool
		UserCredentialPath string
	}
	EventHub struct {
		ConnectionString string
	}
	Local struct {
		Channel string
	}
}

// Conn is an endpoint connection
type Conn interface {
	ExpireNow()
	Expired() bool
	Send(val string) error
}

// Manager manages all endpoints
type Manager struct {
	mu        sync.RWMutex
	conns     map[string]Conn
	publisher LocalPublisher
	shutdown  atomic.Bool    // atomic bool
	wg        sync.WaitGroup // run wait group
}

// NewManager returns a new manager
func NewManager(publisher LocalPublisher) *Manager {
	epc := &Manager{
		conns:     make(map[string]Conn),
		publisher: publisher,
	}
	epc.wg.Add(1)
	go epc.run()
	return epc
}

func (epc *Manager) Shutdown() {
	defer epc.wg.Wait()
	epc.shutdown.Store(true)
	// expire the connections
	epc.mu.Lock()
	defer epc.mu.Unlock()
	for _, conn := range epc.conns {
		conn.ExpireNow()
	}
}

// Run starts the managing of endpoints
func (epc *Manager) run() {
	defer epc.wg.Done()
	for {
		if epc.shutdown.Load() {
			return
		}
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

// Validate an endpoint url
func (epc *Manager) Validate(url string) error {
	_, err := parseEndpoint(url)
	return err
}

// Send send a message to an endpoint
func (epc *Manager) Send(endpoint, msg string) error {
	for {
		epc.mu.Lock()
		conn, exists := epc.conns[endpoint]
		if !exists || conn.Expired() {
			ep, err := parseEndpoint(endpoint)
			if err != nil {
				epc.mu.Unlock()
				return err
			}
			switch ep.Protocol {
			default:
				return errors.New("invalid protocol")
			case HTTP:
				conn = newHTTPConn(ep)
			case Disque:
				conn = newDisqueConn(ep)
			case GRPC:
				conn = newGRPCConn(ep)
			case Redis:
				conn = newRedisConn(ep)
			case Kafka:
				conn = newKafkaConn(ep)
			case MQTT:
				conn = newMQTTConn(ep)
			case AMQP:
				conn = newAMQPConn(ep)
			case PubSub:
				conn = newPubSubConn(ep)
			case SQS:
				conn = newSQSConn(ep)
			case NATS:
				conn = newNATSConn(ep)
			case Local:
				conn = newLocalConn(ep, epc.publisher)
			case EventHub:
				conn = newEventHubConn(ep)
			}
			epc.conns[endpoint] = conn
		}
		epc.mu.Unlock()
		err := conn.Send(msg)
		if err != nil {
			if err == errExpired {
				// it's possible that the connection has expired in-between
				// the last conn.Expired() check and now. If so, we should
				// just try the send again.
				continue
			}
			return err
		}
		return nil
	}
}

func parseEndpoint(s string) (Endpoint, error) {
	var endpoint Endpoint
	endpoint.Original = s
	switch {
	default:
		return endpoint, errors.New("unknown scheme")
	case strings.HasPrefix(s, "local:"):
		endpoint.Protocol = Local
	case strings.HasPrefix(s, "http:"):
		endpoint.Protocol = HTTP
	case strings.HasPrefix(s, "https:"):
		if probeSQS(s) {
			endpoint.SQS.PlainURL = s
			endpoint.Protocol = SQS
		} else {
			endpoint.Protocol = HTTP
		}
	case strings.HasPrefix(s, "disque:"):
		endpoint.Protocol = Disque
	case strings.HasPrefix(s, "grpc:"):
		endpoint.Protocol = GRPC
	case strings.HasPrefix(s, "redis:"):
		endpoint.Protocol = Redis
	case strings.HasPrefix(s, "kafka:"):
		endpoint.Protocol = Kafka
	case strings.HasPrefix(s, "amqp:"):
		endpoint.Protocol = AMQP
	case strings.HasPrefix(s, "amqps:"):
		endpoint.Protocol = AMQP
	case strings.HasPrefix(s, "mqtt:"):
		endpoint.Protocol = MQTT
	case strings.HasPrefix(s, "pubsub:"):
		endpoint.Protocol = PubSub
	case strings.HasPrefix(s, "sqs:"):
		endpoint.Protocol = SQS
	case strings.HasPrefix(s, "nats:"):
		endpoint.Protocol = NATS
	case strings.HasPrefix(s, "Endpoint="):
		endpoint.Protocol = EventHub
	}

	s = s[strings.Index(s, ":")+1:]
	if !strings.HasPrefix(s, "//") {
		return endpoint, errors.New("missing the two slashes")
	}

	sqp := strings.Split(s[2:], "?")
	sp := strings.Split(sqp[0], "/")
	s = sp[0]
	if s == "" {
		if endpoint.Protocol == Local {
			return endpoint, errors.New("missing channel")
		}
		return endpoint, errors.New("missing host")
	}

	// Local PubSub channel
	// local://<channel>
	if endpoint.Protocol == Local {
		endpoint.Local.Channel = s
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

	if endpoint.Protocol == Kafka {
		// Parsing connection from URL string
		hp := strings.Split(s, ":")
		switch len(hp) {
		default:
			return endpoint, errors.New("invalid kafka url")
		case 1:
			endpoint.Kafka.Host = hp[0]
			endpoint.Kafka.Port = 9092
		case 2:
			n, err := strconv.ParseUint(hp[1], 10, 16)
			if err != nil {
				return endpoint, errors.New("invalid kafka url port")
			}

			endpoint.Kafka.Host = hp[0]
			endpoint.Kafka.Port = int(n)
		}

		// Parsing Kafka queue name
		if len(sp) > 1 {
			var err error
			endpoint.Kafka.TopicName, err = url.QueryUnescape(sp[1])
			if err != nil {
				return endpoint, errors.New("invalid kafka topic name")
			}
		}

		// Throw error if we not provide any queue name
		if endpoint.Kafka.TopicName == "" {
			return endpoint, errors.New("missing kafka topic name")
		}

		// Parsing additional params
		if len(sqp) > 1 {
			m, err := url.ParseQuery(sqp[1])
			if err != nil {
				return endpoint, errors.New("invalid kafka url")
			}
			for key, val := range m {
				if len(val) == 0 {
					continue
				}
				switch key {
				case "auth":
					endpoint.Kafka.Auth = val[0]
				case "ssl":
					endpoint.Kafka.SSL, _ = strconv.ParseBool(val[0])
				case "cacert":
					endpoint.Kafka.CACertFile = val[0]
				case "cert":
					endpoint.Kafka.CertFile = val[0]
				case "key":
					endpoint.Kafka.KeyFile = val[0]
				case "sha256":
					endpoint.Kafka.SASLSHA256, _ = strconv.ParseBool(val[0])
				case "sha512":
					endpoint.Kafka.SASLSHA512, _ = strconv.ParseBool(val[0])
				}
			}
		}
	}

	if endpoint.Protocol == MQTT {
		// Parsing connection from URL string
		hp := strings.Split(s, ":")
		switch len(hp) {
		default:
			return endpoint, errors.New("invalid MQTT url")
		case 1:
			endpoint.MQTT.Host = hp[0]
			endpoint.MQTT.Port = 1883
		case 2:
			n, err := strconv.ParseUint(hp[1], 10, 16)
			if err != nil {
				return endpoint, errors.New("invalid MQTT url port")
			}

			endpoint.MQTT.Host = hp[0]
			endpoint.MQTT.Port = int(n)
		}

		// Parsing MQTT queue name
		if len(sp) > 1 {
			var err error
			var parts []string
			for _, part := range sp[1:] {
				part, err = url.QueryUnescape(part)
				if err != nil {
					return endpoint, errors.New("invalid MQTT topic name")
				}
				parts = append(parts, part)
			}
			endpoint.MQTT.QueueName = strings.Join(parts, "/")
		}

		// Parsing additional params
		if len(sqp) > 1 {
			m, err := url.ParseQuery(sqp[1])
			if err != nil {
				return endpoint, errors.New("invalid MQTT url")
			}
			for key, val := range m {
				if len(val) == 0 {
					continue
				}
				switch key {
				case "qos":
					n, err := strconv.ParseUint(val[0], 10, 8)
					if err != nil {
						return endpoint, errors.New("invalid MQTT qos value")
					}
					endpoint.MQTT.Qos = byte(n)
				case "retained":
					n, err := strconv.ParseUint(val[0], 10, 8)
					if err != nil {
						return endpoint, errors.New("invalid MQTT retained value")
					}

					if n != 1 && n != 0 {
						return endpoint, errors.New("invalid MQTT retained, should be [0, 1]")
					}

					if n == 1 {
						endpoint.MQTT.Retained = true
					}
				case "cacert":
					endpoint.MQTT.CACertFile = val[0]
				case "cert":
					endpoint.MQTT.CertFile = val[0]
				case "key":
					endpoint.MQTT.KeyFile = val[0]
				}
			}
		}

		// Throw error if we not provide any queue name
		if endpoint.MQTT.QueueName == "" {
			return endpoint, errors.New("missing MQTT topic name")
		}
	}
	// Basic SQS connection strings in HOOKS interface
	// sqs://<region>:<queue_id>/<queue_name>/?params=value
	//
	//  params are:
	//
	// credpath - path where aws credentials are located
	// credprofile - credential profile
	if endpoint.Protocol == SQS {
		if endpoint.SQS.PlainURL == "" {
			// Parsing connection from URL string
			hp := strings.Split(s, ":")
			switch len(hp) {
			default:
				return endpoint, errors.New("invalid SQS url")
			case 2:
				endpoint.SQS.Region = hp[0]
				endpoint.SQS.QueueID = hp[1]
			}

			// Parsing SQS queue name
			if len(sp) > 1 {
				var err error
				endpoint.SQS.QueueName, err = url.QueryUnescape(sp[1])
				if err != nil {
					return endpoint, errors.New("invalid SQS queue name")
				}
			}
			// Throw error if we not provide any queue name
			if endpoint.SQS.QueueName == "" {
				return endpoint, errors.New("missing SQS queue name")
			}
		}

		// Parsing additional params
		if len(sqp) > 1 {
			m, err := url.ParseQuery(sqp[1])
			if err != nil {
				return endpoint, errors.New("invalid SQS url")
			}
			for key, val := range m {
				if len(val) == 0 {
					continue
				}
				switch key {
				case "credpath":
					endpoint.SQS.CredPath = val[0]
				case "credprofile":
					endpoint.SQS.CredProfile = val[0]
				case "createqueue":
					switch strings.ToLower(val[0]) {
					case "0", "false":
					default:
						endpoint.SQS.CreateQueue = true
					}
				}
			}
		}
	}
	// Basic Pubsub connection strings in HOOKS interface
	// pubsub://<project_name>:<topic_name>?params=value
	//
	//  params are:
	//
	// credpath - path where gcp credentials are located
	if endpoint.Protocol == PubSub {
		split := strings.Split(s, ":")
		if len(split) != 2 {
			return endpoint, errors.New("invalid PubSub format should be project/topic")
		}
		endpoint.PubSub.Project = split[0]
		endpoint.PubSub.Topic = split[1]

		if len(sqp) > 1 {
			m, err := url.ParseQuery(sqp[1])
			if err != nil {
				return endpoint, errors.New("invalid Pubsub url")
			}
			for key, val := range m {
				if len(val) == 0 {
					continue
				}
				switch key {
				case "credpath":
					endpoint.PubSub.CredPath = val[0]
				}
			}
		}

	}

	// Basic AMQP connection strings in HOOKS interface
	// amqp://guest:guest@localhost:5672/<queue_name>/?params=value
	// or amqp://guest:guest@localhost:5672/<namespace>/<queue_name>/?params=value
	//
	// Default params are:
	//
	// Mandatory - false
	// Immeditate - false
	// Durable - true
	// Routing-Key - tile38
	//
	// - "route" - [string] routing key
	//
	if endpoint.Protocol == AMQP {
		// Bind connection information
		endpoint.AMQP.URI = s
		endpoint.AMQP.Type = "direct"
		endpoint.AMQP.Durable = true
		endpoint.AMQP.DeliveryMode = amqp.Transient

		// Fix incase of namespace, e.g. example.com/namespace/queue
		// but not example.com/queue/ - with an endslash.
		if len(sp) > 2 && len(sp[2]) > 0 {
			endpoint.AMQP.URI = endpoint.AMQP.URI + "/" + sp[1]
			sp = append([]string{endpoint.AMQP.URI}, sp[2:]...)
		}

		// Bind queue name with no namespace
		if len(sp) > 1 {
			var err error
			endpoint.AMQP.QueueName, err = url.QueryUnescape(sp[1])
			if err != nil {
				return endpoint, errors.New("invalid AMQP queue name")
			}
		}

		// Parsing additional attributes
		if len(sqp) > 1 {
			m, err := url.ParseQuery(sqp[1])
			if err != nil {
				return endpoint, errors.New("invalid AMQP url")
			}
			for key, val := range m {
				if len(val) == 0 {
					continue
				}
				switch key {
				case "route":
					endpoint.AMQP.RouteKey = val[0]
				case "type":
					endpoint.AMQP.Type = val[0]
				case "durable":
					endpoint.AMQP.Durable = queryBool(val[0])
				case "internal":
					endpoint.AMQP.Internal = queryBool(val[0])
				case "no_wait":
					endpoint.AMQP.NoWait = queryBool(val[0])
				case "auto_delete":
					endpoint.AMQP.AutoDelete = queryBool(val[0])
				case "immediate":
					endpoint.AMQP.Immediate = queryBool(val[0])
				case "mandatory":
					endpoint.AMQP.Mandatory = queryBool(val[0])
				case "delivery_mode":
					endpoint.AMQP.DeliveryMode = uint8(queryInt(val[0]))
				case "priority":
					endpoint.AMQP.Priority = uint8(queryInt(val[0]))
				}
			}
		}

		if strings.HasPrefix(endpoint.Original, "amqps:") {
			endpoint.AMQP.SSL = true
		}

		if endpoint.AMQP.QueueName == "" {
			return endpoint, errors.New("missing AMQP queue name")
		}

		if endpoint.AMQP.RouteKey == "" {
			endpoint.AMQP.RouteKey = "tile38"
		}
	}

	// Basic NATS connection strings in HOOKS interface
	// nats://<host>:<port>/<topic_name>/?params=value
	//
	//  params are:
	//
	// user - username
	// pass - password
	// when user or pass is not set then login without password is used
	if endpoint.Protocol == NATS {
		// Parsing connection from URL string
		hp := strings.Split(s, ":")
		switch len(hp) {
		default:
			return endpoint, errors.New("invalid SQS url")
		case 2:
			endpoint.NATS.Host = hp[0]
			port, err := strconv.Atoi(hp[1])
			if err != nil {
				endpoint.NATS.Port = 4222 // default nats port
			} else {
				endpoint.NATS.Port = port
			}
		}

		// Parsing NATS topic name
		if len(sp) > 1 {
			var err error
			endpoint.NATS.Topic, err = url.QueryUnescape(sp[1])
			if err != nil {
				return endpoint, errors.New("invalid NATS topic name")
			}
		}

		// Parsing additional params
		if len(sqp) > 1 {
			m, err := url.ParseQuery(sqp[1])
			if err != nil {
				return endpoint, errors.New("invalid NATS url")
			}
			for key, val := range m {
				if len(val) == 0 {
					continue
				}
				switch key {
				case "user":
					endpoint.NATS.User = val[0]
				case "pass":
					endpoint.NATS.Pass = val[0]
				case "token":
					endpoint.NATS.Token = val[0]
				case "secure":
					endpoint.NATS.Secure = queryBool(val[0])
				case "credential":
					endpoint.NATS.UserCredentialPath = val[0]
				case "jetstream":
					endpoint.NATS.Jetstream = queryBool(val[0])
				case "tls":
					endpoint.NATS.TLS = queryBool(val[0])
				case "tlscert":
					endpoint.NATS.TLSCert = val[0]
				case "tlskey":
					endpoint.NATS.TLSKey = val[0]
				}
			}
		}
	}

	if endpoint.Protocol == EventHub {
		dp := strings.Split(endpoint.Original, ";")
		if len(dp) != 4 {
			return endpoint, errors.New("malformed EventHub connection string")
		}

		sakn := strings.Split(dp[1], "=")
		if sakn[0] != "SharedAccessKeyName" {
			return endpoint, errors.New("missing SharedAccessKeyName")
		}

		sak := strings.Split(dp[2], "=")
		if sak[0] != "SharedAccessKey" {
			return endpoint, errors.New("missing SharedAccessKey")
		}

		ep := strings.Split(dp[3], "=")
		if ep[0] != "EntityPath" {
			return endpoint, errors.New("missing EntityPath")
		}

		endpoint.EventHub.ConnectionString = endpoint.Original
	}

	return endpoint, nil
}

func queryInt(s string) int {
	x, _ := strconv.ParseInt(s, 10, 64)
	return int(x)
}

func queryBool(s string) bool {
	if len(s) > 0 {
		if s[0] >= '1' && s[0] <= '9' {
			return true
		}
		switch s[0] {
		case 'Y', 'y', 'T', 't':
			return true
		}
	}
	return false
}
