package endpoint

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	lg "log"

	"github.com/IBM/sarama"
	"github.com/tidwall/gjson"
	"github.com/tidwall/tile38/internal/log"
)

const kafkaExpiresAfter = time.Second * 30

// KafkaConn is an endpoint connection
type KafkaConn struct {
	mu   sync.Mutex
	ep   Endpoint
	conn sarama.SyncProducer
	cfg  *sarama.Config
	ex   bool
	t    time.Time
}

// Expired returns true if the connection has expired
func (conn *KafkaConn) Expired() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.ex {
		if time.Since(conn.t) > kafkaExpiresAfter {
			conn.close()
			conn.ex = true
		}
	}
	return conn.ex
}

// ExpireNow forces the connection to expire
func (conn *KafkaConn) ExpireNow() {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.close()
	conn.ex = true
}

func (conn *KafkaConn) close() {
	if conn.conn != nil {
		conn.conn.Close()
		conn.conn = nil
		conn.cfg.MetricRegistry.UnregisterAll()
		conn.cfg = nil
	}
}

// Send sends a message
func (conn *KafkaConn) Send(msg string) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.ex {
		return errExpired
	}
	conn.t = time.Now()

	if log.Level() > 2 {
		sarama.Logger = lg.New(log.Output(), "[sarama] ", 0)
	}

	uri := fmt.Sprintf("%s:%d", conn.ep.Kafka.Host, conn.ep.Kafka.Port)
	if conn.conn == nil {
		cfg := sarama.NewConfig()

		cfg.Net.DialTimeout = time.Second
		cfg.Net.ReadTimeout = time.Second * 5
		cfg.Net.WriteTimeout = time.Second * 5
		// Fix #333 : fix backward incompatibility introduced by sarama library
		cfg.Producer.Return.Successes = true
		cfg.Version = sarama.V0_10_0_0

		switch conn.ep.Kafka.Auth {
		case "sasl":
			// This path allows to either provide a custom ca certificate
			// or, because RootCAs is nil, is using the hosts ca set
			// to verify the server certificate
			if conn.ep.Kafka.SSL {
				tlsConfig := tls.Config{}

				if conn.ep.Kafka.CACertFile != "" {
					caCertPool, err := loadRootTLSCert(conn.ep.Kafka.CACertFile)
					if err != nil {
						return err
					}
					tlsConfig.RootCAs = &caCertPool
				}

				cfg.Net.TLS.Enable = true
				cfg.Net.TLS.Config = &tlsConfig
			}

			cfg.Net.SASL.Enable = true
			cfg.Net.SASL.User = os.Getenv("KAFKA_USERNAME")
			cfg.Net.SASL.Password = os.Getenv("KAFKA_PASSWORD")
			cfg.Net.SASL.Handshake = true
			cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext

			if conn.ep.Kafka.SASLSHA256 {
				cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
				cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			}
			if conn.ep.Kafka.SASLSHA512 {
				cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
				cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			}
		case "tls":
			tlsConfig := tls.Config{}
			cfg.Net.TLS.Enable = true

			certificates, err := loadClientTLSCert(conn.ep.Kafka.KeyFile, conn.ep.Kafka.CertFile)
			if err != nil {
				cfg.MetricRegistry.UnregisterAll()
				return err
			}
			tlsConfig.Certificates = certificates

			// This path allows to either provide a custom ca certificate
			// or, because RootCAs is nil, is using the hosts ca set
			// to verify server certificate
			if conn.ep.Kafka.CACertFile != "" {
				caCertPool, err := loadRootTLSCert(conn.ep.Kafka.CACertFile)
				if err != nil {
					return err
				}
				tlsConfig.RootCAs = &caCertPool
			}

			cfg.Net.TLS.Config = &tlsConfig
		case "none":
			// This path allows to either provide a custom ca certificate
			// or, because RootCAs is nil, is using the hosts ca set
			// to verify the server certificate
			if conn.ep.Kafka.SSL {
				tlsConfig := tls.Config{}

				if conn.ep.Kafka.CACertFile != "" {
					caCertPool, err := loadRootTLSCert(conn.ep.Kafka.CACertFile)
					if err != nil {
						return err
					}
					tlsConfig.RootCAs = &caCertPool
				}

				cfg.Net.TLS.Enable = true
				cfg.Net.TLS.Config = &tlsConfig
			}
		}

		c, err := sarama.NewSyncProducer([]string{uri}, cfg)
		if err != nil {
			cfg.MetricRegistry.UnregisterAll()
			return err
		}

		conn.conn = c
		conn.cfg = cfg
	}

	// parse json again to get out info for our kafka key
	key := gjson.Get(msg, "key")
	id := gjson.Get(msg, "id")
	keyValue := fmt.Sprintf("%s-%s", key.String(), id.String())

	message := &sarama.ProducerMessage{
		Topic: conn.ep.Kafka.TopicName,
		Key:   sarama.StringEncoder(keyValue),
		Value: sarama.StringEncoder(msg),
	}

	_, offset, err := conn.conn.SendMessage(message)
	if err != nil {
		conn.close()
		return err
	}

	if offset < 0 {
		conn.close()
		return errors.New("invalid kafka reply")
	}

	return nil
}

func newKafkaConn(ep Endpoint) *KafkaConn {
	return &KafkaConn{
		ep: ep,
		t:  time.Now(),
	}
}

func loadClientTLSCert(KeyFile, CertFile string) ([]tls.Certificate, error) {
	// load client cert
	cert, err := tls.LoadX509KeyPair(CertFile, KeyFile)

	if err != nil {
		return []tls.Certificate{cert}, err
	}

	return []tls.Certificate{cert}, err
}

func loadRootTLSCert(CACertFile string) (x509.CertPool, error) {
	// Load CA cert
	caCert, err := os.ReadFile(CACertFile)

	if err != nil {
		return x509.CertPool{}, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	return *caCertPool, err
}
