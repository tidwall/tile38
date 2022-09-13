package endpoint

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/tidwall/tile38/internal/log"
)

const (
	mqttExpiresAfter   = time.Second * 30
	mqttPublishTimeout = time.Second * 5
)

// MQTTConn is an endpoint connection
type MQTTConn struct {
	mu   sync.Mutex
	ep   Endpoint
	conn paho.Client
	ex   bool
	t    time.Time
}

// Expired returns true if the connection has expired
func (conn *MQTTConn) Expired() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.ex {
		if time.Since(conn.t) > mqttExpiresAfter {
			conn.close()
			conn.ex = true
		}
	}
	return conn.ex
}

func (conn *MQTTConn) close() {
	if conn.conn != nil {
		if conn.conn.IsConnected() {
			conn.conn.Disconnect(250)
		}

		conn.conn = nil
	}
}

// Send sends a message
func (conn *MQTTConn) Send(msg string) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.ex {
		return errExpired
	}
	conn.t = time.Now()

	if conn.conn == nil {
		uri := fmt.Sprintf("tcp://%s:%d", conn.ep.MQTT.Host, conn.ep.MQTT.Port)
		ops := paho.NewClientOptions()
		if conn.ep.MQTT.CertFile != "" || conn.ep.MQTT.KeyFile != "" ||
			conn.ep.MQTT.CACertFile != "" {
			var config tls.Config
			if conn.ep.MQTT.CertFile != "" || conn.ep.MQTT.KeyFile != "" {
				cert, err := tls.LoadX509KeyPair(conn.ep.MQTT.CertFile,
					conn.ep.MQTT.KeyFile)
				if err != nil {
					return err
				}
				config.Certificates = append(config.Certificates, cert)
			}
			if conn.ep.MQTT.CACertFile != "" {
				// Load CA cert
				caCert, err := os.ReadFile(conn.ep.MQTT.CACertFile)
				if err != nil {
					return err
				}
				caCertPool := x509.NewCertPool()
				caCertPool.AppendCertsFromPEM(caCert)
				config.RootCAs = caCertPool
			}
			ops = ops.SetTLSConfig(&config)
		}
		//generate UUID for the client-id.
		b := make([]byte, 16)
		_, err := rand.Read(b)
		if err != nil {
			log.Debugf("Failed to generate guid for the mqtt client. The endpoint will not work")
			return err
		}
		uuid := fmt.Sprintf("tile38-%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])

		ops = ops.SetClientID(uuid).AddBroker(uri)
		c := paho.NewClient(ops)

		if token := c.Connect(); token.Wait() && token.Error() != nil {
			return token.Error()
		}

		conn.conn = c
	}

	t := conn.conn.Publish(conn.ep.MQTT.QueueName, conn.ep.MQTT.Qos,
		conn.ep.MQTT.Retained, msg)

	if !t.WaitTimeout(mqttPublishTimeout) || t.Error() != nil {
		conn.close()
		return t.Error()
	}

	return nil
}

func newMQTTConn(ep Endpoint) *MQTTConn {
	return &MQTTConn{
		ep: ep,
		t:  time.Now(),
	}
}
