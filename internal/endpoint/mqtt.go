package endpoint

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

const (
	mqttExpiresAfter = time.Second * 30
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
		if time.Now().Sub(conn.t) > mqttExpiresAfter {
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
		ops := paho.NewClientOptions()
		var uri string
		if conn.ep.MQTT.CertFile != "" || conn.ep.MQTT.KeyFile != "" ||
			conn.ep.MQTT.CACertFile != "" {
			uri = fmt.Sprintf("ssl://%s:%d", conn.ep.MQTT.Host, conn.ep.MQTT.Port)
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
				caCert, err := ioutil.ReadFile(conn.ep.MQTT.CACertFile)
				if err != nil {
					return err
				}
				caCertPool := x509.NewCertPool()
				caCertPool.AppendCertsFromPEM(caCert)
				config.RootCAs = caCertPool
			}
			ops = ops.SetTLSConfig(&config)

		} else if conn.ep.MQTT.Login != "" || conn.ep.MQTT.Password != "" {
			uri = fmt.Sprintf("tcp://%s:%d", conn.ep.MQTT.Host, conn.ep.MQTT.Port)
			ops.SetUsername(conn.ep.MQTT.Login)
			ops.SetPassword(conn.ep.MQTT.Password)
		} else {
			uri = fmt.Sprintf("tcp://%s:%d", conn.ep.MQTT.Host, conn.ep.MQTT.Port)
		}
		ops = ops.SetClientID("tile38").AddBroker(uri)
		c := paho.NewClient(ops)

		if token := c.Connect(); token.Wait() && token.Error() != nil {
			return token.Error()
		}

		conn.conn = c
	}

	t := conn.conn.Publish(conn.ep.MQTT.QueueName, conn.ep.MQTT.Qos,
		conn.ep.MQTT.Retained, msg)
	t.Wait()

	if t.Error() != nil {
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
