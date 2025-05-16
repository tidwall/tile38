package endpoint

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const natsExpiresAfter = time.Second * 30

// NATSConn is an endpoint connection
type NATSConn struct {
	mu   sync.Mutex
	ep   Endpoint
	ex   bool
	t    time.Time
	conn *nats.Conn
	js   jetstream.JetStream
}

func newNATSConn(ep Endpoint) *NATSConn {
	return &NATSConn{
		ep: ep,
		t:  time.Now(),
	}
}

// Expired returns true if the connection has expired
func (conn *NATSConn) Expired() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.ex {
		if time.Since(conn.t) > natsExpiresAfter {
			conn.close()
			conn.ex = true
		}
	}
	return conn.ex
}

// ExpireNow forces the connection to expire
func (conn *NATSConn) ExpireNow() {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.close()
	conn.ex = true
}

func (conn *NATSConn) close() {
	if conn.conn != nil {
		conn.conn.Close()
		conn.conn = nil
	}
}

// Send sends a message
func (conn *NATSConn) Send(msg string) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.ex {
		return errExpired
	}
	conn.t = time.Now()
	if conn.conn == nil {
		addr := fmt.Sprintf("%s:%d", conn.ep.NATS.Host, conn.ep.NATS.Port)
		scheme := "nats" // 'nats://' by default
		var err error
		var opts []nats.Option
		if conn.ep.NATS.User != "" && conn.ep.NATS.Pass != "" {
			opts = append(opts, nats.UserInfo(conn.ep.NATS.User, conn.ep.NATS.Pass))
		}
		if conn.ep.NATS.TLS {
			opts = append(opts, nats.ClientCert(
				conn.ep.NATS.TLSCert, conn.ep.NATS.TLSKey,
			))
		}
		if conn.ep.NATS.Token != "" {
			opts = append(opts, nats.Token(conn.ep.NATS.Token))
		}
		if conn.ep.NATS.UserCredentialPath != "" {
			opts = append(opts, nats.UserCredentials(conn.ep.NATS.UserCredentialPath))
		}

		if conn.ep.NATS.Secure {
			scheme = "tls"
		}

		addr = fmt.Sprintf("%s://%s", scheme, addr)
		conn.conn, err = nats.Connect(addr, opts...)
		if err != nil {
			conn.close()
			return err
		}

		if conn.ep.NATS.Jetstream {
			conn.js, err = jetstream.New(conn.conn)
			if err != nil {
				conn.close()
				return err
			}
		}
	}

	if conn.js == nil {
		return conn.publish(msg)
	}

	return conn.publishJS(msg)
}

// publishJS will publish the message to the subject using core nats.
func (conn *NATSConn) publish(msg string) error {
	err := conn.conn.Publish(conn.ep.NATS.Topic, []byte(msg))
	if err != nil {
		conn.close()
		return err
	}

	return nil
}

// publishJS will publish the message expecting a jetstream acknowledgement.
func (conn *NATSConn) publishJS(msg string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err := conn.js.Publish(ctx, conn.ep.NATS.Topic, []byte(msg))
	if err != nil {
		conn.close()
		return err
	}

	return nil
}
