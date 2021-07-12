package endpoint

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const amqpExpiresAfter = time.Second * 30

// AMQPConn is an endpoint connection
type AMQPConn struct {
	mu      sync.Mutex
	ep      Endpoint
	conn    *amqp.Connection
	channel *amqp.Channel
	ex      bool
	t       time.Time
}

// Expired returns true if the connection has expired
func (conn *AMQPConn) Expired() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.ex {
		if time.Since(conn.t) > amqpExpiresAfter {
			conn.ex = true
			conn.close()
		}
	}
	return conn.ex
}

func (conn *AMQPConn) close() {
	if conn.conn != nil {
		conn.conn.Close()
		conn.conn = nil
		conn.channel = nil
	}
}

// Send sends a message
func (conn *AMQPConn) Send(msg string) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.ex {
		return errExpired
	}
	conn.t = time.Now()

	if conn.conn == nil {
		prefix := "amqp://"
		if conn.ep.AMQP.SSL {
			prefix = "amqps://"
		}

		var cfg amqp.Config
		cfg.Dial = func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, time.Second)
		}
		c, err := amqp.DialConfig(fmt.Sprintf("%s%s", prefix, conn.ep.AMQP.URI), cfg)

		if err != nil {
			return err
		}

		channel, err := c.Channel()
		if err != nil {
			return err
		}

		// Declare new exchange
		if err := channel.ExchangeDeclare(
			conn.ep.AMQP.QueueName,
			conn.ep.AMQP.Type,
			conn.ep.AMQP.Durable,
			conn.ep.AMQP.AutoDelete,
			conn.ep.AMQP.Internal,
			conn.ep.AMQP.NoWait,
			nil,
		); err != nil {
			return err
		}
		
		conn.conn = c
		conn.channel = channel
	}

	return conn.channel.Publish(
		conn.ep.AMQP.QueueName,
		conn.ep.AMQP.RouteKey,
		conn.ep.AMQP.Mandatory,
		conn.ep.AMQP.Immediate,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            []byte(msg),
			DeliveryMode:    conn.ep.AMQP.DeliveryMode,
			Priority:        conn.ep.AMQP.Priority,
		},
	)
}

func newAMQPConn(ep Endpoint) *AMQPConn {
	return &AMQPConn{
		ep: ep,
		t:  time.Now(),
	}
}
