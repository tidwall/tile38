package endpoint

import (
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	AMQPExpiresAfter = time.Second * 30
)

type AMQPEndpointConn struct {
	mu      sync.Mutex
	ep      Endpoint
	conn    *amqp.Connection
	channel *amqp.Channel
	ex      bool
	t       time.Time
}

func (conn *AMQPEndpointConn) Expired() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.ex {
		if time.Now().Sub(conn.t) > kafkaExpiresAfter {
			conn.ex = true
			conn.close()
		}
	}
	return conn.ex
}

func (conn *AMQPEndpointConn) close() {
	if conn.conn != nil {
		conn.conn.Close()
		conn.conn = nil
		conn.channel = nil
	}
}

func (conn *AMQPEndpointConn) Send(msg string) error {
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

		c, err := amqp.Dial(fmt.Sprintf("%s%s", prefix, conn.ep.AMQP.URI))

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
			"direct",
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return err
		}

		// Create queue if queue don't exists
		if _, err := channel.QueueDeclare(
			conn.ep.AMQP.QueueName,
			true,

			false,
			false,
			false,
			nil,
		); err != nil {
			return err
		}

		// Binding exchange to queue
		if err := channel.QueueBind(
			conn.ep.AMQP.QueueName,
			conn.ep.AMQP.RouteKey,
			conn.ep.AMQP.QueueName,
			false,
			nil,
		); err != nil {
			return err
		}

		conn.conn = c
		conn.channel = channel
	}

	if err := conn.channel.Publish(
		conn.ep.AMQP.QueueName,
		conn.ep.AMQP.RouteKey,
		false,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            []byte(msg),
			DeliveryMode:    amqp.Transient,
			Priority:        0,
		},
	); err != nil {
		return err
	}

	return nil
}

func newAMQPEndpointConn(ep Endpoint) *AMQPEndpointConn {
	return &AMQPEndpointConn{
		ep: ep,
		t:  time.Now(),
	}
}
