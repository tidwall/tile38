package endpoint

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

const (
	kafkaExpiresAfter = time.Second * 30
)

type KafkaEndpointConn struct {
	mu   sync.Mutex
	ep   Endpoint
	conn sarama.SyncProducer
	ex   bool
	t    time.Time
}

func (conn *KafkaEndpointConn) Expired() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.ex {
		if time.Now().Sub(conn.t) > kafkaExpiresAfter {
			if conn.conn != nil {
				conn.close()
			}
			conn.ex = true
		}
	}
	return conn.ex
}

func (conn *KafkaEndpointConn) close() {
	if conn.conn != nil {
		conn.conn.Close()
		conn.conn = nil
	}
}

func (conn *KafkaEndpointConn) Send(msg string) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.ex {
		return errExpired
	}
	conn.t = time.Now()

	uri := fmt.Sprintf("%s:%d", conn.ep.Kafka.Host, conn.ep.Kafka.Port)
	if conn.conn == nil {
		c, err := sarama.NewSyncProducer([]string{uri}, nil)
		if err != nil {
			return err
		}

		conn.conn = c
	}

	message := &sarama.ProducerMessage{
		Topic: conn.ep.Kafka.QueueName,
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

func newKafkaEndpointConn(ep Endpoint) *KafkaEndpointConn {
	return &KafkaEndpointConn{
		ep: ep,
		t:  time.Now(),
	}
}
