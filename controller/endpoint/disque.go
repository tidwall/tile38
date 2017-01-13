package endpoint

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	disqueExpiresAfter = time.Second * 30
)

type DisqueEndpointConn struct {
	mu   sync.Mutex
	ep   Endpoint
	ex   bool
	t    time.Time
	conn net.Conn
	rd   *bufio.Reader
}

func newDisqueEndpointConn(ep Endpoint) *DisqueEndpointConn {
	return &DisqueEndpointConn{
		ep: ep,
		t:  time.Now(),
	}
}

func (conn *DisqueEndpointConn) Expired() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !conn.ex {
		if time.Now().Sub(conn.t) > disqueExpiresAfter {
			if conn.conn != nil {
				conn.close()
			}
			conn.ex = true
		}
	}
	return conn.ex
}

func (conn *DisqueEndpointConn) close() {
	if conn.conn != nil {
		conn.conn.Close()
		conn.conn = nil
	}
	conn.rd = nil
}

func (conn *DisqueEndpointConn) Send(msg string) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.ex {
		return errors.New("expired")
	}
	conn.t = time.Now()
	if conn.conn == nil {
		addr := fmt.Sprintf("%s:%d", conn.ep.Disque.Host, conn.ep.Disque.Port)
		var err error
		conn.conn, err = net.Dial("tcp", addr)
		if err != nil {
			return err
		}
		conn.rd = bufio.NewReader(conn.conn)
	}
	var args []string
	args = append(args, "ADDJOB", conn.ep.Disque.QueueName, msg, "0")
	if conn.ep.Disque.Options.Replicate > 0 {
		args = append(args, "REPLICATE", strconv.FormatInt(int64(conn.ep.Disque.Options.Replicate), 10))
	}
	cmd := buildRedisCommand(args)
	if _, err := conn.conn.Write(cmd); err != nil {
		conn.close()
		return err
	}
	c, err := conn.rd.ReadByte()
	if err != nil {
		conn.close()
		return err
	}
	if c != '-' && c != '+' {
		conn.close()
		return errors.New("invalid disque reply")
	}
	ln, err := conn.rd.ReadBytes('\n')
	if err != nil {
		conn.close()
		return err
	}
	if len(ln) < 2 || ln[len(ln)-2] != '\r' {
		conn.close()
		return errors.New("invalid disque reply")
	}
	id := string(ln[:len(ln)-2])
	p := strings.Split(id, "-")
	if len(p) != 4 {
		conn.close()
		return errors.New("invalid disque reply")
	}
	return nil
}

func buildRedisCommand(args []string) []byte {
	var cmd []byte
	cmd = append(cmd, '*')
	cmd = strconv.AppendInt(cmd, int64(len(args)), 10)
	cmd = append(cmd, '\r', '\n')
	for _, arg := range args {
		cmd = append(cmd, '$')
		cmd = strconv.AppendInt(cmd, int64(len(arg)), 10)
		cmd = append(cmd, '\r', '\n')
		cmd = append(cmd, arg...)
		cmd = append(cmd, '\r', '\n')
	}
	return cmd
}
