package controller

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
)

type liveBuffer struct {
	key     string
	glob    string
	fence   *liveFenceSwitches
	details []*commandDetailsT
	cond    *sync.Cond
}

func (c *Controller) processLives() {
	for {
		c.lcond.L.Lock()
		for len(c.lstack) > 0 {
			item := c.lstack[0]
			c.lstack = c.lstack[1:]
			if len(c.lstack) == 0 {
				c.lstack = nil
			}
			for lb := range c.lives {
				lb.cond.L.Lock()
				if lb.key != "" && lb.key == item.key {
					lb.details = append(lb.details, item)
					lb.cond.Broadcast()
				}
				lb.cond.L.Unlock()
			}
		}
		c.lcond.Wait()
		c.lcond.L.Unlock()
	}
}

func writeMessage(conn net.Conn, message []byte, wrapRESP bool, connType server.Type, websocket bool) error {
	if len(message) == 0 {
		return nil
	}
	if websocket {
		return server.WriteWebSocketMessage(conn, message)
	}
	var err error
	switch connType {
	case server.RESP:
		if wrapRESP {
			_, err = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(message), string(message))
		} else {
			_, err = conn.Write(message)
		}
	case server.Native:
		_, err = fmt.Fprintf(conn, "$%d %s\r\n", len(message), string(message))
	}
	return err
}

func (c *Controller) goLive(inerr error, conn net.Conn, rd *server.PipelineReader, msg *server.Message, websocket bool) error {
	addr := conn.RemoteAddr().String()
	log.Info("live " + addr)
	defer func() {
		log.Info("not live " + addr)
	}()
	if s, ok := inerr.(liveAOFSwitches); ok {
		return c.liveAOF(s, conn, rd, msg)
	}
	lb := &liveBuffer{
		cond: sync.NewCond(&sync.Mutex{}),
	}
	var err error
	var sw *scanWriter
	var wr bytes.Buffer
	switch s := inerr.(type) {
	default:
		return errors.New("invalid switch")
	case liveFenceSwitches:
		lb.glob = s.glob
		lb.key = s.key
		lb.fence = &s
		c.mu.RLock()
		sw, err = c.newScanWriter(&wr, msg, s.key, s.output, s.precision, s.glob, false, s.cursor, s.limit, s.wheres, s.whereins, s.nofields)
		c.mu.RUnlock()
	}
	// everything below if for live SCAN, NEARBY, WITHIN, INTERSECTS
	if err != nil {
		return err
	}
	c.lcond.L.Lock()
	c.lives[lb] = true
	c.lcond.L.Unlock()
	defer func() {
		c.lcond.L.Lock()
		delete(c.lives, lb)
		c.lcond.L.Unlock()
		conn.Close()
	}()

	var mustQuit bool
	go func() {
		defer func() {
			lb.cond.L.Lock()
			mustQuit = true
			lb.cond.Broadcast()
			lb.cond.L.Unlock()
			conn.Close()
		}()
		for {
			vs, err := rd.ReadMessages()
			if err != nil {
				if err != io.EOF && !(websocket && err == io.ErrUnexpectedEOF) {
					log.Error(err)
				}
				return
			}
			for _, v := range vs {
				if v == nil {
					continue
				}
				switch v.Command {
				default:
					log.Error("received a live command that was not QUIT")
					return
				case "quit", "":
					return
				}
			}
		}
	}()
	outputType := msg.OutputType
	connType := msg.ConnType
	if websocket {
		outputType = server.JSON
	}
	var livemsg []byte
	switch outputType {
	case server.JSON:
		livemsg = []byte(`{"ok":true,"live":true}`)
	case server.RESP:
		livemsg = []byte("+OK\r\n")
	}
	if err := writeMessage(conn, livemsg, false, connType, websocket); err != nil {
		return nil // nil return is fine here
	}
	for {
		lb.cond.L.Lock()
		if mustQuit {
			lb.cond.L.Unlock()
			return nil
		}
		for len(lb.details) > 0 {
			details := lb.details[0]
			lb.details = lb.details[1:]
			if len(lb.details) == 0 {
				lb.details = nil
			}
			fence := lb.fence
			lb.cond.L.Unlock()
			msgs := FenceMatch("", sw, fence, nil, details)
			for _, msg := range msgs {
				if err := writeMessage(conn, []byte(msg), true, connType, websocket); err != nil {
					return nil // nil return is fine here
				}
			}
			lb.cond.L.Lock()
		}
		lb.cond.Wait()
		lb.cond.L.Unlock()
	}
}
