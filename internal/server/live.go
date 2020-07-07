package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/tidwall/redcon"
	"github.com/tidwall/tile38/internal/log"
)

type liveBuffer struct {
	key     string
	glob    string
	fence   *liveFenceSwitches
	details []*commandDetails
	cond    *sync.Cond
}

func (server *Server) processLives() {
	server.lcond.L.Lock()
	defer server.lcond.L.Unlock()
	for {
		if server.stopServer.on() {
			return
		}
		for len(server.lstack) > 0 {
			item := server.lstack[0]
			server.lstack = server.lstack[1:]
			if len(server.lstack) == 0 {
				server.lstack = nil
			}
			for lb := range server.lives {
				lb.cond.L.Lock()
				if lb.key != "" && lb.key == item.key {
					lb.details = append(lb.details, item)
					lb.cond.Broadcast()
				}
				lb.cond.L.Unlock()
			}
		}
		server.lcond.Wait()
	}
}

func writeLiveMessage(
	conn net.Conn,
	message []byte,
	wrapRESP bool,
	connType Type, websocket bool,
) error {
	if len(message) == 0 {
		return nil
	}
	if websocket {
		return WriteWebSocketMessage(conn, message)
	}
	var err error
	switch connType {
	case RESP:
		if wrapRESP {
			_, err = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(message), string(message))
		} else {
			_, err = conn.Write(message)
		}
	case Native:
		_, err = fmt.Fprintf(conn, "$%d %s\r\n", len(message), string(message))
	}
	return err
}

func (server *Server) goLive(
	inerr error, conn net.Conn, rd *PipelineReader, msg *Message, websocket bool,
) error {
	addr := conn.RemoteAddr().String()
	log.Info("live " + addr)
	defer func() {
		log.Info("not live " + addr)
	}()
	switch s := inerr.(type) {
	default:
		return errors.New("invalid live type switches")
	case liveAOFSwitches:
		return server.liveAOF(s.pos, conn, rd, msg)
	case liveSubscriptionSwitches:
		return server.liveSubscription(conn, rd, msg, websocket)
	case liveFenceSwitches:
		// fallthrough
	}

	// everything below is for live geofences
	lb := &liveBuffer{
		cond: sync.NewCond(&sync.Mutex{}),
	}
	var err error
	var sc *scanner
	var wr bytes.Buffer
	coll := &fenceScanCollector{
		buffer: &wr,
	}
	s := inerr.(liveFenceSwitches)
	lb.glob = s.glob
	lb.key = s.key
	lb.fence = &s
	ul := server.ReaderLock()
	sc, err = server.newScanner(
		coll, s.key, s.output, s.precision, s.glob, false,
		s.cursor, s.limit, s.wheres, s.whereins, s.whereevals, s.nofields)
	ul()

	// everything below if for live SCAN, NEARBY, WITHIN, INTERSECTS
	if err != nil {
		return err
	}
	server.lcond.L.Lock()
	server.lives[lb] = true
	server.lcond.L.Unlock()
	defer func() {
		server.lcond.L.Lock()
		delete(server.lives, lb)
		server.lcond.L.Unlock()
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
				switch v.Command() {
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
		outputType = JSON
	}
	var livemsg []byte
	switch outputType {
	case JSON:
		livemsg = redcon.AppendBulkString(nil, `{"ok":true,"live":true}`)
	case RESP:
		livemsg = redcon.AppendOK(nil)
	}
	if err := writeLiveMessage(conn, livemsg, false, connType, websocket); err != nil {
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
			var msgs []string
			func() {
				// safely lock the fence because we are outside the main loop
				defer server.ReaderLock()()
				msgs = FenceMatch("", sc, fence, nil, details)
			}()
			for _, msg := range msgs {
				if err := writeLiveMessage(conn, []byte(msg), true, connType, websocket); err != nil {
					return nil // nil return is fine here
				}
			}
			server.statsTotalMsgsSent.add(len(msgs))
			lb.cond.L.Lock()

		}
		lb.cond.Wait()
		lb.cond.L.Unlock()
	}
}
