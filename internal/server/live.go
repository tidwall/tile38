package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"sync/atomic"

	"github.com/tidwall/redcon"
	"github.com/tidwall/tile38/internal/log"
)

type liveBuffer struct {
	key     string
	globs   []string
	fence   *liveFenceSwitches
	details []*commandDetails
	cond    *sync.Cond
}

func (s *Server) processLives(wg *sync.WaitGroup) {
	defer wg.Done()
	var done atomic.Bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if done.Load() {
				break
			}
			s.lcond.Broadcast()
			time.Sleep(time.Second / 4)
		}
	}()
	s.lcond.L.Lock()
	defer s.lcond.L.Unlock()
	for {
		if s.stopServer.Load() {
			done.Store(true)
			return
		}
		for len(s.lstack) > 0 {
			item := s.lstack[0]
			s.lstack = s.lstack[1:]
			if len(s.lstack) == 0 {
				s.lstack = nil
			}
			for lb := range s.lives {
				lb.cond.L.Lock()
				if lb.key != "" && lb.key == item.key {
					lb.details = append(lb.details, item)
					lb.cond.Broadcast()
				}
				lb.cond.L.Unlock()
			}
		}
		s.lcond.Wait()
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
			_, err = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(message),
				string(message))
		} else {
			_, err = conn.Write(message)
		}
	case Native:
		_, err = fmt.Fprintf(conn, "$%d %s\r\n", len(message), string(message))
	}
	return err
}

func (s *Server) goLive(inerr error, conn net.Conn, rd *PipelineReader,
	msg *Message, websocket bool,
) error {
	addr := conn.RemoteAddr().String()
	log.Info("live " + addr)
	defer func() {
		log.Info("not live " + addr)
	}()
	switch lfs := inerr.(type) {
	default:
		return errors.New("invalid live type switches")
	case liveAOFSwitches:
		return s.liveAOF(lfs.pos, conn, rd)
	case liveSubscriptionSwitches:
		return s.liveSubscription(conn, rd, msg, websocket)
	case liveMonitorSwitches:
		return s.liveMonitor(conn, rd, msg)
	case liveFenceSwitches:
		// fallthrough
	}

	// everything below is for live geofences
	lb := &liveBuffer{
		cond: sync.NewCond(&sync.Mutex{}),
	}
	var err error
	var sw *scanWriter
	var wr bytes.Buffer
	lfs := inerr.(liveFenceSwitches)
	lb.globs = lfs.globs
	lb.key = lfs.key
	lb.fence = &lfs
	s.mu.RLock()
	sw, err = s.newScanWriter(
		&wr, msg, lfs.key, lfs.output, lfs.precision, lfs.globs, false,
		lfs.cursor, lfs.limit, lfs.wheres, lfs.whereins, lfs.whereevals,
		lfs.nofields, lfs.mvt, lfs.tileX, lfs.tileY, lfs.tileZ)
	s.mu.RUnlock()

	// everything below if for live SCAN, NEARBY, WITHIN, INTERSECTS
	if err != nil {
		return err
	}
	s.lcond.L.Lock()
	s.lives[lb] = true
	s.lcond.L.Unlock()
	defer func() {
		s.lcond.L.Lock()
		delete(s.lives, lb)
		s.lcond.L.Unlock()
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
	err = writeLiveMessage(conn, livemsg, false, connType, websocket)
	if err != nil {
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
				s.mu.RLock()
				defer s.mu.RUnlock()
				msgs = FenceMatch("", sw, fence, nil, details)
			}()
			for _, msg := range msgs {
				err := writeLiveMessage(conn, []byte(msg), true, connType,
					websocket)
				if err != nil {
					return nil // nil return is fine here
				}
			}
			s.statsTotalMsgsSent.Add(int64(len(msgs)))
			lb.cond.L.Lock()

		}
		lb.cond.Wait()
		lb.cond.L.Unlock()
	}
}
