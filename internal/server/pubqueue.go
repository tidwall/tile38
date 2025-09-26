package server

import (
	"net"
	"sync"

	"github.com/tidwall/redcon"
)

type pubQueue struct {
	cond    *sync.Cond
	entries []pubQueueEntry // follower publish queue
	closed  bool
}

type pubQueueEntry struct {
	channel  string
	messages []string
}

func (s *Server) startPublishQueue(wg *sync.WaitGroup) {
	defer wg.Done()
	var buf []byte
	var conns []net.Conn
	s.pubq.cond.L.Lock()
	for {
		for len(s.pubq.entries) > 0 {
			entries := s.pubq.entries
			s.pubq.entries = nil
			s.pubq.cond.L.Unlock()
			// Get follower connections
			s.mu.RLock()
			for conn := range s.aofconnM {
				conns = append(conns, conn)
			}
			s.mu.RUnlock()
			// Buffer the PUBLISH command pipeline
			buf = buf[:0]
			for _, entry := range entries {
				for _, message := range entry.messages {
					buf = redcon.AppendArray(buf, 3)
					buf = redcon.AppendBulkString(buf, "PUBLISH")
					buf = redcon.AppendBulkString(buf, entry.channel)
					buf = redcon.AppendBulkString(buf, message)
				}
			}
			// Publish to followers
			for i, conn := range conns {
				conn.Write(buf)
				conns[i] = nil
			}
			conns = conns[:0]
			s.pubq.cond.L.Lock()
		}
		if s.pubq.closed {
			break
		}
		s.pubq.cond.Wait()
	}
	s.pubq.cond.L.Unlock()
}

func (s *Server) stopPublishQueue() {
	s.pubq.cond.L.Lock()
	s.pubq.closed = true
	s.pubq.cond.Broadcast()
	s.pubq.cond.L.Unlock()
}

func (s *Server) sendPublishQueue(channel string, message ...string) {
	s.pubq.cond.L.Lock()
	if !s.pubq.closed {
		s.pubq.entries = append(s.pubq.entries, pubQueueEntry{
			channel:  channel,
			messages: message,
		})
	}
	s.pubq.cond.Broadcast()
	s.pubq.cond.L.Unlock()
}
