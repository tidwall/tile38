package server

import (
	"time"

	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/log"
)

const bgExpireDelay = time.Second / 10

// backgroundExpiring deletes expired items from the database.
// It's executes every 1/10 of a second.
func (s *Server) backgroundExpiring() {
	for {
		if s.stopServer.on() {
			return
		}
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			now := time.Now()
			s.backgroundExpireObjects(now)
			s.backgroundExpireHooks(now)
		}()
		time.Sleep(bgExpireDelay)
	}
}

func (s *Server) backgroundExpireObjects(now time.Time) {
	nano := now.UnixNano()
	var msgs []*Message
	s.cols.Scan(func(key string, col *collection.Collection) bool {
		col.ScanExpires(func(id string, expires int64) bool {
			if nano < expires {
				return false
			}
			msgs = append(msgs, &Message{Args: []string{"del", key, id}})
			return true
		})
		return true
	})
	for _, msg := range msgs {
		_, d, err := s.cmdDel(msg)
		if err != nil {
			log.Fatal(err)
		}
		if err := s.writeAOF(msg.Args, &d); err != nil {
			log.Fatal(err)
		}
	}
	if len(msgs) > 0 {
		log.Debugf("Expired %d objects\n", len(msgs))
	}
}

func (s *Server) backgroundExpireHooks(now time.Time) {
	var msgs []*Message
	s.hookExpires.Ascend(nil, func(v interface{}) bool {
		h := v.(*Hook)
		if h.expires.After(now) {
			return false
		}
		msg := &Message{}
		if h.channel {
			msg.Args = []string{"delchan", h.Name}
		} else {
			msg.Args = []string{"delhook", h.Name}
		}
		msgs = append(msgs, msg)
		return true
	})

	for _, msg := range msgs {
		_, d, err := s.cmdDelHook(msg)
		if err != nil {
			log.Fatal(err)
		}
		if err := s.writeAOF(msg.Args, &d); err != nil {
			log.Fatal(err)
		}
	}
	if len(msgs) > 0 {
		log.Debugf("Expired %d hooks\n", len(msgs))
	}
}
