package server

import (
	"sync"
	"time"

	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/log"
	"github.com/tidwall/tile38/internal/object"
)

const bgExpireDelay = time.Second / 10

// backgroundExpiring deletes expired items from the database.
// It's executes every 1/10 of a second.
func (s *Server) backgroundExpiring(wg *sync.WaitGroup) {
	defer wg.Done()
	s.loopUntilServerStops(bgExpireDelay, func() {
		s.mu.LockLowPriority()
		defer s.mu.Unlock()
		now := time.Now()
		s.backgroundExpireObjects(now)
		s.backgroundExpireHooks(now)
	})
}

func (s *Server) backgroundExpireObjects(now time.Time) {
	nano := now.UnixNano()
	var msgs []*Message
	s.cols.Scan(func(key string, col *collection.Collection) bool {
		col.ScanExpires(func(o *object.Object) bool {
			if nano < o.Expires() {
				return false
			}
			s.statsExpired.Add(1)
			msgs = append(msgs, &Message{Args: []string{"del", key, o.ID()}})
			return true
		})
		return true
	})
	for _, msg := range msgs {
		_, d, err := s.cmdDEL(msg)
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
