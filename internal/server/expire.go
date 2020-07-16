package server

import (
	"math/rand"
	"time"

	"github.com/tidwall/rhh"
	"github.com/tidwall/tile38/internal/log"
)

// clearIDExpires clears a single item from the expires list.
func (s *Server) clearIDExpires(key, id string) (cleared bool) {
	if s.expires.Len() > 0 {
		if idm, ok := s.expires.Get(key); ok {
			if _, ok := idm.(*rhh.Map).Delete(id); ok {
				if idm.(*rhh.Map).Len() == 0 {
					s.expires.Delete(key)
				}
				return true
			}
		}
	}
	return false
}

// clearKeyExpires clears all items that are marked as expires from a single key.
func (s *Server) clearKeyExpires(key string) {
	s.expires.Delete(key)
}

// moveKeyExpires moves all items that are marked as expires from a key to a newKey.
func (s *Server) moveKeyExpires(key, newKey string) {
	if idm, ok := s.expires.Delete(key); ok {
		s.expires.Set(newKey, idm)
	}
}

// expireAt marks an item as expires at a specific time.
func (s *Server) expireAt(key, id string, at time.Time) {
	idm, ok := s.expires.Get(key)
	if !ok {
		idm = rhh.New(0)
		s.expires.Set(key, idm)
	}
	idm.(*rhh.Map).Set(id, at.UnixNano())
}

// getExpires returns the when an item expires.
func (s *Server) getExpires(key, id string) (at time.Time, ok bool) {
	if s.expires.Len() > 0 {
		if idm, ok := s.expires.Get(key); ok {
			if atv, ok := idm.(*rhh.Map).Get(id); ok {
				return time.Unix(0, atv.(int64)), true
			}
		}
	}
	return time.Time{}, false
}

// hasExpired returns true if an item has expired.
func (s *Server) hasExpired(key, id string) bool {
	if at, ok := s.getExpires(key, id); ok {
		return time.Now().After(at)
	}
	return false
}

const bgExpireDelay = time.Second / 10
const bgExpireSegmentSize = 20

// expirePurgeSweep is ran from backgroundExpiring operation and performs
// segmented sweep of the expires list
func (s *Server) expirePurgeSweep(rng *rand.Rand) (purged int) {
	now := time.Now().UnixNano()
	ul := s.WriterLock()
	defer ul()
	if s.expires.Len() == 0 {
		return 0
	}
	for i := 0; i < bgExpireSegmentSize; i++ {
		if key, idm, ok := s.expires.GetPos(rng.Uint64()); ok {
			id, atv, ok := idm.(*rhh.Map).GetPos(rng.Uint64())
			if ok {
				if now > atv.(int64) {
					// expired, purge from database
					msg := &Message{}
					msg.Args = []string{"del", key, id}
					_, d, err := s.cmdDel(msg)
					if err != nil {
						log.Fatal(err)
					}
					if err := s.writeAOF(msg.Args, &d); err != nil {
						log.Fatal(err)
					}
					purged++
				}
			}
		}
		// recycle the lock
		ul()
		ul = s.WriterLock()
	}
	return purged
}

// backgroundExpiring watches for when items that have expired must be purged
// from the database. It's executes 10 times a seconds.
func (s *Server) backgroundExpiring() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		if s.stopServer.on() {
			return
		}
		purged := s.expirePurgeSweep(rng)
		if purged > bgExpireSegmentSize/4 {
			// do another purge immediately
			continue
		} else {
			// back off
			time.Sleep(bgExpireDelay)
		}
	}
}
