package server

import (
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/field"
	"github.com/tidwall/tile38/internal/log"
	"github.com/tidwall/tile38/internal/object"
)

const maxkeys = 8
const maxids = 32
const maxchunk = 4 * 1024 * 1024

func (s *Server) watchAutoAOFShrink(wg *sync.WaitGroup) {
	defer wg.Done()
	var lastShrink time.Time
	s.loopUntilServerStops(time.Second*10, func() {
		minSize := s.config.aofshrinkMinSize()
		if minSize == 0 {
			return
		}
		if time.Since(lastShrink) < time.Minute {
			return
		}
		s.mu.RLock()
		sz := s.aofsz
		shrinking := s.shrinking
		s.mu.RUnlock()
		if shrinking || int64(sz) <= minSize {
			return
		}
		log.Infof("auto aof shrink triggered: aof_size=%d threshold=%d",
			sz, minSize)
		lastShrink = time.Now()
		go s.aofshrink()
	})
}

func (s *Server) aofshrink() {
	start := time.Now()
	s.mu.Lock()
	if s.aof == nil || s.shrinking {
		s.mu.Unlock()
		return
	}
	s.shrinking = true
	s.shrinklog = nil
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.shrinking = false
		s.shrinklog = nil
		s.mu.Unlock()
		log.Infof("aof shrink ended %v", time.Since(start))
	}()

	err := func() error {
		f, err := os.Create(s.opts.AppendFileName + "-shrink")
		if err != nil {
			return err
		}
		defer f.Close()
		var aofbuf []byte
		var values []string
		var keys []string
		var nextkey string
		var keysdone bool
		for {
			if len(keys) == 0 {
				// load more keys
				if keysdone {
					break
				}
				keysdone = true
				func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					s.cols.Ascend(nextkey,
						func(key string, col *collection.Collection) bool {
							if len(keys) == maxkeys {
								keysdone = false
								nextkey = key
								return false
							}
							keys = append(keys, key)
							return true
						},
					)
				}()
				continue
			}

			var idsdone bool
			var nextid string
			for {
				if idsdone {
					keys = keys[1:]
					break
				}

				// load more objects
				func() {
					idsdone = true
					s.mu.Lock()
					defer s.mu.Unlock()
					col, ok := s.cols.Get(keys[0])
					if !ok {
						return
					}
					var now = time.Now().UnixNano() // used for expiration
					var count = 0                   // the object count
					col.ScanGreaterOrEqual(nextid, false, nil, nil,
						func(o *object.Object) bool {
							if count == maxids {
								// we reached the max number of ids for one batch
								nextid = o.ID()
								idsdone = false
								return false
							}
							// here we fill the values array with a new command
							values = values[:0]
							values = append(values, "set")
							values = append(values, keys[0])
							values = append(values, o.ID())
							o.Fields().Scan(func(f field.Field) bool {
								if !f.Value().IsZero() {
									values = append(values, "field")
									values = append(values, f.Name())
									values = append(values, f.Value().JSON())
								}
								return true
							})
							if o.Expires() != 0 {
								ttl := math.Floor(float64(o.Expires()-now)/float64(time.Second)*10) / 10
								if ttl < 0.1 {
									// always leave a little bit of ttl.
									ttl = 0.1
								}
								values = append(values, "ex")
								values = append(values, strconv.FormatFloat(ttl, 'f', -1, 64))
							}
							if objIsSpatial(o.Geo()) {
								values = append(values, "object")
								values = append(values, string(o.Geo().AppendJSON(nil)))
							} else {
								values = append(values, "string")
								values = append(values, o.Geo().String())
							}

							// append the values to the aof buffer
							aofbuf = append(aofbuf, '*')
							aofbuf = append(aofbuf, strconv.FormatInt(int64(len(values)), 10)...)
							aofbuf = append(aofbuf, '\r', '\n')
							for _, value := range values {
								aofbuf = append(aofbuf, '$')
								aofbuf = append(aofbuf, strconv.FormatInt(int64(len(value)), 10)...)
								aofbuf = append(aofbuf, '\r', '\n')
								aofbuf = append(aofbuf, value...)
								aofbuf = append(aofbuf, '\r', '\n')
							}

							// increment the object count
							count++
							return true
						},
					)

				}()
				if len(aofbuf) > maxchunk {
					if _, err := f.Write(aofbuf); err != nil {
						return err
					}
					aofbuf = aofbuf[:0]
				}
			}
		}

		// load hooks
		// first load the names of the hooks
		var hnames []string
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			hnames = make([]string, 0, s.hooks.Len())
			s.hooks.Walk(func(v []interface{}) {
				for _, v := range v {
					hnames = append(hnames, v.(*Hook).Name)
				}
			})
		}()
		var hookHint btree.PathHint
		for _, name := range hnames {
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				hook, _ := s.hooks.GetHint(&Hook{Name: name}, &hookHint).(*Hook)
				if hook == nil {
					return
				}
				hook.cond.L.Lock()
				defer hook.cond.L.Unlock()

				var values []string
				if hook.channel {
					values = append(values, "setchan", name)
				} else {
					values = append(values, "sethook", name,
						strings.Join(hook.Endpoints, ","))
				}
				for _, meta := range hook.Metas {
					values = append(values, "meta", meta.Name, meta.Value)
				}
				if !hook.expires.IsZero() {
					ex := float64(time.Until(hook.expires)) / float64(time.Second)
					values = append(values, "ex",
						strconv.FormatFloat(ex, 'f', 1, 64))
				}
				values = append(values, hook.Message.Args...)
				// append the values to the aof buffer
				aofbuf = append(aofbuf, '*')
				aofbuf = append(aofbuf, strconv.FormatInt(int64(len(values)), 10)...)
				aofbuf = append(aofbuf, '\r', '\n')
				for _, value := range values {
					aofbuf = append(aofbuf, '$')
					aofbuf = append(aofbuf, strconv.FormatInt(int64(len(value)), 10)...)
					aofbuf = append(aofbuf, '\r', '\n')
					aofbuf = append(aofbuf, value...)
					aofbuf = append(aofbuf, '\r', '\n')
				}
			}()
		}
		if len(aofbuf) > 0 {
			if _, err := f.Write(aofbuf); err != nil {
				return err
			}
			aofbuf = aofbuf[:0]
		}
		if err := f.Sync(); err != nil {
			return err
		}

		// finally grab any new data that may have been written since
		// the aofshrink has started and swap out the files.
		return func() error {
			s.mu.Lock()
			defer s.mu.Unlock()

			// kill all followers connections and close their files. This
			// ensures that there is only one opened AOF at a time which is
			// what Windows requires in order to perform the Rename function
			// below.
			for conn, f := range s.aofconnM {
				conn.Close()
				f.Close()
			}

			// send a broadcast to all sleeping followers
			s.fcond.Broadcast()

			// flush the aof buffer
			s.flushAOF(false)

			aofbuf = aofbuf[:0]
			for _, values := range s.shrinklog {
				// append the values to the aof buffer
				aofbuf = append(aofbuf, '*')
				aofbuf = append(aofbuf, strconv.FormatInt(int64(len(values)), 10)...)
				aofbuf = append(aofbuf, '\r', '\n')
				for _, value := range values {
					aofbuf = append(aofbuf, '$')
					aofbuf = append(aofbuf, strconv.FormatInt(int64(len(value)), 10)...)
					aofbuf = append(aofbuf, '\r', '\n')
					aofbuf = append(aofbuf, value...)
					aofbuf = append(aofbuf, '\r', '\n')
				}
			}
			if _, err := f.Write(aofbuf); err != nil {
				return err
			}
			if err := f.Sync(); err != nil {
				return err
			}
			// we now have a shrunken aof file that is fully in-sync with
			// the current dataset. let's swap out the on disk files and
			// point to the new file.

			// anything below this point is unrecoverable. just log and exit process
			// back up the live aof, just in case of fatal error
			if err := s.aof.Close(); err != nil {
				log.Fatalf("shrink live aof close fatal operation: %v", err)
			}
			if err := f.Close(); err != nil {
				log.Fatalf("shrink new aof close fatal operation: %v", err)
			}
			if err := os.Rename(s.opts.AppendFileName, s.opts.AppendFileName+"-bak"); err != nil {
				log.Fatalf("shrink backup fatal operation: %v", err)
			}
			if err := os.Rename(s.opts.AppendFileName+"-shrink", s.opts.AppendFileName); err != nil {
				log.Fatalf("shrink rename fatal operation: %v", err)
			}
			s.aof, err = os.OpenFile(s.opts.AppendFileName, os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				log.Fatalf("shrink openfile fatal operation: %v", err)
			}
			var n int64
			n, err = s.aof.Seek(0, 2)
			if err != nil {
				log.Fatalf("shrink seek end fatal operation: %v", err)
			}
			s.aofsz = int(n)

			os.Remove(s.opts.AppendFileName + "-bak") // ignore error

			return nil
		}()
	}()
	if err != nil {
		log.Errorf("aof shrink failed: %v", err)
		return
	}
}
