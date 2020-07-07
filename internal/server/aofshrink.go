package server

import (
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/geojson"
	"github.com/tidwall/rhh"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/log"
)

const maxkeys = 8
const maxids = 32
const maxchunk = 4 * 1024 * 1024

func (server *Server) aofshrink() {
	if server.aof == nil {
		return
	}
	start := time.Now()
	ul := server.WriterLock()
	if server.shrinking {
		ul()
		return
	}
	server.shrinking = true
	server.shrinklog = nil
	ul()

	defer func() {
		ul := server.WriterLock()
		server.shrinking = false
		server.shrinklog = nil
		ul()
		log.Infof("aof shrink ended %v", time.Now().Sub(start))
		return
	}()

	err := func() error {
		f, err := os.Create(core.AppendFileName + "-shrink")
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
					defer server.WriterLock()()
					server.scanGreaterOrEqual(nextkey, func(key string, col *collection.Collection) bool {
						if len(keys) == maxkeys {
							keysdone = false
							nextkey = key
							return false
						}
						keys = append(keys, key)
						return true
					})
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
					defer server.WriterLock()()
					col := server.getCol(keys[0])
					if col == nil {
						return
					}
					var fnames = col.FieldArr() // reload an array of field names to match each object
					var exm *rhh.Map            // the expiration map
					if value, ok := server.expires.Get(keys[0]); ok {
						exm = value.(*rhh.Map)
					}
					var now = time.Now().UnixNano() // used for expiration
					var count = 0                   // the object count
					col.ScanGreaterOrEqual(nextid, false, nil, nil,
						func(id string, obj geojson.Object, fields []float64) bool {
							if count == maxids {
								// we reached the max number of ids for one batch
								nextid = id
								idsdone = false
								return false
							}

							// here we fill the values array with a new command
							values = values[:0]
							values = append(values, "set")
							values = append(values, keys[0])
							values = append(values, id)
							for i, fvalue := range fields {
								if fvalue != 0 {
									values = append(values, "field")
									values = append(values, fnames[i])
									values = append(values, strconv.FormatFloat(fvalue, 'f', -1, 64))
								}
							}
							if exm != nil {
								if at, ok := exm.Get(id); ok {
									expires := at.(int64) - now
									if expires > 0 {
										values = append(values, "ex")
										values = append(values, strconv.FormatFloat(math.Floor(float64(expires)/float64(time.Second)*10)/10, 'f', -1, 64))
									}
								}
							}
							if objIsSpatial(obj) {
								values = append(values, "object")
								values = append(values, string(obj.AppendJSON(nil)))
							} else {
								values = append(values, "string")
								values = append(values, obj.String())
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
			defer server.WriterLock()()
			for name := range server.hooks {
				hnames = append(hnames, name)
			}
		}()
		// sort the names for consistency
		sort.Strings(hnames)
		for _, name := range hnames {
			func() {
				defer server.WriterLock()
				hook := server.hooks[name]
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
					values = append(values)
				}
				for _, meta := range hook.Metas {
					values = append(values, "meta", meta.Name, meta.Value)
				}
				if !hook.expires.IsZero() {
					ex := float64(hook.expires.Sub(time.Now())) /
						float64(time.Second)
					values = append(values, "ex",
						strconv.FormatFloat(ex, 'f', 1, 64))
				}
				for _, value := range hook.Message.Args {
					values = append(values, value)
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
			defer server.WriterLock()()

			// flush the aof buffer
			server.flushAOF(false)

			aofbuf = aofbuf[:0]
			for _, values := range server.shrinklog {
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
			if err := server.aof.Close(); err != nil {
				log.Fatalf("shrink live aof close fatal operation: %v", err)
			}
			if err := f.Close(); err != nil {
				log.Fatalf("shrink new aof close fatal operation: %v", err)
			}
			if err := os.Rename(core.AppendFileName, core.AppendFileName+"-bak"); err != nil {
				log.Fatalf("shrink backup fatal operation: %v", err)
			}
			if err := os.Rename(core.AppendFileName+"-shrink", core.AppendFileName); err != nil {
				log.Fatalf("shrink rename fatal operation: %v", err)
			}
			server.aof, err = os.OpenFile(core.AppendFileName, os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				log.Fatalf("shrink openfile fatal operation: %v", err)
			}
			var n int64
			n, err = server.aof.Seek(0, 2)
			if err != nil {
				log.Fatalf("shrink seek end fatal operation: %v", err)
			}
			server.aofsz = n

			os.Remove(core.AppendFileName + "-bak") // ignore error

			// kill all followers connections
			for conn := range server.aofconnM {
				conn.Close()
			}
			return nil
		}()
	}()
	if err != nil {
		log.Errorf("aof shrink failed: %v", err)
		return
	}
}
