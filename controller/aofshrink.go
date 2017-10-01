package controller

import (
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/tile38/controller/collection"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/geojson"
)

const maxkeys = 8
const maxids = 32
const maxchunk = 4 * 1024 * 1024

func (c *Controller) aofshrink() {
	start := time.Now()
	c.mu.Lock()
	if c.shrinking {
		c.mu.Unlock()
		return
	}
	c.shrinking = true
	c.shrinklog = nil
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.shrinking = false
		c.shrinklog = nil
		c.mu.Unlock()
		log.Infof("aof shrink ended %v", time.Now().Sub(start))
		return
	}()

	err := func() error {
		f, err := os.Create(path.Join(c.dir, "shrink"))
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
					c.mu.Lock()
					defer c.mu.Unlock()
					c.scanGreaterOrEqual(nextkey, func(key string, col *collection.Collection) bool {
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
					c.mu.Lock()
					defer c.mu.Unlock()
					col := c.getCol(keys[0])
					if col == nil {
						return
					}
					var fnames = col.FieldArr()  // reload an array of field names to match each object
					var exm = c.expires[keys[0]] // the expiration map
					var now = time.Now()         // used for expiration
					var count = 0                // the object count
					col.ScanGreaterOrEqual(nextid, false,
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
								at, ok := exm[id]
								if ok {
									expires := at.Sub(now)
									if expires > 0 {
										values = append(values, "ex")
										values = append(values, strconv.FormatFloat(math.Floor(float64(expires)/float64(time.Second)*10)/10, 'f', -1, 64))
									}
								}
							}
							switch obj := obj.(type) {
							default:
								if obj.IsGeometry() {
									values = append(values, "object")
									values = append(values, obj.JSON())
								} else {
									values = append(values, "string")
									values = append(values, obj.String())
								}
							case geojson.SimplePoint:
								values = append(values, "point")
								values = append(values, strconv.FormatFloat(obj.Y, 'f', -1, 64))
								values = append(values, strconv.FormatFloat(obj.X, 'f', -1, 64))
							case geojson.Point:
								if obj.Coordinates.Z == 0 {
									values = append(values, "point")
									values = append(values, strconv.FormatFloat(obj.Coordinates.Y, 'f', -1, 64))
									values = append(values, strconv.FormatFloat(obj.Coordinates.X, 'f', -1, 64))
									values = append(values, strconv.FormatFloat(obj.Coordinates.Z, 'f', -1, 64))
								} else {
									values = append(values, "point")
									values = append(values, strconv.FormatFloat(obj.Coordinates.Y, 'f', -1, 64))
									values = append(values, strconv.FormatFloat(obj.Coordinates.X, 'f', -1, 64))
								}
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
			}
			if len(aofbuf) > maxchunk {
				if _, err := f.Write(aofbuf); err != nil {
					return err
				}
				aofbuf = aofbuf[:0]
			}
		}

		// load hooks
		// first load the names of the hooks
		var hnames []string
		func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			for name := range c.hooks {
				hnames = append(hnames, name)
			}
		}()
		// sort the names for consistency
		sort.Strings(hnames)
		for _, name := range hnames {
			func() {
				c.mu.Lock()
				defer c.mu.Unlock()
				hook := c.hooks[name]
				if hook == nil {
					return
				}
				hook.mu.Lock()
				defer hook.mu.Unlock()

				var values []string
				values = append(values, "sethook")
				values = append(values, name)
				values = append(values, strings.Join(hook.Endpoints, ","))
				for _, value := range hook.Message.Values {
					values = append(values, value.String())
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
			c.mu.Lock()
			defer c.mu.Unlock()
			aofbuf = aofbuf[:0]
			for _, values := range c.shrinklog {
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
			if err := c.aof.Close(); err != nil {
				log.Fatalf("shink live aof close fatal operation: %v", err)
			}
			if err := os.Rename(path.Join(c.dir, "appendonly.aof"), path.Join(c.dir, "appendonly.bak")); err != nil {
				log.Fatalf("shink backup fatal operation: %v", err)
			}
			if err := os.Rename(path.Join(c.dir, "shrink"), path.Join(c.dir, "appendonly.aof")); err != nil {
				log.Fatalf("shink rename fatal operation: %v", err)
			}
			c.aof, err = os.OpenFile(path.Join(c.dir, "appendonly.aof"), os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				log.Fatalf("shink openfile fatal operation: %v", err)
			}
			var n int64
			n, err = c.aof.Seek(0, 2)
			if err != nil {
				log.Fatalf("shink seek end fatal operation: %v", err)
			}
			c.aofsz = int(n)

			os.Remove(path.Join(c.dir, "appendonly.bak")) // ignore error

			// kill all followers connections
			for conn := range c.aofconnM {
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
