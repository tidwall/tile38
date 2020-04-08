package server

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/mmcloughlin/geohash"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/bing"
	"github.com/tidwall/tile38/internal/glob"
)

const defaultCircleSteps = 64

type liveFenceSwitches struct {
	searchScanBaseTokens
	obj    geojson.Object
	cmd    string
	roam   roamSwitches
	groups map[string]string
}

type roamSwitches struct {
	on      bool
	key     string
	id      string
	pattern bool
	meters  float64
	scan    string
}

type roamMatch struct {
	id     string
	obj    geojson.Object
	meters float64
}

func (s liveFenceSwitches) Error() string {
	return goingLive
}

func (s liveFenceSwitches) Close() {
	for _, whereeval := range s.whereevals {
		whereeval.Close()
	}
}

func (s liveFenceSwitches) usingLua() bool {
	return len(s.whereevals) > 0
}

func (server *Server) cmdSearchArgs(
	fromFenceCmd bool, cmd string, vs []string, types []string,
) (s liveFenceSwitches, err error) {
	var t searchScanBaseTokens
	if fromFenceCmd {
		t.fence = true
	}
	vs, t, err = server.parseSearchScanBaseTokens(cmd, t, vs)
	if err != nil {
		return
	}
	s.searchScanBaseTokens = t
	var typ string
	var ok bool
	if vs, typ, ok = tokenval(vs); !ok || typ == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if s.searchScanBaseTokens.output == outputBounds {
		if cmd == "within" || cmd == "intersects" {
			if _, err := strconv.ParseFloat(typ, 64); err == nil {
				// It's likely that the output was not specified, but rather the search bounds.
				s.searchScanBaseTokens.output = defaultSearchOutput
				vs = append([]string{typ}, vs...)
				typ = "BOUNDS"
			}
		}
	}
	ltyp := strings.ToLower(typ)
	var found bool
	for _, t := range types {
		if ltyp == t {
			found = true
			break
		}
	}
	if !found && s.searchScanBaseTokens.fence && ltyp == "roam" && cmd == "nearby" {
		// allow roaming for nearby fence searches.
		found = true
	}
	if !found {
		err = errInvalidArgument(typ)
		return
	}
	switch ltyp {
	case "point":
		fallthrough
	case "circle":
		if s.clip {
			err = errInvalidArgument("cannot clip with " + ltyp)
			return
		}
		var slat, slon, smeters string
		if vs, slat, ok = tokenval(vs); !ok || slat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, slon, ok = tokenval(vs); !ok || slon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		var lat, lon, meters float64
		if lat, err = strconv.ParseFloat(slat, 64); err != nil {
			err = errInvalidArgument(slat)
			return
		}
		if lon, err = strconv.ParseFloat(slon, 64); err != nil {
			err = errInvalidArgument(slon)
			return
		}
		// radius is optional for nearby, but mandatory for others
		if cmd == "nearby" {
			if vs, smeters, ok = tokenval(vs); ok && smeters != "" {
				if meters, err = strconv.ParseFloat(smeters, 64); err != nil {
					err = errInvalidArgument(smeters)
					return
				}
				if meters < 0 {
					err = errInvalidArgument(smeters)
					return
				}
			} else {
				meters = -1
			}
		} else {
			if vs, smeters, ok = tokenval(vs); !ok || smeters == "" {
				err = errInvalidNumberOfArguments
				return
			}
			if meters, err = strconv.ParseFloat(smeters, 64); err != nil {
				err = errInvalidArgument(smeters)
				return
			}
			if meters < 0 {
				err = errInvalidArgument(smeters)
				return
			}
		}
		s.obj = geojson.NewCircle(geometry.Point{X: lon, Y: lat}, meters, defaultCircleSteps)
	case "object":
		if s.clip {
			err = errInvalidArgument("cannot clip with object")
			return
		}
		var obj string
		if vs, obj, ok = tokenval(vs); !ok || obj == "" {
			err = errInvalidNumberOfArguments
			return
		}
		s.obj, err = geojson.Parse(obj, &server.geomParseOpts)
		if err != nil {
			return
		}
	case "bounds":
		var sminLat, sminLon, smaxlat, smaxlon string
		if vs, sminLat, ok = tokenval(vs); !ok || sminLat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sminLon, ok = tokenval(vs); !ok || sminLon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, smaxlat, ok = tokenval(vs); !ok || smaxlat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, smaxlon, ok = tokenval(vs); !ok || smaxlon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		var minLat, minLon, maxLat, maxLon float64
		if minLat, err = strconv.ParseFloat(sminLat, 64); err != nil {
			err = errInvalidArgument(sminLat)
			return
		}
		if minLon, err = strconv.ParseFloat(sminLon, 64); err != nil {
			err = errInvalidArgument(sminLon)
			return
		}
		if maxLat, err = strconv.ParseFloat(smaxlat, 64); err != nil {
			err = errInvalidArgument(smaxlat)
			return
		}
		if maxLon, err = strconv.ParseFloat(smaxlon, 64); err != nil {
			err = errInvalidArgument(smaxlon)
			return
		}
		s.obj = geojson.NewRect(geometry.Rect{
			Min: geometry.Point{X: minLon, Y: minLat},
			Max: geometry.Point{X: maxLon, Y: maxLat},
		})
	case "hash":
		var hash string
		if vs, hash, ok = tokenval(vs); !ok || hash == "" {
			err = errInvalidNumberOfArguments
			return
		}
		box := geohash.BoundingBox(hash)
		s.obj = geojson.NewRect(geometry.Rect{
			Min: geometry.Point{X: box.MinLng, Y: box.MinLat},
			Max: geometry.Point{X: box.MaxLng, Y: box.MaxLat},
		})
	case "quadkey":
		var key string
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		var minLat, minLon, maxLat, maxLon float64
		minLat, minLon, maxLat, maxLon, err = bing.QuadKeyToBounds(key)
		if err != nil {
			err = errInvalidArgument(key)
			return
		}
		s.obj = geojson.NewRect(geometry.Rect{
			Min: geometry.Point{X: minLon, Y: minLat},
			Max: geometry.Point{X: maxLon, Y: maxLat},
		})
	case "tile":
		var sx, sy, sz string
		if vs, sx, ok = tokenval(vs); !ok || sx == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sy, ok = tokenval(vs); !ok || sy == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sz, ok = tokenval(vs); !ok || sz == "" {
			err = errInvalidNumberOfArguments
			return
		}
		var x, y int64
		var z uint64
		if x, err = strconv.ParseInt(sx, 10, 64); err != nil {
			err = errInvalidArgument(sx)
			return
		}
		if y, err = strconv.ParseInt(sy, 10, 64); err != nil {
			err = errInvalidArgument(sy)
			return
		}
		if z, err = strconv.ParseUint(sz, 10, 64); err != nil {
			err = errInvalidArgument(sz)
			return
		}
		var minLat, minLon, maxLat, maxLon float64
		minLat, minLon, maxLat, maxLon = bing.TileXYToBounds(x, y, z)
		s.obj = geojson.NewRect(geometry.Rect{
			Min: geometry.Point{X: minLon, Y: minLat},
			Max: geometry.Point{X: maxLon, Y: maxLat},
		})
	case "get":
		if s.clip {
			err = errInvalidArgument("cannot clip with get")
		}
		var key, id string
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, id, ok = tokenval(vs); !ok || id == "" {
			err = errInvalidNumberOfArguments
			return
		}
		col := server.getCol(key)
		if col == nil {
			err = errKeyNotFound
			return
		}
		s.obj, _, ok = col.Get(id)
		if !ok {
			err = errIDNotFound
			return
		}
	case "roam":
		s.roam.on = true
		if vs, s.roam.key, ok = tokenval(vs); !ok || s.roam.key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, s.roam.id, ok = tokenval(vs); !ok || s.roam.id == "" {
			err = errInvalidNumberOfArguments
			return
		}
		s.roam.pattern = glob.IsGlob(s.roam.id)
		var smeters string
		if vs, smeters, ok = tokenval(vs); !ok || smeters == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if s.roam.meters, err = strconv.ParseFloat(smeters, 64); err != nil {
			err = errInvalidArgument(smeters)
			return
		}
		var scan string
		if vs, scan, ok = tokenval(vs); ok {
			if strings.ToLower(scan) != "scan" {
				err = errInvalidArgument(scan)
				return
			}
			if vs, scan, ok = tokenval(vs); !ok || scan == "" {
				err = errInvalidNumberOfArguments
				return
			}
			s.roam.scan = scan
		}
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	return
}

var nearbyTypes = []string{"point"}
var withinOrIntersectsTypes = []string{
	"geo", "bounds", "hash", "tile", "quadkey", "get", "object", "circle"}

func (server *Server) cmdNearby(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	wr := &bytes.Buffer{}
	s, err := server.cmdSearchArgs(false, "nearby", vs, nearbyTypes)
	if s.usingLua() {
		defer s.Close()
		defer func() {
			if r := recover(); r != nil {
				res = NOMessage
				err = errors.New(r.(string))
				return
			}
		}()
	}
	if err != nil {
		return NOMessage, err
	}
	s.cmd = "nearby"
	if s.fence {
		return NOMessage, s
	}
	sw, err := server.newScanWriter(
		wr, msg, s.key, s.output, s.precision, s.glob, false,
		s.cursor, s.limit, s.wheres, s.whereins, s.whereevals, s.nofields)
	if err != nil {
		return NOMessage, err
	}
	if msg.OutputType == JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		maxDist := s.obj.(*geojson.Circle).Meters()
		iter := func(id string, o geojson.Object, fields []float64, dist float64) bool {
			if server.hasExpired(s.key, id) {
				return true
			}

			if maxDist > 0 && dist > maxDist {
				return false
			}

			meters := 0.0
			if s.distance {
				meters = dist
			}
			return sw.writeObject(ScanWriterParams{
				id:       id,
				o:        o,
				fields:   fields,
				distance: meters,
				noLock:   true,
			})
		}
		sw.col.Nearby(s.obj, sw, msg.Deadline, iter)
	}
	sw.writeFoot()
	if msg.OutputType == JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
}

func (server *Server) cmdWithin(msg *Message) (res resp.Value, err error) {
	return server.cmdWithinOrIntersects("within", msg)
}

func (server *Server) cmdIntersects(msg *Message) (res resp.Value, err error) {
	return server.cmdWithinOrIntersects("intersects", msg)
}

func (server *Server) cmdWithinOrIntersects(cmd string, msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]

	wr := &bytes.Buffer{}
	s, err := server.cmdSearchArgs(false, cmd, vs, withinOrIntersectsTypes)
	if s.usingLua() {
		defer s.Close()
		defer func() {
			if r := recover(); r != nil {
				res = NOMessage
				err = errors.New(r.(string))
				return
			}
		}()
	}
	if err != nil {
		return NOMessage, err
	}
	s.cmd = cmd
	if s.fence {
		return NOMessage, s
	}
	sw, err := server.newScanWriter(
		wr, msg, s.key, s.output, s.precision, s.glob, false,
		s.cursor, s.limit, s.wheres, s.whereins, s.whereevals, s.nofields)
	if err != nil {
		return NOMessage, err
	}
	if msg.OutputType == JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		if cmd == "within" {
			sw.col.Within(s.obj, s.sparse, sw, msg.Deadline, func(
				id string, o geojson.Object, fields []float64,
			) bool {
				if server.hasExpired(s.key, id) {
					return true
				}
				return sw.writeObject(ScanWriterParams{
					id:     id,
					o:      o,
					fields: fields,
					noLock: true,
				})
			})
		} else if cmd == "intersects" {
			sw.col.Intersects(s.obj, s.sparse, sw, msg.Deadline, func(
				id string,
				o geojson.Object,
				fields []float64,
			) bool {
				if server.hasExpired(s.key, id) {
					return true
				}
				params := ScanWriterParams{
					id:     id,
					o:      o,
					fields: fields,
					noLock: true,
				}
				if s.clip {
					params.clip = s.obj
				}
				return sw.writeObject(params)
			})
		}
	}
	sw.writeFoot()
	if msg.OutputType == JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
}

func (server *Server) cmdSeachValuesArgs(vs []string) (
	s liveFenceSwitches, err error,
) {
	var t searchScanBaseTokens
	vs, t, err = server.parseSearchScanBaseTokens("search", t, vs)
	if err != nil {
		return
	}
	s.searchScanBaseTokens = t
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	return
}

func (server *Server) cmdSearch(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]

	wr := &bytes.Buffer{}
	s, err := server.cmdSeachValuesArgs(vs)
	if s.usingLua() {
		defer s.Close()
		defer func() {
			if r := recover(); r != nil {
				res = NOMessage
				err = errors.New(r.(string))
				return
			}
		}()
	}
	if err != nil {
		return NOMessage, err
	}
	sw, err := server.newScanWriter(
		wr, msg, s.key, s.output, s.precision, s.glob, true,
		s.cursor, s.limit, s.wheres, s.whereins, s.whereevals, s.nofields)
	if err != nil {
		return NOMessage, err
	}
	if msg.OutputType == JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		if sw.output == outputCount && len(sw.wheres) == 0 && sw.globEverything == true {
			count := sw.col.Count() - int(s.cursor)
			if count < 0 {
				count = 0
			}
			sw.count = uint64(count)
		} else {
			g := glob.Parse(sw.globPattern, s.desc)
			if g.Limits[0] == "" && g.Limits[1] == "" {
				sw.col.SearchValues(s.desc, sw, msg.Deadline,
					func(id string, o geojson.Object, fields []float64) bool {
						return sw.writeObject(ScanWriterParams{
							id:     id,
							o:      o,
							fields: fields,
							noLock: true,
						})
					},
				)
			} else {
				// must disable globSingle for string value type matching because
				// globSingle is only for ID matches, not values.
				sw.globSingle = false
				sw.col.SearchValuesRange(g.Limits[0], g.Limits[1], s.desc, sw,
					msg.Deadline,
					func(id string, o geojson.Object, fields []float64) bool {
						return sw.writeObject(ScanWriterParams{
							id:     id,
							o:      o,
							fields: fields,
							noLock: true,
						})
					},
				)
			}
		}
	}
	sw.writeFoot()
	if msg.OutputType == JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
}
