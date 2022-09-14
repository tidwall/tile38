package server

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/iwpnd/sectr"
	"github.com/mmcloughlin/geohash"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/bing"
	"github.com/tidwall/tile38/internal/buffer"
	"github.com/tidwall/tile38/internal/clip"
	"github.com/tidwall/tile38/internal/glob"
)

const defaultCircleSteps = 64

type liveFenceSwitches struct {
	searchScanBaseTokens
	obj  geojson.Object
	cmd  string
	roam roamSwitches
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

func (lfs liveFenceSwitches) Error() string {
	return goingLive
}

func (lfs liveFenceSwitches) Close() {
	for _, whereeval := range lfs.whereevals {
		whereeval.Close()
	}
}

func (lfs liveFenceSwitches) usingLua() bool {
	return len(lfs.whereevals) > 0
}

func parseRectArea(ltyp string, vs []string) (
	nvs []string, rect *geojson.Rect, tileX, tileY, tileZ int, err error,
) {

	var ok bool

	switch ltyp {
	default:
		err = errNotRectangle
		return
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
		rect = geojson.NewRect(geometry.Rect{
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
		rect = geojson.NewRect(geometry.Rect{
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
		rect = geojson.NewRect(geometry.Rect{
			Min: geometry.Point{X: minLon, Y: minLat},
			Max: geometry.Point{X: maxLon, Y: maxLat},
		})
	case "tile", "mvt":
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
		var x, y, z int
		if x, err = strconv.Atoi(sx); err != nil || x < 0 {
			err = errInvalidArgument(sx)
			return
		}
		if y, err = strconv.Atoi(sy); err != nil || y < 0 {
			err = errInvalidArgument(sy)
			return
		}
		if z, err = strconv.Atoi(sz); err != nil || z < 0 || z > 23 {
			err = errInvalidArgument(sz)
			return
		}
		var minLat, minLon, maxLat, maxLon float64
		minLat, minLon, maxLat, maxLon =
			bing.TileXYToBounds(int64(x), int64(y), uint64(z))
		rect = geojson.NewRect(geometry.Rect{
			Min: geometry.Point{X: minLon, Y: minLat},
			Max: geometry.Point{X: maxLon, Y: maxLat},
		})
		tileX = x
		tileY = y
		tileZ = z
	}
	nvs = vs
	return
}

func (s *Server) cmdSearchArgs(
	fromFenceCmd bool, cmd string, vs []string, types map[string]bool,
) (lfs liveFenceSwitches, err error) {
	var t searchScanBaseTokens
	if fromFenceCmd {
		t.fence = true
	}
	vs, t, err = s.parseSearchScanBaseTokens(cmd, t, vs)
	if err != nil {
		return
	}
	lfs.searchScanBaseTokens = t
	var typ string
	var ok bool
	if vs, typ, ok = tokenval(vs); !ok || typ == "" {
		err = errInvalidNumberOfArguments
		return
	}

	if lfs.searchScanBaseTokens.output == outputBounds {
		if cmd == "within" || cmd == "intersects" {
			if _, err := strconv.ParseFloat(typ, 64); err == nil {
				// It's likely that the output was not specified, but rather the search bounds.
				lfs.searchScanBaseTokens.output = defaultSearchOutput
				vs = append([]string{typ}, vs...)
				typ = "BOUNDS"
			}
		}
	}
	ltyp := strings.ToLower(typ)
	found := types[ltyp]
	if !found && lfs.searchScanBaseTokens.fence && ltyp == "roam" && cmd == "nearby" {
		// allow roaming for nearby fence searches.
		found = true
	}
	if !found {
		err = errInvalidArgument(typ)
		return
	}

	switch ltyp {
	case "point":
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
				meters, err = strconv.ParseFloat(smeters, 64)
				if err != nil || meters < 0 {
					err = errInvalidArgument(smeters)
					return
				}
			} else {
				meters = -1
			}
			// Nearby used the Circle type
			lfs.obj = geojson.NewCircle(geometry.Point{X: lon, Y: lat}, meters, defaultCircleSteps)
		} else {
			// Intersects and Within use the Point type
			lfs.obj = geojson.NewPoint(geometry.Point{X: lon, Y: lat})
		}
	case "circle":
		if lfs.clip {
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
		if vs, smeters, ok = tokenval(vs); !ok || smeters == "" {
			err = errInvalidNumberOfArguments
			return
		}
		meters, err = strconv.ParseFloat(smeters, 64)
		if err != nil || meters < 0 {
			err = errInvalidArgument(smeters)
			return
		}
		lfs.obj = geojson.NewCircle(geometry.Point{X: lon, Y: lat}, meters, defaultCircleSteps)
	case "object":
		if lfs.clip {
			err = errInvalidArgument("cannot clip with object")
			return
		}
		var obj string
		if vs, obj, ok = tokenval(vs); !ok || obj == "" {
			err = errInvalidNumberOfArguments
			return
		}
		lfs.obj, err = geojson.Parse(obj, &s.geomParseOpts)
		if err != nil {
			return
		}
	case "sector":
		if lfs.clip {
			err = errInvalidArgument("cannot clip with " + ltyp)
			return
		}
		var slat, slon, smeters, sb1, sb2 string
		if vs, slat, ok = tokenval(vs); !ok || slat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, slon, ok = tokenval(vs); !ok || slon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, smeters, ok = tokenval(vs); !ok || smeters == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sb1, ok = tokenval(vs); !ok || sb1 == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sb2, ok = tokenval(vs); !ok || sb2 == "" {
			err = errInvalidNumberOfArguments
			return
		}
		var lat, lon, meters, b1, b2 float64
		if lat, err = strconv.ParseFloat(slat, 64); err != nil {
			err = errInvalidArgument(slat)
			return
		}
		if lon, err = strconv.ParseFloat(slon, 64); err != nil {
			err = errInvalidArgument(slon)
			return
		}
		if meters, err = strconv.ParseFloat(smeters, 64); err != nil {
			err = errInvalidArgument(smeters)
			return
		}
		if b1, err = strconv.ParseFloat(sb1, 64); err != nil {
			err = errInvalidArgument(sb1)
			return
		}
		if b2, err = strconv.ParseFloat(sb2, 64); err != nil {
			err = errInvalidArgument(sb2)
			return
		}

		if b1 == b2 {
			err = fmt.Errorf("equal bearings (%s == %s), use CIRCLE instead", sb1, sb2)
			return
		}

		origin := sectr.Point{Lng: lon, Lat: lat}
		sector := sectr.NewSector(origin, meters, b1, b2)

		lfs.obj, err = geojson.Parse(string(sector.JSON()), &s.geomParseOpts)
		if err != nil {
			return
		}
	case "bounds", "hash", "tile", "mvt", "quadkey":
		vs, lfs.obj, lfs.tileX, lfs.tileY, lfs.tileZ, err =
			parseRectArea(ltyp, vs)
		if err != nil {
			return
		}
		lfs.mvt = ltyp == "mvt"
	case "get":
		if lfs.clip {
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
		col, _ := s.cols.Get(key)
		if col == nil {
			err = errKeyNotFound
			return
		}
		lfs.obj, _, _, ok = col.Get(id)
		if !ok {
			err = errIDNotFound
			return
		}
	case "roam":
		lfs.roam.on = true
		if vs, lfs.roam.key, ok = tokenval(vs); !ok || lfs.roam.key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, lfs.roam.id, ok = tokenval(vs); !ok || lfs.roam.id == "" {
			err = errInvalidNumberOfArguments
			return
		}
		lfs.roam.pattern = glob.IsGlob(lfs.roam.id)
		var smeters string
		if vs, smeters, ok = tokenval(vs); !ok || smeters == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if lfs.roam.meters, err = strconv.ParseFloat(smeters, 64); err != nil {
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
			lfs.roam.scan = scan
		}
	}

	var clip_rect *geojson.Rect
	var tok, ltok string
	for len(vs) > 0 {
		if vs, tok, ok = tokenval(vs); !ok || tok == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if strings.ToLower(tok) != "clipby" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, tok, ok = tokenval(vs); !ok || tok == "" {
			err = errInvalidNumberOfArguments
			return
		}
		ltok = strings.ToLower(tok)
		switch ltok {
		case "bounds", "hash", "tile", "quadkey":
			vs, clip_rect, lfs.tileX, lfs.tileY, lfs.tileZ, err =
				parseRectArea(ltok, vs)
			if err == errNotRectangle {
				err = errInvalidArgument("cannot clipby " + ltok)
				return
			}
			if err != nil {
				return
			}
			lfs.obj = clip.Clip(lfs.obj, clip_rect, &s.geomIndexOpts)
		default:
			err = errInvalidArgument("cannot clipby " + ltok)
			return
		}
	}

	if lfs.hasbuffer {
		lfs.obj, err = buffer.Simple(lfs.obj, lfs.buffer)
		if err != nil {
			return
		}

	}
	return
}

var nearbyTypes = map[string]bool{
	"point": true,
}
var withinOrIntersectsTypes = map[string]bool{
	"geo": true, "bounds": true, "hash": true, "tile": true, "quadkey": true,
	"get": true, "object": true, "circle": true, "point": true, "sector": true,
	"mvt": true,
}

func (s *Server) cmdNearby(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	wr := &bytes.Buffer{}
	sargs, err := s.cmdSearchArgs(false, "nearby", vs, nearbyTypes)
	if sargs.usingLua() {
		defer sargs.Close()
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
	sargs.cmd = "nearby"
	if sargs.fence {
		return NOMessage, sargs
	}
	sw, err := s.newScanWriter(
		wr, msg, sargs.key, sargs.output, sargs.precision, sargs.globs, false,
		sargs.cursor, sargs.limit, sargs.wheres, sargs.whereins,
		sargs.whereevals, sargs.nofields,
		sargs.mvt, sargs.tileX, sargs.tileY, sargs.tileZ)
	if err != nil {
		return NOMessage, err
	}
	if msg.OutputType == JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		iterStep := func(id string, o geojson.Object, fields []float64, meters float64) bool {
			return sw.writeObject(ScanWriterParams{
				id:              id,
				o:               o,
				fields:          fields,
				distance:        meters,
				distOutput:      sargs.distance,
				noLock:          true,
				ignoreGlobMatch: true,
				skipTesting:     true,
			})
		}
		maxDist := sargs.obj.(*geojson.Circle).Meters()
		if sargs.sparse > 0 {
			if maxDist < 0 {
				// error cannot use SPARSE and KNN together
				return NOMessage,
					errors.New("cannot use SPARSE without a point distance")
			}
			// An intersects operation is required for SPARSE
			iter := func(id string, o geojson.Object, fields []float64) bool {
				var meters float64
				if sargs.distance {
					meters = o.Distance(sargs.obj)
				}
				return iterStep(id, o, fields, meters)
			}
			sw.col.Intersects(sargs.obj, sargs.sparse, sw, msg.Deadline, iter)
		} else {
			iter := func(id string, o geojson.Object, fields []float64, dist float64) bool {
				if maxDist > 0 && dist > maxDist {
					return false
				}
				var meters float64
				if sargs.distance {
					meters = dist
				}
				return iterStep(id, o, fields, meters)
			}
			sw.col.Nearby(sargs.obj, sw, msg.Deadline, iter)
		}
	}
	sw.writeFoot()
	if msg.OutputType == JSON {
		wr.WriteString(`,"elapsed":"` + time.Since(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
}

func (s *Server) cmdWithin(msg *Message) (res resp.Value, err error) {
	return s.cmdWithinOrIntersects("within", msg)
}

func (s *Server) cmdIntersects(msg *Message) (res resp.Value, err error) {
	return s.cmdWithinOrIntersects("intersects", msg)
}

func (s *Server) cmdWithinOrIntersects(cmd string, msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]

	wr := &bytes.Buffer{}
	sargs, err := s.cmdSearchArgs(false, cmd, vs, withinOrIntersectsTypes)
	if sargs.usingLua() {
		defer sargs.Close()
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
	sargs.cmd = cmd
	if sargs.fence {
		return NOMessage, sargs
	}
	sw, err := s.newScanWriter(
		wr, msg, sargs.key, sargs.output, sargs.precision, sargs.globs, false,
		sargs.cursor, sargs.limit, sargs.wheres, sargs.whereins,
		sargs.whereevals, sargs.nofields,
		sargs.mvt, sargs.tileX, sargs.tileY, sargs.tileZ)
	if err != nil {
		return NOMessage, err
	}
	if msg.OutputType == JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		if cmd == "within" {
			sw.col.Within(sargs.obj, sargs.sparse, sw, msg.Deadline, func(
				id string, o geojson.Object, fields []float64,
			) bool {
				return sw.writeObject(ScanWriterParams{
					id:     id,
					o:      o,
					fields: fields,
					noLock: true,
				})
			})
		} else if cmd == "intersects" {
			sw.col.Intersects(sargs.obj, sargs.sparse, sw, msg.Deadline, func(
				id string,
				o geojson.Object,
				fields []float64,
			) bool {
				params := ScanWriterParams{
					id:     id,
					o:      o,
					fields: fields,
					noLock: true,
				}
				if sargs.clip {
					params.clip = sargs.obj
				}
				return sw.writeObject(params)
			})
		}
	}
	sw.writeFoot()
	if msg.OutputType == JSON {
		wr.WriteString(`,"elapsed":"` + time.Since(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
}

func (s *Server) cmdSeachValuesArgs(vs []string) (
	lfs liveFenceSwitches, err error,
) {
	var t searchScanBaseTokens
	vs, t, err = s.parseSearchScanBaseTokens("search", t, vs)
	if err != nil {
		return
	}
	lfs.searchScanBaseTokens = t
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	return
}

func multiGlobParse(globs []string, desc bool) [2]string {
	var limits [2]string
	for i, pattern := range globs {
		g := glob.Parse(pattern, desc)
		if g.Limits[0] == "" && g.Limits[1] == "" {
			limits[0], limits[1] = "", ""
			break
		}
		if i == 0 {
			limits[0], limits[1] = g.Limits[0], g.Limits[1]
		} else if desc {
			if g.Limits[0] > limits[0] {
				limits[0] = g.Limits[0]
			}
			if g.Limits[1] < limits[1] {
				limits[1] = g.Limits[1]
			}
		} else {
			if g.Limits[0] < limits[0] {
				limits[0] = g.Limits[0]
			}
			if g.Limits[1] > limits[1] {
				limits[1] = g.Limits[1]
			}
		}
	}
	return limits
}

func (s *Server) cmdSearch(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]

	wr := &bytes.Buffer{}
	sargs, err := s.cmdSeachValuesArgs(vs)
	if sargs.usingLua() {
		defer sargs.Close()
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
	sw, err := s.newScanWriter(
		wr, msg, sargs.key, sargs.output, sargs.precision, sargs.globs, true,
		sargs.cursor, sargs.limit, sargs.wheres, sargs.whereins,
		sargs.whereevals, sargs.nofields,
		sargs.mvt, sargs.tileX, sargs.tileY, sargs.tileZ)
	if err != nil {
		return NOMessage, err
	}
	if msg.OutputType == JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		if sw.output == outputCount && len(sw.wheres) == 0 && sw.globEverything {
			count := sw.col.Count() - int(sargs.cursor)
			if count < 0 {
				count = 0
			}
			sw.count = uint64(count)
		} else {
			limits := multiGlobParse(sw.globs, sargs.desc)
			if limits[0] == "" && limits[1] == "" {
				sw.col.SearchValues(sargs.desc, sw, msg.Deadline,
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
				sw.col.SearchValuesRange(limits[0], limits[1], sargs.desc, sw,
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
		wr.WriteString(`,"elapsed":"` + time.Since(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
}
