package controller

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/bing"
	"github.com/tidwall/tile38/controller/glob"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/geojson"
	"github.com/tidwall/tile38/geojson/geohash"
	"sort"
)

type liveFenceSwitches struct {
	searchScanBaseTokens
	lat, lon, meters float64
	o                geojson.Object
	minLat, minLon   float64
	maxLat, maxLon   float64
	cmd              string
	roam             roamSwitches
	groups           map[string]string
}

type roamSwitches struct {
	on      bool
	key     string
	id      string
	pattern bool
	meters  float64
	scan    string
}

func (s liveFenceSwitches) Error() string {
	return "going live"
}

func (c *Controller) cmdSearchArgs(cmd string, vs []resp.Value, types []string) (s liveFenceSwitches, err error) {
	if vs, s.searchScanBaseTokens, err = parseSearchScanBaseTokens(cmd, vs); err != nil {
		return
	}
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
				vs = append([]resp.Value{resp.StringValue(typ)}, vs...)
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
		var slat, slon, smeters string
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
		if s.lat, err = strconv.ParseFloat(slat, 64); err != nil {
			err = errInvalidArgument(slat)
			return
		}
		if s.lon, err = strconv.ParseFloat(slon, 64); err != nil {
			err = errInvalidArgument(slon)
			return
		}
		if s.meters, err = strconv.ParseFloat(smeters, 64); err != nil {
			err = errInvalidArgument(smeters)
			return
		}
	case "object":
		var obj string
		if vs, obj, ok = tokenval(vs); !ok || obj == "" {
			err = errInvalidNumberOfArguments
			return
		}
		s.o, err = geojson.ObjectJSON(obj)
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
		if s.minLat, err = strconv.ParseFloat(sminLat, 64); err != nil {
			err = errInvalidArgument(sminLat)
			return
		}
		if s.minLon, err = strconv.ParseFloat(sminLon, 64); err != nil {
			err = errInvalidArgument(sminLon)
			return
		}
		if s.maxLat, err = strconv.ParseFloat(smaxlat, 64); err != nil {
			err = errInvalidArgument(smaxlat)
			return
		}
		if s.maxLon, err = strconv.ParseFloat(smaxlon, 64); err != nil {
			err = errInvalidArgument(smaxlon)
			return
		}
	case "hash":
		var hash string
		if vs, hash, ok = tokenval(vs); !ok || hash == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if s.minLat, s.minLon, s.maxLat, s.maxLon, err = geohash.Bounds(hash); err != nil {
			err = errInvalidArgument(hash)
			return
		}
	case "quadkey":
		var key string
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if s.minLat, s.minLon, s.maxLat, s.maxLon, err = bing.QuadKeyToBounds(key); err != nil {
			err = errInvalidArgument(key)
			return
		}
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
		s.minLat, s.minLon, s.maxLat, s.maxLon = bing.TileXYToBounds(x, y, z)
	case "get":
		var key, id string
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, id, ok = tokenval(vs); !ok || id == "" {
			err = errInvalidNumberOfArguments
			return
		}
		col := c.getCol(key)
		if col == nil {
			err = errKeyNotFound
			return
		}
		o, _, ok := col.Get(id)
		if !ok {
			err = errIDNotFound
			return
		}
		if o.IsBBoxDefined() {
			bbox := o.CalculatedBBox()
			s.minLat = bbox.Min.Y
			s.minLon = bbox.Min.X
			s.maxLat = bbox.Max.Y
			s.maxLon = bbox.Max.X
		} else {
			s.o = o
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
var withinOrIntersectsTypes = []string{"geo", "bounds", "hash", "tile", "quadkey", "get", "object"}

func (c *Controller) cmdNearby(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	wr := &bytes.Buffer{}
	s, err := c.cmdSearchArgs("nearby", vs, nearbyTypes)
	if err != nil {
		return "", err
	}
	s.cmd = "nearby"
	if s.fence {
		return "", s
	}

	minZ, maxZ := zMinMaxFromWheres(s.wheres)
	sw, err := c.newScanWriter(wr, msg, s.key, s.output, s.precision, s.glob, false, s.limit, s.wheres, s.nofields)
	if err != nil {
		return "", err
	}
	if msg.OutputType == server.JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		s.cursor = sw.col.Nearby(s.cursor, s.sparse, s.lat, s.lon, s.meters, minZ, maxZ, func(id string, o geojson.Object, fields []float64) bool {
			// Calculate distance if we need to
			distance := 0.0
			if s.distance {
				distance = o.CalculatedPoint().DistanceTo(geojson.Position{X: s.lon, Y: s.lat, Z: 0})
			}

			opts := ScanWriterParams{
				id:          id,
				o:           o,
				fields:      fields,
				distance:    distance,
			}

			// TODO: Remove this after KNN, please!
			if distance > 0 && s.limit > 0 {
				return sw.poolObject(opts)
			}

			return sw.writeObject(opts)
		})
	}

	// TODO: remove this after KNN search implementation
	// TODO: This is totally hacky right now
	if len(sw.pool) > 0 {
		sort.Sort(ScanWriterPool(sw.pool))

		for i := range sw.pool {
			if !sw.writeObject(sw.pool[i]) {
				break
			}
		}
	}

	sw.writeFoot(s.cursor)
	if msg.OutputType == server.JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	}
	return string(wr.Bytes()), nil
}

func (c *Controller) cmdWithin(msg *server.Message) (res string, err error) {
	return c.cmdWithinOrIntersects("within", msg)
}

func (c *Controller) cmdIntersects(msg *server.Message) (res string, err error) {
	return c.cmdWithinOrIntersects("intersects", msg)
}

func (c *Controller) cmdWithinOrIntersects(cmd string, msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	wr := &bytes.Buffer{}
	s, err := c.cmdSearchArgs(cmd, vs, withinOrIntersectsTypes)
	if err != nil {
		return "", err
	}
	s.cmd = cmd
	if s.fence {
		return "", s
	}
	sw, err := c.newScanWriter(wr, msg, s.key, s.output, s.precision, s.glob, false, s.limit, s.wheres, s.nofields)
	if err != nil {
		return "", err
	}
	if sw.col == nil {
		return "", errKeyNotFound
	}
	if msg.OutputType == server.JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	minZ, maxZ := zMinMaxFromWheres(s.wheres)
	if cmd == "within" {
		s.cursor = sw.col.Within(s.cursor, s.sparse, s.o, s.minLat, s.minLon, s.maxLat, s.maxLon, minZ, maxZ,
			func(id string, o geojson.Object, fields []float64) bool {
				return sw.writeObject(ScanWriterParams{
					id:     id,
					o:      o,
					fields: fields,
				})
			},
		)
	} else if cmd == "intersects" {
		s.cursor = sw.col.Intersects(s.cursor, s.sparse, s.o, s.minLat, s.minLon, s.maxLat, s.maxLon, minZ, maxZ,
			func(id string, o geojson.Object, fields []float64) bool {
				return sw.writeObject(ScanWriterParams{
					id:     id,
					o:      o,
					fields: fields,
				})
			},
		)
	}
	sw.writeFoot(s.cursor)
	if msg.OutputType == server.JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	}
	return string(wr.Bytes()), nil
}

func cmdSeachValuesArgs(vs []resp.Value) (s liveFenceSwitches, err error) {
	if vs, s.searchScanBaseTokens, err = parseSearchScanBaseTokens("search", vs); err != nil {
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	return
}

func (c *Controller) cmdSearch(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	wr := &bytes.Buffer{}
	s, err := cmdSeachValuesArgs(vs)
	if err != nil {
		return "", err
	}
	sw, err := c.newScanWriter(wr, msg, s.key, s.output, s.precision, s.glob, true, s.limit, s.wheres, s.nofields)
	if err != nil {
		return "", err
	}
	if msg.OutputType == server.JSON {
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
				s.cursor = sw.col.SearchValues(s.cursor, s.desc,
					func(id string, o geojson.Object, fields []float64) bool {
						return sw.writeObject(ScanWriterParams{
							id:     id,
							o:      o,
							fields: fields,
						})
					},
				)
			} else {
				// must disable globSingle for string value type matching because
				// globSingle is only for ID matches, not values.
				sw.globSingle = false
				s.cursor = sw.col.SearchValuesRange(
					s.cursor, g.Limits[0], g.Limits[1], s.desc,
					func(id string, o geojson.Object, fields []float64) bool {
						return sw.writeObject(ScanWriterParams{
							id:     id,
							o:      o,
							fields: fields,
						})
					},
				)
			}
		}
	}
	sw.writeFoot(s.cursor)
	if msg.OutputType == server.JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	}
	return string(wr.Bytes()), nil
}
