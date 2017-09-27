package controller

import (
	"bytes"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/bing"
	"github.com/tidwall/tile38/controller/glob"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/geojson"
	"github.com/tidwall/tile38/geojson/geohash"
)

type liveFenceSwitches struct {
	searchScanBaseTokens
	lat, lon, meters float64
	o                geojson.Object
	minLat, minLon   float64
	maxLat, maxLon   float64
	cmd              string
	roam             roamSwitches
	knn              bool
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

		umeters := true
		if vs, smeters, ok = tokenval(vs); !ok || smeters == "" {
			umeters = false
			if cmd == "nearby" {
				// possible that this is KNN search
				s.knn = s.searchScanBaseTokens.ulimit && // must be true
					!s.searchScanBaseTokens.usparse // must be false
			}
			if !s.knn {
				err = errInvalidArgument(slat)
				return
			}
		}

		if s.lat, err = strconv.ParseFloat(slat, 64); err != nil {
			err = errInvalidArgument(slat)
			return
		}
		if s.lon, err = strconv.ParseFloat(slon, 64); err != nil {
			err = errInvalidArgument(slon)
			return
		}

		if umeters {
			if s.meters, err = strconv.ParseFloat(smeters, 64); err != nil {
				err = errInvalidArgument(smeters)
				return
			}
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

func (c *Controller) cmdNearby(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	empty_response := resp.SimpleStringValue("")
	wr := &bytes.Buffer{}
	s, err := c.cmdSearchArgs("nearby", vs, nearbyTypes)
	if err != nil {
		return empty_response, err
	}
	s.cmd = "nearby"
	if s.fence {
		return empty_response, s
	}
	minZ, maxZ := zMinMaxFromWheres(s.wheres)
	sw, err := c.newScanWriter(wr, msg, s.key, s.output, s.precision, s.glob, false, s.cursor, s.limit, s.wheres, s.whereins, s.nofields)
	if err != nil {
		return empty_response, err
	}
	if msg.OutputType == server.JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		iter := func(id string, o geojson.Object, fields []float64, dist *float64) bool {
			if c.hasExpired(s.key, id) {
				return true
			}
			// Calculate distance if we need to
			distance := 0.0
			if s.distance {
				if dist != nil {
					distance = *dist
				} else {
					distance = o.CalculatedPoint().DistanceTo(geojson.Position{X: s.lon, Y: s.lat, Z: 0})
				}
			}
			return sw.writeObject(ScanWriterParams{
				id:       id,
				o:        o,
				fields:   fields,
				distance: distance,
				noLock:   true,
			})
		}
		if s.knn {
			nearestNeighbors(sw, s.lat, s.lon, iter)
		} else {
			sw.col.Nearby(s.sparse, s.lat, s.lon, s.meters, minZ, maxZ,
				func(id string, o geojson.Object, fields []float64) bool {
					return iter(id, o, fields, nil)
				},
			)
		}
	}
	sw.writeFoot()
	if msg.OutputType == server.JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
}

type iterItem struct {
	id     string
	o      geojson.Object
	fields []float64
	dist   float64
}

func nearestNeighbors(sw *scanWriter, lat, lon float64, iter func(id string, o geojson.Object, fields []float64, dist *float64) bool) {
	limit := sw.limit * 10
	if limit > limitItems*10 {
		limit = limitItems * 10
	}
	k := sw.cursor + limit
	var items []iterItem
	sw.col.NearestNeighbors(lat, lon, func(id string, o geojson.Object, fields []float64) bool {
		if k == 0 {
			return false
		}
		dist := o.CalculatedPoint().DistanceTo(geojson.Position{X: lon, Y: lat, Z: 0})
		items = append(items, iterItem{id: id, o: o, fields: fields, dist: dist})
		k--
		return true
	})
	sort.Slice(items, func(i, j int) bool {
		return items[i].dist < items[j].dist
	})
	for _, item := range items {
		if !iter(item.id, item.o, item.fields, &item.dist) {
			return
		}
	}
}

func (c *Controller) cmdWithin(msg *server.Message) (res resp.Value, err error) {
	return c.cmdWithinOrIntersects("within", msg)
}

func (c *Controller) cmdIntersects(msg *server.Message) (res resp.Value, err error) {
	return c.cmdWithinOrIntersects("intersects", msg)
}

func (c *Controller) cmdWithinOrIntersects(cmd string, msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	empty_response := resp.SimpleStringValue("")

	wr := &bytes.Buffer{}
	s, err := c.cmdSearchArgs(cmd, vs, withinOrIntersectsTypes)
	if err != nil {
		return empty_response, err
	}
	s.cmd = cmd
	if s.fence {
		return empty_response, s
	}
	sw, err := c.newScanWriter(wr, msg, s.key, s.output, s.precision, s.glob, false, s.cursor, s.limit, s.wheres, s.whereins, s.nofields)
	if err != nil {
		return empty_response, err
	}
	if msg.OutputType == server.JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		minZ, maxZ := zMinMaxFromWheres(s.wheres)
		if cmd == "within" {
			sw.col.Within(s.sparse, s.o, s.minLat, s.minLon, s.maxLat, s.maxLon, minZ, maxZ,
				func(id string, o geojson.Object, fields []float64) bool {
					if c.hasExpired(s.key, id) {
						return true
					}
					return sw.writeObject(ScanWriterParams{
						id:     id,
						o:      o,
						fields: fields,
						noLock: true,
					})
				},
			)
		} else if cmd == "intersects" {
			sw.col.Intersects(s.sparse, s.o, s.minLat, s.minLon, s.maxLat, s.maxLon, minZ, maxZ,
				func(id string, o geojson.Object, fields []float64) bool {
					if c.hasExpired(s.key, id) {
						return true
					}
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
	sw.writeFoot()
	if msg.OutputType == server.JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
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

func (c *Controller) cmdSearch(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	empty_response := resp.SimpleStringValue("")

	wr := &bytes.Buffer{}
	s, err := cmdSeachValuesArgs(vs)
	if err != nil {
		return empty_response, err
	}
	sw, err := c.newScanWriter(wr, msg, s.key, s.output, s.precision, s.glob, true, s.cursor, s.limit, s.wheres, s.whereins, s.nofields)
	if err != nil {
		return empty_response, err
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
				sw.col.SearchValues(s.desc,
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
				sw.col.SearchValuesRange(g.Limits[0], g.Limits[1], s.desc,
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
	if msg.OutputType == server.JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
}
