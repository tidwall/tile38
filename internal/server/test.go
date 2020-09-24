package server

// TEST command: spatial tests without walking the tree.

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mmcloughlin/geohash"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/bing"
)

func (s *Server) parseArea(ovs []string, doClip bool) (vs []string, o geojson.Object, err error) {
	var ok bool
	var typ string
	vs = ovs[:]
	if vs, typ, ok = tokenval(vs); !ok || typ == "" {
		err = errInvalidNumberOfArguments
		return
	}
	ltyp := strings.ToLower(typ)
	switch ltyp {
	case "point":
		var slat, slon string
		if vs, slat, ok = tokenval(vs); !ok || slat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, slon, ok = tokenval(vs); !ok || slon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		var lat, lon float64
		if lat, err = strconv.ParseFloat(slat, 64); err != nil {
			err = errInvalidArgument(slat)
			return
		}
		if lon, err = strconv.ParseFloat(slon, 64); err != nil {
			err = errInvalidArgument(slon)
			return
		}
		o = geojson.NewPoint(geometry.Point{X: lon, Y: lat})
	case "circle":
		if doClip {
			err = fmt.Errorf("invalid clip type '%s'", typ)
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
		if meters, err = strconv.ParseFloat(smeters, 64); err != nil {
			err = errInvalidArgument(smeters)
			return
		}
		if meters < 0 {
			err = errInvalidArgument(smeters)
			return
		}
		o = geojson.NewCircle(geometry.Point{X: lon, Y: lat}, meters, defaultCircleSteps)
	case "object":
		if doClip {
			err = fmt.Errorf("invalid clip type '%s'", typ)
			return
		}
		var obj string
		if vs, obj, ok = tokenval(vs); !ok || obj == "" {
			err = errInvalidNumberOfArguments
			return
		}
		o, err = geojson.Parse(obj, &s.geomParseOpts)
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
		o = geojson.NewRect(geometry.Rect{
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
		o = geojson.NewRect(geometry.Rect{
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
		o = geojson.NewRect(geometry.Rect{
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
		o = geojson.NewRect(geometry.Rect{
			Min: geometry.Point{X: minLon, Y: minLat},
			Max: geometry.Point{X: maxLon, Y: maxLat},
		})
	case "get":
		if doClip {
			err = fmt.Errorf("invalid clip type '%s'", typ)
			return
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
		col := s.getCol(key)
		if col == nil {
			err = errKeyNotFound
			return
		}
		o, _, ok = col.Get(id)
		if !ok {
			err = errIDNotFound
			return
		}
	}
	return
}

func (s *Server) cmdTest(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]

	var ok bool
	var test string
	var clipped geojson.Object
	var area1, area2 *areaExpression
	if vs, area1, err = s.parseAreaExpression(vs, false); err != nil {
		return
	}
	if vs, test, ok = tokenval(vs); !ok || test == "" {
		err = errInvalidNumberOfArguments
		return
	}
	lTest := strings.ToLower(test)
	if lTest != "within" && lTest != "intersects" {
		err = errInvalidArgument(test)
		return
	}
	var wtok string
	var nvs []string
	var doClip bool
	nvs, wtok, ok = tokenval(vs)
	if ok && len(wtok) > 0 {
		switch strings.ToLower(wtok) {
		case "clip":
			vs = nvs
			if lTest != "intersects" {
				err = errInvalidArgument(wtok)
				return
			}
			doClip = true
		}
	}
	if vs, area2, err = s.parseAreaExpression(vs, doClip); err != nil {
		return
	}
	if doClip && (area1.obj == nil || area2.obj == nil) {
		err = errInvalidArgument("clip")
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
	}

	var result int
	if lTest == "within" {
		if area1.WithinExpr(area2) {
			result = 1
		}
	} else if lTest == "intersects" {
		if area1.IntersectsExpr(area2) {
			result = 1
			if doClip {
				clipped = geojson.Clip(area1.obj, area2.obj, nil)
			}
		}
	}
	switch msg.OutputType {
	case JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		if result != 0 {
			buf.WriteString(`,"result":true`)
		} else {
			buf.WriteString(`,"result":false`)
		}
		if clipped != nil {
			buf.WriteString(`,"object":` + clipped.JSON())
		}
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case RESP:
		if clipped != nil {
			return resp.ArrayValue([]resp.Value{
				resp.IntegerValue(result),
				resp.StringValue(clipped.JSON())}), nil
		}
		return resp.IntegerValue(result), nil
	}
	return NOMessage, nil
}
