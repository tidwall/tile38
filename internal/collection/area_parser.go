package collection

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/mmcloughlin/geohash"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/tile38/internal/bing"
	"github.com/tidwall/tile38/internal/glob"
)

type RoamSwitches struct {
	On      bool
	Key     string
	Id      string
	Pattern bool
	Meters  float64
	Scan    string
	Nearbys map[string]map[string]bool
}

var errInvalidNumberOfArguments = errors.New("invalid number of arguments")
var errKeyNotFound = errors.New("key not found")
var errIDNotFound = errors.New("id not found")

func errInvalidArgument(arg string) error {
	return fmt.Errorf("invalid argument '%s'", arg)
}

func nextToken(vs []string) (nvs []string, token string, ok bool) {
	if len(vs) > 0 {
		token = vs[0]
		nvs = vs[1:]
		ok = true
	}
	return
}

func parseArea(
	ovs []string, doClip bool, getCol func(string) *Collection, geomParseOpts geojson.ParseOptions,
) (vs []string, o geojson.Object, err error) {
	var ok bool
	var typ string
	vs = ovs[:]
	if vs, typ, ok = nextToken(vs); !ok || typ == "" {
		err = errInvalidNumberOfArguments
		return
	}
	ltyp := strings.ToLower(typ)
	switch ltyp {
	case "point":
		var slat, slon string
		if vs, slat, ok = nextToken(vs); !ok || slat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, slon, ok = nextToken(vs); !ok || slon == "" {
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
		if vs, slat, ok = nextToken(vs); !ok || slat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, slon, ok = nextToken(vs); !ok || slon == "" {
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
		if vs, smeters, ok = nextToken(vs); !ok || smeters == "" {
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
		if vs, obj, ok = nextToken(vs); !ok || obj == "" {
			err = errInvalidNumberOfArguments
			return
		}
		o, err = geojson.Parse(obj, &geomParseOpts)
		if err != nil {
			return
		}
	case "bounds":
		var sminLat, sminLon, smaxlat, smaxlon string
		if vs, sminLat, ok = nextToken(vs); !ok || sminLat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sminLon, ok = nextToken(vs); !ok || sminLon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, smaxlat, ok = nextToken(vs); !ok || smaxlat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, smaxlon, ok = nextToken(vs); !ok || smaxlon == "" {
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
		if vs, hash, ok = nextToken(vs); !ok || hash == "" {
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
		if vs, key, ok = nextToken(vs); !ok || key == "" {
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
		if vs, sx, ok = nextToken(vs); !ok || sx == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sy, ok = nextToken(vs); !ok || sy == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sz, ok = nextToken(vs); !ok || sz == "" {
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
		if vs, key, ok = nextToken(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, id, ok = nextToken(vs); !ok || id == "" {
			err = errInvalidNumberOfArguments
			return
		}
		col := getCol(key)
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


func parseNearbyArea(ovs []string, fence bool) (vs []string, o geojson.Object, rs RoamSwitches, err error) {
	var ok bool
	var typ string
	vs = ovs[:]
	if vs, typ, ok = nextToken(vs); !ok || typ == "" {
		err = errInvalidNumberOfArguments
		return
	}
	ltyp := strings.ToLower(typ)
	switch ltyp {
	case "roam":
		if !fence {
			err = errInvalidArgument("roam")
			return
		}
		rs.On = true
		if vs, rs.Key, ok = nextToken(vs); !ok || rs.Key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, rs.Id, ok = nextToken(vs); !ok || rs.Id == "" {
			err = errInvalidNumberOfArguments
			return
		}
		rs.Pattern = glob.IsGlob(rs.Id)
		var smeters string
		if vs, smeters, ok = nextToken(vs); !ok || smeters == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if rs.Meters, err = strconv.ParseFloat(smeters, 64); err != nil {
			err = errInvalidArgument(smeters)
			return
		}
		var scan string
		if vs, scan, ok = nextToken(vs); ok {
			if strings.ToLower(scan) != "Scan" {
				err = errInvalidArgument(scan)
				return
			}
			if vs, scan, ok = nextToken(vs); !ok || scan == "" {
				err = errInvalidNumberOfArguments
				return
			}
			rs.Scan = scan
		}
	case "point":
		var slat, slon, smeters string
		if vs, slat, ok = nextToken(vs); !ok || slat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, slon, ok = nextToken(vs); !ok || slon == "" {
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
		if vs, smeters, ok = nextToken(vs); ok && smeters != "" {
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
		o = geojson.NewCircle(geometry.Point{X: lon, Y: lat}, meters, defaultCircleSteps)
	}
	return
}
