package server

import (
	"net/url"
	"strings"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/mvt"
)

type mvtObj struct {
	id  string
	obj geojson.Object
}

func mvtDrawRing(f *mvt.Feature, tileX, tileY, tileZ int, ring geometry.Series,
	hole bool,
) {
	npoints := ring.NumPoints()
	if npoints < 3 {
		return
	}
	cw := ring.Clockwise()
	reverse := (cw && hole) || (!cw && !hole)
	if reverse {
		p := ring.PointAt(npoints - 1)
		f.MoveTo(mvt.LatLonXY(p.Y, p.X, tileX, tileY, tileZ))
		for i := npoints - 2; i >= 0; i-- {
			p := ring.PointAt(i)
			f.LineTo(mvt.LatLonXY(p.Y, p.X, tileX, tileY, tileZ))
		}
	} else {
		p := ring.PointAt(0)
		f.MoveTo(mvt.LatLonXY(p.Y, p.X, tileX, tileY, tileZ))
		for i := 1; i < npoints; i++ {
			p := ring.PointAt(i)
			f.LineTo(mvt.LatLonXY(p.Y, p.X, tileX, tileY, tileZ))
		}
	}
	f.ClosePath()
}

func mvtAddFeature(l *mvt.Layer, tileX, tileY, tileZ int, o mvtObj) {
	var f *mvt.Feature
	switch g := o.obj.(type) {
	case *geojson.Point:
		f = l.AddFeature(mvt.Point)
		p := g.Base()
		f.MoveTo(mvt.LatLonXY(p.Y, p.X, tileX, tileY, tileZ))
		f.AddTag("type", "point")
	case *geojson.SimplePoint:
		f = l.AddFeature(mvt.Point)
		p := g
		f.MoveTo(mvt.LatLonXY(p.Y, p.X, tileX, tileY, tileZ))
		f.AddTag("type", "point")
	case *geojson.LineString:
		f = l.AddFeature(mvt.LineString)
		line := g.Base()
		npoints := line.NumPoints()
		if npoints < 2 {
			return
		}
		p := line.PointAt(0)
		f.MoveTo(mvt.LatLonXY(p.Y, p.X, tileX, tileY, tileZ))
		for i := 1; i < npoints; i++ {
			p := line.PointAt(i)
			f.LineTo(mvt.LatLonXY(p.Y, p.X, tileX, tileY, tileZ))
		}
		f.AddTag("type", "linestring")
	case *geojson.Rect:
		f = l.AddFeature(mvt.Polygon)
		mvtDrawRing(f, tileX, tileY, tileZ, g.Base(), false)
		f.AddTag("type", "polygon")
	case *geojson.Polygon:
		f = l.AddFeature(mvt.Polygon)
		poly := g.Base()
		mvtDrawRing(f, tileX, tileY, tileZ, poly.Exterior, false)
		for _, hole := range poly.Holes {
			mvtDrawRing(f, tileX, tileY, tileZ, hole, true)
		}
		f.AddTag("type", "polygon")
	case *geojson.Feature:
		mvtAddFeature(l, tileX, tileY, tileZ, mvtObj{o.id, g.Base()})
		return
	default:
		if g, ok := g.(geojson.Collection); ok {
			for _, g := range g.Children() {
				mvtAddFeature(l, tileX, tileY, tileZ, mvtObj{o.id, g})
			}
		}
		return
	}
	f.AddTag("id", o.id)
}

func mvtRender(tileX, tileY, tileZ int, objs []mvtObj) []byte {
	var tile mvt.Tile
	l := tile.AddLayer("tile38")
	l.SetExtent(4096)
	for _, obj := range objs {
		mvtAddFeature(l, tileX, tileY, tileZ, obj)
	}
	return tile.Render()
}

func mvtFilterHTTPArgs(msg *Message, query string) (modified bool) {
	path := msg.Args[0]
	parts := strings.Split(path, "/")
	if len(parts) != 4 {
		return false
	}
	parts[3] = parts[3][:len(parts[3])-4]
	for i := 0; i < len(parts); i++ {
		var err error
		parts[i], err = url.PathUnescape(parts[i])
		if err != nil {
			return false
		}
	}
	var limit string
	var sparse string
	if query != "" {
		q, _ := url.ParseQuery(query)
		sparse = q.Get("sparse")
		limit = q.Get("limit")
	}
	msg._command = ""
	msg.Args = []string{"INTERSECTS", parts[0]}
	if sparse != "" {
		msg.Args = append(msg.Args, "SPARSE", sparse)
	} else if limit != "" {
		msg.Args = append(msg.Args, "LIMIT", limit)
	} else {
		msg.Args = append(msg.Args, "LIMIT", "100000000")
	}
	msg.Args = append(msg.Args, "MVT", parts[2], parts[3], parts[1])
	return true
}
