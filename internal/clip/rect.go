package clip

import (
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

func clipRect(
	rect *geojson.Rect, clipper geojson.Object, opts *geometry.IndexOptions,
) geojson.Object {
	base := rect.Base()
	points := make([]geometry.Point, base.NumPoints())
	for i := 0; i < len(points); i++ {
		points[i] = base.PointAt(i)
	}
	poly := geometry.NewPoly(points, nil, opts)
	gPoly := geojson.NewPolygon(poly)
	return Clip(gPoly, clipper, opts)
}
