package clip

import (
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

func clipPoint(
	point *geojson.Point, clipper geojson.Object, opts *geometry.IndexOptions,
) geojson.Object {
	if point.IntersectsRect(clipper.Rect()) {
		return point
	}
	return geojson.NewMultiPoint(nil)
}
