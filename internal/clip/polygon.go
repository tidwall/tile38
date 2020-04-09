package clip

import (
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

func clipPolygon(
	polygon *geojson.Polygon, clipper geojson.Object,
	opts *geometry.IndexOptions,
) geojson.Object {
	rect := clipper.Rect()
	var newPoints [][]geometry.Point
	base := polygon.Base()
	rings := []geometry.Ring{base.Exterior}
	rings = append(rings, base.Holes...)
	for _, ring := range rings {
		ringPoints := make([]geometry.Point, ring.NumPoints())
		for i := 0; i < len(ringPoints); i++ {
			ringPoints[i] = ring.PointAt(i)
		}
		if clippedRing := clipRing(ringPoints, rect); len(clippedRing) > 0 {
			newPoints = append(newPoints, clippedRing)
		}
	}

	var exterior []geometry.Point
	var holes [][]geometry.Point
	if len(newPoints) > 0 {
		exterior = newPoints[0]
	}
	if len(newPoints) > 1 {
		holes = newPoints[1:]
	}
	newPoly := geojson.NewPolygon(
		geometry.NewPoly(exterior, holes, opts),
	)
	if newPoly.Empty() {
		return geojson.NewMultiPolygon(nil)
	}
	return newPoly
}
