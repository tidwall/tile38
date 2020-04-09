package clip

import (
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

func clipLineString(
	lineString *geojson.LineString, clipper geojson.Object,
	opts *geometry.IndexOptions,
) geojson.Object {
	bbox := clipper.Rect()
	var newPoints [][]geometry.Point
	var clipped geometry.Segment
	var rejected bool
	var line []geometry.Point
	base := lineString.Base()
	nSegments := base.NumSegments()
	for i := 0; i < nSegments; i++ {
		clipped, rejected = clipSegment(base.SegmentAt(i), bbox)
		if rejected {
			continue
		}
		if len(line) > 0 && line[len(line)-1] != clipped.A {
			newPoints = append(newPoints, line)
			line = []geometry.Point{clipped.A}
		} else if len(line) == 0 {
			line = append(line, clipped.A)
		}
		line = append(line, clipped.B)
	}
	if len(line) > 0 {
		newPoints = append(newPoints, line)
	}
	var children []*geometry.Line
	for _, points := range newPoints {
		children = append(children,
			geometry.NewLine(points, opts))
	}
	if len(children) == 1 {
		return geojson.NewLineString(children[0])
	}
	return geojson.NewMultiLineString(children)
}
