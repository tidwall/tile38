package geojson

import (
	"github.com/tidwall/geojson/geometry"
)

// Clip clips the contents of a geojson object and return
func Clip(
	obj Object, clipper Object, opts *geometry.IndexOptions,
) (clipped Object) {
	switch obj := obj.(type) {
	case *Point:
		return clipPoint(obj, clipper, opts)
	case *Rect:
		return clipRect(obj, clipper, opts)
	case *LineString:
		return clipLineString(obj, clipper, opts)
	case *Polygon:
		return clipPolygon(obj, clipper, opts)
	case *Circle:
		return NewClippedCircle(obj, clipper, opts)
	case *Feature:
		return clipFeature(obj, clipper, opts)
	case Collection:
		return clipCollection(obj, clipper, opts)
	}
	return obj
}

// clipSegment is Cohen-Sutherland Line Clipping
// https://www.cs.helsinki.fi/group/goa/viewing/leikkaus/lineClip.html
func clipSegment(seg geometry.Segment, rect geometry.Rect) (
	res geometry.Segment, rejected bool,
) {
	startCode := getCode(rect, seg.A)
	endCode := getCode(rect, seg.B)
	if (startCode | endCode) == 0 {
		// trivially accept
		res = seg
	} else if (startCode & endCode) != 0 {
		// trivially reject
		rejected = true
	} else if startCode != 0 {
		// start is outside. get new start.
		newStart := intersect(rect, startCode, seg.A, seg.B)
		res, rejected =
			clipSegment(geometry.Segment{A: newStart, B: seg.B}, rect)
	} else {
		// end is outside. get new end.
		newEnd := intersect(rect, endCode, seg.A, seg.B)
		res, rejected = clipSegment(geometry.Segment{A: seg.A, B: newEnd}, rect)
	}
	return
}

// clipRing is Sutherland-Hodgman Polygon Clipping
// https://www.cs.helsinki.fi/group/goa/viewing/leikkaus/intro2.html
func clipRing(ring []geometry.Point, bbox geometry.Rect) (
	resRing []geometry.Point,
) {
	if len(ring) < 4 {
		// under 4 elements this is not a polygon ring!
		return
	}
	var edge uint8
	var inside, prevInside bool
	var prev geometry.Point
	for edge = 1; edge <= 8; edge *= 2 {
		prev = ring[len(ring)-2]
		prevInside = (getCode(bbox, prev) & edge) == 0
		for _, p := range ring {
			inside = (getCode(bbox, p) & edge) == 0
			if prevInside && inside {
				// Staying inside
				resRing = append(resRing, p)
			} else if prevInside && !inside {
				// Leaving
				resRing = append(resRing, intersect(bbox, edge, prev, p))
			} else if !prevInside && inside {
				// Entering
				resRing = append(resRing, intersect(bbox, edge, prev, p))
				resRing = append(resRing, p)
			} else {
				// Staying outside
			}
			prev, prevInside = p, inside
		}
		if len(resRing) > 0 && resRing[0] != resRing[len(resRing)-1] {
			resRing = append(resRing, resRing[0])
		}
		ring, resRing = resRing, []geometry.Point{}
		if len(ring) == 0 {
			break
		}
	}
	resRing = ring
	return
}

func getCode(bbox geometry.Rect, point geometry.Point) (code uint8) {
	code = 0

	if point.X < bbox.Min.X {
		code |= 1 // left
	} else if point.X > bbox.Max.X {
		code |= 2 // right
	}

	if point.Y < bbox.Min.Y {
		code |= 4 // bottom
	} else if point.Y > bbox.Max.Y {
		code |= 8 // top
	}

	return
}

func intersect(bbox geometry.Rect, code uint8, start, end geometry.Point) (
	new geometry.Point,
) {
	if (code & 8) != 0 { // top
		new = geometry.Point{
			X: start.X + (end.X-start.X)*(bbox.Max.Y-start.Y)/(end.Y-start.Y),
			Y: bbox.Max.Y,
		}
	} else if (code & 4) != 0 { // bottom
		new = geometry.Point{
			X: start.X + (end.X-start.X)*(bbox.Min.Y-start.Y)/(end.Y-start.Y),
			Y: bbox.Min.Y,
		}
	} else if (code & 2) != 0 { //right
		new = geometry.Point{
			X: bbox.Max.X,
			Y: start.Y + (end.Y-start.Y)*(bbox.Max.X-start.X)/(end.X-start.X),
		}
	} else if (code & 1) != 0 { // left
		new = geometry.Point{
			X: bbox.Min.X,
			Y: start.Y + (end.Y-start.Y)*(bbox.Min.X-start.X)/(end.X-start.X),
		}
	} else { // should not call intersect with the zero code
	}

	return
}

func clipPoint(
	point *Point, clipper Object, opts *geometry.IndexOptions,
) Object {
	if point.IntersectsRect(clipper.Rect()) {
		return point
	}
	return NewMultiPoint(nil)
}

func clipRect(
	rect *Rect, clipper Object, opts *geometry.IndexOptions,
) Object {
	base := rect.Base()
	points := make([]geometry.Point, base.NumPoints())
	for i := 0; i < len(points); i++ {
		points[i] = base.PointAt(i)
	}
	poly := geometry.NewPoly(points, nil, opts)
	gPoly := NewPolygon(poly)
	return Clip(gPoly, clipper, opts)
}

func clipLineString(
	lineString *LineString, clipper Object,
	opts *geometry.IndexOptions,
) Object {
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
		return NewLineString(children[0])
	}
	return NewMultiLineString(children)
}

func clipPolygon(
	polygon *Polygon, clipper Object,
	opts *geometry.IndexOptions,
) Object {
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
	newPoly := NewPolygon(
		geometry.NewPoly(exterior, holes, opts),
	)
	if newPoly.Empty() {
		return NewMultiPolygon(nil)
	}
	return newPoly
}

func clipFeature(
	feature *Feature, clipper Object,
	opts *geometry.IndexOptions,
) Object {
	newFeature := Clip(feature.Base(), clipper, opts)
	if _, ok := newFeature.(*Feature); !ok {
		newFeature = NewFeature(newFeature, feature.Members())
	}
	return newFeature
}

func clipCollection(
	collection Collection, clipper Object,
	opts *geometry.IndexOptions,
) Object {
	var features []Object
	for _, feature := range collection.Children() {
		feature = Clip(feature, clipper, opts)
		if feature.Empty() {
			continue
		}
		if _, ok := feature.(*Feature); !ok {
			feature = NewFeature(feature, "")
		}
		features = append(features, feature)
	}
	return NewFeatureCollection(features)
}
