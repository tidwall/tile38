package geojson

import (
	"github.com/tidwall/geojson/geometry"
)

// Clipped Circle ...
type ClippedCircle struct {
	circle    *Circle
	clipper   Object
	clipped   Object
}

// NewClippedCircle returns a clipped circle object
func NewClippedCircle(circle *Circle, clipper Object, opts *geometry.IndexOptions) *ClippedCircle {
	g := new(ClippedCircle)
	g.circle = circle
	g.clipper = clipper
	g.clipped = clipPolygon(circle.getObject().(*Polygon), clipper, opts)
	return g
}

// AppendJSON ...
func (g *ClippedCircle) AppendJSON(dst []byte) []byte {
	dst = append(dst, `{"type":"FeatureCollection","features":[`...)
	dst = g.circle.AppendJSON(dst)
	dst = g.clipper.AppendJSON(dst)
	dst = append(dst, '}')
	return dst
}

// JSON ...
func (g *ClippedCircle) JSON() string {
	return string(g.AppendJSON(nil))
}

// MarshalJSON ...
func (g *ClippedCircle) MarshalJSON() ([]byte, error) {
	return g.AppendJSON(nil), nil
}

// String ...
func (g *ClippedCircle) String() string {
	return string(g.AppendJSON(nil))
}

// Center returns the circle's center point
func (g *ClippedCircle) Center() geometry.Point {
	return g.clipped.Center()
}

// Within returns true if circle is contained inside object
func (g *ClippedCircle) Within(obj Object) bool {
	return g.clipped.Within(obj)
}

// Contains returns true if the circle contains other object
func (g *ClippedCircle) Contains(obj Object) bool {
	// contains can be exact, without approximation
	return g.circle.Contains(obj) && g.clipper.Contains(obj)
}

// Intersects returns true the circle intersects other object
func (g *ClippedCircle) Intersects(obj Object) bool {
	switch other := obj.(type) {
	case *Point:
		return g.circle.Intersects(obj) && g.clipper.Intersects(obj)
	case Collection:
		for _, p := range other.Children() {
			if g.Intersects(p) {
				return true
			}
		}
		return false
	case *Feature:
		return g.Intersects(other.base)
	default:
		// No simple cases, so using polygon approximation.
		return g.clipped.Intersects(obj)
	}
}

// Empty ...
func (g *ClippedCircle) Empty() bool {
	return !g.clipper.Intersects(g.circle.getObject())
}

// Valid ...
func (g *ClippedCircle) Valid() bool {
	return g.circle.getObject().Valid() && g.clipper.Valid()
}

// ForEach ...
func (g *ClippedCircle) ForEach(iter func(geom Object) bool) bool {
	return g.circle.ForEach(iter)
}

// NumPoints ...
func (g *ClippedCircle) NumPoints() int {
	// should this be g.steps?
	return g.circle.NumPoints()
}

// Distance ...
func (g *ClippedCircle) Distance(other Object) float64 {
	return g.circle.Distance(other)
}

// Rect ...
func (g *ClippedCircle) Rect() geometry.Rect {
	return g.clipper.Rect()
}

// Spatial ...
func (g *ClippedCircle) Spatial() Spatial {
	return g.circle.Spatial()
}
