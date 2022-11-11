package collection

import (
	"encoding/json"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

// String ...
type String string

var _ geojson.Object = String("")

// Spatial ...
func (s String) Spatial() geojson.Spatial {
	return geojson.EmptySpatial{}
}

// ForEach ...
func (s String) ForEach(iter func(geom geojson.Object) bool) bool {
	return iter(s)
}

// Empty ...
func (s String) Empty() bool {
	return true
}

// Valid ...
func (s String) Valid() bool {
	return false
}

// Rect ...
func (s String) Rect() geometry.Rect {
	return geometry.Rect{}
}

// Center ...
func (s String) Center() geometry.Point {
	return geometry.Point{}
}

// AppendJSON ...
func (s String) AppendJSON(dst []byte) []byte {
	data, _ := json.Marshal(string(s))
	return append(dst, data...)
}

// String ...
func (s String) String() string {
	return string(s)
}

// JSON ...
func (s String) JSON() string {
	return string(s.AppendJSON(nil))
}

// MarshalJSON ...
func (s String) MarshalJSON() ([]byte, error) {
	return s.AppendJSON(nil), nil
}

// Within ...
func (s String) Within(obj geojson.Object) bool {
	return false
}

// Contains ...
func (s String) Contains(obj geojson.Object) bool {
	return false
}

// Intersects ...
func (s String) Intersects(obj geojson.Object) bool {
	return false
}

// NumPoints ...
func (s String) NumPoints() int {
	return 0
}

// Distance ...
func (s String) Distance(obj geojson.Object) float64 {
	return 0
}

func (s String) Members() string {
	return ""
}
