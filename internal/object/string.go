package object

import (
	"encoding/json"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

type String string

var _ geojson.Object = String("")

func (s String) Spatial() geojson.Spatial {
	return geojson.EmptySpatial{}
}

func (s String) ForEach(iter func(geom geojson.Object) bool) bool {
	return iter(s)
}

func (s String) Empty() bool {
	return true
}

func (s String) Valid() bool {
	return false
}

func (s String) Rect() geometry.Rect {
	return geometry.Rect{}
}

func (s String) Center() geometry.Point {
	return geometry.Point{}
}

func (s String) AppendJSON(dst []byte) []byte {
	data, _ := json.Marshal(string(s))
	return append(dst, data...)
}

func (s String) String() string {
	return string(s)
}

func (s String) JSON() string {
	return string(s.AppendJSON(nil))
}

func (s String) MarshalJSON() ([]byte, error) {
	return s.AppendJSON(nil), nil
}

func (s String) Within(obj geojson.Object) bool {
	return false
}

func (s String) Contains(obj geojson.Object) bool {
	return false
}

func (s String) Intersects(obj geojson.Object) bool {
	return false
}

func (s String) NumPoints() int {
	return 0
}

func (s String) Distance(obj geojson.Object) float64 {
	return 0
}

func (s String) Members() string {
	return ""
}
