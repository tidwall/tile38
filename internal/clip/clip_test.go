package clip

import (
	"testing"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

func LO(points []geometry.Point) *geojson.LineString {
	return geojson.NewLineString(geometry.NewLine(points, nil))
}

func RO(minX, minY, maxX, maxY float64) *geojson.Rect {
	return geojson.NewRect(geometry.Rect{
		Min: geometry.Point{X: minX, Y: minY},
		Max: geometry.Point{X: maxX, Y: maxY},
	})
}

func PPO(exterior []geometry.Point, holes [][]geometry.Point) *geojson.Polygon {
	return geojson.NewPolygon(geometry.NewPoly(exterior, holes, nil))
}

func TestClipLineStringSimple(t *testing.T) {
	ls := LO([]geometry.Point{
		{X: 1, Y: 1},
		{X: 2, Y: 2},
		{X: 3, Y: 1}})
	clipped := Clip(ls, RO(1.5, 0.5, 2.5, 1.8), nil)
	cl, ok := clipped.(*geojson.MultiLineString)
	if !ok {
		t.Fatal("wrong type")
	}
	if len(cl.Children()) != 2 {
		t.Fatal("result must have two parts in MultiString")
	}
}

func TestClipPolygonSimple(t *testing.T) {
	exterior := []geometry.Point{
		{X: 2, Y: 2},
		{X: 1, Y: 2},
		{X: 1.5, Y: 1.5},
		{X: 1, Y: 1},
		{X: 2, Y: 1},
		{X: 2, Y: 2},
	}
	holes := [][]geometry.Point{
		[]geometry.Point{
			{X: 1.9, Y: 1.9},
			{X: 1.2, Y: 1.9},
			{X: 1.45, Y: 1.65},
			{X: 1.9, Y: 1.5},
			{X: 1.9, Y: 1.9},
		},
	}
	polygon := PPO(exterior, holes)
	clipped := Clip(polygon, RO(1.3, 1.3, 1.4, 2.15), nil)
	cp, ok := clipped.(*geojson.Polygon)
	if !ok {
		t.Fatal("wrong type")
	}
	if cp.Base().Exterior.Empty() {
		t.Fatal("Empty result.")
	}
	if len(cp.Base().Holes) != 1 {
		t.Fatal("result must be a two-ring Polygon")
	}
}

func TestClipPolygon2(t *testing.T) {
	exterior := []geometry.Point{
		{X: 2, Y: 2},
		{X: 1, Y: 2},
		{X: 1.5, Y: 1.5},
		{X: 1, Y: 1},
		{X: 2, Y: 1},
		{X: 2, Y: 2},
	}
	holes := [][]geometry.Point{
		[]geometry.Point{
			{X: 1.9, Y: 1.9},
			{X: 1.2, Y: 1.9},
			{X: 1.45, Y: 1.65},
			{X: 1.9, Y: 1.5},
			{X: 1.9, Y: 1.9},
		},
	}
	polygon := PPO(exterior, holes)
	clipped := Clip(polygon, RO(1.1, 0.8, 1.15, 2.1))
	cp, ok := clipped.(*geojson.Polygon)
	if !ok {
		t.Fatal("wrong type")
	}
	if cp.Base().Exterior.Empty() {
		t.Fatal("Empty result.")
	}
	if len(cp.Base().Holes) != 0 {
		t.Fatal("result must be a single-ring Polygon")
	}
}

// func TestClipLineString(t *testing.T) {
// 	featuresJSON := `
// 		{"type": "FeatureCollection","features": [
// 			{"type": "Feature","properties":{},"geometry": {"type": "LineString","coordinates": [[-71.46537780761717,42.594290856363344],[-71.37714385986328,42.600861802789524],[-71.37508392333984,42.538156868495555],[-71.43756866455078,42.535374141307415],[-71.44683837890625,42.466018925787495],[-71.334228515625,42.465005871175755],[-71.32736206054688,42.52424199254517]]}},
// 			{"type": "Feature","properties":{},"geometry": {"type": "Polygon","coordinates": [[[-71.49284362792969,42.527784255084676],[-71.35791778564453,42.527784255084676],[-71.35791778564453,42.61096959812047],[-71.49284362792969,42.61096959812047],[-71.49284362792969,42.527784255084676]]]}},
// 			{"type": "Feature","properties":{},"geometry": {"type": "Polygon","coordinates": [[[-71.47396087646484,42.48247876554176],[-71.30744934082031,42.48247876554176],[-71.30744934082031,42.576596402826894],[-71.47396087646484,42.576596402826894],[-71.47396087646484,42.48247876554176]]]}},
// 			{"type": "Feature","properties":{},"geometry": {"type": "Polygon","coordinates": [[[-71.33491516113281,42.613496290695196],[-71.29920959472656,42.613496290695196],[-71.29920959472656,42.643556064374536],[-71.33491516113281,42.643556064374536],[-71.33491516113281,42.613496290695196]]]}},
// 			{"type": "Feature","properties":{},"geometry": {"type": "Polygon","coordinates": [[[-71.37130737304686,42.530061317794775],[-71.3287353515625,42.530061317794775],[-71.3287353515625,42.60414701616359],[-71.37130737304686,42.60414701616359],[-71.37130737304686,42.530061317794775]]]}},
// 			{"type": "Feature","properties":{},"geometry": {"type": "Polygon","coordinates": [[[-71.52889251708984,42.564460160624115],[-71.45713806152342,42.54043355305221],[-71.53266906738281,42.49969365675931],[-71.36547088623047,42.508552415528634],[-71.43962860107422,42.58999409368092],[-71.52889251708984,42.564460160624115]]]}},
// 			{"type": "Feature","properties": {},"geometry": {"type": "Point","coordinates": [-71.33079528808594,42.55940269610327]}},
// 			{"type": "Feature","properties": {},"geometry": {"type": "Point","coordinates": [-71.27208709716797,42.53107331902133]}}
// 		]}
// 	`
// 	rectJSON := `{"type": "Feature","properties": {},"geometry": {"type": "Polygon","coordinates": [[[-71.44065856933594,42.51740991900762],[-71.29131317138672,42.51740991900762],[-71.29131317138672,42.62663343969058],[-71.44065856933594,42.62663343969058],[-71.44065856933594,42.51740991900762]]]}}`
// 	features := expectJSON(t, featuresJSON, nil)
// 	rect := expectJSON(t, rectJSON, nil)
// 	clipped := features.Clipped(rect)
// 	println(clipped.String())

// }
