package server

import (
	"bytes"
	"testing"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/mvt"
)

func ls(points []geometry.Point) *geojson.LineString {
	return geojson.NewLineString(geometry.NewLine(points, nil))
}

// Ensure that LineString features are encoded using
// all points in the geometry, not just the first one.
func TestMVTAddFeatureLineStringUsesAllPoints(t *testing.T) {
	tileX, tileY, tileZ := 0, 0, 0

	line := ls([]geometry.Point{
		{X: 1, Y: 1},
		{X: 2, Y: 2},
		{X: 3, Y: 3},
	})
	id := "line"

	actual := mvtRender(tileX, tileY, tileZ, []mvtObj{{id: id, obj: line}})

	var tile mvt.Tile
	layer := tile.AddLayer("tile38")
	layer.SetExtent(4096)

	f := layer.AddFeature(mvt.LineString)
	series := line.Base()
	npoints := series.NumPoints()
	if npoints < 2 {
		t.Fatalf("expected at least two points, got %d", npoints)
	}
	p := series.PointAt(0)
	x, y := mvt.LatLonXY(p.Y, p.X, tileX, tileY, tileZ)
	f.MoveTo(x, y)
	for i := 1; i < npoints; i++ {
		p = series.PointAt(i)
		x, y = mvt.LatLonXY(p.Y, p.X, tileX, tileY, tileZ)
		f.LineTo(x, y)
	}
	f.AddTag("type", "linestring")
	f.AddTag("id", id)

	expected := tile.Render()

	if !bytes.Equal(actual, expected) {
		t.Fatalf("mvtAddFeature LineString encoding mismatch")
	}
}

// LineStrings with fewer than two points should not
// produce any geometry commands.
func TestMVTAddFeatureLineStringTooShort(t *testing.T) {
	tileX, tileY, tileZ := 0, 0, 0

	line := ls([]geometry.Point{
		{X: 1, Y: 1},
	})

	actual := mvtRender(tileX, tileY, tileZ, []mvtObj{{id: "short", obj: line}})

	var tile mvt.Tile
	layer := tile.AddLayer("tile38")
	layer.SetExtent(4096)
	_ = layer.AddFeature(mvt.LineString)

	expected := tile.Render()

	if !bytes.Equal(actual, expected) {
		t.Fatalf("mvtAddFeature LineString with <2 points should not encode geometry")
	}
}

