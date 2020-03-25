package server

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

type testPointItem struct {
	object geojson.Object
	fields []float64
}

func PO(x, y float64) *geojson.Point {
	return geojson.NewPoint(geometry.Point{X: x, Y: y})
}


func BenchmarkFieldMatch(t *testing.B) {
	rand.Seed(time.Now().UnixNano())
	items := make([]testPointItem, t.N)
	for i := 0; i < t.N; i++ {
		items[i] = testPointItem{
			PO(rand.Float64()*360-180, rand.Float64()*180-90),
			[]float64{rand.Float64()*9+1, math.Round(rand.Float64()*30) + 1},
		}
	}
	sw := &scanWriter{
		wheres: []whereT{
			{"foo", false, 1, false, 3},
			{"bar", false, 10, false, 30},
		},
		whereins: []whereinT{
			{"foo", map[float64]struct{}{1: {}, 2: {}}},
			{"bar", map[float64]struct{}{11: {}, 25: {}}},
		},
		fmap: map[string]int{"foo": 0, "bar": 1},
		farr: []string{"bar", "foo"},
	}
	sw.fvals = make([]float64, len(sw.farr))
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		// one call is super fast, measurements are not reliable, let's do 100
		for ix := 0; ix < 100; ix++ {
			sw.fieldMatch(items[i].fields, items[i].object)
		}
	}
}
