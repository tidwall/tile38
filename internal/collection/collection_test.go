package collection

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/gjson"
)

func PO(x, y float64) *geojson.Point {
	return geojson.NewPoint(geometry.Point{X: x, Y: y})
}

func init() {
	seed := time.Now().UnixNano()
	println(seed)
	rand.Seed(seed)
}

func expect(t testing.TB, expect bool) {
	t.Helper()
	if !expect {
		t.Fatal("not what you expected")
	}
}

func bounds(c *Collection) geometry.Rect {
	minX, minY, maxX, maxY := c.Bounds()
	return geometry.Rect{
		Min: geometry.Point{X: minX, Y: minY},
		Max: geometry.Point{X: maxX, Y: maxY},
	}
}

func TestCollectionNewCollection(t *testing.T) {
	const numItems = 10000
	objs := make(map[string]geojson.Object)
	c := New()
	for i := 0; i < numItems; i++ {
		id := strconv.FormatInt(int64(i), 10)
		var obj geojson.Object
		obj = PO(rand.Float64()*360-180, rand.Float64()*180-90)
		objs[id] = obj
		c.Set(id, obj, nil, nil)
	}
	count := 0
	bbox := geometry.Rect{
		Min: geometry.Point{X: -180, Y: -90},
		Max: geometry.Point{X: 180, Y: 90},
	}
	c.geoSearch(bbox, func(id string, obj geojson.Object, field []float64) bool {
		count++
		return true
	})
	if count != len(objs) {
		t.Fatalf("count = %d, expect %d", count, len(objs))
	}
	count = c.Count()
	if count != len(objs) {
		t.Fatalf("c.Count() = %d, expect %d", count, len(objs))
	}
	testCollectionVerifyContents(t, c, objs)
}

func TestCollectionSet(t *testing.T) {
	t.Run("AddString", func(t *testing.T) {
		c := New()
		str1 := String("hello")
		oldObject, oldFields, newFields := c.Set("str", str1, nil, nil)
		expect(t, oldObject == nil)
		expect(t, len(oldFields) == 0)
		expect(t, len(newFields) == 0)
	})
	t.Run("UpdateString", func(t *testing.T) {
		c := New()
		str1 := String("hello")
		str2 := String("world")
		oldObject, oldFields, newFields := c.Set("str", str1, nil, nil)
		expect(t, oldObject == nil)
		expect(t, len(oldFields) == 0)
		expect(t, len(newFields) == 0)
		oldObject, oldFields, newFields = c.Set("str", str2, nil, nil)
		expect(t, oldObject == str1)
		expect(t, len(oldFields) == 0)
		expect(t, len(newFields) == 0)
	})
	t.Run("AddPoint", func(t *testing.T) {
		c := New()
		point1 := PO(-112.1, 33.1)
		oldObject, oldFields, newFields := c.Set("point", point1, nil, nil)
		expect(t, oldObject == nil)
		expect(t, len(oldFields) == 0)
		expect(t, len(newFields) == 0)
	})
	t.Run("UpdatePoint", func(t *testing.T) {
		c := New()
		point1 := PO(-112.1, 33.1)
		point2 := PO(-112.2, 33.2)
		oldObject, oldFields, newFields := c.Set("point", point1, nil, nil)
		expect(t, oldObject == nil)
		expect(t, len(oldFields) == 0)
		expect(t, len(newFields) == 0)
		oldObject, oldFields, newFields = c.Set("point", point2, nil, nil)
		expect(t, oldObject == point1)
		expect(t, len(oldFields) == 0)
		expect(t, len(newFields) == 0)
	})
	t.Run("Fields", func(t *testing.T) {
		c := New()
		str1 := String("hello")
		fNames := []string{"a", "b", "c"}
		fValues := []float64{1, 2, 3}
		oldObj, oldFlds, newFlds := c.Set("str", str1, fNames, fValues)
		expect(t, oldObj == nil)
		expect(t, len(oldFlds) == 0)
		expect(t, reflect.DeepEqual(newFlds, fValues))
		str2 := String("hello")
		fNames = []string{"d", "e", "f"}
		fValues = []float64{4, 5, 6}
		oldObj, oldFlds, newFlds = c.Set("str", str2, fNames, fValues)
		expect(t, oldObj == str1)
		expect(t, reflect.DeepEqual(oldFlds, []float64{1, 2, 3}))
		expect(t, reflect.DeepEqual(newFlds, []float64{1, 2, 3, 4, 5, 6}))
		fValues = []float64{7, 8, 9, 10, 11, 12}
		oldObj, oldFlds, newFlds = c.Set("str", str1, nil, fValues)
		expect(t, oldObj == str2)
		expect(t, reflect.DeepEqual(oldFlds, []float64{1, 2, 3, 4, 5, 6}))
		expect(t, reflect.DeepEqual(newFlds, []float64{7, 8, 9, 10, 11, 12}))
	})
	t.Run("Delete", func(t *testing.T) {
		c := New()

		c.Set("1", String("1"), nil, nil)
		c.Set("2", String("2"), nil, nil)
		c.Set("3", PO(1, 2), nil, nil)

		expect(t, c.Count() == 3)
		expect(t, c.StringCount() == 2)
		expect(t, c.PointCount() == 1)
		expect(t, bounds(c) == geometry.Rect{
			Min: geometry.Point{X: 1, Y: 2},
			Max: geometry.Point{X: 1, Y: 2}})
		var v geojson.Object
		var ok bool
		var flds []float64
		var updated bool
		var updateCount int

		v, _, ok = c.Delete("2")
		expect(t, v.String() == "2")
		expect(t, ok)
		expect(t, c.Count() == 2)
		expect(t, c.StringCount() == 1)
		expect(t, c.PointCount() == 1)

		v, _, ok = c.Delete("1")
		expect(t, v.String() == "1")
		expect(t, ok)
		expect(t, c.Count() == 1)
		expect(t, c.StringCount() == 0)
		expect(t, c.PointCount() == 1)

		expect(t, len(c.FieldMap()) == 0)

		v, flds, updated, ok = c.SetField("3", "hello", 123)
		expect(t, ok)
		expect(t, reflect.DeepEqual(flds, []float64{123}))
		expect(t, updated)
		expect(t, c.FieldMap()["hello"] == 0)

		v, flds, updated, ok = c.SetField("3", "hello", 1234)
		expect(t, ok)
		expect(t, reflect.DeepEqual(flds, []float64{1234}))
		expect(t, updated)

		v, flds, updated, ok = c.SetField("3", "hello", 1234)
		expect(t, ok)
		expect(t, reflect.DeepEqual(flds, []float64{1234}))
		expect(t, !updated)

		v, flds, updateCount, ok = c.SetFields("3",
			[]string{"planet", "world"}, []float64{55, 66})
		expect(t, ok)
		expect(t, reflect.DeepEqual(flds, []float64{1234, 55, 66}))
		expect(t, updateCount == 2)
		expect(t, c.FieldMap()["hello"] == 0)
		expect(t, c.FieldMap()["planet"] == 1)
		expect(t, c.FieldMap()["world"] == 2)

		v, _, ok = c.Delete("3")
		expect(t, v.String() == `{"type":"Point","coordinates":[1,2]}`)
		expect(t, ok)
		expect(t, c.Count() == 0)
		expect(t, c.StringCount() == 0)
		expect(t, c.PointCount() == 0)
		v, _, ok = c.Delete("3")
		expect(t, v == nil)
		expect(t, !ok)
		expect(t, c.Count() == 0)
		expect(t, bounds(c) == geometry.Rect{})
		v, _, ok = c.Get("3")
		expect(t, v == nil)
		expect(t, !ok)
		_, _, _, ok = c.SetField("3", "hello", 123)
		expect(t, !ok)
		_, _, _, ok = c.SetFields("3", []string{"hello"}, []float64{123})
		expect(t, !ok)
		expect(t, c.TotalWeight() == 0)
		expect(t, c.FieldMap()["hello"] == 0)
		expect(t, c.FieldMap()["planet"] == 1)
		expect(t, c.FieldMap()["world"] == 2)
		expect(t, reflect.DeepEqual(
			c.FieldArr(), []string{"hello", "planet", "world"}),
		)
	})
}

func TestCollectionScan(t *testing.T) {
	N := 256
	c := New()
	for _, i := range rand.Perm(N) {
		id := fmt.Sprintf("%04d", i)
		c.Set(id, String(id), []string{"ex"}, []float64{float64(i)})
	}
	var n int
	var prevID string
	c.Scan(false, nil, nil, func(id string, obj geojson.Object, fields []float64) bool {
		if n > 0 {
			expect(t, id > prevID)
		}
		expect(t, id == fmt.Sprintf("%04d", int(fields[0])))
		n++
		prevID = id
		return true
	})
	expect(t, n == c.Count())
	n = 0
	c.Scan(true, nil, nil, func(id string, obj geojson.Object, fields []float64) bool {
		if n > 0 {
			expect(t, id < prevID)
		}
		expect(t, id == fmt.Sprintf("%04d", int(fields[0])))
		n++
		prevID = id
		return true
	})
	expect(t, n == c.Count())

	n = 0
	c.ScanRange("0060", "0070", false, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			if n > 0 {
				expect(t, id > prevID)
			}
			expect(t, id == fmt.Sprintf("%04d", int(fields[0])))
			n++
			prevID = id
			return true
		})
	expect(t, n == 10)

	n = 0
	c.ScanRange("0070", "0060", true, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			if n > 0 {
				expect(t, id < prevID)
			}
			expect(t, id == fmt.Sprintf("%04d", int(fields[0])))
			n++
			prevID = id
			return true
		})
	expect(t, n == 10)

	n = 0
	c.ScanGreaterOrEqual("0070", true, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			if n > 0 {
				expect(t, id < prevID)
			}
			expect(t, id == fmt.Sprintf("%04d", int(fields[0])))
			n++
			prevID = id
			return true
		})
	expect(t, n == 71)

	n = 0
	c.ScanGreaterOrEqual("0070", false, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			if n > 0 {
				expect(t, id > prevID)
			}
			expect(t, id == fmt.Sprintf("%04d", int(fields[0])))
			n++
			prevID = id
			return true
		})
	expect(t, n == c.Count()-70)

}

func TestCollectionSearch(t *testing.T) {
	N := 256
	c := New()
	for i, j := range rand.Perm(N) {
		id := fmt.Sprintf("%04d", j)
		ex := fmt.Sprintf("%04d", i)
		c.Set(id, String(ex), []string{"i", "j"},
			[]float64{float64(i), float64(j)})
	}
	var n int
	var prevValue string
	c.SearchValues(false, nil, nil, func(id string, obj geojson.Object, fields []float64) bool {
		if n > 0 {
			expect(t, obj.String() > prevValue)
		}
		expect(t, id == fmt.Sprintf("%04d", int(fields[1])))
		n++
		prevValue = obj.String()
		return true
	})
	expect(t, n == c.Count())
	n = 0
	c.SearchValues(true, nil, nil, func(id string, obj geojson.Object, fields []float64) bool {
		if n > 0 {
			expect(t, obj.String() < prevValue)
		}
		expect(t, id == fmt.Sprintf("%04d", int(fields[1])))
		n++
		prevValue = obj.String()
		return true
	})
	expect(t, n == c.Count())

	n = 0
	c.SearchValuesRange("0060", "0070", false, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			if n > 0 {
				expect(t, obj.String() > prevValue)
			}
			expect(t, id == fmt.Sprintf("%04d", int(fields[1])))
			n++
			prevValue = obj.String()
			return true
		})
	expect(t, n == 10)

	n = 0
	c.SearchValuesRange("0070", "0060", true, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			if n > 0 {
				expect(t, obj.String() < prevValue)
			}
			expect(t, id == fmt.Sprintf("%04d", int(fields[1])))
			n++
			prevValue = obj.String()
			return true
		})
	expect(t, n == 10)
}

func TestCollectionWeight(t *testing.T) {
	c := New()
	c.Set("1", String("1"), nil, nil)
	expect(t, c.TotalWeight() > 0)
	c.Delete("1")
	expect(t, c.TotalWeight() == 0)
	c.Set("1", String("1"),
		[]string{"a", "b", "c"},
		[]float64{1, 2, 3},
	)
	expect(t, c.TotalWeight() > 0)
	c.Delete("1")
	expect(t, c.TotalWeight() == 0)
	c.Set("1", String("1"),
		[]string{"a", "b", "c"},
		[]float64{1, 2, 3},
	)
	c.Set("2", String("2"),
		[]string{"d", "e", "f"},
		[]float64{4, 5, 6},
	)
	c.Set("1", String("1"),
		[]string{"d", "e", "f"},
		[]float64{4, 5, 6},
	)
	c.Delete("1")
	c.Delete("2")
	expect(t, c.TotalWeight() == 0)
}

func TestSpatialSearch(t *testing.T) {
	json := `
		{"type":"FeatureCollection","features":[
			{"type":"Feature","id":"p1","properties":{"marker-color":"#962d28","stroke":"#962d28","fill":"#962d28"},"geometry":{"type":"Point","coordinates":[-71.4743041992187,42.51867517417283]}},
			{"type":"Feature","id":"p2","properties":{"marker-color":"#962d28","stroke":"#962d28","fill":"#962d28"},"geometry":{"type":"Point","coordinates":[-71.4056396484375,42.50197174319114]}},
			{"type":"Feature","id":"p3","properties":{"marker-color":"#962d28","stroke":"#962d28","fill":"#962d28"},"geometry":{"type":"Point","coordinates":[-71.4619445800781,42.49437779897246]}},
			{"type":"Feature","id":"p4","properties":{"marker-color":"#962d28","stroke":"#962d28","fill":"#962d28"},"geometry":{"type":"Point","coordinates":[-71.4337921142578,42.53891577257117]}},
			{"type":"Feature","id":"r1","properties":{"marker-color":"#962d28","stroke":"#962d28","fill":"#962d28"},"geometry":{"type":"Polygon","coordinates":[[[-71.4279556274414,42.48804880765346],[-71.37439727783203,42.48804880765346],[-71.37439727783203,42.52322988064187],[-71.4279556274414,42.52322988064187],[-71.4279556274414,42.48804880765346]]]}},
			{"type":"Feature","id":"r2","properties":{"marker-color":"#962d28","stroke":"#962d28","fill":"#962d28"},"geometry":{"type":"Polygon","coordinates":[[[-71.4825439453125,42.53588010092859],[-71.45027160644531,42.53588010092859],[-71.45027160644531,42.55839115400447],[-71.4825439453125,42.55839115400447],[-71.4825439453125,42.53588010092859]]]}},
			{"type":"Feature","id":"r3","properties":{"marker-color":"#962d28","stroke":"#962d28","fill":"#962d28"},"geometry":{"type":"Polygon","coordinates": [[[-71.4111328125,42.53512115995963],[-71.3833236694336,42.53512115995963],[-71.3833236694336,42.54953946116446],[-71.4111328125,42.54953946116446],[-71.4111328125,42.53512115995963]]]}},
			{"type":"Feature","id":"q1","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-71.55258178710938,42.51361399979923],[-71.42074584960938,42.51361399979923],[-71.42074584960938,42.59100512331456],[-71.55258178710938,42.59100512331456],[-71.55258178710938,42.51361399979923]]]}},
			{"type":"Feature","id":"q2","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-71.52992248535156,42.48121277771616],[-71.36375427246092,42.48121277771616],[-71.36375427246092,42.57786045892046],[-71.52992248535156,42.57786045892046],[-71.52992248535156,42.48121277771616]]]}},
			{"type":"Feature","id":"q3","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-71.49490356445312,42.56673588590953],[-71.52236938476562,42.47462922809497],[-71.42898559570312,42.464499337722344],[-71.43241882324219,42.522217752342236],[-71.37954711914061,42.56420729713456],[-71.49490356445312,42.56673588590953]]]}},
			{"type":"Feature","id":"q4","properties":{},"geometry":{"type":"Point","coordinates": [-71.46366119384766,42.54043355305221]}}
		]}
	`
	p1, _ := geojson.Parse(gjson.Get(json, `features.#[id=="p1"]`).Raw, nil)
	p2, _ := geojson.Parse(gjson.Get(json, `features.#[id=="p2"]`).Raw, nil)
	p3, _ := geojson.Parse(gjson.Get(json, `features.#[id=="p3"]`).Raw, nil)
	p4, _ := geojson.Parse(gjson.Get(json, `features.#[id=="p4"]`).Raw, nil)
	r1, _ := geojson.Parse(gjson.Get(json, `features.#[id=="r1"]`).Raw, nil)
	r2, _ := geojson.Parse(gjson.Get(json, `features.#[id=="r2"]`).Raw, nil)
	r3, _ := geojson.Parse(gjson.Get(json, `features.#[id=="r3"]`).Raw, nil)
	q1, _ := geojson.Parse(gjson.Get(json, `features.#[id=="q1"]`).Raw, nil)
	q2, _ := geojson.Parse(gjson.Get(json, `features.#[id=="q2"]`).Raw, nil)
	q3, _ := geojson.Parse(gjson.Get(json, `features.#[id=="q3"]`).Raw, nil)
	q4, _ := geojson.Parse(gjson.Get(json, `features.#[id=="q4"]`).Raw, nil)

	c := New()
	c.Set("p1", p1, nil, nil)
	c.Set("p2", p2, nil, nil)
	c.Set("p3", p3, nil, nil)
	c.Set("p4", p4, nil, nil)
	c.Set("r1", r1, nil, nil)
	c.Set("r2", r2, nil, nil)
	c.Set("r3", r3, nil, nil)

	var n int

	n = 0
	c.Within(q1, 0, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			n++
			return true
		},
	)
	expect(t, n == 3)

	n = 0
	c.Within(q2, 0, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			n++
			return true
		},
	)
	expect(t, n == 7)

	n = 0
	c.Within(q3, 0, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			n++
			return true
		},
	)
	expect(t, n == 4)

	n = 0
	c.Intersects(q1, 0, nil, nil,
		func(_ string, _ geojson.Object, _ []float64) bool {
			n++
			return true
		},
	)
	expect(t, n == 4)

	n = 0
	c.Intersects(q2, 0, nil, nil,
		func(_ string, _ geojson.Object, _ []float64) bool {
			n++
			return true
		},
	)
	expect(t, n == 7)

	n = 0
	c.Intersects(q3, 0, nil, nil,
		func(_ string, _ geojson.Object, _ []float64) bool {
			n++
			return true
		},
	)
	expect(t, n == 5)

	n = 0
	c.Intersects(q3, 0, nil, nil,
		func(_ string, _ geojson.Object, _ []float64) bool {
			n++
			return n <= 1
		},
	)
	expect(t, n == 2)

	var items []geojson.Object
	exitems := []geojson.Object{
		r2, p1, p4, r1, p3, r3, p2,
	}
	c.Nearby(q4, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			items = append(items, obj)
			return true
		},
	)
	expect(t, len(items) == 7)
	expect(t, reflect.DeepEqual(items, exitems))
}

func TestCollectionSparse(t *testing.T) {
	rect := geojson.NewRect(geometry.Rect{
		Min: geometry.Point{X: -71.598930, Y: 42.4586739},
		Max: geometry.Point{X: -71.37302, Y: 42.607937},
	})
	N := 10000
	c := New()
	r := rect.Rect()
	for i := 0; i < N; i++ {
		x := (r.Max.X-r.Min.X)*rand.Float64() + r.Min.X
		y := (r.Max.Y-r.Min.Y)*rand.Float64() + r.Min.Y
		point := PO(x, y)
		c.Set(fmt.Sprintf("%d", i), point, nil, nil)
	}
	var n int
	n = 0
	c.Within(rect, 1, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			n++
			return true
		},
	)
	expect(t, n == 4)

	n = 0
	c.Within(rect, 2, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			n++
			return true
		},
	)
	expect(t, n == 16)

	n = 0
	c.Within(rect, 3, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			n++
			return true
		},
	)
	expect(t, n == 64)

	n = 0
	c.Within(rect, 3, nil, nil,
		func(id string, obj geojson.Object, fields []float64) bool {
			n++
			return n <= 30
		},
	)
	expect(t, n == 31)

	n = 0
	c.Intersects(rect, 3, nil, nil,
		func(id string, _ geojson.Object, _ []float64) bool {
			n++
			return true
		},
	)
	expect(t, n == 64)

	n = 0
	c.Intersects(rect, 3, nil, nil,
		func(id string, _ geojson.Object, _ []float64) bool {
			n++
			return n <= 30
		},
	)
	expect(t, n == 31)

}

func testCollectionVerifyContents(t *testing.T, c *Collection, objs map[string]geojson.Object) {
	for id, o2 := range objs {
		o1, _, ok := c.Get(id)
		if !ok {
			t.Fatalf("ok[%s] = false, expect true", id)
		}
		j1 := string(o1.AppendJSON(nil))
		j2 := string(o2.AppendJSON(nil))
		if j1 != j2 {
			t.Fatalf("j1 == %s, expect %s", j1, j2)
		}
	}
}

func TestManyCollections(t *testing.T) {
	colsM := make(map[string]*Collection)
	cols := 100
	objs := 1000
	k := 0
	for i := 0; i < cols; i++ {
		key := strconv.FormatInt(int64(i), 10)
		for j := 0; j < objs; j++ {
			id := strconv.FormatInt(int64(j), 10)
			p := geometry.Point{
				X: rand.Float64()*360 - 180,
				Y: rand.Float64()*180 - 90,
			}
			obj := geojson.Object(PO(p.X, p.Y))
			col, ok := colsM[key]
			if !ok {
				col = New()
				colsM[key] = col
			}
			col.Set(id, obj, nil, nil)
			k++
		}
	}

	col := colsM["13"]
	//println(col.Count())
	bbox := geometry.Rect{
		Min: geometry.Point{X: -180, Y: 30},
		Max: geometry.Point{X: 34, Y: 100},
	}
	col.geoSearch(bbox, func(id string, obj geojson.Object, fields []float64) bool {
		//println(id)
		return true
	})
}

type testPointItem struct {
	id     string
	object geojson.Object
}

func BenchmarkInsert(t *testing.B) {
	rand.Seed(time.Now().UnixNano())
	items := make([]testPointItem, t.N)
	for i := 0; i < t.N; i++ {
		items[i] = testPointItem{
			fmt.Sprintf("%d", i),
			PO(rand.Float64()*360-180, rand.Float64()*180-90),
		}
	}
	col := New()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		col.Set(items[i].id, items[i].object, nil, nil)
	}
}

func BenchmarkReplace(t *testing.B) {
	rand.Seed(time.Now().UnixNano())
	items := make([]testPointItem, t.N)
	for i := 0; i < t.N; i++ {
		items[i] = testPointItem{
			fmt.Sprintf("%d", i),
			PO(rand.Float64()*360-180, rand.Float64()*180-90),
		}
	}
	col := New()
	for i := 0; i < t.N; i++ {
		col.Set(items[i].id, items[i].object, nil, nil)
	}
	t.ResetTimer()
	for _, i := range rand.Perm(t.N) {
		o, _, _ := col.Set(items[i].id, items[i].object, nil, nil)
		if o != items[i].object {
			t.Fatal("shoot!")
		}
	}
}

func BenchmarkGet(t *testing.B) {
	rand.Seed(time.Now().UnixNano())
	items := make([]testPointItem, t.N)
	for i := 0; i < t.N; i++ {
		items[i] = testPointItem{
			fmt.Sprintf("%d", i),
			PO(rand.Float64()*360-180, rand.Float64()*180-90),
		}
	}
	col := New()
	for i := 0; i < t.N; i++ {
		col.Set(items[i].id, items[i].object, nil, nil)
	}
	t.ResetTimer()
	for _, i := range rand.Perm(t.N) {
		o, _, _ := col.Get(items[i].id)
		if o != items[i].object {
			t.Fatal("shoot!")
		}
	}
}

func BenchmarkRemove(t *testing.B) {
	rand.Seed(time.Now().UnixNano())
	items := make([]testPointItem, t.N)
	for i := 0; i < t.N; i++ {
		items[i] = testPointItem{
			fmt.Sprintf("%d", i),
			PO(rand.Float64()*360-180, rand.Float64()*180-90),
		}
	}
	col := New()
	for i := 0; i < t.N; i++ {
		col.Set(items[i].id, items[i].object, nil, nil)
	}
	t.ResetTimer()
	for _, i := range rand.Perm(t.N) {
		o, _, _ := col.Delete(items[i].id)
		if o != items[i].object {
			t.Fatal("shoot!")
		}
	}
}
