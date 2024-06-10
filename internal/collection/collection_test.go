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
	"github.com/tidwall/tile38/internal/field"
	"github.com/tidwall/tile38/internal/object"
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
		obj := PO(rand.Float64()*360-180, rand.Float64()*180-90)
		objs[id] = obj
		c.Set(object.New(id, obj, 0, field.List{}))
	}
	count := 0
	bbox := geometry.Rect{
		Min: geometry.Point{X: -180, Y: -90},
		Max: geometry.Point{X: 180, Y: 90},
	}
	c.geoSearch(bbox, func(o *object.Object) bool {
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

func toFields(fNames, fValues []string) field.List {
	var fields field.List
	for i := 0; i < len(fNames); i++ {
		fields = fields.Set(field.Make(fNames[i], fValues[i]))
	}
	return fields
}

func TestCollectionSet(t *testing.T) {
	t.Run("AddString", func(t *testing.T) {
		c := New()
		str1 := object.String("hello")
		old := c.Set(object.New("str", str1, 0, field.List{}))
		expect(t, old == nil)
	})
	t.Run("UpdateString", func(t *testing.T) {
		c := New()
		str1 := object.String("hello")
		str2 := object.String("world")
		old := c.Set(object.New("str", str1, 0, field.List{}))
		expect(t, old == nil)
		old = c.Set(object.New("str", str2, 0, field.List{}))
		expect(t, old.Geo() == str1)
	})
	t.Run("AddPoint", func(t *testing.T) {
		c := New()
		point1 := PO(-112.1, 33.1)
		old := c.Set(object.New("point", point1, 0, field.List{}))
		expect(t, old == nil)
	})
	t.Run("UpdatePoint", func(t *testing.T) {
		c := New()
		point1 := PO(-112.1, 33.1)
		point2 := PO(-112.2, 33.2)
		old := c.Set(object.New("point", point1, 0, field.List{}))
		expect(t, old == nil)
		old = c.Set(object.New("point", point2, 0, field.List{}))
		expect(t, old.Geo().Center() == point1.Base())
	})
	t.Run("Fields", func(t *testing.T) {
		c := New()
		str1 := object.String("hello")

		fNames := []string{"a", "b", "c"}
		fValues := []string{"1", "2", "3"}
		fields1 := toFields(fNames, fValues)
		old := c.Set(object.New("str", str1, 0, fields1))
		expect(t, old == nil)

		str2 := object.String("hello")

		fNames = []string{"d", "e", "f"}
		fValues = []string{"4", "5", "6"}
		fields2 := toFields(fNames, fValues)

		old = c.Set(object.New("str", str2, 0, fields2))
		expect(t, old.Geo() == str1)
		expect(t, reflect.DeepEqual(old.Fields(), fields1))

		fNames = []string{"a", "b", "c", "d", "e", "f"}
		fValues = []string{"7", "8", "9", "10", "11", "12"}
		fields3 := toFields(fNames, fValues)
		old = c.Set(object.New("str", str1, 0, fields3))
		expect(t, old.Geo() == str2)
		expect(t, reflect.DeepEqual(old.Fields(), fields2))
	})
	t.Run("Delete", func(t *testing.T) {
		c := New()

		c.Set(object.New("1", object.String("1"), 0, field.List{}))
		c.Set(object.New("2", object.String("2"), 0, field.List{}))
		c.Set(object.New("3", PO(1, 2), 0, field.List{}))

		expect(t, c.Count() == 3)
		expect(t, c.StringCount() == 2)
		expect(t, c.PointCount() == 1)
		expect(t, bounds(c) == geometry.Rect{
			Min: geometry.Point{X: 1, Y: 2},
			Max: geometry.Point{X: 1, Y: 2}})
		var prev *object.Object

		prev = c.Delete("2")
		expect(t, prev.Geo().String() == "2")
		expect(t, c.Count() == 2)
		expect(t, c.StringCount() == 1)
		expect(t, c.PointCount() == 1)

		prev = c.Delete("1")
		expect(t, prev.Geo().String() == "1")
		expect(t, c.Count() == 1)
		expect(t, c.StringCount() == 0)
		expect(t, c.PointCount() == 1)

		prev = c.Delete("3")
		expect(t, prev.Geo().String() == `{"type":"Point","coordinates":[1,2]}`)
		expect(t, c.Count() == 0)
		expect(t, c.StringCount() == 0)
		expect(t, c.PointCount() == 0)
		prev = c.Delete("3")
		expect(t, prev == nil)
		expect(t, c.Count() == 0)
		expect(t, bounds(c) == geometry.Rect{})
		expect(t, c.Get("3") == nil)
	})
}

func fieldValueAt(fields field.List, index int) string {
	if index < 0 || index >= fields.Len() {
		panic("out of bounds")
	}
	var retval string
	var i int
	fields.Scan(func(f field.Field) bool {
		if i == index {
			retval = f.Value().Data()
		}
		i++
		return true
	})
	return retval
}

func TestCollectionScan(t *testing.T) {
	N := 256
	c := New()
	for _, i := range rand.Perm(N) {
		id := fmt.Sprintf("%04d", i)
		c.Set(object.New(id, object.String(id), 0, makeFields(
			field.Make("ex", id),
		)))
	}
	var n int
	var prevID string
	c.Scan(false, nil, nil, func(o *object.Object) bool {
		if n > 0 {
			expect(t, o.ID() > prevID)
		}
		expect(t, o.ID() == fieldValueAt(o.Fields(), 0))
		n++
		prevID = o.ID()
		return true
	})
	expect(t, n == c.Count())
	n = 0
	c.Scan(true, nil, nil, func(o *object.Object) bool {
		if n > 0 {
			expect(t, o.ID() < prevID)
		}
		expect(t, o.ID() == fieldValueAt(o.Fields(), 0))
		n++
		prevID = o.ID()
		return true
	})
	expect(t, n == c.Count())

	n = 0
	c.ScanRange("0060", "0070", false, nil, nil,
		func(o *object.Object) bool {
			if n > 0 {
				expect(t, o.ID() > prevID)
			}
			expect(t, o.ID() == fieldValueAt(o.Fields(), 0))
			n++
			prevID = o.ID()
			return true
		})
	expect(t, n == 10)

	n = 0
	c.ScanRange("0070", "0060", true, nil, nil,
		func(o *object.Object) bool {
			if n > 0 {
				expect(t, o.ID() < prevID)
			}
			expect(t, o.ID() == fieldValueAt(o.Fields(), 0))
			n++
			prevID = o.ID()
			return true
		})
	expect(t, n == 10)

	n = 0
	c.ScanGreaterOrEqual("0070", true, nil, nil,
		func(o *object.Object) bool {
			if n > 0 {
				expect(t, o.ID() < prevID)
			}
			expect(t, o.ID() == fieldValueAt(o.Fields(), 0))
			n++
			prevID = o.ID()
			return true
		})
	expect(t, n == 71)

	n = 0
	c.ScanGreaterOrEqual("0070", false, nil, nil,
		func(o *object.Object) bool {
			if n > 0 {
				expect(t, o.ID() > prevID)
			}
			expect(t, o.ID() == fieldValueAt(o.Fields(), 0))
			n++
			prevID = o.ID()
			return true
		})
	expect(t, n == c.Count()-70)

}

func makeFields(entries ...field.Field) field.List {
	var fields field.List
	for _, f := range entries {
		fields = fields.Set(f)
	}
	return fields
}

func TestCollectionSearch(t *testing.T) {
	N := 256
	c := New()
	for i, j := range rand.Perm(N) {
		id := fmt.Sprintf("%04d", j)
		ex := fmt.Sprintf("%04d", i)
		c.Set(object.New(id, object.String(ex),
			0, makeFields(
				field.Make("i", ex),
				field.Make("j", id),
			)))
	}
	var n int
	var prevValue string
	c.SearchValues(false, nil, nil, func(o *object.Object) bool {
		if n > 0 {
			expect(t, o.Geo().String() > prevValue)
		}
		expect(t, o.ID() == fieldValueAt(o.Fields(), 1))
		n++
		prevValue = o.Geo().String()
		return true
	})
	expect(t, n == c.Count())
	n = 0
	c.SearchValues(true, nil, nil, func(o *object.Object) bool {
		if n > 0 {
			expect(t, o.Geo().String() < prevValue)
		}
		expect(t, o.ID() == fieldValueAt(o.Fields(), 1))
		n++
		prevValue = o.Geo().String()
		return true
	})
	expect(t, n == c.Count())

	n = 0
	c.SearchValuesRange("0060", "0070", false, nil, nil,
		func(o *object.Object) bool {
			if n > 0 {
				expect(t, o.Geo().String() > prevValue)
			}
			expect(t, o.ID() == fieldValueAt(o.Fields(), 1))
			n++
			prevValue = o.Geo().String()
			return true
		})
	expect(t, n == 10)

	n = 0
	c.SearchValuesRange("0070", "0060", true, nil, nil,
		func(o *object.Object) bool {
			if n > 0 {
				expect(t, o.Geo().String() < prevValue)
			}
			expect(t, o.ID() == fieldValueAt(o.Fields(), 1))
			n++
			prevValue = o.Geo().String()
			return true
		})
	expect(t, n == 10)
}

func TestCollectionWeight(t *testing.T) {
	c := New()
	c.Set(object.New("1", object.String("1"), 0, field.List{}))
	expect(t, c.TotalWeight() > 0)
	c.Delete("1")
	expect(t, c.TotalWeight() == 0)
	c.Set(object.New("1", object.String("1"), 0,
		toFields(
			[]string{"a", "b", "c"},
			[]string{"1", "2", "3"},
		),
	))
	expect(t, c.TotalWeight() > 0)
	c.Delete("1")
	expect(t, c.TotalWeight() == 0)
	c.Set(object.New("1", object.String("1"), 0,
		toFields(
			[]string{"a", "b", "c"},
			[]string{"1", "2", "3"},
		),
	))
	c.Set(object.New("2", object.String("2"), 0,
		toFields(
			[]string{"d", "e", "f"},
			[]string{"4", "5", "6"},
		),
	))
	c.Set(object.New("1", object.String("1"), 0,
		toFields(
			[]string{"d", "e", "f"},
			[]string{"4", "5", "6"},
		),
	))
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
	c.Set(object.New("p1", p1, 0, field.List{}))
	c.Set(object.New("p2", p2, 0, field.List{}))
	c.Set(object.New("p3", p3, 0, field.List{}))
	c.Set(object.New("p4", p4, 0, field.List{}))
	c.Set(object.New("r1", r1, 0, field.List{}))
	c.Set(object.New("r2", r2, 0, field.List{}))
	c.Set(object.New("r3", r3, 0, field.List{}))

	var n int

	n = 0
	c.Within(q1, 0, nil, nil, func(o *object.Object) bool {
		n++
		return true
	})
	expect(t, n == 3)

	n = 0
	c.Within(q2, 0, nil, nil, func(o *object.Object) bool {
		n++
		return true
	})
	expect(t, n == 7)

	n = 0
	c.Within(q3, 0, nil, nil, func(o *object.Object) bool {
		n++
		return true
	})
	expect(t, n == 4)

	n = 0
	c.Intersects(q1, 0, nil, nil, func(o *object.Object) bool {
		n++
		return true
	})
	expect(t, n == 4)

	n = 0
	c.Intersects(q2, 0, nil, nil, func(o *object.Object) bool {
		n++
		return true
	})
	expect(t, n == 7)

	n = 0
	c.Intersects(q3, 0, nil, nil, func(o *object.Object) bool {
		n++
		return true
	})
	expect(t, n == 5)

	n = 0
	c.Intersects(q3, 0, nil, nil, func(o *object.Object) bool {
		n++
		return n <= 1
	})
	expect(t, n == 2)

	var items []geojson.Object
	exitems := []geojson.Object{
		r2, p4, p1, r1, r3, p3, p2,
	}

	lastDist := float64(-1)
	distsMonotonic := true
	c.Nearby(q4, nil, nil, func(o *object.Object, dist float64) bool {
		if dist < lastDist {
			distsMonotonic = false
		}
		items = append(items, o.Geo())
		return true
	})
	expect(t, len(items) == 7)
	expect(t, distsMonotonic)
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
		c.Set(object.New(fmt.Sprintf("%d", i), point, 0, field.List{}))
	}
	var n int
	n = 0
	c.Within(rect, 1, nil, nil, func(o *object.Object) bool {
		n++
		return true
	})
	expect(t, n == 4)

	n = 0
	c.Within(rect, 2, nil, nil, func(o *object.Object) bool {
		n++
		return true
	})
	expect(t, n == 16)

	n = 0
	c.Within(rect, 3, nil, nil, func(o *object.Object) bool {
		n++
		return true
	})
	expect(t, n == 64)

	n = 0
	c.Within(rect, 3, nil, nil, func(o *object.Object) bool {
		n++
		return n <= 30
	})
	expect(t, n == 31)

	n = 0
	c.Intersects(rect, 3, nil, nil, func(o *object.Object) bool {
		n++
		return true
	})
	expect(t, n == 64)

	n = 0
	c.Intersects(rect, 3, nil, nil, func(o *object.Object) bool {
		n++
		return n <= 30
	})
	expect(t, n == 31)

}

func testCollectionVerifyContents(t *testing.T, c *Collection, objs map[string]geojson.Object) {
	for id, o2 := range objs {
		o := c.Get(id)
		if o == nil {
			t.Fatalf("ok[%s] = false, expect true", id)
		}
		j1 := string(o.Geo().AppendJSON(nil))
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
			col.Set(object.New(id, obj, 0, field.List{}))
			k++
		}
	}

	col := colsM["13"]
	//println(col.Count())
	bbox := geometry.Rect{
		Min: geometry.Point{X: -180, Y: 30},
		Max: geometry.Point{X: 34, Y: 100},
	}
	col.geoSearch(bbox, func(o *object.Object) bool {
		//println(id)
		return true
	})
}

type testPointItem struct {
	id     string
	object geojson.Object
	fields field.List
}

func makeBenchFields(nFields int) field.List {
	var fields field.List
	for i := 0; i < nFields; i++ {
		key := fmt.Sprintf("%d", i)
		val := key
		fields = fields.Set(field.Make(key, val))
	}
	return fields
}

func BenchmarkInsert_Fields(t *testing.B) {
	benchmarkInsert(t, 1)
}

func BenchmarkInsert_NoFields(t *testing.B) {
	benchmarkInsert(t, 0)
}

func benchmarkInsert(t *testing.B, nFields int) {
	rand.Seed(time.Now().UnixNano())
	items := make([]testPointItem, t.N)
	for i := 0; i < t.N; i++ {
		items[i] = testPointItem{
			fmt.Sprintf("%d", i),
			PO(rand.Float64()*360-180, rand.Float64()*180-90),
			makeBenchFields(nFields),
		}
	}
	col := New()
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		col.Set(object.New(items[i].id, items[i].object, 0, items[i].fields))
	}
}

func BenchmarkReplace_Fields(t *testing.B) {
	benchmarkReplace(t, 1)
}

func BenchmarkReplace_NoFields(t *testing.B) {
	benchmarkReplace(t, 0)
}

func benchmarkReplace(t *testing.B, nFields int) {
	rand.Seed(time.Now().UnixNano())
	items := make([]testPointItem, t.N)
	for i := 0; i < t.N; i++ {
		items[i] = testPointItem{
			fmt.Sprintf("%d", i),
			PO(rand.Float64()*360-180, rand.Float64()*180-90),
			makeBenchFields(nFields),
		}
	}
	col := New()
	for i := 0; i < t.N; i++ {
		col.Set(object.New(items[i].id, items[i].object, 0, items[i].fields))
	}
	t.ResetTimer()
	for _, i := range rand.Perm(t.N) {
		o := col.Set(object.New(items[i].id, items[i].object, 0, field.List{}))
		if o.Geo() != items[i].object {
			t.Fatal("shoot!")
		}
	}
}

func BenchmarkGet_Fields(t *testing.B) {
	benchmarkGet(t, 1)
}

func BenchmarkGet_NoFields(t *testing.B) {
	benchmarkGet(t, 0)
}

func benchmarkGet(t *testing.B, nFields int) {
	rand.Seed(time.Now().UnixNano())
	items := make([]testPointItem, t.N)
	for i := 0; i < t.N; i++ {
		items[i] = testPointItem{
			fmt.Sprintf("%d", i),
			PO(rand.Float64()*360-180, rand.Float64()*180-90),
			makeBenchFields(nFields),
		}
	}
	col := New()
	for i := 0; i < t.N; i++ {
		col.Set(object.New(items[i].id, items[i].object, 0, items[i].fields))
	}
	t.ResetTimer()
	for _, i := range rand.Perm(t.N) {
		o := col.Get(items[i].id)
		if o.Geo() != items[i].object {
			t.Fatal("shoot!")
		}
	}
}

func BenchmarkRemove_Fields(t *testing.B) {
	benchmarkRemove(t, 1)
}

func BenchmarkRemove_NoFields(t *testing.B) {
	benchmarkRemove(t, 0)
}

func benchmarkRemove(t *testing.B, nFields int) {
	rand.Seed(time.Now().UnixNano())
	items := make([]testPointItem, t.N)
	for i := 0; i < t.N; i++ {
		items[i] = testPointItem{
			fmt.Sprintf("%d", i),
			PO(rand.Float64()*360-180, rand.Float64()*180-90),
			makeBenchFields(nFields),
		}
	}
	col := New()
	for i := 0; i < t.N; i++ {
		col.Set(object.New(items[i].id, items[i].object, 0, items[i].fields))
	}
	t.ResetTimer()
	for _, i := range rand.Perm(t.N) {
		prev := col.Delete(items[i].id)
		if prev.Geo() != items[i].object {
			t.Fatal("shoot!")
		}
	}
}

func BenchmarkScan_Fields(t *testing.B) {
	benchmarkScan(t, 1)
}

func BenchmarkScan_NoFields(t *testing.B) {
	benchmarkScan(t, 0)
}

func benchmarkScan(t *testing.B, nFields int) {
	rand.Seed(time.Now().UnixNano())
	items := make([]testPointItem, t.N)
	for i := 0; i < t.N; i++ {
		items[i] = testPointItem{
			fmt.Sprintf("%d", i),
			PO(rand.Float64()*360-180, rand.Float64()*180-90),
			makeBenchFields(nFields),
		}
	}
	col := New()
	for i := 0; i < t.N; i++ {
		col.Set(object.New(items[i].id, items[i].object, 0, items[i].fields))
	}
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		var scanIteration int
		col.Scan(true, nil, nil, func(o *object.Object) bool {
			scanIteration++
			return scanIteration <= 500
		})
	}
}
