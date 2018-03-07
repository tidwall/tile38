package rtreebase

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type Rect struct {
	min, max [D]float64
	item     interface{}
}

func ptrMakePoint(vals ...float64) *Rect {
	var r Rect
	for i := 0; i < D && i < len(vals); i++ {
		r.min[i] = vals[i]
		r.max[i] = vals[i]
	}
	r.item = &r
	return &r
}

func ptrMakeRect(vals ...float64) *Rect {
	var r Rect
	for i := 0; i < D && i < len(vals); i++ {
		r.min[i] = vals[i]
		r.max[i] = vals[i+D]
	}
	r.item = &r
	return &r
}

func TestRTree(t *testing.T) {
	tr := New()
	p := ptrMakePoint(10, 10)
	tr.Insert(p.min, p.max, p.item)
}

func TestPtrBasic2D(t *testing.T) {
	if D != 2 {
		return
	}
	tr := New()
	p1 := ptrMakePoint(-115, 33)
	p2 := ptrMakePoint(-113, 35)
	tr.Insert(p1.min, p1.max, p1.item)
	tr.Insert(p2.min, p2.max, p2.item)
	assert.Equal(t, 2, tr.Count())

	var points []*Rect
	bbox := ptrMakeRect(-116, 32, -114, 34)
	tr.Search(bbox.min, bbox.max, func(item interface{}) bool {
		points = append(points, item.(*Rect))
		return true
	})
	assert.Equal(t, 1, len(points))
	tr.Remove(p1.min, p1.max, p1.item)
	assert.Equal(t, 1, tr.Count())

	points = nil
	bbox = ptrMakeRect(-116, 33, -114, 34)
	tr.Search(bbox.min, bbox.max, func(item interface{}) bool {
		points = append(points, item.(*Rect))
		return true
	})
	assert.Equal(t, 0, len(points))
	tr.Remove(p2.min, p2.max, p2.item)
	assert.Equal(t, 0, tr.Count())
}

func getMemStats() runtime.MemStats {
	runtime.GC()
	time.Sleep(time.Millisecond)
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms
}

func ptrMakeRandom(what string) *Rect {
	if what == "point" {
		vals := make([]float64, D)
		for i := 0; i < D; i++ {
			if i == 0 {
				vals[i] = rand.Float64()*360 - 180
			} else if i == 1 {
				vals[i] = rand.Float64()*180 - 90
			} else {
				vals[i] = rand.Float64()*100 - 50
			}
		}
		return ptrMakePoint(vals...)
	} else if what == "rect" {
		vals := make([]float64, D)
		for i := 0; i < D; i++ {
			if i == 0 {
				vals[i] = rand.Float64()*340 - 170
			} else if i == 1 {
				vals[i] = rand.Float64()*160 - 80
			} else {
				vals[i] = rand.Float64()*80 - 30
			}
		}
		rvals := make([]float64, D*2)
		for i := 0; i < D; i++ {
			rvals[i] = vals[i] - rand.Float64()*10
			rvals[D+i] = vals[i] + rand.Float64()*10
		}
		return ptrMakeRect(rvals...)
	}
	panic("??")
}

func TestPtrRandom(t *testing.T) {
	t.Run(fmt.Sprintf("%dD", D), func(t *testing.T) {
		t.Run("point", func(t *testing.T) { ptrTestRandom(t, "point", 10000) })
		t.Run("rect", func(t *testing.T) { ptrTestRandom(t, "rect", 10000) })
	})
}

func ptrTestRandom(t *testing.T, which string, n int) {
	fmt.Println("-------------------------------------------------")
	fmt.Printf("Testing Random %dD %ss\n", D, which)
	fmt.Println("-------------------------------------------------")
	rand.Seed(time.Now().UnixNano())
	tr := New()
	min, max := tr.Bounds()
	assert.Equal(t, make([]float64, D), min[:])
	assert.Equal(t, make([]float64, D), max[:])

	// create random objects
	m1 := getMemStats()
	objs := make([]*Rect, n)
	for i := 0; i < n; i++ {
		objs[i] = ptrMakeRandom(which)
	}

	// insert the objects into tree
	m2 := getMemStats()
	start := time.Now()
	for _, r := range objs {
		tr.Insert(r.min, r.max, r.item)
	}
	durInsert := time.Since(start)
	m3 := getMemStats()
	assert.Equal(t, len(objs), tr.Count())
	fmt.Printf("Inserted %d random %ss in %dms -- %d ops/sec\n",
		len(objs), which, int(durInsert.Seconds()*1000),
		int(float64(len(objs))/durInsert.Seconds()))
	fmt.Printf("  total cost is %d bytes/%s\n", int(m3.HeapAlloc-m1.HeapAlloc)/len(objs), which)
	fmt.Printf("  tree cost is %d bytes/%s\n", int(m3.HeapAlloc-m2.HeapAlloc)/len(objs), which)
	fmt.Printf("  tree overhead %d%%\n", int((float64(m3.HeapAlloc-m2.HeapAlloc)/float64(len(objs)))/(float64(m3.HeapAlloc-m1.HeapAlloc)/float64(len(objs)))*100))
	fmt.Printf("  complexity %f\n", tr.Complexity())

	start = time.Now()
	// count all nodes and leaves
	var nodes int
	var leaves int
	var maxLevel int
	tr.Traverse(func(min, max [D]float64, level int, item interface{}) bool {
		if level != 0 {
			nodes++
		}
		if level == 1 {
			leaves++
		}
		if level > maxLevel {
			maxLevel = level
		}
		return true
	})
	fmt.Printf("  nodes: %d, leaves: %d, level: %d\n", nodes, leaves, maxLevel)

	// verify mbr
	for i := 0; i < D; i++ {
		min[i] = math.Inf(+1)
		max[i] = math.Inf(-1)
	}
	for _, o := range objs {
		for i := 0; i < D; i++ {
			if o.min[i] < min[i] {
				min[i] = o.min[i]
			}
			if o.max[i] > max[i] {
				max[i] = o.max[i]
			}
		}
	}
	minb, maxb := tr.Bounds()
	assert.Equal(t, min, minb)
	assert.Equal(t, max, maxb)

	// scan
	var arr []*Rect
	tr.Scan(func(item interface{}) bool {
		arr = append(arr, item.(*Rect))
		return true
	})
	assert.True(t, ptrTestHasSameItems(objs, arr))

	// search
	ptrTestSearch(t, tr, objs, 0.10, true)
	ptrTestSearch(t, tr, objs, 0.50, true)
	ptrTestSearch(t, tr, objs, 1.00, true)

	// knn
	ptrTestKNN(t, tr, objs, int(float64(len(objs))*0.01), true)
	ptrTestKNN(t, tr, objs, int(float64(len(objs))*0.50), true)
	ptrTestKNN(t, tr, objs, int(float64(len(objs))*1.00), true)

	// remove all objects
	indexes := rand.Perm(len(objs))
	start = time.Now()
	for _, i := range indexes {
		tr.Remove(objs[i].min, objs[i].max, objs[i].item)
	}
	durRemove := time.Since(start)
	assert.Equal(t, 0, tr.Count())
	fmt.Printf("Removed %d random %ss in %dms -- %d ops/sec\n",
		len(objs), which, int(durRemove.Seconds()*1000),
		int(float64(len(objs))/durRemove.Seconds()))

	min, max = tr.Bounds()
	assert.Equal(t, make([]float64, D), min[:])
	assert.Equal(t, make([]float64, D), max[:])
}

func ptrTestHasSameItems(a1, a2 []*Rect) bool {
	if len(a1) != len(a2) {
		return false
	}
	for _, p1 := range a1 {
		var found bool
		for _, p2 := range a2 {
			if *p1 == *p2 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func ptrTestSearch(t *testing.T, tr *RTree, objs []*Rect, percent float64, check bool) {
	var found int
	var start time.Time
	var stop time.Time
	defer func() {
		dur := stop.Sub(start)
		fmt.Printf("Searched %.0f%% (%d/%d items) in %dms -- %d ops/sec\n",
			percent*100, found, len(objs), int(dur.Seconds()*1000),
			int(float64(1)/dur.Seconds()),
		)
	}()
	min, max := tr.Bounds()
	vals := make([]float64, D*2)
	for i := 0; i < D; i++ {
		vals[i] = (max[i]+min[i])/2 - ((max[i]-min[i])*percent)/2
		vals[D+i] = (max[i]+min[i])/2 + ((max[i]-min[i])*percent)/2
	}
	var arr1 []*Rect
	var box *Rect
	if percent == 1 {
		box = ptrMakeRect(append(append([]float64{}, min[:]...), max[:]...)...)
	} else {
		box = ptrMakeRect(vals...)
	}
	start = time.Now()
	tr.Search(box.min, box.max, func(item interface{}) bool {
		if check {
			arr1 = append(arr1, item.(*Rect))
		}
		found++
		return true
	})
	stop = time.Now()
	if !check {
		return
	}
	var arr2 []*Rect
	for _, obj := range objs {
		if ptrTestIntersects(obj, box) {
			arr2 = append(arr2, obj)
		}
	}
	assert.Equal(t, len(arr1), len(arr2))
	for _, o1 := range arr1 {
		var found bool
		for _, o2 := range arr2 {
			if *o2 == *o1 {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("not found")
		}
	}
}

func ptrTestKNN(t *testing.T, tr *RTree, objs []*Rect, n int, check bool) {
	var start time.Time
	var stop time.Time
	defer func() {
		dur := stop.Sub(start)
		fmt.Printf("KNN %d items in %dms -- %d ops/sec\n",
			n, int(dur.Seconds()*1000),
			int(float64(1)/dur.Seconds()),
		)
	}()
	min, max := tr.Bounds()
	pvals := make([]float64, D)
	for i := 0; i < D; i++ {
		pvals[i] = (max[i] + min[i]) / 2
	}
	point := ptrMakePoint(pvals...)

	// gather the results, make sure that is matches exactly
	var arr1 []Rect
	var dists1 []float64
	pdist := math.Inf(-1)
	start = time.Now()
	tr.KNN(point.min, point.max, false, func(item interface{}, dist float64) bool {
		if len(arr1) == n {
			return false
		}
		arr1 = append(arr1, Rect{min: min, max: max, item: item})
		dists1 = append(dists1, dist)
		if dist < pdist {
			panic("dist out of order")
		}
		pdist = dist
		return true
	})
	stop = time.Now()
	assert.True(t, n > len(objs) || n == len(arr1))

	// get the KNN for the original array
	nobjs := make([]*Rect, len(objs))
	copy(nobjs, objs)
	sort.Slice(nobjs, func(i, j int) bool {
		idist := ptrTestBoxDist(pvals, nobjs[i].min, nobjs[i].max)
		jdist := ptrTestBoxDist(pvals, nobjs[j].min, nobjs[j].max)
		return idist < jdist
	})
	arr2 := nobjs[:len(arr1)]
	var dists2 []float64
	for i := 0; i < len(arr2); i++ {
		dist := ptrTestBoxDist(pvals, arr2[i].min, arr2[i].max)
		dists2 = append(dists2, dist)
	}
	// only compare the distances, not the objects because rectangles with
	// a dist of zero will not be ordered.
	assert.Equal(t, dists1, dists2)

}

func ptrTestBoxDist(point []float64, min, max [D]float64) float64 {
	var dist float64
	for i := 0; i < len(point); i++ {
		d := ptrTestAxisDist(point[i], min[i], max[i])
		dist += d * d
	}
	return dist
}
func ptrTestAxisDist(k, min, max float64) float64 {
	if k < min {
		return min - k
	}
	if k <= max {
		return 0
	}
	return k - max
}
func ptrTestIntersects(obj, box *Rect) bool {
	for i := 0; i < D; i++ {
		if box.min[i] > obj.max[i] || box.max[i] < obj.min[i] {
			return false
		}
	}
	return true
}

// func TestPtrInsertFlatPNG2D(t *testing.T) {
// 	fmt.Println("-------------------------------------------------")
// 	fmt.Println("Generating Cities PNG 2D (flat-insert-2d.png)")
// 	fmt.Println("-------------------------------------------------")
// 	tr := New()
// 	var items []*Rect
// 	c := cities.Cities
// 	for i := 0; i < len(c); i++ {
// 		x := c[i].Longitude
// 		y := c[i].Latitude
// 		items = append(items, ptrMakePoint(x, y))
// 	}
// 	start := time.Now()
// 	for _, item := range items {
// 		tr.Insert(item.min, item.max, item.item)
// 	}
// 	dur := time.Since(start)
// 	fmt.Printf("wrote %d cities (flat) in %s (%.0f/ops)\n", len(c), dur, float64(len(c))/dur.Seconds())
// 	withGIF := os.Getenv("GIFOUTPUT") != ""
// 	if err := tr.SavePNG("ptr-flat-insert-2d.png", 1000, 1000, 1.25/360.0, 0, true, withGIF, os.Stdout); err != nil {
// 		t.Fatal(err)
// 	}
// 	if !withGIF {
// 		fmt.Println("use GIFOUTPUT=1 for animated gif")
// 	}
// }

// func TestPtrLoadFlatPNG2D(t *testing.T) {
// 	fmt.Println("-------------------------------------------------")
// 	fmt.Println("Generating Cities 2D PNG (flat-load-2d.png)")
// 	fmt.Println("-------------------------------------------------")
// 	tr := New()
// 	var items []*Rect
// 	c := cities.Cities
// 	for i := 0; i < len(c); i++ {
// 		x := c[i].Longitude
// 		y := c[i].Latitude
// 		items = append(items, ptrMakePoint(x, y))
// 	}

// 	var mins [][D]float64
// 	var maxs [][D]float64
// 	var ifs []interface{}
// 	for i := 0; i < len(items); i++ {
// 		mins = append(mins, items[i].min)
// 		maxs = append(maxs, items[i].max)
// 		ifs = append(ifs, items[i].item)
// 	}

// 	start := time.Now()
// 	tr.Load(mins, maxs, ifs)
// 	dur := time.Since(start)

// 	if true {
// 		var all []*Rect
// 		tr.Scan(func(min, max [D]float64, item interface{}) bool {
// 			all = append(all, &Rect{min: min, max: max, item: item})
// 			return true
// 		})
// 		assert.Equal(t, len(all), len(items))

// 		for len(all) > 0 {
// 			item := all[0]
// 			var found bool
// 			for _, city := range items {
// 				if *city == *item {
// 					found = true
// 					break
// 				}
// 			}
// 			if !found {
// 				t.Fatal("item not found")
// 			}
// 			all = all[1:]
// 		}
// 	}
// 	fmt.Printf("wrote %d cities (flat) in %s (%.0f/ops)\n", len(c), dur, float64(len(c))/dur.Seconds())
// 	withGIF := os.Getenv("GIFOUTPUT") != ""
// 	if err := tr.SavePNG("ptr-flat-load-2d.png", 1000, 1000, 1.25/360.0, 0, true, withGIF, os.Stdout); err != nil {
// 		t.Fatal(err)
// 	}
// 	if !withGIF {
// 		fmt.Println("use GIFOUTPUT=1 for animated gif")
// 	}
// }

func TestBenchmarks(t *testing.T) {
	var points []*Rect
	for i := 0; i < 2000000; i++ {
		x := rand.Float64()*360 - 180
		y := rand.Float64()*180 - 90
		points = append(points, ptrMakePoint(x, y))
	}
	tr := New()
	start := time.Now()
	for i := len(points) / 2; i < len(points); i++ {
		tr.Insert(points[i].min, points[i].max, points[i].item)
	}
	dur := time.Since(start)
	log.Printf("insert 1M items one by one: %.3fs", dur.Seconds())
	////
	rarr := rand.Perm(len(points) / 2)
	start = time.Now()
	for i := 0; i < len(points)/2; i++ {
		a := points[rarr[i]+len(points)/2]
		b := points[rarr[i]]
		tr.Remove(a.min, a.max, a.item)
		tr.Insert(b.min, b.max, b.item)
	}
	dur = time.Since(start)
	log.Printf("replaced 1M items one by one: %.3fs", dur.Seconds())
	points = points[:len(points)/2]
	////
	start = time.Now()
	for i := 0; i < 1000; i++ {
		tr.Remove(points[i].min, points[i].max, points[i].item)
	}
	dur = time.Since(start)
	log.Printf("remove 100 items one by one: %.3fs", dur.Seconds())
	////
	bbox := ptrMakeRect(0, 0, 0+(360*0.0001), 0+(180*0.0001))
	start = time.Now()
	for i := 0; i < 1000; i++ {
		tr.Search(bbox.min, bbox.max, func(_ interface{}) bool { return true })
	}
	dur = time.Since(start)
	log.Printf("1000 searches of 0.01%% area: %.3fs", dur.Seconds())
	////
	bbox = ptrMakeRect(0, 0, 0+(360*0.01), 0+(180*0.01))
	start = time.Now()
	for i := 0; i < 1000; i++ {
		tr.Search(bbox.min, bbox.max, func(_ interface{}) bool { return true })
	}
	dur = time.Since(start)
	log.Printf("1000 searches of 1%% area: %.3fs", dur.Seconds())
	////
	bbox = ptrMakeRect(0, 0, 0+(360*0.10), 0+(180*0.10))
	start = time.Now()
	for i := 0; i < 1000; i++ {
		tr.Search(bbox.min, bbox.max, func(_ interface{}) bool { return true })
	}
	dur = time.Since(start)
	log.Printf("1000 searches of 10%% area: %.3fs", dur.Seconds())
	///

	var mins [][D]float64
	var maxs [][D]float64
	var items []interface{}
	for i := 0; i < len(points); i++ {
		mins = append(mins, points[i].min)
		maxs = append(maxs, points[i].max)
		items = append(items, points[i].item)
	}

	tr = New()
	start = time.Now()
	tr.Load(mins, maxs, items)
	dur = time.Since(start)
	log.Printf("bulk-insert 1M items: %.3fs", dur.Seconds())
}
