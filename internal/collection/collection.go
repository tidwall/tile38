package collection

import (
	"runtime"

	"github.com/tidwall/btree"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/rtree"
	"github.com/tidwall/tile38/internal/deadline"
	"github.com/tidwall/tile38/internal/field"
	"github.com/tidwall/tile38/internal/object"
)

// yieldStep forces the iterator to yield goroutine every 256 steps.
const yieldStep = 256

// Cursor allows for quickly paging through Scan, Within, Intersects, and Nearby
type Cursor interface {
	Offset() uint64
	Step(count uint64)
}

func byID(a, b *object.Object) bool {
	return a.ID() < b.ID()
}

func byValue(a, b *object.Object) bool {
	value1 := a.String()
	value2 := b.String()
	if value1 < value2 {
		return true
	}
	if value1 > value2 {
		return false
	}
	// the values match so we'll compare IDs, which are always unique.
	return byID(a, b)
}

func byExpires(a, b *object.Object) bool {
	if a.Expires() < b.Expires() {
		return true
	}
	if a.Expires() > b.Expires() {
		return false
	}
	// the values match so we'll compare IDs, which are always unique.
	return byID(a, b)
}

// Collection represents a collection of geojson objects.
type Collection struct {
	objs     btree.Map[string, *object.Object]      // sorted by id
	spatial  rtree.RTreeGN[float32, *object.Object] // geospatially indexed
	values   *btree.BTreeG[*object.Object]          // sorted by value+id
	expires  *btree.BTreeG[*object.Object]          // sorted by ex+id
	weight   int
	points   int
	objects  int // geometry count
	nobjects int // non-geometry count
}

var optsNoLock = btree.Options{NoLocks: true}

// New creates an empty collection
func New() *Collection {
	col := &Collection{
		values:  btree.NewBTreeGOptions(byValue, optsNoLock),
		expires: btree.NewBTreeGOptions(byExpires, optsNoLock),
	}
	return col
}

// Count returns the number of objects in collection.
func (c *Collection) Count() int {
	return c.objects + c.nobjects
}

// StringCount returns the number of string values.
func (c *Collection) StringCount() int {
	return c.nobjects
}

// PointCount returns the number of points (lat/lon coordinates) in collection.
func (c *Collection) PointCount() int {
	return c.points
}

// TotalWeight calculates the in-memory cost of the collection in bytes.
func (c *Collection) TotalWeight() int {
	return c.weight
}

// Bounds returns the bounds of all the items in the collection.
func (c *Collection) Bounds() (minX, minY, maxX, maxY float64) {
	_, _, left := c.spatial.LeftMost()
	_, _, bottom := c.spatial.BottomMost()
	_, _, right := c.spatial.RightMost()
	_, _, top := c.spatial.TopMost()
	if left == nil {
		return
	}
	return left.Rect().Min.X, bottom.Rect().Min.Y,
		right.Rect().Max.X, top.Rect().Max.Y
}

func (c *Collection) indexDelete(item *object.Object) {
	if !item.Geo().Empty() {
		c.spatial.Delete(rtreeItem(item))
	}
}

func (c *Collection) indexInsert(item *object.Object) {
	if !item.Geo().Empty() {
		c.spatial.Insert(rtreeItem(item))
	}
}

const dRNDTOWARDS = (1.0 - 1.0/8388608.0) /* Round towards zero */
const dRNDAWAY = (1.0 + 1.0/8388608.0)    /* Round away from zero */

func rtreeValueDown(d float64) float32 {
	f := float32(d)
	if float64(f) > d {
		if d < 0 {
			f = float32(d * dRNDAWAY)
		} else {
			f = float32(d * dRNDTOWARDS)
		}
	}
	return f
}
func rtreeValueUp(d float64) float32 {
	f := float32(d)
	if float64(f) < d {
		if d < 0 {
			f = float32(d * dRNDTOWARDS)
		} else {
			f = float32(d * dRNDAWAY)
		}
	}
	return f
}

func rtreeItem(item *object.Object) (min, max [2]float32, data *object.Object) {
	min, max = rtreeRect(item.Rect())
	return min, max, item
}

func rtreeRect(rect geometry.Rect) (min, max [2]float32) {
	return [2]float32{
			rtreeValueDown(rect.Min.X),
			rtreeValueDown(rect.Min.Y),
		}, [2]float32{
			rtreeValueUp(rect.Max.X),
			rtreeValueUp(rect.Max.Y),
		}
}

// Set adds or replaces an object in the collection and returns the fields
// array.
func (c *Collection) Set(obj *object.Object) (prev *object.Object) {
	prev, _ = c.objs.Set(obj.ID(), obj)
	c.setFill(prev, obj)
	return prev
}

func (c *Collection) setFill(prev, obj *object.Object) {
	if prev != nil {
		if prev.IsSpatial() {
			c.indexDelete(prev)
			c.objects--
		} else {
			c.values.Delete(prev)
			c.nobjects--
		}
		if prev.Expires() != 0 {
			c.expires.Delete(prev)
		}
		c.points -= prev.Geo().NumPoints()
		c.weight -= prev.Weight()
	}
	if obj.IsSpatial() {
		c.indexInsert(obj)
		c.objects++
	} else {
		c.values.Set(obj)
		c.nobjects++
	}
	if obj.Expires() != 0 {
		c.expires.Set(obj)
	}
	c.points += obj.Geo().NumPoints()
	c.weight += obj.Weight()
}

// Delete removes an object and returns it.
// If the object does not exist then the 'ok' return value will be false.
func (c *Collection) Delete(id string) (prev *object.Object) {
	prev, _ = c.objs.Delete(id)
	if prev == nil {
		return nil
	}
	if prev.IsSpatial() {
		if !prev.Geo().Empty() {
			c.indexDelete(prev)
		}
		c.objects--
	} else {
		c.values.Delete(prev)
		c.nobjects--
	}
	if prev.Expires() != 0 {
		c.expires.Delete(prev)
	}
	c.points -= prev.Geo().NumPoints()
	c.weight -= prev.Weight()
	return prev
}

// Get returns an object.
// If the object does not exist then the 'ok' return value will be false.
func (c *Collection) Get(id string) *object.Object {
	obj, _ := c.objs.Get(id)
	return obj
}

// Scan iterates though the collection ids.
func (c *Collection) Scan(
	desc bool,
	cursor Cursor,
	deadline *deadline.Deadline,
	iterator func(obj *object.Object) bool,
) bool {
	var keepon = true
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	iter := func(_ string, obj *object.Object) bool {
		count++
		if count <= offset {
			return true
		}
		nextStep(count, cursor, deadline)
		keepon = iterator(obj)
		return keepon
	}
	if desc {
		c.objs.Reverse(iter)
	} else {
		c.objs.Scan(iter)
	}
	return keepon
}

// ScanRange iterates though the collection starting with specified id.
func (c *Collection) ScanRange(
	start, end string,
	desc bool,
	cursor Cursor,
	deadline *deadline.Deadline,
	iterator func(o *object.Object) bool,
) bool {
	var keepon = true
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	iter := func(_ string, o *object.Object) bool {
		count++
		if count <= offset {
			return true
		}
		nextStep(count, cursor, deadline)
		if !desc {
			if o.ID() >= end {
				return false
			}
		} else {
			if o.ID() <= end {
				return false
			}
		}
		keepon = iterator(o)
		return keepon
	}

	if desc {
		c.objs.Descend(start, iter)
	} else {
		c.objs.Ascend(start, iter)
	}
	return keepon
}

// SearchValues iterates though the collection values.
func (c *Collection) SearchValues(
	desc bool,
	cursor Cursor,
	deadline *deadline.Deadline,
	iterator func(o *object.Object) bool,
) bool {
	var keepon = true
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	iter := func(o *object.Object) bool {
		count++
		if count <= offset {
			return true
		}
		nextStep(count, cursor, deadline)
		keepon = iterator(o)
		return keepon
	}
	if desc {
		c.values.Reverse(iter)
	} else {
		c.values.Scan(iter)
	}
	return keepon
}

// SearchValuesRange iterates though the collection values.
func (c *Collection) SearchValuesRange(start, end string, desc bool,
	cursor Cursor,
	deadline *deadline.Deadline,
	iterator func(o *object.Object) bool,
) bool {
	var keepon = true
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	iter := func(o *object.Object) bool {
		count++
		if count <= offset {
			return true
		}
		nextStep(count, cursor, deadline)
		keepon = iterator(o)
		return keepon
	}

	pstart := object.New("", object.String(start), 0, field.List{})
	pend := object.New("", object.String(end), 0, field.List{})
	if desc {
		// descend range
		c.values.Descend(pstart, func(item *object.Object) bool {
			return bGT(c.values, item, pend) && iter(item)
		})
	} else {
		c.values.Ascend(pstart, func(item *object.Object) bool {
			return bLT(c.values, item, pend) && iter(item)
		})
	}
	return keepon
}

func bLT(tr *btree.BTreeG[*object.Object], a, b *object.Object) bool { return tr.Less(a, b) }
func bGT(tr *btree.BTreeG[*object.Object], a, b *object.Object) bool { return tr.Less(b, a) }

// ScanGreaterOrEqual iterates though the collection starting with specified id.
func (c *Collection) ScanGreaterOrEqual(id string, desc bool,
	cursor Cursor,
	deadline *deadline.Deadline,
	iterator func(o *object.Object) bool,
) bool {
	var keepon = true
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	iter := func(_ string, o *object.Object) bool {
		count++
		if count <= offset {
			return true
		}
		nextStep(count, cursor, deadline)
		keepon = iterator(o)
		return keepon
	}
	if desc {
		c.objs.Descend(id, iter)
	} else {
		c.objs.Ascend(id, iter)
	}
	return keepon
}

func (c *Collection) geoSearch(
	rect geometry.Rect,
	iter func(o *object.Object) bool,
) bool {
	alive := true
	min, max := rtreeRect(rect)
	c.spatial.Search(
		min, max,
		func(_, _ [2]float32, o *object.Object) bool {
			alive = iter(o)
			return alive
		},
	)
	return alive
}

func (c *Collection) geoSparse(
	obj geojson.Object, sparse uint8,
	iter func(o *object.Object) (match, ok bool),
) bool {
	matches := make(map[string]bool)
	alive := true
	c.geoSparseInner(obj.Rect(), sparse, func(o *object.Object) (match, ok bool) {
		ok = true
		if !matches[o.ID()] {
			match, ok = iter(o)
			if match {
				matches[o.ID()] = true
			}
		}
		return match, ok
	})
	return alive
}
func (c *Collection) geoSparseInner(
	rect geometry.Rect, sparse uint8,
	iter func(o *object.Object) (match, ok bool),
) bool {
	if sparse > 0 {
		w := rect.Max.X - rect.Min.X
		h := rect.Max.Y - rect.Min.Y
		quads := [4]geometry.Rect{
			{
				Min: geometry.Point{X: rect.Min.X, Y: rect.Min.Y + h/2},
				Max: geometry.Point{X: rect.Min.X + w/2, Y: rect.Max.Y},
			},
			{
				Min: geometry.Point{X: rect.Min.X + w/2, Y: rect.Min.Y + h/2},
				Max: geometry.Point{X: rect.Max.X, Y: rect.Max.Y},
			},
			{
				Min: geometry.Point{X: rect.Min.X, Y: rect.Min.Y},
				Max: geometry.Point{X: rect.Min.X + w/2, Y: rect.Min.Y + h/2},
			},
			{
				Min: geometry.Point{X: rect.Min.X + w/2, Y: rect.Min.Y},
				Max: geometry.Point{X: rect.Max.X, Y: rect.Min.Y + h/2},
			},
		}
		for _, quad := range quads {
			if !c.geoSparseInner(quad, sparse-1, iter) {
				return false
			}
		}
		return true
	}
	alive := true
	c.geoSearch(rect, func(o *object.Object) bool {
		match, ok := iter(o)
		if !ok {
			alive = false
			return false
		}
		return !match
	})
	return alive
}

// Within returns all object that are fully contained within an object or
// bounding box. Set obj to nil in order to use the bounding box.
func (c *Collection) Within(
	obj geojson.Object,
	sparse uint8,
	cursor Cursor,
	deadline *deadline.Deadline,
	iter func(o *object.Object) bool,
) bool {
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	if sparse > 0 {
		return c.geoSparse(obj, sparse, func(o *object.Object) (match, ok bool) {
			count++
			if count <= offset {
				return false, true
			}
			nextStep(count, cursor, deadline)
			if match = o.Geo().Within(obj); match {
				ok = iter(o)
			}
			return match, ok
		})
	}
	return c.geoSearch(obj.Rect(), func(o *object.Object) bool {
		count++
		if count <= offset {
			return true
		}
		nextStep(count, cursor, deadline)
		if o.Geo().Within(obj) {
			return iter(o)
		}
		return true
	})
}

// Intersects returns all object that are intersect an object or bounding box.
// Set obj to nil in order to use the bounding box.
func (c *Collection) Intersects(
	gobj geojson.Object,
	sparse uint8,
	cursor Cursor,
	deadline *deadline.Deadline,
	iter func(o *object.Object) bool,
) bool {
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	if sparse > 0 {
		return c.geoSparse(gobj, sparse, func(o *object.Object) (match, ok bool) {
			count++
			if count <= offset {
				return false, true
			}
			nextStep(count, cursor, deadline)
			if match = o.Geo().Intersects(gobj); match {
				ok = iter(o)
			}
			return match, ok
		})
	}
	return c.geoSearch(gobj.Rect(), func(o *object.Object) bool {
		count++
		if count <= offset {
			return true
		}
		nextStep(count, cursor, deadline)
		if o.Geo().Intersects(gobj) {
			return iter(o)
		}
		return true
	},
	)
}

// Nearby returns the nearest neighbors
func (c *Collection) Nearby(
	target geojson.Object,
	cursor Cursor,
	deadline *deadline.Deadline,
	iter func(o *object.Object, dist float64) bool,
) bool {
	alive := true
	center := target.Center()
	var count uint64
	var offset uint64
	if cursor != nil {
		offset = cursor.Offset()
		cursor.Step(offset)
	}
	distFn := geodeticDistAlgo([2]float64{center.X, center.Y})
	c.spatial.Nearby(
		func(min, max [2]float32, data *object.Object, item bool) float64 {
			return distFn(
				[2]float64{float64(min[0]), float64(min[1])},
				[2]float64{float64(max[0]), float64(max[1])},
				data, item,
			)
		},
		func(_, _ [2]float32, o *object.Object, dist float64) bool {
			count++
			if count <= offset {
				return true
			}
			nextStep(count, cursor, deadline)
			alive = iter(o, dist)
			return alive
		},
	)
	return alive
}

func nextStep(step uint64, cursor Cursor, deadline *deadline.Deadline) {
	if step&(yieldStep-1) == (yieldStep - 1) {
		runtime.Gosched()
		deadline.Check()
	}
	if cursor != nil {
		cursor.Step(1)
	}
}

// ScanExpires returns a list of all objects that have expired.
func (c *Collection) ScanExpires(iter func(o *object.Object) bool) {
	c.expires.Scan(iter)
}
