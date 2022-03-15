package rbang

import (
	"fmt"

	"github.com/tidwall/geoindex/child"
)

const (
	defaultMaxEntries   = 32
	DefaultSplitEntries = defaultMaxEntries * 60 / 100
	DefaultJoinEntries  = defaultMaxEntries * 40 / 100
)

type rect struct {
	min, max [2]float64
	data     interface{}
}

type node struct {
	count int
	rects [defaultMaxEntries + 1]rect
}

// RTree ...
type RTree struct {
	height   int
	root     rect
	count    int
	reinsert []rect

	splitEntries int
	joinEntries  int

	stats        RTreeStats
	statsEnabled bool
}

func (r *rect) expand(b *rect) {
	if b.min[0] < r.min[0] {
		r.min[0] = b.min[0]
	}
	if b.max[0] > r.max[0] {
		r.max[0] = b.max[0]
	}
	if b.min[1] < r.min[1] {
		r.min[1] = b.min[1]
	}
	if b.max[1] > r.max[1] {
		r.max[1] = b.max[1]
	}
}

func (r *rect) area() float64 {
	return (r.max[0] - r.min[0]) * (r.max[1] - r.min[1])
}

func (r *rect) overlapArea(b *rect) float64 {
	area := 1.0
	var max, min float64
	if r.max[0] < b.max[0] {
		max = r.max[0]
	} else {
		max = b.max[0]
	}
	if r.min[0] > b.min[0] {
		min = r.min[0]
	} else {
		min = b.min[0]
	}
	if max > min {
		area *= max - min
	} else {
		return 0
	}
	if r.max[1] < b.max[1] {
		max = r.max[1]
	} else {
		max = b.max[1]
	}
	if r.min[1] > b.min[1] {
		min = r.min[1]
	} else {
		min = b.min[1]
	}
	if max > min {
		area *= max - min
	} else {
		return 0
	}
	return area
}

func (r *rect) enlargedArea(b *rect) float64 {
	area := 1.0
	if b.max[0] > r.max[0] {
		if b.min[0] < r.min[0] {
			area *= b.max[0] - b.min[0]
		} else {
			area *= b.max[0] - r.min[0]
		}
	} else {
		if b.min[0] < r.min[0] {
			area *= r.max[0] - b.min[0]
		} else {
			area *= r.max[0] - r.min[0]
		}
	}
	if b.max[1] > r.max[1] {
		if b.min[1] < r.min[1] {
			area *= b.max[1] - b.min[1]
		} else {
			area *= b.max[1] - r.min[1]
		}
	} else {
		if b.min[1] < r.min[1] {
			area *= r.max[1] - b.min[1]
		} else {
			area *= r.max[1] - r.min[1]
		}
	}
	return area
}

func (tr *RTree) Stats() *RTreeStats {
	return &tr.stats
}

func (tr *RTree) SetStatsEnabled(value bool) {
	tr.statsEnabled = value
}

func (tr *RTree) GetSplitEntries() int {
	if tr.splitEntries == 0 {
		return defaultMaxEntries
	}

	return tr.splitEntries
}

func (tr *RTree) SetSplitEntries(value int) error {
	if value < tr.joinEntries || value > defaultMaxEntries {
		return fmt.Errorf("split entries value must be between %d and %d", tr.joinEntries, defaultMaxEntries)
	}

	tr.splitEntries = value
	return nil
}

func (tr *RTree) GetJoinEntries() int {
	if tr.joinEntries == 0 {
		return DefaultJoinEntries
	}

	return tr.joinEntries
}

func (tr *RTree) SetJoinEntries(value int) error {
	if value < 0 || value > tr.GetJoinEntries() {
		return fmt.Errorf("join entries value must be between %d and %d", 0, tr.GetJoinEntries())
	}

	tr.joinEntries = value
	return nil
}

// Insert inserts an item into the RTree
func (tr *RTree) Insert(min, max [2]float64, value interface{}) {
	var item rect

	fit(min, max, value, &item)
	tr.insert(&item)

	if tr.statsEnabled {
		tr.stats.Height.SetCount(uint64(tr.height))
		tr.stats.SplitEntries.SetCount(uint64(tr.GetSplitEntries()))
		tr.stats.JoinEntries.SetCount(uint64(tr.GetJoinEntries()))
	}
}

func (tr *RTree) insert(item *rect) {
	if tr.root.data == nil {
		fit(item.min, item.max, new(node), &tr.root)
	}

	// if the splitEntries specified, only fill tree nodes up to the specified capacity
	splitEntries := tr.GetSplitEntries()

	stats := &tr.stats
	if !tr.statsEnabled {
		stats = nil
	}

	grown := tr.root.insert(item, tr.height, splitEntries, stats)
	if grown {
		tr.root.expand(item)
	}

	if tr.root.data.(*node).count == splitEntries+1 {
		newRoot := new(node)
		tr.root.splitLargestAxisEdgeSnap(&newRoot.rects[1])
		newRoot.rects[0] = tr.root
		newRoot.count = 2
		tr.root.data = newRoot
		tr.root.recalc()
		tr.height++

		if stats != nil {
			tr.stats.Split.IncCount(1)
		}
	}
	tr.count++
}

func (r *rect) chooseLeastEnlargement(b *rect) int {
	j, jenlargement, jarea := -1, 0.0, 0.0
	n := r.data.(*node)
	for i := 0; i < n.count; i++ {
		area := n.rects[i].area()
		enlargement := n.rects[i].enlargedArea(b) - area
		if j == -1 || enlargement < jenlargement {
			j, jenlargement, jarea = i, enlargement, area
		} else if enlargement == jenlargement {
			if area < jarea {
				j, jenlargement, jarea = i, enlargement, area
			}
		}
	}
	return j
}

func (r *rect) recalc() {
	n := r.data.(*node)
	r.min = n.rects[0].min
	r.max = n.rects[0].max
	for i := 1; i < n.count; i++ {
		r.expand(&n.rects[i])
	}
}

// contains return struct when b is fully contained inside of n
func (r *rect) contains(b *rect) bool {
	if b.min[0] < r.min[0] || b.max[0] > r.max[0] {
		return false
	}
	if b.min[1] < r.min[1] || b.max[1] > r.max[1] {
		return false
	}
	return true
}

func (r *rect) largestAxis() (axis int, size float64) {
	if r.max[1]-r.min[1] > r.max[0]-r.min[0] {
		return 1, r.max[1] - r.min[1]
	}
	return 0, r.max[0] - r.min[0]
}

func (r *rect) splitLargestAxisEdgeSnap(right *rect) {
	axis, _ := r.largestAxis()
	left := r
	leftNode := left.data.(*node)
	rightNode := new(node)
	right.data = rightNode

	var equals []rect
	for i := 0; i < leftNode.count; i++ {
		minDist := leftNode.rects[i].min[axis] - left.min[axis]
		maxDist := left.max[axis] - leftNode.rects[i].max[axis]
		if minDist < maxDist {
			// stay left
		} else {
			if minDist > maxDist {
				// move to right
				rightNode.rects[rightNode.count] = leftNode.rects[i]
				rightNode.count++
			} else {
				// move to equals, at the end of the left array
				equals = append(equals, leftNode.rects[i])
			}
			leftNode.rects[i] = leftNode.rects[leftNode.count-1]
			leftNode.rects[leftNode.count-1].data = nil
			leftNode.count--
			i--
		}
	}
	for _, b := range equals {
		if leftNode.count < rightNode.count {
			leftNode.rects[leftNode.count] = b
			leftNode.count++
		} else {
			rightNode.rects[rightNode.count] = b
			rightNode.count++
		}
	}
	left.recalc()
	right.recalc()
}

func (r *rect) insert(item *rect, height, splitTrigger int, stats *RTreeStats) (grown bool) {
	n := r.data.(*node)
	if height == 0 {
		n.rects[n.count] = *item
		n.count++
		grown = !r.contains(item)
		return grown
	}
	// choose subtree
	index := r.chooseLeastEnlargement(item)
	child := &n.rects[index]
	grown = child.insert(item, height-1, splitTrigger, stats)
	if grown {
		child.expand(item)
		grown = !r.contains(item)
	}
	if child.data.(*node).count == splitTrigger+1 {
		child.splitLargestAxisEdgeSnap(&n.rects[n.count])
		n.count++

		if stats != nil {
			stats.Split.IncCount(1)
		}
	}
	return grown
}

// fit an external item into a rect type
func fit(min, max [2]float64, value interface{}, target *rect) {
	target.min = min
	target.max = max
	target.data = value
}

// contains return struct when b is fully contained inside of n
func (r *rect) intersects(b *rect) bool {
	if b.min[0] > r.max[0] || b.max[0] < r.min[0] {
		return false
	}
	if b.min[1] > r.max[1] || b.max[1] < r.min[1] {
		return false
	}
	return true
}

func (r *rect) search(
	target *rect, height int,
	iter func(min, max [2]float64, value interface{}) bool,
) bool {
	n := r.data.(*node)
	if height == 0 {
		for i := 0; i < n.count; i++ {
			if target.intersects(&n.rects[i]) {
				if !iter(n.rects[i].min, n.rects[i].max,
					n.rects[i].data) {
					return false
				}
			}
		}
	} else if height == 1 {
		for i := 0; i < n.count; i++ {
			if target.intersects(&n.rects[i]) {
				cn := n.rects[i].data.(*node)
				for i := 0; i < cn.count; i++ {
					if target.intersects(&cn.rects[i]) {
						if !iter(cn.rects[i].min, cn.rects[i].max,
							cn.rects[i].data) {
							return false
						}
					}
				}
			}
		}
	} else {
		for i := 0; i < n.count; i++ {
			if target.intersects(&n.rects[i]) {
				if !n.rects[i].search(target, height-1, iter) {
					return false
				}
			}
		}
	}
	return true
}

func (tr *RTree) search(
	target *rect,
	iter func(min, max [2]float64, value interface{}) bool,
) {
	if tr.root.data == nil {
		return
	}
	if target.intersects(&tr.root) {
		tr.root.search(target, tr.height, iter)
	}
}

// Search ...
func (tr *RTree) Search(
	min, max [2]float64,
	iter func(min, max [2]float64, value interface{}) bool,
) {
	var target rect
	fit(min, max, nil, &target)
	tr.search(&target, iter)
}

func (r *rect) scan(
	height int,
	iter func(min, max [2]float64, value interface{}) bool,
) bool {
	n := r.data.(*node)
	if height == 0 {
		for i := 0; i < n.count; i++ {
			if !iter(n.rects[i].min, n.rects[i].max, n.rects[i].data) {
				return false
			}
		}
	} else if height == 1 {
		for i := 0; i < n.count; i++ {
			cn := n.rects[i].data.(*node)
			for j := 0; j < cn.count; j++ {
				if !iter(cn.rects[i].min, cn.rects[j].max, cn.rects[j].data) {
					return false
				}
			}
		}
	} else {
		for i := 0; i < n.count; i++ {
			if !n.rects[i].scan(height-1, iter) {
				return false
			}
		}
	}
	return true
}

// Scan iterates through all data in tree.
func (tr *RTree) Scan(iter func(min, max [2]float64, data interface{}) bool) {
	if tr.root.data == nil {
		return
	}
	tr.root.scan(tr.height, iter)
}

// Delete data from tree
func (tr *RTree) Delete(min, max [2]float64, data interface{}) {
	var item rect
	fit(min, max, data, &item)
	if tr.root.data == nil || !tr.root.contains(&item) {
		return
	}

	stats := &tr.stats
	if !tr.statsEnabled {
		stats = nil
	}

	var removed, recalced bool
	removed, recalced, tr.reinsert =
		tr.root.delete(&item, tr.height, tr.GetJoinEntries(), tr.reinsert[:0], stats)
	if !removed {
		return
	}
	tr.count -= len(tr.reinsert) + 1
	if tr.count == 0 {
		tr.root = rect{}
		recalced = false
	} else {
		for tr.height > 0 && tr.root.data.(*node).count == 1 {
			tr.root = tr.root.data.(*node).rects[0]
			tr.height--
			tr.root.recalc()
		}
	}
	if recalced {
		tr.root.recalc()
	}
	for i := range tr.reinsert {
		tr.insert(&tr.reinsert[i])
		tr.reinsert[i].data = nil
	}
}

func (r *rect) delete(item *rect, height int, joinTrigger int, reinsert []rect, stats *RTreeStats) (
	removed, recalced bool, reinsertOut []rect,
) {
	n := r.data.(*node)
	if height == 0 {
		for i := 0; i < n.count; i++ {
			if n.rects[i].data == item.data {
				// found the target item to delete
				recalced = r.onEdge(&n.rects[i])
				n.rects[i] = n.rects[n.count-1]
				n.rects[n.count-1].data = nil
				n.count--
				if recalced {
					r.recalc()
				}
				return true, recalced, reinsert
			}
		}
	} else {
		for i := 0; i < n.count; i++ {
			if !n.rects[i].contains(item) {
				continue
			}
			removed, recalced, reinsert =
				n.rects[i].delete(item, height-1, joinTrigger, reinsert, stats)
			if !removed {
				continue
			}
			if n.rects[i].data.(*node).count < joinTrigger {
				// underflow
				if !recalced {
					recalced = r.onEdge(&n.rects[i])
				}
				reinsert = n.rects[i].flatten(reinsert, height-1)
				n.rects[i] = n.rects[n.count-1]
				n.rects[n.count-1].data = nil
				n.count--

				if stats != nil {
					stats.Join.IncCount(1)
				}
			}
			if recalced {
				r.recalc()
			}
			return removed, recalced, reinsert
		}
	}
	return false, false, reinsert
}

// flatten flattens all leaf rects into a single list
func (r *rect) flatten(all []rect, height int) []rect {
	n := r.data.(*node)
	if height == 0 {
		all = append(all, n.rects[:n.count]...)
	} else {
		for i := 0; i < n.count; i++ {
			all = n.rects[i].flatten(all, height-1)
		}
	}
	return all
}

// onedge returns true when b is on the edge of r
func (r *rect) onEdge(b *rect) bool {
	if r.min[0] == b.min[0] || r.max[0] == b.max[0] {
		return true
	}
	if r.min[1] == b.min[1] || r.max[1] == b.max[1] {
		return true
	}
	return false
}

// Len returns the number of items in tree
func (tr *RTree) Len() int {
	return tr.count
}

// Bounds returns the minimum bounding rect
func (tr *RTree) Bounds() (min, max [2]float64) {
	if tr.root.data == nil {
		return
	}
	return tr.root.min, tr.root.max
}

// Children is a utility function that returns all children for parent node.
// If parent node is nil then the root nodes should be returned. The min, max,
// data, and items slices all must have the same lengths. And, each element
// from all slices must be associated. Returns true for `items` when the the
// item at the leaf level. The reuse buffers are empty length slices that can
// optionally be used to avoid extra allocations.
func (tr *RTree) Children(
	parent interface{},
	reuse []child.Child,
) []child.Child {
	children := reuse
	if parent == nil {
		if tr.Len() > 0 {
			// fill with the root
			children = append(children, child.Child{
				Min:  tr.root.min,
				Max:  tr.root.max,
				Data: tr.root.data,
				Item: false,
			})
		}
	} else {
		// fill with child items
		n := parent.(*node)
		item := true
		if n.count > 0 {
			if _, ok := n.rects[0].data.(*node); ok {
				item = false
			}
		}
		for i := 0; i < n.count; i++ {
			children = append(children, child.Child{
				Min:  n.rects[i].min,
				Max:  n.rects[i].max,
				Data: n.rects[i].data,
				Item: item,
			})
		}
	}
	return children
}

// Replace an item in the structure. This is effectively just a Delete
// followed by an Insert.
func (tr *RTree) Replace(
	oldMin, oldMax [2]float64, oldData interface{},
	newMin, newMax [2]float64, newData interface{},
) {
	tr.Delete(oldMin, oldMax, oldData)
	tr.Insert(newMin, newMax, newData)
}
