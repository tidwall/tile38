// Copyright 2018 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package geometry

import (
	"math"
)

const complexRingMinPoints = 16

// Ring ...
type Ring = Series

func newRing(points []Point, opts *IndexOptions) Ring {
	series := makeSeries(points, true, true, opts)
	return &series
}

type ringResult struct {
	hit bool // contains/intersects
	idx int  // edge index
}

func ringContainsPoint(ring Ring, point Point, allowOnEdge bool) ringResult {
	var in bool
	var idx int
	rect := Rect{Point{math.Inf(-1), point.Y}, Point{math.Inf(+1), point.Y}}
	if bs, ok := ring.(*baseSeries); ok {
		in, idx = ringContainsPointBaseSeries(rect, bs, point, allowOnEdge)
	} else {
		in, idx = ringContainsPointGeneric(rect, ring, point, allowOnEdge)
	}
	return ringResult{hit: in, idx: idx}
}

func containsPointSearcher(point Point, allowOnEdge bool, idx *int, in *bool, seg Segment, index int) bool {
	// perform a raycast operation on the segments
	res := seg.Raycast(point)
	if res.On {
		*in = allowOnEdge
		*idx = index
		return false
	}
	if res.In {
		*in = !*in
	}
	return true
}

// NOTE: Although it may seem that ringContainsPointBaseSeries and ringContainsPointGeneric
// are the same, they are not. Because the type of the `ring` argument is known in
// ringContainsPointBaseSeries, the compiler can prove that the closure passed to
// ring.Search does not escape, and hence save us 3 heap allocations and ~6% runtime.

func ringContainsPointBaseSeries(rect Rect, ring *baseSeries, point Point, allowOnEdge bool) (bool, int) {
	var idx = -1
	var in bool

	ring.Search(
		rect,
		func(seg Segment, index int) bool {
			return containsPointSearcher(point, allowOnEdge, &idx, &in, seg, index)
		},
	)
	return in, idx
}

func ringContainsPointGeneric(rect Rect, ring Ring, point Point, allowOnEdge bool) (bool, int) {
	var idx = -1
	var in bool
	ring.Search(
		rect,
		func(seg Segment, index int) bool {
			return containsPointSearcher(point, allowOnEdge, &idx, &in, seg, index)
		},
	)
	return in, idx
}

func ringIntersectsPoint(ring Ring, point Point, allowOnEdge bool) ringResult {
	return ringContainsPoint(ring, point, allowOnEdge)
}

// func segmentsIntersects(seg, other Segment, allowOnEdge bool) bool {
// 	if seg.IntersectsSegment(other) {

// 	}
// 	return false
// }

func ringContainsSegment(ring Ring, seg Segment, allowOnEdge bool) bool {
	// fmt.Printf("%v %v\n", seg.A, seg.B)
	// Test that segment points are contained in the ring.
	resA := ringContainsPoint(ring, seg.A, allowOnEdge)
	if !resA.hit {
		// seg A is not inside ring
		return false
	}
	if seg.B == seg.A {
		return true
	}
	resB := ringContainsPoint(ring, seg.B, allowOnEdge)
	if !resB.hit {
		// seg B is not inside ring
		return false
	}
	if ring.Convex() {
		// ring is convex so the segment must be contained
		return true
	}

	// The ring is concave so it's possible that the segment crosses over the
	// edge of the ring.
	if allowOnEdge {
		// do some logic around seg points that are on the edge of the ring.
		if resA.idx != -1 {
			// seg A is on a ring segment
			if resB.idx != -1 {
				// seg B is on a ring segment
				if resB.idx == resA.idx {
					// case (3)
					// seg A and B share the same ring segment, so it must be
					// on the inside.
					return true
				}
				// case (1)
				// seg A and seg B are on different segments.
				// determine if the space that the seg passes over is inside or
				// outside of the ring. To do so we create a ring from the two
				// ring segments and check if that ring winding order matches
				// the winding order of the ring.
				// -- create a ring

				rSegA := ring.SegmentAt(resA.idx)
				rSegB := ring.SegmentAt(resB.idx)
				if rSegA.A == seg.A || rSegA.B == seg.A ||
					rSegB.A == seg.A || rSegB.B == seg.A ||
					rSegA.A == seg.B || rSegA.B == seg.B ||
					rSegB.A == seg.B || rSegB.B == seg.B {
					return true
				}

				// fix the order of the
				if resB.idx < resA.idx {
					rSegA, rSegB = rSegB, rSegA
				}

				pts := [5]Point{rSegA.A, rSegA.B, rSegB.A, rSegB.B, rSegA.A}
				// -- calc winding order
				var cwc float64
				for i := 0; i < len(pts)-1; i++ {
					a, b := pts[i], pts[i+1]
					cwc += (b.X - a.X) * (b.Y + a.Y)
				}
				clockwise := cwc > 0
				if clockwise != ring.Clockwise() {
					// -- on the outside
					return false
				}
				// the passover space is on the inside of the ring.
				// check if seg intersects any ring segments where A and B are
				// not on.
				var intersects bool
				ring.Search(seg.Rect(), func(seg2 Segment, index int) bool {
					if seg.IntersectsSegment(seg2) {
						if !seg2.Raycast(seg.A).On && !seg2.Raycast(seg.B).On {
							intersects = true
							return false
						}
					}
					return true
				})
				return !intersects
			}
			// case (4)
			// seg A is on a ring segment, but seg B is not.
			// check if seg intersects any ring segments where A is not on.
			var intersects bool
			ring.Search(seg.Rect(), func(seg2 Segment, index int) bool {
				if seg.IntersectsSegment(seg2) {
					if !seg2.Raycast(seg.A).On {
						intersects = true
						return false
					}
				}
				return true
			})
			return !intersects
		} else if resB.idx != -1 {
			// case (2)
			// seg B is on a ring segment, but seg A is not.
			// check if seg intersects any ring segments where B is not on.
			var intersects bool
			ring.Search(seg.Rect(), func(seg2 Segment, index int) bool {
				if seg.IntersectsSegment(seg2) {
					if !seg2.Raycast(seg.B).On {
						intersects = true
						return false
					}
				}
				return true
			})
			return !intersects
		}
		// case (5) (15)
		var intersects bool
		ring.Search(seg.Rect(), func(seg2 Segment, index int) bool {
			if seg.IntersectsSegment(seg2) {
				if !seg.Raycast(seg2.A).On && !seg.Raycast(seg2.B).On {
					intersects = true
					return false
				}
			}
			return true
		})
		return !intersects
	}

	// allowOnEdge is false. (not allow on edge)
	var intersects bool
	ring.Search(seg.Rect(), func(seg2 Segment, index int) bool {
		if seg.IntersectsSegment(seg2) {
			// if seg.Raycast(seg2.A).On || seg.Raycast(seg2.B).On {
			intersects = true
			// 	return false
			// }
			return false
		}
		return true
	})
	return !intersects
}

// ringIntersectsSegment detect if the segment intersects the ring
func ringIntersectsSegment(ring Ring, seg Segment, allowOnEdge bool) bool {
	// Quick check that either point is inside of the ring
	if ringContainsPoint(ring, seg.A, allowOnEdge).hit {
		return true
	}
	if ringContainsPoint(ring, seg.B, allowOnEdge).hit {
		return true
	}
	// Neither point A or B is inside the the ring. It's possible that both
	// are on the outside and are passing over segments. If the segment passes
	// over at least two ring segments then it's intersecting.
	var count int
	var segAOn bool
	var segBOn bool
	ring.Search(seg.Rect(), func(seg2 Segment, index int) bool {
		if seg.IntersectsSegment(seg2) {
			if !allowOnEdge {
				// for segments that are not allowed on the edge, extra care
				// must be taken.
				if !(seg.CollinearPoint(seg2.A) && seg.CollinearPoint(seg2.B)) {
					if !segAOn {
						if seg.A == seg2.A || seg.A == seg2.B {
							segAOn = true
							return true
						}
					}
					if !segBOn {
						if seg.B == seg2.A || seg.B == seg2.B {
							segBOn = true
							return true
						}
					}
					count++
				}
			} else {
				count++
			}
		}
		return count < 2
	})
	return count >= 2
}

func ringContainsRing(ring, other Ring, allowOnEdge bool) bool {
	if ring.Empty() || other.Empty() {
		return false
	}
	if other.NumPoints() >= complexRingMinPoints {
		// inner ring has a lot of points, and is convex, so let just check if
		// the rect ring is fully contained before we do the complicated stuff.
		if ringContainsRing(ring, other.Rect(), allowOnEdge) {
			return true
		}
	}
	// test if the inner rect does not contain the outer rect
	if !ring.Rect().ContainsRect(other.Rect()) {
		// not contained so it's not possible for the outer ring to contain
		// the inner ring
		return false
	}
	if ring.Convex() {
		// outer ring is convex so test that all inner points are inside of
		// the outer ring
		otherNumPoints := other.NumPoints()
		for i := 0; i < otherNumPoints; i++ {
			if !ringContainsPoint(ring, other.PointAt(i), allowOnEdge).hit {
				// point is on the outside the outer ring
				return false
			}
		}
	} else {
		// outer ring is concave so let's make sure that all inner segments are
		// fully contained inside of the outer ring.
		otherNumSegments := other.NumSegments()
		for i := 0; i < otherNumSegments; i++ {
			if !ringContainsSegment(ring, other.SegmentAt(i), allowOnEdge) {
				// fmt.Printf("%v %v\n", ring, other.SegmentAt(i))
				return false
			}
		}
	}
	return true
}

func ringIntersectsRing(ring, other Ring, allowOnEdge bool) bool {
	if ring.Empty() || other.Empty() {
		return false
	}
	// check outer and innter rects intersection first
	if !ring.Rect().IntersectsRect(other.Rect()) {
		return false
	}
	if other.Rect().Area() > ring.Rect().Area() {
		// swap the rings so that the inner ring is smaller than the outer ring
		ring, other = other, ring
	}

	otherNumSegments := other.NumSegments()
	for i := 0; i < otherNumSegments; i++ {
		if ringIntersectsSegment(ring, other.SegmentAt(i), allowOnEdge) {
			return true
		}
	}
	return false
}

func ringContainsLine(ring Ring, line *Line, allowOnEdge bool) bool {
	// shares the same logic
	return ringContainsRing(ring, Ring(&line.baseSeries), allowOnEdge)
}

func ringIntersectsLine(ring Ring, line *Line, allowOnEdge bool) bool {
	if ring.Empty() || line.Empty() {
		return false
	}
	// check outer and innter rects intersection first
	if !ring.Rect().IntersectsRect(line.Rect()) {
		return false
	}
	// check if any points are inside ring
	lineNumPoints := line.NumPoints()
	for i := 0; i < lineNumPoints; i++ {
		if ringContainsPoint(ring, line.PointAt(i), allowOnEdge).hit {
			return true
		}
	}
	lineNumSegments := line.NumSegments()
	for i := 0; i < lineNumSegments; i++ {
		if ringIntersectsSegment(ring, line.SegmentAt(i), allowOnEdge) {
			return true
		}
	}
	return false
}
