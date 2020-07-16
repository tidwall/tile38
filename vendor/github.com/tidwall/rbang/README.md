# `rbang`

[![GoDoc](https://godoc.org/github.com/tidwall/rbang?status.svg)](https://godoc.org/github.com/tidwall/rbang)

This package provides an in-memory R-Tree implementation for Go. It's designed
for [Tile38](https://github.com/tidwall/tile38) and is optimized for fast rect 
inserts and replacements.

<img src="cities.png" width="512" height="256" border="0" alt="Cities">

## Usage

### Installing

To start using rbang, install Go and run `go get`:

```sh
$ go get -u github.com/tidwall/rbang
```

### Basic operations

```go
// create a 2D RTree
var tr rbang.RTree

// insert a point
tr.Insert([2]float64{-112.0078, 33.4373}, [2]float64{-112.0078, 33.4373}, "PHX")

// insert a box
tr.Insert([2]float64{10, 10}, [2]float64{20, 20}, "rect")

// search 
tr.Search([2]float64{-112.1, 33.4}, [2]float64{-112.0, 33.5}, 
 	func(min, max [2]float64, value interface{}) bool {
		println(value.(string)) // prints "PHX"
	},
)

// delete 
tr.Delete([2]float64{-112.0078, 33.4373}, [2]float64{-112.0078, 33.4373}, "PHX")
```

## Algorithms

This implementation is a variant of the original paper:  
[R-TREES. A DYNAMIC INDEX STRUCTURE FOR SPATIAL SEARCHING](http://www-db.deis.unibo.it/courses/SI-LS/papers/Gut84.pdf)

### Inserting

Same as the original algorithm. From the root to the leaf, the boxes which will incur the least enlargment are chosen. Ties go to boxes with the smallest area.

### Deleting

Same as the original algorithm. A target box is deleted directly. When the number of children in a box falls below it's minumum entries, it is removed from the tree and it's items are re-inserted.

### Splitting

This is a custom algorithm.
It attempts to minimize intensive operations such as pre-sorting the children and comparing overlaps & area sizes.
The desire is to do simple single axis distance calculations each child only once, with a target 50/50 chance that the child might be moved in-memory.

When a box has reached it's max number of entries it's largest axis is calculated and the box is split into two smaller boxes, named `left` and `right`.
Each child boxes is then evaluated to determine which smaller box it should be placed into.
Two values, `min-dist` and `max-dist`, are calcuated for each child. 

- `min-dist` is the distance from the parent's minumum value of it's largest axis to the child's minumum value of the parent largest axis.
- `max-dist` is the distance from the parent's maximum value of it's largest axis to the child's maximum value of the parent largest axis.

When the `min-dist` is less than `max-dist` then the child is placed into the `left` box. 
When the `max-dist` is less than `min-dist` then the child is placed into the `right` box. 
When the `min-dist` is equal to `max-dist` then the child is placed into an `equal` bucket until all of the children are evaluated.
Each `equal` box is then one-by-one placed in either `left` or `right`, whichever has less children.


## Performance

In my testing:

- Insert show similar performance as the quadratic R-tree and ~1.2x - 1.5x faster than R*tree. 
- Search and Delete is ~1.5x - 2x faster than quadratic and about the same as R*tree.

I hope to provide more details in the future.

## License

`rbang` source code is available under the MIT License.

