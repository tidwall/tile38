// Copyright 2014 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package btree

import (
	"flag"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func init() {
	seed := time.Now().Unix()
	fmt.Println(seed)
	rand.Seed(seed)
}

// perm returns a random permutation of n Int items in the range [0, n).
func perm(n int) (out []Item) {
	for _, v := range rand.Perm(n) {
		out = append(out, Int(v))
	}
	return
}

// rang returns an ordered list of Int items in the range [0, n).
func rang(n int) (out []Item) {
	for i := 0; i < n; i++ {
		out = append(out, Int(i))
	}
	return
}

// all extracts all items from a tree in order as a slice.
func all(t *BTree) (out []Item) {
	t.Ascend(func(a Item) bool {
		out = append(out, a)
		return true
	})
	return
}

// rangerev returns a reversed ordered list of Int items in the range [0, n).
func rangrev(n int) (out []Item) {
	for i := n - 1; i >= 0; i-- {
		out = append(out, Int(i))
	}
	return
}

// allrev extracts all items from a tree in reverse order as a slice.
func allrev(t *BTree) (out []Item) {
	t.Descend(func(a Item) bool {
		out = append(out, a)
		return true
	})
	return
}

var btreeDegree = flag.Int("degree", 32, "B-Tree degree")

func TestBTree(t *testing.T) {
	tr := New(*btreeDegree, nil)
	const treeSize = 10000
	for i := 0; i < 10; i++ {
		if min := tr.Min(); min != nil {
			t.Fatalf("empty min, got %+v", min)
		}
		if max := tr.Max(); max != nil {
			t.Fatalf("empty max, got %+v", max)
		}
		for _, item := range perm(treeSize) {
			if x := tr.ReplaceOrInsert(item); x != nil {
				t.Fatal("insert found item", item)
			}
		}
		for _, item := range perm(treeSize) {
			if x := tr.ReplaceOrInsert(item); x == nil {
				t.Fatal("insert didn't find item", item)
			}
		}
		if min, want := tr.Min(), Item(Int(0)); min != want {
			t.Fatalf("min: want %+v, got %+v", want, min)
		}
		if max, want := tr.Max(), Item(Int(treeSize-1)); max != want {
			t.Fatalf("max: want %+v, got %+v", want, max)
		}
		got := all(tr)
		want := rang(treeSize)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("mismatch:\n got: %v\nwant: %v", got, want)
		}

		gotrev := allrev(tr)
		wantrev := rangrev(treeSize)
		if !reflect.DeepEqual(gotrev, wantrev) {
			t.Fatalf("mismatch:\n got: %v\nwant: %v", got, want)
		}

		for _, item := range perm(treeSize) {
			if x := tr.Delete(item); x == nil {
				t.Fatalf("didn't find %v", item)
			}
		}
		if got = all(tr); len(got) > 0 {
			t.Fatalf("some left!: %v", got)
		}
	}
}

func ExampleBTree() {
	tr := New(*btreeDegree, nil)
	for i := Int(0); i < 10; i++ {
		tr.ReplaceOrInsert(i)
	}
	fmt.Println("len:       ", tr.Len())
	fmt.Println("get3:      ", tr.Get(Int(3)))
	fmt.Println("get100:    ", tr.Get(Int(100)))
	fmt.Println("del4:      ", tr.Delete(Int(4)))
	fmt.Println("del100:    ", tr.Delete(Int(100)))
	fmt.Println("replace5:  ", tr.ReplaceOrInsert(Int(5)))
	fmt.Println("replace100:", tr.ReplaceOrInsert(Int(100)))
	fmt.Println("min:       ", tr.Min())
	fmt.Println("delmin:    ", tr.DeleteMin())
	fmt.Println("max:       ", tr.Max())
	fmt.Println("delmax:    ", tr.DeleteMax())
	fmt.Println("len:       ", tr.Len())
	// Output:
	// len:        10
	// get3:       3
	// get100:     <nil>
	// del4:       4
	// del100:     <nil>
	// replace5:   5
	// replace100: <nil>
	// min:        0
	// delmin:     0
	// max:        100
	// delmax:     100
	// len:        8
}

func TestDeleteMin(t *testing.T) {
	tr := New(3, nil)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v)
	}
	var got []Item
	for v := tr.DeleteMin(); v != nil; v = tr.DeleteMin() {
		got = append(got, v)
	}
	if want := rang(100); !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
}

func TestDeleteMax(t *testing.T) {
	tr := New(3, nil)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v)
	}
	var got []Item
	for v := tr.DeleteMax(); v != nil; v = tr.DeleteMax() {
		got = append(got, v)
	}
	// Reverse our list.
	for i := 0; i < len(got)/2; i++ {
		got[i], got[len(got)-i-1] = got[len(got)-i-1], got[i]
	}
	if want := rang(100); !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
}

func TestAscendRange(t *testing.T) {
	tr := New(2, nil)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v)
	}
	var got []Item
	tr.AscendRange(Int(40), Int(60), func(a Item) bool {
		got = append(got, a)
		return true
	})
	if want := rang(100)[40:60]; !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
	got = got[:0]
	tr.AscendRange(Int(40), Int(60), func(a Item) bool {
		if a.(Int) > 50 {
			return false
		}
		got = append(got, a)
		return true
	})
	if want := rang(100)[40:51]; !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
}

func TestDescendRange(t *testing.T) {
	tr := New(2, nil)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v)
	}
	var got []Item
	tr.DescendRange(Int(60), Int(40), func(a Item) bool {
		got = append(got, a)
		return true
	})
	if want := rangrev(100)[39:59]; !reflect.DeepEqual(got, want) {
		t.Fatalf("descendrange:\n got: %v\nwant: %v", got, want)
	}
	got = got[:0]
	tr.DescendRange(Int(60), Int(40), func(a Item) bool {
		if a.(Int) < 50 {
			return false
		}
		got = append(got, a)
		return true
	})
	if want := rangrev(100)[39:50]; !reflect.DeepEqual(got, want) {
		t.Fatalf("descendrange:\n got: %v\nwant: %v", got, want)
	}
}
func TestAscendLessThan(t *testing.T) {
	tr := New(*btreeDegree, nil)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v)
	}
	var got []Item
	tr.AscendLessThan(Int(60), func(a Item) bool {
		got = append(got, a)
		return true
	})
	if want := rang(100)[:60]; !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
	got = got[:0]
	tr.AscendLessThan(Int(60), func(a Item) bool {
		if a.(Int) > 50 {
			return false
		}
		got = append(got, a)
		return true
	})
	if want := rang(100)[:51]; !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
}

func TestDescendLessOrEqual(t *testing.T) {
	tr := New(*btreeDegree, nil)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v)
	}
	var got []Item
	tr.DescendLessOrEqual(Int(40), func(a Item) bool {
		got = append(got, a)
		return true
	})
	if want := rangrev(100)[59:]; !reflect.DeepEqual(got, want) {
		t.Fatalf("descendlessorequal:\n got: %v\nwant: %v", got, want)
	}
	got = got[:0]
	tr.DescendLessOrEqual(Int(60), func(a Item) bool {
		if a.(Int) < 50 {
			return false
		}
		got = append(got, a)
		return true
	})
	if want := rangrev(100)[39:50]; !reflect.DeepEqual(got, want) {
		t.Fatalf("descendlessorequal:\n got: %v\nwant: %v", got, want)
	}
}
func TestAscendGreaterOrEqual(t *testing.T) {
	tr := New(*btreeDegree, nil)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v)
	}
	var got []Item
	tr.AscendGreaterOrEqual(Int(40), func(a Item) bool {
		got = append(got, a)
		return true
	})
	if want := rang(100)[40:]; !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
	got = got[:0]
	tr.AscendGreaterOrEqual(Int(40), func(a Item) bool {
		if a.(Int) > 50 {
			return false
		}
		got = append(got, a)
		return true
	})
	if want := rang(100)[40:51]; !reflect.DeepEqual(got, want) {
		t.Fatalf("ascendrange:\n got: %v\nwant: %v", got, want)
	}
}

func TestDescendGreaterThan(t *testing.T) {
	tr := New(*btreeDegree, nil)
	for _, v := range perm(100) {
		tr.ReplaceOrInsert(v)
	}
	var got []Item
	tr.DescendGreaterThan(Int(40), func(a Item) bool {
		got = append(got, a)
		return true
	})
	if want := rangrev(100)[:59]; !reflect.DeepEqual(got, want) {
		t.Fatalf("descendgreaterthan:\n got: %v\nwant: %v", got, want)
	}
	got = got[:0]
	tr.DescendGreaterThan(Int(40), func(a Item) bool {
		if a.(Int) < 50 {
			return false
		}
		got = append(got, a)
		return true
	})
	if want := rangrev(100)[:50]; !reflect.DeepEqual(got, want) {
		t.Fatalf("descendgreaterthan:\n got: %v\nwant: %v", got, want)
	}
}

const benchmarkTreeSize = 10000

func BenchmarkInsert(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	b.StartTimer()
	i := 0
	for i < b.N {
		tr := New(*btreeDegree, nil)
		for _, item := range insertP {
			tr.ReplaceOrInsert(item)
			i++
			if i >= b.N {
				return
			}
		}
	}
}

func BenchmarkDeleteInsert(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	tr := New(*btreeDegree, nil)
	for _, item := range insertP {
		tr.ReplaceOrInsert(item)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tr.Delete(insertP[i%benchmarkTreeSize])
		tr.ReplaceOrInsert(insertP[i%benchmarkTreeSize])
	}
}

func BenchmarkDeleteInsertCloneOnce(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	tr := New(*btreeDegree, nil)
	for _, item := range insertP {
		tr.ReplaceOrInsert(item)
	}
	tr = tr.Clone()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tr.Delete(insertP[i%benchmarkTreeSize])
		tr.ReplaceOrInsert(insertP[i%benchmarkTreeSize])
	}
}

func BenchmarkDeleteInsertCloneEachTime(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	tr := New(*btreeDegree, nil)
	for _, item := range insertP {
		tr.ReplaceOrInsert(item)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tr = tr.Clone()
		tr.Delete(insertP[i%benchmarkTreeSize])
		tr.ReplaceOrInsert(insertP[i%benchmarkTreeSize])
	}
}

func BenchmarkDelete(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	removeP := perm(benchmarkTreeSize)
	b.StartTimer()
	i := 0
	for i < b.N {
		b.StopTimer()
		tr := New(*btreeDegree, nil)
		for _, v := range insertP {
			tr.ReplaceOrInsert(v)
		}
		b.StartTimer()
		for _, item := range removeP {
			tr.Delete(item)
			i++
			if i >= b.N {
				return
			}
		}
		if tr.Len() > 0 {
			panic(tr.Len())
		}
	}
}

func BenchmarkGet(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	removeP := perm(benchmarkTreeSize)
	b.StartTimer()
	i := 0
	for i < b.N {
		b.StopTimer()
		tr := New(*btreeDegree, nil)
		for _, v := range insertP {
			tr.ReplaceOrInsert(v)
		}
		b.StartTimer()
		for _, item := range removeP {
			tr.Get(item)
			i++
			if i >= b.N {
				return
			}
		}
	}
}

func BenchmarkGetCloneEachTime(b *testing.B) {
	b.StopTimer()
	insertP := perm(benchmarkTreeSize)
	removeP := perm(benchmarkTreeSize)
	b.StartTimer()
	i := 0
	for i < b.N {
		b.StopTimer()
		tr := New(*btreeDegree, nil)
		for _, v := range insertP {
			tr.ReplaceOrInsert(v)
		}
		b.StartTimer()
		for _, item := range removeP {
			tr = tr.Clone()
			tr.Get(item)
			i++
			if i >= b.N {
				return
			}
		}
	}
}

type byInts []Item

func (a byInts) Len() int {
	return len(a)
}

func (a byInts) Less(i, j int) bool {
	return a[i].(Int) < a[j].(Int)
}

func (a byInts) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func BenchmarkAscend(b *testing.B) {
	arr := perm(benchmarkTreeSize)
	tr := New(*btreeDegree, nil)
	for _, v := range arr {
		tr.ReplaceOrInsert(v)
	}
	sort.Sort(byInts(arr))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := 0
		tr.Ascend(func(item Item) bool {
			if item.(Int) != arr[j].(Int) {
				b.Fatalf("mismatch: expected: %v, got %v", arr[j].(Int), item.(Int))
			}
			j++
			return true
		})
	}
}

func BenchmarkDescend(b *testing.B) {
	arr := perm(benchmarkTreeSize)
	tr := New(*btreeDegree, nil)
	for _, v := range arr {
		tr.ReplaceOrInsert(v)
	}
	sort.Sort(byInts(arr))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := len(arr) - 1
		tr.Descend(func(item Item) bool {
			if item.(Int) != arr[j].(Int) {
				b.Fatalf("mismatch: expected: %v, got %v", arr[j].(Int), item.(Int))
			}
			j--
			return true
		})
	}
}
func BenchmarkAscendRange(b *testing.B) {
	arr := perm(benchmarkTreeSize)
	tr := New(*btreeDegree, nil)
	for _, v := range arr {
		tr.ReplaceOrInsert(v)
	}
	sort.Sort(byInts(arr))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := 100
		tr.AscendRange(Int(100), arr[len(arr)-100], func(item Item) bool {
			if item.(Int) != arr[j].(Int) {
				b.Fatalf("mismatch: expected: %v, got %v", arr[j].(Int), item.(Int))
			}
			j++
			return true
		})
		if j != len(arr)-100 {
			b.Fatalf("expected: %v, got %v", len(arr)-100, j)
		}
	}
}

func BenchmarkDescendRange(b *testing.B) {
	arr := perm(benchmarkTreeSize)
	tr := New(*btreeDegree, nil)
	for _, v := range arr {
		tr.ReplaceOrInsert(v)
	}
	sort.Sort(byInts(arr))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := len(arr) - 100
		tr.DescendRange(arr[len(arr)-100], Int(100), func(item Item) bool {
			if item.(Int) != arr[j].(Int) {
				b.Fatalf("mismatch: expected: %v, got %v", arr[j].(Int), item.(Int))
			}
			j--
			return true
		})
		if j != 100 {
			b.Fatalf("expected: %v, got %v", len(arr)-100, j)
		}
	}
}
func BenchmarkAscendGreaterOrEqual(b *testing.B) {
	arr := perm(benchmarkTreeSize)
	tr := New(*btreeDegree, nil)
	for _, v := range arr {
		tr.ReplaceOrInsert(v)
	}
	sort.Sort(byInts(arr))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := 100
		k := 0
		tr.AscendGreaterOrEqual(Int(100), func(item Item) bool {
			if item.(Int) != arr[j].(Int) {
				b.Fatalf("mismatch: expected: %v, got %v", arr[j].(Int), item.(Int))
			}
			j++
			k++
			return true
		})
		if j != len(arr) {
			b.Fatalf("expected: %v, got %v", len(arr), j)
		}
		if k != len(arr)-100 {
			b.Fatalf("expected: %v, got %v", len(arr)-100, k)
		}
	}
}
func BenchmarkDescendLessOrEqual(b *testing.B) {
	arr := perm(benchmarkTreeSize)
	tr := New(*btreeDegree, nil)
	for _, v := range arr {
		tr.ReplaceOrInsert(v)
	}
	sort.Sort(byInts(arr))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := len(arr) - 100
		k := len(arr)
		tr.DescendLessOrEqual(arr[len(arr)-100], func(item Item) bool {
			if item.(Int) != arr[j].(Int) {
				b.Fatalf("mismatch: expected: %v, got %v", arr[j].(Int), item.(Int))
			}
			j--
			k--
			return true
		})
		if j != -1 {
			b.Fatalf("expected: %v, got %v", -1, j)
		}
		if k != 99 {
			b.Fatalf("expected: %v, got %v", 99, k)
		}
	}
}

const cloneTestSize = 10000

func cloneTest(t *testing.T, b *BTree, start int, p []Item, wg *sync.WaitGroup, trees *[]*BTree) {
	t.Logf("Starting new clone at %v", start)
	*trees = append(*trees, b)
	for i := start; i < cloneTestSize; i++ {
		b.ReplaceOrInsert(p[i])
		if i%(cloneTestSize/5) == 0 {
			wg.Add(1)
			go cloneTest(t, b.Clone(), i+1, p, wg, trees)
		}
	}
	wg.Done()
}

func TestCloneConcurrentOperations(t *testing.T) {
	b := New(*btreeDegree, nil)
	trees := []*BTree{}
	p := perm(cloneTestSize)
	var wg sync.WaitGroup
	wg.Add(1)
	go cloneTest(t, b, 0, p, &wg, &trees)
	wg.Wait()
	want := rang(cloneTestSize)
	t.Logf("Starting equality checks on %d trees", len(trees))
	for i, tree := range trees {
		if !reflect.DeepEqual(want, all(tree)) {
			t.Errorf("tree %v mismatch", i)
		}
	}
	t.Log("Removing half from first half")
	toRemove := rang(cloneTestSize)[cloneTestSize/2:]
	for i := 0; i < len(trees)/2; i++ {
		tree := trees[i]
		wg.Add(1)
		go func() {
			for _, item := range toRemove {
				tree.Delete(item)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	t.Log("Checking all values again")
	for i, tree := range trees {
		var wantpart []Item
		if i < len(trees)/2 {
			wantpart = want[:cloneTestSize/2]
		} else {
			wantpart = want
		}
		if got := all(tree); !reflect.DeepEqual(wantpart, got) {
			t.Errorf("tree %v mismatch, want %v got %v", i, len(want), len(got))
		}
	}
}

func TestCursor(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tr := New(3, nil)
	for i := 0; i < 20; i += 2 {
		tr.ReplaceOrInsert(Int(i))
	}

	var a []string
	c := tr.Cursor()
	for item := c.First(); item != nil; item = c.Next() {
		a = append(a, fmt.Sprintf("%v", item))
	}
	x := strings.Join(a, ",")
	e := "0,2,4,6,8,10,12,14,16,18"
	if x != e {
		t.Fatal("expected '%v', got '%v'", e, x)
	}

	c = tr.Cursor()
	a = nil
	for item := c.Last(); item != nil; item = c.Prev() {
		a = append(a, fmt.Sprintf("%v", item))
	}
	x = strings.Join(a, ",")
	e = "18,16,14,12,10,8,6,4,2,0"
	if x != e {
		t.Fatal("expected '%v', got '%v'", e, x)
	}

	for i := 0; i < 20; i++ {
		c = tr.Cursor()
		a = nil
		for item := c.Seek(Int(i)); item != nil; item = c.Next() {
			a = append(a, fmt.Sprintf("%v", item))
		}

		var b []string
		for j := 0; j < 20; j += 2 {
			if j < i {
				continue
			}
			b = append(b, fmt.Sprintf("%v", Int(j)))
		}
		x = strings.Join(a, ",")
		y := strings.Join(b, ",")
		if x != y {
			t.Fatalf("expected '%v', '%v'", x, y)
		}
	}

	for x := 0; x < 1000; x++ {
		n := rand.Int() % 1000
		tr := New(4, nil)

		for i := Int(0); i < Int(n); i++ {
			tr.ReplaceOrInsert(i)
		}

		//tr.root.print(os.Stdout, 1)

		var i int
		var tt int
		var c *Cursor
		// test forward cursor
		i = 0
		tt = 0
		c = tr.Cursor()
		for item := c.First(); item != nil; item = c.Next() {
			if int(item.(Int)) != i {
				t.Fatalf("expected '%v', got '%v'", i, item)
			}
			i++
			tt++
		}
		if tt != n {
			t.Fatalf("expected '%v', got '%v'", n, tt)
		}

		// test reverse cursor
		i = n - 1
		tt = 0
		c = tr.Cursor()
		for item := c.Last(); item != nil; item = c.Prev() {
			if int(item.(Int)) != i {
				t.Fatalf("expected '%v', got '%v'", i, item)
			}
			i--
			tt++
		}
		if tt != n {
			t.Fatalf("expected '%v', got '%v'", n, tt)
		}

		// test forward half way and reverse
		i = 0
		c = tr.Cursor()
		for item := c.First(); item != nil; item = c.Next() {
			if int(item.(Int)) != i {
				t.Fatalf("expected '%v', got '%v'", i, item)
			}
			i++
			if i > n/2 {
				item = c.Prev()
				i -= 2
				for ; item != nil; item = c.Prev() {
					if int(item.(Int)) != i {
						t.Fatalf("expected '%v', got '%v'", i, item)
					}
					i--
				}
				break
			}
		}

		// test reverse half way and forward
		i = n - 1
		c = tr.Cursor()
		for item := c.Last(); item != nil; item = c.Prev() {
			if int(item.(Int)) != i {
				t.Fatalf("expected '%v', got '%v'", i, item)
			}
			i--
			if i < n/2 {
				item = c.Next()
				i += 2
				for ; item != nil; item = c.Next() {
					if int(item.(Int)) != i {
						t.Fatalf("expected '%v', got '%v'", i, item)
					}
					i++
				}
				break
			}
		}

		// seek forward half way
		i = n / 2
		c = tr.Cursor()
		for item := c.Seek(Int(i)); item != nil; item = c.Next() {
			if int(item.(Int)) != i {
				t.Fatalf("expected '%v', got '%v'", i, item)
			}
			i++
		}
	}
}
