package expire

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

type testItem struct {
	str string
	exp time.Time
}

func (item *testItem) Expires() time.Time {
	return item.exp
}

func TestBasic(t *testing.T) {
	var list List
	list.bgrun = true // Prevent expiration handlers from being called
	now := time.Now()
	list.Push(&testItem{"13", now.Add(13)})
	list.Push(&testItem{"11", now.Add(11)})
	list.Push(&testItem{"14", now.Add(14)})
	list.Push(&testItem{"10", now.Add(10)})
	list.Push(&testItem{"15", now.Add(15)})
	list.Push(&testItem{"12", now.Add(12)})

	var lunix int64
	for list.queue.len > 0 {
		n2 := list.queue.pop()
		if n2.unix < lunix {
			t.Fatal("out of order")
		}
	}
}

func TestRandomQueue(t *testing.T) {
	N := 1000
	now := time.Now()
	var list List
	list.bgrun = true // Prevent expiration handlers from being called
	for i := 0; i < N; i++ {
		list.Push(&testItem{fmt.Sprintf("%d", i),
			now.Add(time.Duration(rand.Float64() * float64(time.Second)))})
	}
	var items []Item
	for list.queue.len > 0 {
		n1 := list.queue.peek()
		n2 := list.queue.pop()
		if n1 != n2 {
			t.Fatal("mismatch")
		}
		if n1.unix > n2.unix {
			t.Fatal("out of order")
		}
		items = append(items, n2.item)
	}

	if !sort.SliceIsSorted(items, func(i, j int) bool {
		return items[i].Expires().Before(items[j].Expires())
	}) {
		t.Fatal("out of order")
	}

}

func TestExpires(t *testing.T) {
	N := 1000
	now := time.Now()
	var list List
	var wg sync.WaitGroup
	wg.Add(N)
	var items []Item
	list.Expired = func(item Item) {
		items = append(items, item)
		wg.Done()
	}

	for i := 0; i < N; i++ {
		list.Push(&testItem{fmt.Sprintf("%d", i),
			now.Add(time.Duration(rand.Float64() * float64(time.Second)))})
	}

	wg.Wait()
	if len(items) != N {
		t.Fatal("wrong result")
	}
}
