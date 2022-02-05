package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/rhh"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/txn"
	lua "github.com/yuin/gopher-lua"
)

func runScriptFunc(callback lua.LGFunction, fn string, deadline time.Time) ([]lua.LValue, *lua.LState) {
	itemCount := 5000
	s := &Server{
		config:  &Config{},
		expires: rhh.New(0),
	}

	collect := collection.New()
	s.setCol("test", collect)
	for i := 0; i < itemCount; i++ {
		x := float64(i) / float64(itemCount) * 90
		collect.Set(fmt.Sprintf("%v", i), geojson.NewPoint(geometry.Point{X: x, Y: 0}), []string{"v"}, []float64{float64(i % 2)})
	}

	pool := s.newPool()
	ls, err := pool.Get()
	if err != nil {
		panic(err)
	}

	sched := txn.NewScheduler(0, 0)
	scanDone, ts := sched.Scan()
	defer func() {
		if scanDone != nil {
			scanDone()
		}
	}()
	if !deadline.IsZero() {
		ts = ts.WithDeadline(deadline)
		ctx, done := context.WithDeadline(context.Background(), deadline)
		defer done()
		ls.SetContext(ctx)
	}

	writeRan := false
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer sched.Write()()
		writeRan = true
	}()

	tsud := ls.NewUserData()
	tsud.Value = ts
	luaSetRawGlobals(
		ls, map[string]lua.LValue{
			"EVAL_CMD":   lua.LString("evalro"),
			"TXN_STATUS": tsud,
		})

	for !ts.IsAborted() {
		time.Sleep(1)
	}

	top := ls.GetTop()
	ls.Push(ls.GetGlobal("tile38").(*lua.LTable).RawGetString(fn))
	var nargs int
	if callback != nil {
		ls.Push(ls.NewFunction(callback))
		nargs++
	}
	for _, a := range []string{"nearby", "test", "cursor", "100", "limit", "2000/4000", "where", "v", "1", "1", "ids", "point", "0", "0"} {
		ls.Push(lua.LString(a))
		nargs++
	}

	ls.Call(nargs, lua.MultRet)
	scanDone()
	scanDone = nil
	nret := ls.GetTop() - top

	var results []lua.LValue
	for i := -nret; i < 0; i++ {
		results = append(results, ls.Get(i))
	}

	wg.Wait()
	if !writeRan {
		panic("expected write to have run")
	}

	return results, ls
}

func TestScriptCallInterruptedCall(t *testing.T) {
	results, _ := runScriptFunc(nil, "call", time.Time{})

	if len(results) != 1 {
		t.Fatal("expected 1 results")
	}
}

func TestScriptCallTimeoutCall(t *testing.T) {
	var panicResult interface{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicResult = r
			}
		}()
		_, _ = runScriptFunc(nil, "call", time.Now().Add(-1))
	}()

	if panicResult == nil {
		t.Fatal("expected a panic")
	}

	if err, ok := panicResult.(error); !ok || !errors.Is(err, txn.DeadlineError{}) {
		t.Fatal("expected a deadline error, got", panicResult)
	}
}

func TestScriptIterateInterruptedCall(t *testing.T) {
	var collectedIds []string
	results, _ := runScriptFunc(lua.LGFunction(func(ls *lua.LState) int {
		ud := ls.ToUserData(1)
		itr := ud.Value.(*luaScanIterator)
		collectedIds = append(collectedIds, itr.currentParams.id)
		ls.Push(lua.LTrue)
		return 1
	}), "iterate", time.Time{})

	if len(results) != 1 {
		t.Fatal("expected 1 results")
	}

	if results[0].String() != "4100" {
		t.Fatal("expected cursor 4100, got", results[0])
	}

	if len(collectedIds) != 2000 {
		t.Fatal("expected", 2000, "got", len(collectedIds))
	}

	if collectedIds[0] != "101" {
		t.Fatal("expected first id to be 101 not", collectedIds[0])
	}

	if collectedIds[1999] != "4099" {
		t.Fatal("expected last id to be 4099 not", collectedIds[1999])
	}
}

func TestScriptIterateTimeoutCall(t *testing.T) {
	var panicResult interface{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicResult = r
			}
		}()
		_, _ = runScriptFunc(lua.LGFunction(func(ls *lua.LState) int {
			ls.Push(lua.LTrue)
			return 1
		}), "iterate", time.Now().Add(-1))
	}()

	if panicResult == nil {
		t.Fatal("expected a panic")
	}

	if err, ok := panicResult.(error); !ok || !errors.Is(err, txn.DeadlineError{}) {
		t.Fatal("expected a deadline error, got", panicResult)
	}
}

func TestScriptPiterateInterruptedCall(t *testing.T) {
	results, _ := runScriptFunc(lua.LGFunction(func(ls *lua.LState) int {
		ls.Push(lua.LTrue)
		return 1
	}), "piterate", time.Time{})

	if len(results) != 2 {
		t.Fatal("expected 2 results")
	}

	if results[0] != lua.LFalse {
		t.Fatal("expected result 0 to be LFalse, got ", results[0])
	}

	if s, ok := results[1].(lua.LString); !ok || s != "interrupted" {
		t.Fatal("expected result 1 to be \"interrupted\", got ", results[1])
	}
}

func TestScriptPiterateTimeoutCall(t *testing.T) {
	results, _ := runScriptFunc(lua.LGFunction(func(ls *lua.LState) int {
		ls.Push(lua.LTrue)
		return 1
	}), "piterate", time.Now().Add(-1))

	if len(results) != 2 {
		t.Fatal("expected 2 results")
	}

	if results[0] != lua.LFalse {
		t.Fatal("expected result 0 to be LFalse, got ", results[0])
	}

	if s, ok := results[1].(lua.LString); !ok || s != "deadline" {
		t.Fatal("expected result 1 to be \"deadline\", got ", results[1])
	}
}

func TestScriptPiterateTimeoutCall_ignored(t *testing.T) {
	results, ls := runScriptFunc(lua.LGFunction(func(ls *lua.LState) int {
		ls.Push(lua.LTrue)
		return 1
	}), "piterate", time.Now().Add(-1))

	if len(results) != 2 {
		t.Fatal("expected 2 results")
	}

	if results[0] != lua.LFalse {
		t.Fatal("expected result 0 to be LFalse, got ", results[0])
	}

	if s, ok := results[1].(lua.LString); !ok || s != "deadline" {
		t.Fatal("expected result 1 to be \"interrupted\", got ", results[1])
	}

	err := ls.DoString("print(\"hello\")")
	if err == nil {
		t.Fatal("expected to get an error")
	}
}
