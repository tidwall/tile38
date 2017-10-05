package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"

	"github.com/tidwall/resp"
	"github.com/yuin/gopher-lua"
	"strings"
)

var errCmdNotSupported = errors.New("command not supported in scripts")

func Sha1Sum(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

// Convert lua LValue to RESP value
func ConvertToResp(val lua.LValue) resp.Value {
	switch val.Type() {
	case lua.LTNil:
		return resp.NullValue()
	case lua.LTBool:
		if val == lua.LTrue {
			return resp.IntegerValue(1)
		} else {
			return resp.NullValue()
		}
	case lua.LTNumber:
		if float := float64(val.(lua.LNumber)); math.IsNaN(float) || math.IsInf(float, 0) {
			return resp.FloatValue(float)
		} else {
			return resp.IntegerValue(int(math.Floor(float)))
		}
	case lua.LTString:
		return resp.StringValue(val.String())
	case lua.LTTable:
		var values []resp.Value
		var specialValues []resp.Value
		var cb func(lk lua.LValue, lv lua.LValue)
		tbl := val.(*lua.LTable)

		if tbl.Len() != 0 { // list
			cb = func(lk lua.LValue, lv lua.LValue) {
				values = append(values, ConvertToResp(lv))
			}
		} else { // map
			cb = func(lk lua.LValue, lv lua.LValue) {
				if lk.Type() == lua.LTString {
					lks := lk.String()
					switch lks {
					case "ok":
						specialValues = append(specialValues, resp.SimpleStringValue(lv.String()))
					case "err":
						specialValues = append(specialValues, resp.ErrorValue(errors.New(lv.String())))
					}
				}
				values = append(values, resp.ArrayValue(
					[]resp.Value{ConvertToResp(lk), ConvertToResp(lv)}))
			}
		}
		tbl.ForEach(cb)
		if len(values) == 1 && len(specialValues) == 1 {
			return specialValues[0]
		}
		return resp.ArrayValue(values)
	}
	return resp.ErrorValue(errors.New("Unsupported lua type: " + val.Type().String()))
}

// Convert RESP value to lua LValue
func ConvertToLua(L *lua.LState, val resp.Value) lua.LValue {
	if val.IsNull() {
		return lua.LFalse
	}
	switch val.Type() {
	case resp.Integer:
		return lua.LNumber(val.Integer())
	case resp.BulkString:
		return lua.LString(val.String())
	case resp.Error:
		tbl := L.CreateTable(0, 1)
		tbl.RawSetString("err", lua.LString(val.String()))
		return tbl
	case resp.SimpleString:
		tbl := L.CreateTable(0, 1)
		tbl.RawSetString("ok", lua.LString(val.String()))
		return tbl
	case resp.Array:
		tbl := L.CreateTable(len(val.Array()), 0)
		for _, item := range val.Array() {
			tbl.Append(ConvertToLua(L, item))
		}
		return tbl
	}
	return lua.LString("ERR: unknown RESP type: " + val.Type().String())
}

func luaTile38Call(evalcmd string, cmd string, args ...string) (resp.Value, error) {
	var values []resp.Value
	values = append(values, resp.StringValue("RUNNING:"))
	values = append(values, resp.StringValue(evalcmd))
	values = append(values, resp.StringValue(cmd))
	for _, arg := range args {
		values = append(values, resp.StringValue(arg))
	}

	return resp.ArrayValue(values), nil
}

func NewLuaState() *lua.LState {
	L := lua.NewState()

	get_args := func(ls *lua.LState) (evalCmd string, args []string) {
		evalCmd = ls.GetGlobal("EVAL_CMD").String()
		//log.Debugf("EVAL_CMD %s\n", evalCmd)

		// Trying to work with unknown number of args.
		// When we see empty arg we call it enough.
		for i := 1; ; i++ {
			if arg := ls.ToString(i); arg == "" {
				break
			} else {
				args = append(args, arg)
			}
		}
		return
	}
	call := func(ls *lua.LState) int {
		evalCmd, args := get_args(ls)
		if res, err := luaTile38Call(evalCmd, args[0], args[1:]...); err != nil {
			//log.Debugf("RES type: %s value: %s ERR %s\n", res.Type(), res.String(), err);
			ls.RaiseError("ERR %s", err.Error())
			return 0
		} else {
			//log.Debugf("RES type: %s value: %s\n", res.Type(), res.String());
			ls.Push(ConvertToLua(ls, res))
			return 1
		}
	}
	pcall := func(ls *lua.LState) int {
		evalCmd, args := get_args(ls)
		if res, err := luaTile38Call(evalCmd, args[0], args[1:]...); err != nil {
			//log.Debugf("RES type: %s value: %s ERR %s\n", res.Type(), res.String(), err);
			ls.Push(ConvertToLua(ls, resp.ErrorValue(err)))
			return 1
		} else {
			//log.Debugf("RES type: %s value: %s\n", res.Type(), res.String());
			ls.Push(ConvertToLua(ls, res))
			return 1
		}
	}
	error_reply := func(ls *lua.LState) int {
		tbl := L.CreateTable(0, 1)
		tbl.RawSetString("err", lua.LString(ls.ToString(1)))
		ls.Push(tbl)
		return 1
	}
	status_reply := func(ls *lua.LState) int {
		tbl := L.CreateTable(0, 1)
		tbl.RawSetString("ok", lua.LString(ls.ToString(1)))
		ls.Push(tbl)
		return 1
	}
	sha1hex := func(ls *lua.LState) int {
		sha_sum := Sha1Sum(ls.ToString(1))
		ls.Push(lua.LString(sha_sum))
		return 1
	}
	var exports = map[string]lua.LGFunction{
		"call":         call,
		"pcall":        pcall,
		"error_reply":  error_reply,
		"status_reply": status_reply,
		"sha1hex":      sha1hex,
	}
	L.SetGlobal("tile38", L.SetFuncs(L.NewTable(), exports))
	return L
}

func makeSafeErr(err error) error {
	return errors.New(strings.Replace(err.Error(), "\n", `\n`, -1))
}

func runLuaFunc(luaState *lua.LState, script string, name string) resp.Value {
	luaState.SetGlobal("EVAL_CMD", lua.LString("FAKE_EVAL"))
	fn, err := luaState.Load(strings.NewReader(script), name)
	if err != nil {
		return resp.ErrorValue(makeSafeErr(err))
	}
	luaState.Push(fn)
	if err := luaState.PCall(0, 1, nil); err != nil {
		return resp.ErrorValue(makeSafeErr(err))
	}
	ret := luaState.Get(-1) // returned value
	luaState.Pop(1)
	luaState.SetGlobal("EVAL_CMD", lua.LNil)
	return ConvertToResp(ret)
}

func runMany(luaState *lua.LState, start int, num int) int {
	fmt.Printf("\nRunning %d lua calls... ", num)
	for i := 0; i < num; i++ {
		script := fmt.Sprintf("return tile38.call('foo', 'bar', %d)", i)
		name := fmt.Sprintf("f_%020d", i)
		ret := runLuaFunc(luaState, script, name)
		if ret.Type() == resp.Error {
			panic(ret.String())
		}
	}
	fmt.Printf("done.\n")
	return start + num
}

func printMemStats() {
	var mem runtime.MemStats
	runtime.GC()
	debug.FreeOSMemory()
	runtime.GC()
	debug.FreeOSMemory()
	runtime.GC()
	debug.FreeOSMemory()
	runtime.GC()
	debug.FreeOSMemory()
	runtime.ReadMemStats(&mem)
	fmt.Printf("MemStats:  Alloc %d, HeapAlloc %d, HeapSys %d, GCSys %d, HeapObjects %d.\n",
		mem.Alloc, mem.HeapAlloc, mem.HeapSys, mem.GCSys, mem.HeapObjects)
}

func testLua() {
	var luaState *lua.LState
	start := 12345
	luaState = NewLuaState()

	printMemStats()

	fmt.Printf("\nRunning single call as a test\n")
	ret := runLuaFunc(luaState, "return tile38.call('fake_cmd', 'a', 'b')", "test_call")
	fmt.Printf("Result: %s\n", ret.String())

	printMemStats()

	start = runMany(luaState, start, 100)
	printMemStats()

	start = runMany(luaState, start, 100)
	printMemStats()

	start = runMany(luaState, start, 100)
	printMemStats()

	start = runMany(luaState, start, 100)
	printMemStats()

	start = runMany(luaState, start, 1000)
	printMemStats()

	start = runMany(luaState, start, 10000)
	printMemStats()

	start = runMany(luaState, start, 1000)
	printMemStats()

	start = runMany(luaState, start, 100)
	printMemStats()

	start = runMany(luaState, start, 1000)
	printMemStats()

	luaState.Close()
}

func main() {
	fmt.Printf("Starting memtest.\n")
	testLua()
}
