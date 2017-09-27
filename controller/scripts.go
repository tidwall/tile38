package controller

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/resp"
	"github.com/yuin/gopher-lua"
)

var errLuaCompileFailure = errors.New("LUA script compilation error")
var errLuaRunFailure = errors.New("LUA script runtime error")
var errShaNotFound = errors.New("SHA not found")


func ConvertTableToResp(tbl *lua.LTable) resp.Value {
	var values []resp.Value
	var cb func(lnum lua.LValue, lv lua.LValue)

	if tbl.Len() != 0 { // list
		cb = func(lnum lua.LValue, lv lua.LValue){
			values = append(values, ConvertToResp(lv))
		}
	} else { // map
		cb = func(lk lua.LValue, lv lua.LValue){
			values = append(values, resp.ArrayValue(
				[]resp.Value{ConvertToResp(lk), ConvertToResp(lv)}))
		}
	}
	tbl.ForEach(cb)
	return resp.ArrayValue(values)
}

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
		return resp.IntegerValue(int(math.Ceil(float64(val.(lua.LNumber)))))
	case lua.LTString:
		return resp.StringValue(val.String())
	case lua.LTTable:
		return ConvertTableToResp(val.(*lua.LTable))
	}
	return resp.StringValue("ERROR")
}

func ConvertTableToJson(tbl *lua.LTable) string {
	var values []string
	var cb func(lnum lua.LValue, lv lua.LValue)
	var start, end string

	if tbl.Len() != 0 { // list
		start = `[`
		end = `]`
		cb = func(lnum lua.LValue, lv lua.LValue){
			values = append(values, ConvertToJson(lv))
		}
	} else { // map
		start = `{`
		end=`}`
		cb = func(lk lua.LValue, lv lua.LValue){
			values = append(
				values, ConvertToJson(lk) + `:` + ConvertToJson(lv))
		}
	}
	tbl.ForEach(cb)
	return start + strings.Join(values, `,`) + end
}

func ConvertToJson(val lua.LValue) string {
	switch val.Type() {
	case lua.LTNil:
		return "null"
	case lua.LTBool:
		if val == lua.LTrue {
			return "true"
		} else {
			return "false"
		}
	case lua.LTNumber:
		return val.String()
	case lua.LTString:
		return `"` + val.String() + `"`
	case lua.LTTable:
		return ConvertTableToJson(val.(*lua.LTable))
	}
	return "ERROR"
}


// TODO: Refactor common bits from all these functions
func (c* Controller) cmdEval(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	empty_response := resp.SimpleStringValue("")
	var ok bool
	var script, numkeys_str, key, arg string
	if vs, script, ok = tokenval(vs); !ok || script == "" {
		return empty_response, errInvalidNumberOfArguments
	}

	if vs, numkeys_str, ok = tokenval(vs); !ok || numkeys_str == "" {
		return empty_response, errInvalidNumberOfArguments
	}

	var i, numkeys uint64
	if numkeys, err = strconv.ParseUint(numkeys_str, 10, 64); err != nil {
		err = errInvalidArgument(numkeys_str)
		return
	}

	keys_tbl := c.luastate.CreateTable(int(numkeys), 0)
	for i = 0; i < numkeys; i++ {
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		keys_tbl.Append(lua.LString(key))
	}

	args_tbl := c.luastate.CreateTable(len(vs), 0)
	for len(vs) > 0 {
		if vs, arg, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		args_tbl.Append(lua.LString(arg))
	}

	fmt.Printf("SCRIPT source:\n%s\n\n", script)
	sha_sum := fmt.Sprintf("%x", sha1.Sum([]byte(script)))

	c.luastate.SetGlobal("KEYS", keys_tbl)
	c.luastate.SetGlobal("ARGS", args_tbl)

	compiled, ok := c.luascripts[sha_sum]
	var fn *lua.LFunction
	if ok {
		fn = &lua.LFunction{
			IsG: false,
			Env: c.luastate.Env,

			Proto:     compiled,
			GFunction: nil,
			Upvalues:  make([]*lua.Upvalue, 0),
		}
		fmt.Printf("RETRIEVED %s\n", sha_sum)
	} else {
		fn, err = c.luastate.Load(strings.NewReader(script), sha_sum)
		if err != nil {
			return empty_response, errLuaCompileFailure
		}
		c.luascripts[sha_sum] = fn.Proto
		fmt.Printf("STORED %s\n", sha_sum)
	}
	c.luastate.Push(fn)
	if err := c.luastate.PCall(0, 1, nil); err != nil {
		return empty_response, errLuaRunFailure
	}
	ret := c.luastate.Get(-1) // returned value
	c.luastate.Pop(1)  // remove received value

	fmt.Printf("RET type %s, val %s\n", ret.Type(), ret.String())

	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"result":"` + ConvertToJson(ret) + `"`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case server.RESP:
		return ConvertToResp(ret), nil
	}
	return empty_response, nil
}

func (c* Controller) cmdEvalSha(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	empty_response := resp.SimpleStringValue("")
	var ok bool
	var sha_sum, numkeys_str, key, arg string
	if vs, sha_sum, ok = tokenval(vs); !ok || sha_sum == "" {
		return empty_response, errInvalidNumberOfArguments
	}

	if vs, numkeys_str, ok = tokenval(vs); !ok || numkeys_str == "" {
		return empty_response, errInvalidNumberOfArguments
	}

	var i, numkeys uint64
	if numkeys, err = strconv.ParseUint(numkeys_str, 10, 64); err != nil {
		err = errInvalidArgument(numkeys_str)
		return
	}

	keys_tbl := c.luastate.CreateTable(int(numkeys), 0)
	for i = 0; i < numkeys; i++ {
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		keys_tbl.Append(lua.LString(key))
	}

	args_tbl := c.luastate.CreateTable(len(vs), 0)
	for len(vs) > 0 {
		if vs, arg, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		args_tbl.Append(lua.LString(arg))
	}

	c.luastate.SetGlobal("KEYS", keys_tbl)
	c.luastate.SetGlobal("ARGS", args_tbl)

	compiled, ok := c.luascripts[sha_sum]
	if !ok {
		err = errShaNotFound
		return
	}
	fn := &lua.LFunction{
		IsG: false,
		Env: c.luastate.Env,

		Proto:     compiled,
		GFunction: nil,
		Upvalues:  make([]*lua.Upvalue, 0),
	}
	fmt.Printf("RETRIEVED %s\n", sha_sum)
	c.luastate.Push(fn)
	if err := c.luastate.PCall(0, 1, nil); err != nil {
		return empty_response, errLuaRunFailure
	}
	ret := c.luastate.Get(-1) // returned value
	c.luastate.Pop(1)  // remove received value

	fmt.Printf("RET type %s, val %s\n", ret.Type(), ret.String())

	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"result":` + ConvertToJson(ret))
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case server.RESP:
		return ConvertToResp(ret), nil
	}
	return empty_response, nil
}

func (c* Controller) cmdScriptLoad(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	empty_response := resp.SimpleStringValue("")
	var ok bool
	var script string
	if vs, script, ok = tokenval(vs); !ok || script == "" {
		return empty_response, errInvalidNumberOfArguments
	}

	fmt.Printf("SCRIPT source:\n%s\n\n", script)
	sha_sum := fmt.Sprintf("%x", sha1.Sum([]byte(script)))

	fn, err := c.luastate.Load(strings.NewReader(script), sha_sum)
	if err != nil {
		return empty_response, errLuaCompileFailure
	}
	c.luascripts[sha_sum] = fn.Proto
	fmt.Printf("STORED %s\n", sha_sum)

	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"result":"` + sha_sum + `"`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case server.RESP:
		return resp.StringValue(sha_sum), nil
	}
	return empty_response, nil
}

func (c* Controller) cmdScriptExists(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var sha_sum string
	var results []int
	var ires int
	for len(vs) > 0 {
		if vs, sha_sum, ok = tokenval(vs); !ok || sha_sum == "" {
			err = errInvalidNumberOfArguments
			return
		}
		_, ok = c.luascripts[sha_sum]
		if ok {
			ires = 1
		} else {
			ires = 0
		}
		results = append(results, ires)
	}

	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		var res_array []string
		for _, ires := range results {
			res_array = append(res_array, fmt.Sprintf("%d", ires))
		}
		buf.WriteString(`,"result":[` + strings.Join(res_array, ",") + `]`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case server.RESP:
		var res_array []resp.Value
		for _, ires := range results {
			res_array = append(res_array, resp.IntegerValue(ires))
		}
		return resp.ArrayValue(res_array), nil
	}
	return resp.SimpleStringValue(""), nil
}

func (c* Controller) cmdScriptFlush(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	c.luascripts = make(map[string]*lua.FunctionProto)

	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case server.RESP:
		return resp.StringValue("OK"), nil
	}
	return resp.SimpleStringValue(""), nil
}
