package controller

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
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

type CompiledLuaScript *lua.FunctionProto

var allScripts = make(map[string]CompiledLuaScript)

// TODO: Refactor common bits from all these functions
func (c* Controller) cmdEval(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var script, numkeys_str, key, arg string
	if vs, script, ok = tokenval(vs); !ok || script == "" {
		return "", errInvalidNumberOfArguments
	}

	if vs, numkeys_str, ok = tokenval(vs); !ok || numkeys_str == "" {
		return "", errInvalidNumberOfArguments
	}

	var i, numkeys uint64
	if numkeys, err = strconv.ParseUint(numkeys_str, 10, 64); err != nil {
		err = errInvalidArgument(numkeys_str)
		return
	}

	L := lua.NewState()
	defer L.Close()

	keys_tbl := L.CreateTable(int(numkeys), 0)
	for i = 0; i < numkeys; i++ {
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		keys_tbl.Append(lua.LString(key))
	}

	args_tbl := L.CreateTable(len(vs), 0)
	for len(vs) > 0 {
		if vs, arg, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		args_tbl.Append(lua.LString(arg))
	}

	fmt.Printf("SCRIPT source:\n%s\n\n", script)
	sha_sum := fmt.Sprintf("%x", sha1.Sum([]byte(script)))

	L.SetGlobal("KEYS", keys_tbl)
	L.SetGlobal("ARGS", args_tbl)

	compiled, ok := allScripts[sha_sum]
	var fn *lua.LFunction
	if ok {
		fn = &lua.LFunction{
			IsG: false,
			Env: L.Env,

			Proto:     compiled,
			GFunction: nil,
			Upvalues:  make([]*lua.Upvalue, 0),
		}
		fmt.Printf("RETRIEVED %s\n", sha_sum)
	} else {
		fn, err = L.Load(strings.NewReader(script), sha_sum)
		if err != nil {
			return "", errLuaCompileFailure
		}
		allScripts[sha_sum] = fn.Proto
		fmt.Printf("STORED %s\n", sha_sum)
	}
	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		return "", errLuaRunFailure
	}
	ret := L.Get(-1) // returned value
	L.Pop(1)  // remove received value

	fmt.Printf("RET type %s, val %s\n", ret.Type(), ret.String())

	if ret.Type() == lua.LTTable {
		if tbl, ok := ret.(*lua.LTable); ok {
			fmt.Printf("LEN %d\n", L.ObjLen(tbl))
			tbl.ForEach(func(num lua.LValue, val lua.LValue) {fmt.Printf("num %s val %s\n", num, val)})
		}
	}

	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		if msg.OutputType == server.JSON {
			buf.WriteString(`{"ok":true`)
		}
		buf.WriteString(`,"result":"` + ret.String() + `"`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return buf.String(), nil
	case server.RESP:
		oval := resp.StringValue(ret.String())
		data, err := oval.MarshalRESP()
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	return "", nil
}

func (c* Controller) cmdEvalSha(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var sha_sum, numkeys_str, key, arg string
	if vs, sha_sum, ok = tokenval(vs); !ok || sha_sum == "" {
		return "", errInvalidNumberOfArguments
	}

	if vs, numkeys_str, ok = tokenval(vs); !ok || numkeys_str == "" {
		return "", errInvalidNumberOfArguments
	}

	var i, numkeys uint64
	if numkeys, err = strconv.ParseUint(numkeys_str, 10, 64); err != nil {
		err = errInvalidArgument(numkeys_str)
		return
	}

	L := lua.NewState()
	defer L.Close()

	keys_tbl := L.CreateTable(int(numkeys), 0)
	for i = 0; i < numkeys; i++ {
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		keys_tbl.Append(lua.LString(key))
	}

	args_tbl := L.CreateTable(len(vs), 0)
	for len(vs) > 0 {
		if vs, arg, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		args_tbl.Append(lua.LString(arg))
	}

	L.SetGlobal("KEYS", keys_tbl)
	L.SetGlobal("ARGS", args_tbl)

	compiled, ok := allScripts[sha_sum]
	if !ok {
		err = errShaNotFound
		return
	}
	fn := &lua.LFunction{
		IsG: false,
		Env: L.Env,

		Proto:     compiled,
		GFunction: nil,
		Upvalues:  make([]*lua.Upvalue, 0),
	}
	fmt.Printf("RETRIEVED %s\n", sha_sum)
	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		return "", errLuaRunFailure
	}
	ret := L.Get(-1) // returned value
	L.Pop(1)  // remove received value

	fmt.Printf("RET type %s, val %s\n", ret.Type(), ret.String())

	if ret.Type() == lua.LTTable {
		if tbl, ok := ret.(*lua.LTable); ok {
			fmt.Printf("LEN %d\n", L.ObjLen(tbl))
			tbl.ForEach(func(num lua.LValue, val lua.LValue) {fmt.Printf("num %s val %s\n", num, val)})
		}
	}

	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		if msg.OutputType == server.JSON {
			buf.WriteString(`{"ok":true`)
		}
		buf.WriteString(`,"result":"` + ret.String() + `"`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return buf.String(), nil
	case server.RESP:
		oval := resp.StringValue(ret.String())
		data, err := oval.MarshalRESP()
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	return "", nil
}

func (c* Controller) cmdScriptLoad(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var script string
	if vs, script, ok = tokenval(vs); !ok || script == "" {
		return "", errInvalidNumberOfArguments
	}

	L := lua.NewState()
	defer L.Close()

	fmt.Printf("SCRIPT source:\n%s\n\n", script)
	sha_sum := fmt.Sprintf("%x", sha1.Sum([]byte(script)))

	fn, err := L.Load(strings.NewReader(script), sha_sum)
	if err != nil {
		return "", errLuaCompileFailure
	}
	allScripts[sha_sum] = fn.Proto
	fmt.Printf("STORED %s\n", sha_sum)

	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"result":"` + sha_sum + `"`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return buf.String(), nil
	case server.RESP:
		oval := resp.StringValue(sha_sum)
		data, err := oval.MarshalRESP()
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	return "", nil
}

func (c* Controller) cmdScriptExists(msg *server.Message) (res string, err error) {
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
		_, ok = allScripts[sha_sum]
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
		if msg.OutputType == server.JSON {
			buf.WriteString(`{"ok":true`)
		}
		var res_array []string
		for _, ires := range results {
			res_array = append(res_array, fmt.Sprintf("%d", ires))
		}
		buf.WriteString(`,"result":[` + strings.Join(res_array, ",") + `]`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return buf.String(), nil
	case server.RESP:
		var res_array []resp.Value
		for _, ires := range results {
			res_array = append(res_array, resp.IntegerValue(ires))
		}
		oval := resp.ArrayValue(res_array)
		data, err := oval.MarshalRESP()
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	return "", nil
}

func (c* Controller) cmdScriptFlush(msg *server.Message) (res string, err error) {
	start := time.Now()
	allScripts = make(map[string]CompiledLuaScript)

	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		if msg.OutputType == server.JSON {
			buf.WriteString(`{"ok":true`)
		}
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return buf.String(), nil
	case server.RESP:
		oval := resp.StringValue("OK")
		data, err := oval.MarshalRESP()
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	return "", nil
}
