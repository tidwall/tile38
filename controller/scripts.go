package controller

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/resp"
	"github.com/yuin/gopher-lua"
	"fmt"
)

var errLuaCompileFailure = errors.New("LUA script compilation error")
var errLuaRunFailure = errors.New("LUA script runtime error")

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

	var keys, args []string
	for i = 0; i < numkeys; i++ {
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		keys = append(keys, key)
	}
	for len(vs) > 0 {
		if vs, arg, ok = tokenval(vs); !ok || arg == "" {
			err = errInvalidNumberOfArguments
			return
		}
		args = append(args, arg)
	}

	fmt.Printf("SCRIPT:\n%s\n\n", script)

	sha_sum := fmt.Sprintf("%x", sha1.Sum([]byte(script)))

	L := lua.NewState()
	defer L.Close()

	keys_tbl := L.CreateTable(len(keys), 0)
	for _, key := range keys {
		keys_tbl.Append(lua.LString(key))
	}
	L.SetGlobal("KEYS", keys_tbl)

	args_tbl := L.CreateTable(len(args), 0)
	for _, arg := range args {
		args_tbl.Append(lua.LString(arg))
	}
	L.SetGlobal("ARGS", args_tbl)

	fn, err := L.Load(strings.NewReader(script), sha_sum)
	if err != nil {
		return "", errLuaCompileFailure
	}
	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		return "", errLuaRunFailure
	}
	ret := L.Get(-1) // returned value
	fmt.Printf("RET type %s, val %s\n", ret.Type(), ret.String())
	if ret.Type() == lua.LTTable {
		if tbl, ok := ret.(*lua.LTable); ok {
			fmt.Printf("LEN %d\n", L.ObjLen(tbl))
			tbl.ForEach(func(num lua.LValue, val lua.LValue) {fmt.Printf("num %s val %s\n", num, val)})
		}
	}
	L.Pop(1)  // remove received value

	var buf bytes.Buffer
	if msg.OutputType == server.JSON {
		buf.WriteString(`{"ok":true`)
	}

	switch msg.OutputType {
	case server.JSON:
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
