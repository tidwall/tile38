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
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/resp"
	"github.com/yuin/gopher-lua"
)

var errLuaCompileFailure = errors.New("LUA script compilation error")
var errLuaRunFailure = errors.New("LUA script runtime error")
var errShaNotFound = errors.New("SHA not found")
var errCmdNotSupported = errors.New("Command not support in scripts")
var errNotLeader = errors.New("not the leader")
var errReadOnly = errors.New("read only")
var errCatchingUp = errors.New("catching up to leader")


func ConvertToLua(L *lua.LState, val resp.Value) lua.LValue {
	if val.IsNull() {
		return lua.LNil
	}
	switch val.Type() {
	case resp.Integer:
		return lua.LNumber(val.Integer())
	case resp.SimpleString, resp.BulkString:
		return lua.LString(val.String())
	case resp.Error:
		return lua.LString("ERR: " + val.String())
	case resp.Array:
		tbl := L.CreateTable(len(val.Array()), 0)
		for _, item := range val.Array() {
			tbl.Append(ConvertToLua(L, item))
		}
		return tbl
	}
	return lua.LNil
}

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
		log.Debugf("RETRIEVED %s\n", sha_sum)
	} else {
		fn, err = c.luastate.Load(strings.NewReader(script), "f_" + sha_sum)
		if err != nil {
			return empty_response, errLuaCompileFailure
		}
		c.luascripts[sha_sum] = fn.Proto
		log.Debugf("STORED %s\n", sha_sum)
	}
	c.luastate.Push(fn)
	if err := c.luastate.PCall(0, 1, nil); err != nil {
		return empty_response, errLuaRunFailure
	}
	ret := c.luastate.Get(-1) // returned value
	c.luastate.Pop(1)  // remove received value

	log.Debugf("RET type %s, val %s\n", ret.Type(), ret.String())

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
	log.Debugf("RETRIEVED %s\n", sha_sum)
	c.luastate.Push(fn)
	if err := c.luastate.PCall(0, 1, nil); err != nil {
		return empty_response, errLuaRunFailure
	}
	ret := c.luastate.Get(-1) // returned value
	c.luastate.Pop(1)  // remove received value

	log.Debugf("RET type %s, val %s\n", ret.Type(), ret.String())

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

	log.Debugf("SCRIPT source:\n%s\n\n", script)
	sha_sum := fmt.Sprintf("%x", sha1.Sum([]byte(script)))

	fn, err := c.luastate.Load(strings.NewReader(script), "f_" + sha_sum)
	if err != nil {
		return empty_response, errLuaCompileFailure
	}
	c.luascripts[sha_sum] = fn.Proto
	log.Debugf("STORED %s\n", sha_sum)

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

func (c *Controller) commandInScript(msg *server.Message) (
	res resp.Value, d commandDetailsT, err error,
) {
	switch msg.Command {
	default:
		err = fmt.Errorf("unknown command '%s'", msg.Values[0])
	case "set":
		res, d, err = c.cmdSet(msg)
	case "fset":
		res, d, err = c.cmdFset(msg)
	case "del":
		res, d, err = c.cmdDel(msg)
	case "pdel":
		res, d, err = c.cmdPdel(msg)
	case "drop":
		res, d, err = c.cmdDrop(msg)
	case "expire":
		res, d, err = c.cmdExpire(msg)
	case "persist":
		res, d, err = c.cmdPersist(msg)
	case "ttl":
		res, err = c.cmdTTL(msg)
	case "stats":
		res, err = c.cmdStats(msg)
	case "scan":
		res, err = c.cmdScan(msg)
	case "nearby":
		res, err = c.cmdNearby(msg)
	case "within":
		res, err = c.cmdWithin(msg)
	case "intersects":
		res, err = c.cmdIntersects(msg)
	case "search":
		res, err = c.cmdSearch(msg)
	case "bounds":
		res, err = c.cmdBounds(msg)
	case "get":
		res, err = c.cmdGet(msg)
	case "jget":
		res, err = c.cmdJget(msg)
	case "jset":
		res, d, err = c.cmdJset(msg)
	case "jdel":
		res, d, err = c.cmdJdel(msg)
	case "type":
		res, err = c.cmdType(msg)
	case "keys":
		res, err = c.cmdKeys(msg)
	}
	return
}

func (c *Controller) handleCommandInScript(cmd string, args ...string) (result resp.Value, errval resp.Value) {
	msg := &server.Message{}
	msg.OutputType = server.RESP
	msg.Command = strings.ToLower(cmd)
	msg.Values = append(msg.Values, resp.StringValue(msg.Command))
	for _, arg := range args {
		msg.Values = append(msg.Values, resp.StringValue(arg))
	}

	var write bool

	// choose the locking strategy
	switch msg.Command {
	default:
		c.mu.RLock()
		defer c.mu.RUnlock()
	case "ping", "echo", "auth", "massinsert", "shutdown", "gc",
		"sethook", "pdelhook", "delhook",
		"follow", "readonly", "config", "output", "client",
		"aofshrink",
		"eval", "evalsha", "script load", "script exists", "script flush":
		return resp.NullValue(), resp.ErrorValue(errCmdNotSupported)
	case "set", "del", "drop", "fset", "flushdb", "expire", "persist", "jset", "pdel":
		// write operations
		write = true
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.config.FollowHost != "" {
			return resp.NullValue(), resp.ErrorValue(errNotLeader)
		}
		if c.config.ReadOnly {
			return resp.NullValue(), resp.ErrorValue(errReadOnly)
		}
	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks", "search",
		"ttl", "bounds", "server", "info", "type", "jget":
		// read operations
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.config.FollowHost != "" && !c.fcuponce {
			return resp.NullValue(), resp.ErrorValue(errCatchingUp)
		}
	}

	res, d, err := c.commandInScript(msg)
	if err != nil {
		return resp.NullValue(), resp.ErrorValue(err)
	}

	if write {
		if err := c.writeAOF(resp.ArrayValue(msg.Values), &d); err != nil {
			return resp.NullValue(), resp.ErrorValue(err)
		}
	}

	return res, resp.NullValue()
}
