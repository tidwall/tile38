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

var errShaNotFound = errors.New("sha not found")
var errCmdNotSupported = errors.New("command not supported in scripts")
var errNotLeader = errors.New("not the leader")
var errReadOnly = errors.New("read only")
var errCatchingUp = errors.New("catching up to leader")


// Convert RESP value to lua LValue
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
	return lua.LString("ERR: unknown RESP type: " + val.Type().String())
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
		var cb func(lk lua.LValue, lv lua.LValue)
		tbl := val.(*lua.LTable)

		if tbl.Len() != 0 { // list
			cb = func(lk lua.LValue, lv lua.LValue){
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
	return resp.ErrorValue(errors.New("Unsupported lua type: " + val.Type().String()))
}

// Convert lua LValue to JSON string
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
		var values []string
		var cb func(lk lua.LValue, lv lua.LValue)
		var start, end string

		tbl := val.(*lua.LTable)
		if tbl.Len() != 0 { // list
			start = `[`
			end = `]`
			cb = func(lk lua.LValue, lv lua.LValue){
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
	return "Unsupported lua type: " + val.Type().String()
}

func luaStateCleanup(ls *lua.LState) {
	ls.SetGlobal("KEYS", lua.LNil)
	ls.SetGlobal("ARGS", lua.LNil)
	ls.Pop(1)
}

// TODO: Refactor common bits from all these functions
func (c* Controller) cmdEval(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var script, numkeys_str, key, arg string
	if vs, script, ok = tokenval(vs); !ok || script == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	if vs, numkeys_str, ok = tokenval(vs); !ok || numkeys_str == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	var i, numkeys uint64
	if numkeys, err = strconv.ParseUint(numkeys_str, 10, 64); err != nil {
		err = errInvalidArgument(numkeys_str)
		return
	}

	luaState := c.luapool.Get()
	defer c.luapool.Put(luaState)

	keys_tbl := luaState.CreateTable(int(numkeys), 0)
	for i = 0; i < numkeys; i++ {
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		keys_tbl.Append(lua.LString(key))
	}

	args_tbl := luaState.CreateTable(len(vs), 0)
	for len(vs) > 0 {
		if vs, arg, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		args_tbl.Append(lua.LString(arg))
	}

	sha_sum := fmt.Sprintf("%x", sha1.Sum([]byte(script)))

	luaState.SetGlobal("KEYS", keys_tbl)
	luaState.SetGlobal("ARGS", args_tbl)

	compiled, ok := c.luascripts[sha_sum]
	var fn *lua.LFunction
	if ok {
		fn = &lua.LFunction{
			IsG: false,
			Env: luaState.Env,

			Proto:     compiled,
			GFunction: nil,
			Upvalues:  make([]*lua.Upvalue, 0),
		}
		log.Debugf("RETRIEVED %s\n", sha_sum)
	} else {
		fn, err = luaState.Load(strings.NewReader(script), "f_" + sha_sum)
		if err != nil {
			return server.NOMessage, err
		}
		c.luascripts[sha_sum] = fn.Proto
		log.Debugf("STORED %s\n", sha_sum)
	}
	luaState.Push(fn)
	defer luaStateCleanup(luaState)

	if err := luaState.PCall(0, 1, nil); err != nil {
		return server.NOMessage, err
	}
	ret := luaState.Get(-1) // returned value

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
	return server.NOMessage, nil
}

func (c* Controller) cmdEvalSha(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var sha_sum, numkeys_str, key, arg string
	if vs, sha_sum, ok = tokenval(vs); !ok || sha_sum == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	if vs, numkeys_str, ok = tokenval(vs); !ok || numkeys_str == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	var i, numkeys uint64
	if numkeys, err = strconv.ParseUint(numkeys_str, 10, 64); err != nil {
		err = errInvalidArgument(numkeys_str)
		return
	}

	luaState := c.luapool.Get()
	defer c.luapool.Put(luaState)

	keys_tbl := luaState.CreateTable(int(numkeys), 0)
	for i = 0; i < numkeys; i++ {
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		keys_tbl.Append(lua.LString(key))
	}

	args_tbl := luaState.CreateTable(len(vs), 0)
	for len(vs) > 0 {
		if vs, arg, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		args_tbl.Append(lua.LString(arg))
	}

	luaState.SetGlobal("KEYS", keys_tbl)
	luaState.SetGlobal("ARGS", args_tbl)

	compiled, ok := c.luascripts[sha_sum]
	if !ok {
		err = errShaNotFound
		return
	}
	fn := &lua.LFunction{
		IsG: false,
		Env: luaState.Env,

		Proto:     compiled,
		GFunction: nil,
		Upvalues:  make([]*lua.Upvalue, 0),
	}
	log.Debugf("RETRIEVED %s\n", sha_sum)
	luaState.Push(fn)
	defer luaStateCleanup(luaState)

	if err := luaState.PCall(0, 1, nil); err != nil {
		return server.NOMessage, err
	}
	ret := luaState.Get(-1) // returned value

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
	return server.NOMessage, nil
}

func (c* Controller) cmdScriptLoad(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var script string
	if vs, script, ok = tokenval(vs); !ok || script == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	log.Debugf("SCRIPT source:\n%s\n\n", script)
	sha_sum := fmt.Sprintf("%x", sha1.Sum([]byte(script)))

	luaState := c.luapool.Get()
	defer c.luapool.Put(luaState)

	fn, err := luaState.Load(strings.NewReader(script), "f_" + sha_sum)
	if err != nil {
		return server.NOMessage, err
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
	return server.NOMessage, nil
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

func (c *Controller) handleCommandInScript(cmd string, args ...string) (result resp.Value, err error) {
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
		return resp.NullValue(), errCmdNotSupported
	case "set", "del", "drop", "fset", "flushdb", "expire", "persist", "jset", "pdel":
		// write operations
		write = true
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.config.FollowHost != "" {
			return resp.NullValue(), errNotLeader
		}
		if c.config.ReadOnly {
			return resp.NullValue(), errReadOnly
		}
	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks", "search",
		"ttl", "bounds", "server", "info", "type", "jget":
		// read operations
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.config.FollowHost != "" && !c.fcuponce {
			return resp.NullValue(), errCatchingUp
		}
	}

	res, d, err := c.commandInScript(msg)
	if err != nil {
		return resp.NullValue(), err
	}

	if write {
		if err := c.writeAOF(resp.ArrayValue(msg.Values), &d); err != nil {
			return resp.NullValue(), err
		}
	}

	return res, nil
}
