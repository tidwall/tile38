package controller

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/tile38/controller/server"
	//"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/resp"
	"github.com/yuin/gopher-lua"
)

const (
	INI_LUA_POOL_SIZE = 5
	MAX_LUA_POOL_SIZE = 1000
)

var errShaNotFound = errors.New("sha not found")
var errCmdNotSupported = errors.New("command not supported in scripts")
var errNotLeader = errors.New("not the leader")
var errReadOnly = errors.New("read only")
var errCatchingUp = errors.New("catching up to leader")
var errNoLuasAvailable = errors.New("no interpreters available")

// Go-routine-safe pool of read-to-go lua states
type lStatePool struct {
	m     sync.Mutex
	c     *Controller
	saved []*lua.LState
	total int
}

func (c *Controller) NewPool() *lStatePool {
	pl := &lStatePool{
		saved: make([]*lua.LState, 0),
		c: c,
	}
	// Fill the pool with some ready handlers
	for i := 0; i < INI_LUA_POOL_SIZE; i++ {
		pl.Put(pl.New())
		pl.total += 1
	}
	return pl
}

func (pl *lStatePool) Get() (*lua.LState, error) {
	pl.m.Lock()
	defer pl.m.Unlock()
	n := len(pl.saved)
	if n == 0 {
		if pl.total >= MAX_LUA_POOL_SIZE {
			return nil, errNoLuasAvailable
		}
		pl.total += 1
		return pl.New(), nil
	}
	x := pl.saved[n-1]
	pl.saved = pl.saved[0 : n-1]
	return x, nil
}

func (pl *lStatePool) New() *lua.LState {
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
		//log.Debugf("ARGS %s\n", args)
		return
	}
	call := func(ls *lua.LState) int {
		evalCmd, args := get_args(ls)
		if res, err := pl.c.luaTile38Call(evalCmd, args[0], args[1:]...); err != nil {
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
		if res, err := pl.c.luaTile38Call(evalCmd, args[0], args[1:]...); err != nil {
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
	var exports = map[string]lua.LGFunction {
		"call": call,
		"pcall": pcall,
		"error_reply": error_reply,
		"status_reply": status_reply,
		"sha1hex": sha1hex,
	}
	L.SetGlobal("tile38", L.SetFuncs(L.NewTable(), exports))
	return L
}

func (pl *lStatePool) Put(L *lua.LState) {
	pl.m.Lock()
	pl.saved = append(pl.saved, L)
	pl.m.Unlock()
}

func (pl *lStatePool) Shutdown() {
	pl.m.Lock()
	for _, L := range pl.saved {
		L.Close()
	}
	pl.m.Unlock()
}

// Go-routine-safe map of compiled scripts
type lScriptMap struct {
	m       sync.Mutex
	scripts map[string]*lua.FunctionProto
}

func (sm *lScriptMap) Get(key string) (script *lua.FunctionProto, ok bool) {
	sm.m.Lock()
	script, ok = sm.scripts[key]
	sm.m.Unlock()
	return
}

func (sm *lScriptMap) Put(key string, script *lua.FunctionProto) {
	sm.m.Lock()
	sm.scripts[key] = script
	sm.m.Unlock()
}

func (sm *lScriptMap) Flush() {
	sm.m.Lock()
	sm.scripts = make(map[string]*lua.FunctionProto)
	sm.m.Unlock()
}

func (c *Controller) NewScriptMap() *lScriptMap {
	return &lScriptMap{
		scripts: make(map[string]*lua.FunctionProto),
	}
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
			cb = func(lk lua.LValue, lv lua.LValue){
				values = append(values, ConvertToResp(lv))
			}
		} else { // map
			cb = func(lk lua.LValue, lv lua.LValue){
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
		if b, err := json.Marshal(val.String()); err == nil {
			return string(b)
		} else {
			panic(err)
		}
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
	ls.SetGlobal("EVAL_CMD", lua.LNil)
}

func Sha1Sum(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
    return hex.EncodeToString(h.Sum(nil))
}

// Replace newlines with literal \n since RESP errors cannot have newlines
func makeSafeErr(err error) error {
	return errors.New(strings.Replace(err.Error(), "\n", `\n`, -1))
}

// Run eval/evalro/evalna command or it's -sha variant
func (c* Controller) cmdEvalUnified(scriptIsSha bool, msg *server.Message) (res resp.Value, err error) {
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

	luaState, err := c.luapool.Get()
	if err != nil {
		return
	}
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
		if vs, arg, ok = tokenval(vs); !ok || arg == "" {
			err = errInvalidNumberOfArguments
			return
		}
		args_tbl.Append(lua.LString(arg))
	}

	var sha_sum string
	if scriptIsSha {
		sha_sum = script
	} else {
		sha_sum = Sha1Sum(script)
	}

	luaState.SetGlobal("KEYS", keys_tbl)
	luaState.SetGlobal("ARGS", args_tbl)
	luaState.SetGlobal("EVAL_CMD", lua.LString(msg.Command))

	compiled, ok := c.luascripts.Get(sha_sum)
	var fn *lua.LFunction
	if ok {
		fn = &lua.LFunction{
			IsG: false,
			Env: luaState.Env,

			Proto:     compiled,
			GFunction: nil,
			Upvalues:  make([]*lua.Upvalue, 0),
		}
		//log.Debugf("RETRIEVED %s\n", sha_sum)
	} else if scriptIsSha {
		err = errShaNotFound
		return
	} else {
		fn, err = luaState.Load(strings.NewReader(script), "f_" + sha_sum)
		if err != nil {
			return server.NOMessage, makeSafeErr(err)
		}
		c.luascripts.Put(sha_sum, fn.Proto)
		//log.Debugf("STORED %s\n", sha_sum)
	}
	luaState.Push(fn)
	defer luaStateCleanup(luaState)

	if err := luaState.PCall(0, 1, nil); err != nil {
		return server.NOMessage, makeSafeErr(err)
	}
	ret := luaState.Get(-1) // returned value
	luaState.Pop(1)

	//log.Debugf("RET type %s, val %s\n", ret.Type(), ret.String())

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

func (c* Controller) cmdScriptLoad(msg *server.Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var script string
	if vs, script, ok = tokenval(vs); !ok || script == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	//log.Debugf("SCRIPT source:\n%s\n\n", script)
	sha_sum := Sha1Sum(script)

	luaState, err := c.luapool.Get()
	if err != nil {
		return server.NOMessage, err
	}
	defer c.luapool.Put(luaState)

	fn, err := luaState.Load(strings.NewReader(script), "f_" + sha_sum)
	if err != nil {
		return server.NOMessage, makeSafeErr(err)
	}
	c.luascripts.Put(sha_sum, fn.Proto)
	//log.Debugf("STORED %s\n", sha_sum)

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

func (c* Controller) cmdScriptExists(msg *server.Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var sha_sum string
	var results []int
	var ires int
	for len(vs) > 0 {
		if vs, sha_sum, ok = tokenval(vs); !ok || sha_sum == "" {
			return server.NOMessage, errInvalidNumberOfArguments
		}
		_, ok = c.luascripts.Get(sha_sum)
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

func (c* Controller) cmdScriptFlush(msg *server.Message) (resp.Value, error) {
	start := time.Now()
	c.luascripts.Flush()

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

func (c *Controller) luaTile38Call(evalcmd string, cmd string, args ...string) (resp.Value, error) {
	msg := &server.Message{}
	msg.OutputType = server.RESP
	msg.Command = strings.ToLower(cmd)
	msg.Values = append(msg.Values, resp.StringValue(msg.Command))
	for _, arg := range args {
		msg.Values = append(msg.Values, resp.StringValue(arg))
	}

	switch msg.Command {
	case "ping", "echo", "auth", "massinsert", "shutdown", "gc",
		"sethook", "pdelhook", "delhook",
		"follow", "readonly", "config", "output", "client",
		"aofshrink",
		"script load", "script exists", "script flush",
		"eval", "evalsha", "evalro", "evalrosha", "evalna", "evalnasha":
		return resp.NullValue(), errCmdNotSupported
	}

	switch evalcmd {
	case "eval", "evalsha":
		return c.luaTile38AtomicRW(msg)
	case "evalro", "evalrosha":
		return c.luaTile38AtomicRO(msg)
	case "evalna", "evalnasha":
		return c.luaTile38NonAtomic(msg)
	}

	return resp.NullValue(), errCmdNotSupported
}


// The eval command has already got the lock. No locking on the call from within the script.
func (c *Controller) luaTile38AtomicRW(msg *server.Message) (resp.Value, error) {
	var write bool

	switch msg.Command {
	default:
		return resp.NullValue(), errCmdNotSupported
	case "set", "del", "drop", "fset", "flushdb", "expire", "persist", "jset", "pdel":
		// write operations
		write = true
		if c.config.followHost() != "" {
			return resp.NullValue(), errNotLeader
		}
		if c.config.readOnly() {
			return resp.NullValue(), errReadOnly
		}
	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks", "search",
		"ttl", "bounds", "server", "info", "type", "jget":
		// read operations
		if c.config.followHost() != "" && !c.fcuponce {
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

func (c *Controller) luaTile38AtomicRO(msg *server.Message) (resp.Value, error) {
	switch msg.Command {
	default:
		return resp.NullValue(), errCmdNotSupported

	case "set", "del", "drop", "fset", "flushdb", "expire", "persist", "jset", "pdel":
		return resp.NullValue(), errReadOnly

	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks", "search",
		"ttl", "bounds", "server", "info", "type", "jget":
		// read operations
		if c.config.followHost() != "" && !c.fcuponce {
			return resp.NullValue(), errCatchingUp
		}
	}

	res, _, err := c.commandInScript(msg)
	if err != nil {
		return resp.NullValue(), err
	}

	return res, nil
}

func (c *Controller) luaTile38NonAtomic(msg *server.Message) (resp.Value, error){
	var write bool

	// choose the locking strategy
	switch msg.Command {
	default:
		return resp.NullValue(), errCmdNotSupported
	case "set", "del", "drop", "fset", "flushdb", "expire", "persist", "jset", "pdel":
		// write operations
		write = true
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.config.followHost() != "" {
			return resp.NullValue(), errNotLeader
		}
		if c.config.readOnly() {
			return resp.NullValue(), errReadOnly
		}
	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks", "search",
		"ttl", "bounds", "server", "info", "type", "jget":
		// read operations
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.config.followHost() != "" && !c.fcuponce {
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
