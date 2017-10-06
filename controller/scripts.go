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

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/server"
	"github.com/yuin/gopher-lua"
)

const (
	iniLuaPoolSize = 5
	maxLuaPoolSize = 1000
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

// NewPool returns a new pool of lua states
func (c *Controller) NewPool() *lStatePool {
	pl := &lStatePool{
		saved: make([]*lua.LState, iniLuaPoolSize),
		c:     c,
	}
	// Fill the pool with some ready handlers
	for i := 0; i < iniLuaPoolSize; i++ {
		pl.saved[i] = pl.New()
		pl.total++
	}
	return pl
}

func (pl *lStatePool) Get() (*lua.LState, error) {
	pl.m.Lock()
	defer pl.m.Unlock()
	n := len(pl.saved)
	if n == 0 {
		if pl.total >= maxLuaPoolSize {
			return nil, errNoLuasAvailable
		}
		pl.total++
		return pl.New(), nil
	}
	x := pl.saved[n-1]
	pl.saved = pl.saved[0 : n-1]
	return x, nil
}

// Prune removes some of the idle lua states from the pool
func (pl *lStatePool) Prune() {
	pl.m.Lock()
	n := len(pl.saved)
	if n > iniLuaPoolSize {
		// drop half of the idle states that is above the minimum
		dropNum := (n - iniLuaPoolSize) / 2
		if dropNum < 1 {
			dropNum = 1
		}
		newSaved := make([]*lua.LState, n-dropNum)
		copy(newSaved, pl.saved[dropNum:])
		pl.saved = newSaved
	}
	pl.m.Unlock()
}

func (pl *lStatePool) New() *lua.LState {
	L := lua.NewState()

	getArgs := func(ls *lua.LState) (evalCmd string, args []string) {
		evalCmd = ls.GetGlobal("EVAL_CMD").String()

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
		evalCmd, args := getArgs(ls)
		var numRet int
		if res, err := pl.c.luaTile38Call(evalCmd, args[0], args[1:]...); err != nil {
			ls.RaiseError("ERR %s", err.Error())
			numRet = 0
		} else {
			ls.Push(ConvertToLua(ls, res))
			numRet = 1
		}
		return numRet
	}
	pcall := func(ls *lua.LState) int {
		evalCmd, args := getArgs(ls)
		if res, err := pl.c.luaTile38Call(evalCmd, args[0], args[1:]...); err != nil {
			ls.Push(ConvertToLua(ls, resp.ErrorValue(err)))
		} else {
			ls.Push(ConvertToLua(ls, res))
		}
		return 1

	}
	errorReply := func(ls *lua.LState) int {
		tbl := L.CreateTable(0, 1)
		tbl.RawSetString("err", lua.LString(ls.ToString(1)))
		ls.Push(tbl)
		return 1
	}
	statusReply := func(ls *lua.LState) int {
		tbl := L.CreateTable(0, 1)
		tbl.RawSetString("ok", lua.LString(ls.ToString(1)))
		ls.Push(tbl)
		return 1
	}
	sha1hex := func(ls *lua.LState) int {
		shaSum := Sha1Sum(ls.ToString(1))
		ls.Push(lua.LString(shaSum))
		return 1
	}
	var exports = map[string]lua.LGFunction{
		"call":         call,
		"pcall":        pcall,
		"error_reply":  errorReply,
		"status_reply": statusReply,
		"sha1hex":      sha1hex,
	}
	L.SetGlobal("tile38", L.SetFuncs(L.NewTable(), exports))

	// Prohibit creating new globals in this state
	lockNewGlobals := func(ls *lua.LState) int {
		ls.RaiseError("attempt to create global variable '%s'", ls.ToString(2))
		return 0
	}
	mt := L.CreateTable(0, 1)
	mt.RawSetString("__newindex", L.NewFunction(lockNewGlobals))
	L.SetMetatable(L.Get(lua.GlobalsIndex), mt)

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

// NewScriptMap returns a new map with lua scripts
func (c *Controller) NewScriptMap() *lScriptMap {
	return &lScriptMap{
		scripts: make(map[string]*lua.FunctionProto),
	}
}

// ConvertToLua converts RESP value to lua LValue
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

// ConvertToRESP convert lua LValue to RESP value
func ConvertToRESP(val lua.LValue) resp.Value {
	switch val.Type() {
	case lua.LTNil:
		return resp.NullValue()
	case lua.LTBool:
		if val == lua.LTrue {
			return resp.IntegerValue(1)
		}
		return resp.NullValue()
	case lua.LTNumber:
		float := float64(val.(lua.LNumber))
		if math.IsNaN(float) || math.IsInf(float, 0) {
			return resp.FloatValue(float)
		}
		return resp.IntegerValue(int(math.Floor(float)))
	case lua.LTString:
		return resp.StringValue(val.String())
	case lua.LTTable:
		var values []resp.Value
		var specialValues []resp.Value
		var cb func(lk lua.LValue, lv lua.LValue)
		tbl := val.(*lua.LTable)

		if tbl.Len() != 0 { // list
			cb = func(lk lua.LValue, lv lua.LValue) {
				values = append(values, ConvertToRESP(lv))
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
					[]resp.Value{ConvertToRESP(lk), ConvertToRESP(lv)}))
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

// ConvertToJSON converts lua LValue to JSON string
func ConvertToJSON(val lua.LValue) string {
	switch val.Type() {
	case lua.LTNil:
		return "null"
	case lua.LTBool:
		if val == lua.LTrue {
			return "true"
		}
		return "false"
	case lua.LTNumber:
		return val.String()
	case lua.LTString:
		if b, err := json.Marshal(val.String()); err != nil {
			panic(err)
		} else {
			return string(b)
		}
	case lua.LTTable:
		var values []string
		var cb func(lk lua.LValue, lv lua.LValue)
		var start, end string

		tbl := val.(*lua.LTable)
		if tbl.Len() != 0 { // list
			start = `[`
			end = `]`
			cb = func(lk lua.LValue, lv lua.LValue) {
				values = append(values, ConvertToJSON(lv))
			}
		} else { // map
			start = `{`
			end = `}`
			cb = func(lk lua.LValue, lv lua.LValue) {
				values = append(
					values, ConvertToJSON(lk)+`:`+ConvertToJSON(lv))
			}
		}
		tbl.ForEach(cb)
		return start + strings.Join(values, `,`) + end
	}
	return "Unsupported lua type: " + val.Type().String()
}

func luaSetRawGlobals(ls *lua.LState, tbl map[string]lua.LValue) {
	gt := ls.Get(lua.GlobalsIndex).(*lua.LTable)
	for key, val := range tbl {
		gt.RawSetString(key, val)
	}
}

// Sha1Sum returns a string with hex representation of sha1 sum of a given string
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
func (c *Controller) cmdEvalUnified(scriptIsSha bool, msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var script, numkeysStr, key, arg string
	if vs, script, ok = tokenval(vs); !ok || script == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	if vs, numkeysStr, ok = tokenval(vs); !ok || numkeysStr == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	var i, numkeys uint64
	if numkeys, err = strconv.ParseUint(numkeysStr, 10, 64); err != nil {
		err = errInvalidArgument(numkeysStr)
		return
	}

	luaState, err := c.luapool.Get()
	if err != nil {
		return
	}
	defer c.luapool.Put(luaState)

	keysTbl := luaState.CreateTable(int(numkeys), 0)
	for i = 0; i < numkeys; i++ {
		if vs, key, ok = tokenval(vs); !ok || key == "" {
			err = errInvalidNumberOfArguments
			return
		}
		keysTbl.Append(lua.LString(key))
	}

	argsTbl := luaState.CreateTable(len(vs), 0)
	for len(vs) > 0 {
		if vs, arg, ok = tokenval(vs); !ok || arg == "" {
			err = errInvalidNumberOfArguments
			return
		}
		argsTbl.Append(lua.LString(arg))
	}

	var shaSum string
	if scriptIsSha {
		shaSum = script
	} else {
		shaSum = Sha1Sum(script)
	}

	luaSetRawGlobals(
		luaState, map[string]lua.LValue{
			"KEYS":     keysTbl,
			"ARGV":     argsTbl,
			"EVAL_CMD": lua.LString(msg.Command),
		})

	compiled, ok := c.luascripts.Get(shaSum)
	var fn *lua.LFunction
	if ok {
		fn = &lua.LFunction{
			IsG: false,
			Env: luaState.Env,

			Proto:     compiled,
			GFunction: nil,
			Upvalues:  make([]*lua.Upvalue, 0),
		}
	} else if scriptIsSha {
		err = errShaNotFound
		return
	} else {
		fn, err = luaState.Load(strings.NewReader(script), "f_"+shaSum)
		if err != nil {
			return server.NOMessage, makeSafeErr(err)
		}
		c.luascripts.Put(shaSum, fn.Proto)
	}
	luaState.Push(fn)
	defer luaSetRawGlobals(
		luaState, map[string]lua.LValue{
			"KEYS":     lua.LNil,
			"ARGV":     lua.LNil,
			"EVAL_CMD": lua.LNil,
		})

	if err := luaState.PCall(0, 1, nil); err != nil {
		return server.NOMessage, makeSafeErr(err)
	}
	ret := luaState.Get(-1) // returned value
	luaState.Pop(1)

	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"result":` + ConvertToJSON(ret))
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case server.RESP:
		return ConvertToRESP(ret), nil
	}
	return server.NOMessage, nil
}

func (c *Controller) cmdScriptLoad(msg *server.Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var script string
	if vs, script, ok = tokenval(vs); !ok || script == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	shaSum := Sha1Sum(script)

	luaState, err := c.luapool.Get()
	if err != nil {
		return server.NOMessage, err
	}
	defer c.luapool.Put(luaState)

	fn, err := luaState.Load(strings.NewReader(script), "f_"+shaSum)
	if err != nil {
		return server.NOMessage, makeSafeErr(err)
	}
	c.luascripts.Put(shaSum, fn.Proto)

	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"result":"` + shaSum + `"`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case server.RESP:
		return resp.StringValue(shaSum), nil
	}
	return server.NOMessage, nil
}

func (c *Controller) cmdScriptExists(msg *server.Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var shaSum string
	var results []int
	var ires int
	for len(vs) > 0 {
		if vs, shaSum, ok = tokenval(vs); !ok || shaSum == "" {
			return server.NOMessage, errInvalidNumberOfArguments
		}
		_, ok = c.luascripts.Get(shaSum)
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
		var resArray []string
		for _, ires := range results {
			resArray = append(resArray, fmt.Sprintf("%d", ires))
		}
		buf.WriteString(`,"result":[` + strings.Join(resArray, ",") + `]`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case server.RESP:
		var resArray []resp.Value
		for _, ires := range results {
			resArray = append(resArray, resp.IntegerValue(ires))
		}
		return resp.ArrayValue(resArray), nil
	}
	return resp.SimpleStringValue(""), nil
}

func (c *Controller) cmdScriptFlush(msg *server.Message) (resp.Value, error) {
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

func (c *Controller) luaTile38NonAtomic(msg *server.Message) (resp.Value, error) {
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
