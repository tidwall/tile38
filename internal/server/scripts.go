package server

import (
	"bytes"
	"context"
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

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geo"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/deadline"
	"github.com/tidwall/tile38/internal/glob"
	"github.com/tidwall/tile38/internal/log"
	lua "github.com/yuin/gopher-lua"
	luajson "layeh.com/gopher-json"
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
var errTimeout = errors.New("timeout")

// Go-routine-safe pool of read-to-go lua states
type lStatePool struct {
	m     sync.Mutex
	s     *Server
	saved []*lua.LState
	total int
}

// newPool returns a new pool of lua states
func (s *Server) newPool() *lStatePool {
	pl := &lStatePool{
		saved: make([]*lua.LState, iniLuaPoolSize),
		s:     s,
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
		pl.total -= dropNum
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
		if res, err := pl.s.luaTile38Call(evalCmd, args[0], args[1:]...); err != nil {
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
		if res, err := pl.s.luaTile38Call(evalCmd, args[0], args[1:]...); err != nil {
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
	distanceTo := func(ls *lua.LState) int {
		dt := geo.DistanceTo(
			float64(ls.ToNumber(1)),
			float64(ls.ToNumber(2)),
			float64(ls.ToNumber(3)),
			float64(ls.ToNumber(4)))
		ls.Push(lua.LNumber(dt))
		return 1
	}
	iterate := func(ls *lua.LState) int {
		evalCmd := ls.GetGlobal("EVAL_CMD").String()
		callback := ls.ToFunction(1)
		cmd := ls.ToString(2)
		nargs := ls.GetTop()

		var vs []string
		for i := 3; i <= nargs; i++ {
			vs = append(vs, ls.ToString(i))
		}

		ctx := ls.Context()
		var dl *deadline.Deadline
		if ctx != nil {
			dlt, ok := ls.Context().Deadline()
			if ok {
				dl = deadline.New(dlt)
			}
		}

		itr := ls.NewUserData()
		itr.Value = &luaScanIterator{
			gomt: ls.GetTypeMetatable(luaGeoJSONObjectTypeName),
		}
		itr.Metatable = ls.GetTypeMetatable(luaScanIteratorTypeName)

		coll := &luaScanCollector{
			ls:  ls,
			f:   callback,
			itr: itr,
		}

		err := pl.s.luaTile38Iterate(coll, dl, evalCmd, strings.ToLower(cmd), vs)
		if err != nil {
			ls.RaiseError("%v", err)
		}
		ls.Push(lua.LString(strconv.FormatUint(coll.cursor, 10)))
		return 1
	}
	fieldIndexes := func(ls *lua.LState) int {
		colName := ls.ToString(1)
		col := pl.s.getCol(colName)
		if col == nil {
			ls.RaiseError("unknown key %s", colName)
		}
		fmap := col.FieldMap()

		nargs := ls.GetTop()
		nret := 0
		for i := 2; i <= nargs; i++ {
			name := ls.ToString(i)
			fi, ok := fmap[name]
			if !ok {
				ls.RaiseError("unknown field %s", name)
			}
			ls.Push(lua.LNumber(fi + 1))
			nret++
		}
		return nret
	}
	getObject := func(ls *lua.LState) int {
		evalCmd := ls.GetGlobal("EVAL_CMD").String()
		colName := ls.ToString(1)
		id := ls.ToString(2)
		result, err := pl.s.luaTile38Get(ls, evalCmd, colName, id)
		if err != nil {
			ls.RaiseError("%v", err)
		}
		ls.Push(result)
		return 1
	}
	var exports = map[string]lua.LGFunction{
		"call":          call,
		"pcall":         pcall,
		"error_reply":   errorReply,
		"status_reply":  statusReply,
		"sha1hex":       sha1hex,
		"distance_to":   distanceTo,
		"iterate":       iterate,
		"field_indexes": fieldIndexes,
		"get":           getObject,
	}
	L.SetGlobal("tile38", L.SetFuncs(L.NewTable(), exports))

	// Load json
	L.SetGlobal("json", L.Get(luajson.Loader(L)))

	// register the custom types to expose call results
	registerLuaResultTypes(L)

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
func (s *Server) newScriptMap() *lScriptMap {
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
func (s *Server) cmdEvalUnified(scriptIsSha bool, msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]

	var ok bool
	var script, numkeysStr, key, arg string
	if vs, script, ok = tokenval(vs); !ok || script == "" {
		return NOMessage, errInvalidNumberOfArguments
	}

	if vs, numkeysStr, ok = tokenval(vs); !ok || numkeysStr == "" {
		return NOMessage, errInvalidNumberOfArguments
	}

	var i, numkeys uint64
	if numkeys, err = strconv.ParseUint(numkeysStr, 10, 64); err != nil {
		err = errInvalidArgument(numkeysStr)
		return
	}

	luaState, err := s.luapool.Get()
	if err != nil {
		return
	}
	luaDeadline := lua.LNil
	if msg.Deadline != nil {
		dlTime := msg.Deadline.GetDeadlineTime()
		ctx, cancel := context.WithDeadline(context.Background(), dlTime)
		defer cancel()
		luaState.SetContext(ctx)
		defer luaState.RemoveContext()
		luaDeadline = lua.LNumber(float64(dlTime.UnixNano()) / 1e9)
	}
	defer s.luapool.Put(luaState)

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
			"DEADLINE": luaDeadline,
			"EVAL_CMD": lua.LString(msg.Command()),
		})

	compiled, ok := s.luascripts.Get(shaSum)
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
			return NOMessage, makeSafeErr(err)
		}
		s.luascripts.Put(shaSum, fn.Proto)
	}
	luaState.Push(fn)
	defer luaSetRawGlobals(
		luaState, map[string]lua.LValue{
			"KEYS":     lua.LNil,
			"ARGV":     lua.LNil,
			"DEADLINE": lua.LNil,
			"EVAL_CMD": lua.LNil,
		})
	if err := luaState.PCall(0, 1, nil); err != nil {
		if strings.Contains(err.Error(), "context deadline exceeded") {
			msg.Deadline.Check()
		}
		log.Debugf("%v", err.Error())
		return NOMessage, makeSafeErr(err)
	}
	ret := luaState.Get(-1) // returned value
	luaState.Pop(1)

	switch msg.OutputType {
	case JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"result":` + ConvertToJSON(ret))
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case RESP:
		return ConvertToRESP(ret), nil
	}
	return NOMessage, nil
}

func (s *Server) cmdScriptLoad(msg *Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Args[1:]

	var ok bool
	var script string
	if vs, script, ok = tokenval(vs); !ok || script == "" {
		return NOMessage, errInvalidNumberOfArguments
	}

	shaSum := Sha1Sum(script)

	luaState, err := s.luapool.Get()
	if err != nil {
		return NOMessage, err
	}
	defer s.luapool.Put(luaState)

	fn, err := luaState.Load(strings.NewReader(script), "f_"+shaSum)
	if err != nil {
		return NOMessage, makeSafeErr(err)
	}
	s.luascripts.Put(shaSum, fn.Proto)

	switch msg.OutputType {
	case JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"result":"` + shaSum + `"`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case RESP:
		return resp.StringValue(shaSum), nil
	}
	return NOMessage, nil
}

func (s *Server) cmdScriptExists(msg *Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Args[1:]

	var ok bool
	var shaSum string
	var results []int
	var ires int
	for len(vs) > 0 {
		if vs, shaSum, ok = tokenval(vs); !ok || shaSum == "" {
			return NOMessage, errInvalidNumberOfArguments
		}
		_, ok = s.luascripts.Get(shaSum)
		if ok {
			ires = 1
		} else {
			ires = 0
		}
		results = append(results, ires)
	}

	switch msg.OutputType {
	case JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		var resArray []string
		for _, ires := range results {
			resArray = append(resArray, fmt.Sprintf("%d", ires))
		}
		buf.WriteString(`,"result":[` + strings.Join(resArray, ",") + `]`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case RESP:
		var resArray []resp.Value
		for _, ires := range results {
			resArray = append(resArray, resp.IntegerValue(ires))
		}
		return resp.ArrayValue(resArray), nil
	}
	return resp.SimpleStringValue(""), nil
}

func (s *Server) cmdScriptFlush(msg *Message) (resp.Value, error) {
	start := time.Now()
	s.luascripts.Flush()

	switch msg.OutputType {
	case JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case RESP:
		return resp.StringValue("OK"), nil
	}
	return resp.SimpleStringValue(""), nil
}

func (s *Server) commandInScript(msg *Message) (
	res resp.Value, d commandDetails, err error,
) {
	switch msg.Command() {
	default:
		err = fmt.Errorf("unknown command '%s'", msg.Args[0])
	case "set":
		res, d, err = s.cmdSet(msg, true)
	case "fset":
		res, d, err = s.cmdFset(msg)
	case "del":
		res, d, err = s.cmdDel(msg)
	case "pdel":
		res, d, err = s.cmdPdel(msg)
	case "drop":
		res, d, err = s.cmdDrop(msg)
	case "expire":
		res, d, err = s.cmdExpire(msg)
	case "rename":
		res, d, err = s.cmdRename(msg, false)
	case "renamenx":
		res, d, err = s.cmdRename(msg, true)
	case "persist":
		res, d, err = s.cmdPersist(msg)
	case "ttl":
		res, err = s.cmdTTL(msg)
	case "stats":
		res, err = s.cmdStats(msg)
	case "scan":
		res, err = s.cmdScan(msg)
	case "nearby":
		res, err = s.cmdNearby(msg)
	case "within":
		res, err = s.cmdWithin(msg)
	case "intersects":
		res, err = s.cmdIntersects(msg)
	case "search":
		res, err = s.cmdSearch(msg)
	case "bounds":
		res, err = s.cmdBounds(msg)
	case "get":
		res, err = s.cmdGet(msg)
	case "jget":
		res, err = s.cmdJget(msg)
	case "jset":
		res, d, err = s.cmdJset(msg)
	case "jdel":
		res, d, err = s.cmdJdel(msg)
	case "type":
		res, err = s.cmdType(msg)
	case "keys":
		res, err = s.cmdKeys(msg)
	case "test":
		res, err = s.cmdTest(msg)
	case "server":
		res, err = s.cmdServer(msg)
	}
	return
}

func (s *Server) luaTile38Call(evalcmd string, cmd string, args ...string) (resp.Value, error) {
	msg := &Message{}
	msg.OutputType = RESP
	msg.Args = append([]string{cmd}, args...)

	if msg.Command() == "timeout" {
		if err := rewriteTimeoutMsg(msg); err != nil {
			return resp.NullValue(), err
		}
	}

	switch msg.Command() {
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
		return s.luaTile38AtomicRW(msg)
	case "evalro", "evalrosha":
		return s.luaTile38AtomicRO(msg)
	case "evalna", "evalnasha":
		return s.luaTile38NonAtomic(msg)
	}

	return resp.NullValue(), errCmdNotSupported
}

// The eval command has already got the lock. No locking on the call from within the script.
func (s *Server) luaTile38AtomicRW(msg *Message) (resp.Value, error) {
	var write bool

	switch msg.Command() {
	default:
		return resp.NullValue(), errCmdNotSupported
	case "set", "del", "drop", "fset", "flushdb", "expire", "persist", "jset", "pdel",
		"rename", "renamenx":
		// write operations
		write = true
		if s.config.followHost() != "" {
			return resp.NullValue(), errNotLeader
		}
		if s.config.readOnly() {
			return resp.NullValue(), errReadOnly
		}
	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks", "search",
		"ttl", "bounds", "server", "info", "type", "jget", "test":
		// read operations
		if s.config.followHost() != "" && !s.fcuponce {
			return resp.NullValue(), errCatchingUp
		}
	}

	res, d, err := func() (res resp.Value, d commandDetails, err error) {
		if msg.Deadline != nil {
			if write {
				res = NOMessage
				err = errTimeoutOnCmd(msg.Command())
				return
			}
			defer func() {
				if msg.Deadline.Hit() {
					v := recover()
					if v != nil {
						if s, ok := v.(string); !ok || s != "deadline" {
							panic(v)
						}
					}
					res = NOMessage
					err = errTimeout
				}
			}()
		}
		return s.commandInScript(msg)
	}()
	if err != nil {
		return resp.NullValue(), err
	}

	if write {
		if err := s.writeAOF(msg.Args, &d); err != nil {
			return resp.NullValue(), err
		}
	}

	return res, nil
}

func (s *Server) luaTile38AtomicRO(msg *Message) (resp.Value, error) {
	switch msg.Command() {
	default:
		return resp.NullValue(), errCmdNotSupported

	case "set", "del", "drop", "fset", "flushdb", "expire", "persist", "jset", "pdel",
		"rename", "renamenx":
		// write operations
		return resp.NullValue(), errReadOnly

	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks", "search",
		"ttl", "bounds", "server", "info", "type", "jget", "test":
		// read operations
		if s.config.followHost() != "" && !s.fcuponce {
			return resp.NullValue(), errCatchingUp
		}
	}

	res, _, err := func() (res resp.Value, d commandDetails, err error) {
		if msg.Deadline != nil {
			defer func() {
				if msg.Deadline.Hit() {
					v := recover()
					if v != nil {
						if s, ok := v.(string); !ok || s != "deadline" {
							panic(v)
						}
					}
					res = NOMessage
					err = errTimeout
				}
			}()
		}
		return s.commandInScript(msg)
	}()
	if err != nil {
		return resp.NullValue(), err
	}

	return res, nil
}

func (s *Server) luaTile38NonAtomic(msg *Message) (resp.Value, error) {
	var write bool

	// choose the locking strategy
	switch msg.Command() {
	default:
		return resp.NullValue(), errCmdNotSupported
	case "set", "del", "drop", "fset", "flushdb", "expire", "persist", "jset", "pdel",
		"rename", "renamenx":
		// write operations
		write = true
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.config.followHost() != "" {
			return resp.NullValue(), errNotLeader
		}
		if s.config.readOnly() {
			return resp.NullValue(), errReadOnly
		}
	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks", "search",
		"ttl", "bounds", "server", "info", "type", "jget", "test":
		// read operations
		s.mu.RLock()
		defer s.mu.RUnlock()
		if s.config.followHost() != "" && !s.fcuponce {
			return resp.NullValue(), errCatchingUp
		}
	}

	res, d, err := func() (res resp.Value, d commandDetails, err error) {
		if msg.Deadline != nil {
			if write {
				res = NOMessage
				err = errTimeoutOnCmd(msg.Command())
				return
			}
			defer func() {
				if msg.Deadline.Hit() {
					v := recover()
					if v != nil {
						if s, ok := v.(string); !ok || s != "deadline" {
							panic(v)
						}
					}
					res = NOMessage
					err = errTimeout
				}
			}()
		}
		return s.commandInScript(msg)
	}()
	if err != nil {
		return resp.NullValue(), err
	}

	if write {
		if err := s.writeAOF(msg.Args, &d); err != nil {
			return resp.NullValue(), err
		}
	}

	return res, nil
}

func (s *Server) luaTile38Iterate(coll *luaScanCollector, dl *deadline.Deadline, evalcmd, cmd string, vs []string) (err error) {
	// Acquire a lock if we don't already have one
	switch evalcmd {
	case "evalna", "evalnasha":
		s.mu.RLock()
		defer s.mu.RUnlock()
	}

	// Ensure fully up to date
	if s.config.followHost() != "" && !s.fcuponce {
		return errCatchingUp
	}

	// Parse the command args
	var lfs liveFenceSwitches
	switch cmd {
	case "nearby":
		lfs, err = s.cmdSearchArgs(false, cmd, vs, nearbyTypes)
	case "within", "intersects":
		lfs, err = s.cmdSearchArgs(false, cmd, vs, withinOrIntersectsTypes)
	case "scan":
		lfs, err = s.cmdScanArgs(vs)
	case "search":
		lfs, err = s.cmdSeachValuesArgs(vs)
	default:
		err = errors.New("expected command to be nearby, within, intersects or scan")
	}

	if err != nil {
		return
	}

	// Ensure we clean up lfs if needed
	if lfs.usingLua() {
		defer lfs.Close()
		defer func() {
			if r := recover(); r != nil {
				err = errors.New(r.(string))
				return
			}
		}()
	}

	// Fencing doesn't make sense
	if lfs.fence {
		return errors.New("fence not supported")
	}

	sc, err := s.newScanner(
		coll, lfs.key, lfs.output, lfs.precision, lfs.glob, cmd == "search",
		lfs.cursor, lfs.limit, lfs.wheres, lfs.whereins, lfs.whereevals, lfs.nofields)

	if err != nil {
		return err
	}

	// If collection doesn't exist, just return
	if sc.col == nil {
		return nil
	}

	// Handle deadline exceeded errors
	if dl != nil {
		defer func() {
			if dl.Hit() {
				v := recover()
				if v != nil {
					if s, ok := v.(string); !ok || s != "deadline" {
						panic(v)
					}
				}
				err = errTimeout
			}
		}()
	}

	// For the duration of this call, we need to pretend we are operating as
	// a "evalro" command. This ensures that if the Lua callback function
	// makes any tile38 calls we disallow any write commands and do not
	// acquire additional locks if the original command was eval(sha?)na
	// to prevent deadlock
	coll.ls.SetGlobal("EVAL_CMD", lua.LString("evalro"))
	defer coll.ls.SetGlobal("EVAL_CMD", lua.LString(evalcmd))

	// Run the scan operation
	switch cmd {
	case "nearby":
		maxDist := lfs.obj.(*geojson.Circle).Meters()
		iter := func(id string, o geojson.Object, fields []float64, dist float64) bool {
			if s.hasExpired(lfs.key, id) {
				return true
			}

			if maxDist > 0 && dist > maxDist {
				return false
			}

			meters := 0.0
			if lfs.distance {
				meters = dist
			}
			return sc.writeObject(ScanObjectParams{
				id:       id,
				o:        o,
				fields:   fields,
				distance: meters,
				noLock:   true,
			})
		}
		sc.col.Nearby(lfs.obj, sc, dl, iter)
	case "within":
		sc.col.Within(lfs.obj, lfs.sparse, sc, dl, func(
			id string, o geojson.Object, fields []float64,
		) bool {
			if s.hasExpired(lfs.key, id) {
				return true
			}
			return sc.writeObject(ScanObjectParams{
				id:     id,
				o:      o,
				fields: fields,
				noLock: true,
			})
		})
	case "intersects":
		sc.col.Intersects(lfs.obj, lfs.sparse, sc, dl, func(
			id string,
			o geojson.Object,
			fields []float64,
		) bool {
			if s.hasExpired(lfs.key, id) {
				return true
			}
			params := ScanObjectParams{
				id:     id,
				o:      o,
				fields: fields,
				noLock: true,
			}
			if lfs.clip {
				params.clip = lfs.obj
			}
			return sc.writeObject(params)
		})
	case "scan":
		g := glob.Parse(sc.globPattern, lfs.desc)
		if g.Limits[0] == "" && g.Limits[1] == "" {
			sc.col.Scan(lfs.desc, sc,
				dl,
				func(id string, o geojson.Object, fields []float64) bool {
					return sc.writeObject(ScanObjectParams{
						id:     id,
						o:      o,
						fields: fields,
					})
				},
			)
		} else {
			sc.col.ScanRange(g.Limits[0], g.Limits[1], lfs.desc, sc,
				dl,
				func(id string, o geojson.Object, fields []float64) bool {
					return sc.writeObject(ScanObjectParams{
						id:     id,
						o:      o,
						fields: fields,
					})
				},
			)
		}
	case "search":
		if sc.output == outputCount && len(sc.wheres) == 0 && sc.globEverything {
			count := sc.col.Count() - int(lfs.cursor)
			if count < 0 {
				count = 0
			}
			sc.count = uint64(count)
		} else {
			g := glob.Parse(sc.globPattern, lfs.desc)
			if g.Limits[0] == "" && g.Limits[1] == "" {
				sc.col.SearchValues(lfs.desc, sc, dl,
					func(id string, o geojson.Object, fields []float64) bool {
						return sc.writeObject(ScanObjectParams{
							id:     id,
							o:      o,
							fields: fields,
							noLock: true,
						})
					},
				)
			} else {
				// must disable globSingle for string value type matching because
				// globSingle is only for ID matches, not values.
				sc.globSingle = false
				sc.col.SearchValuesRange(g.Limits[0], g.Limits[1], lfs.desc, sc,
					dl,
					func(id string, o geojson.Object, fields []float64) bool {
						return sc.writeObject(ScanObjectParams{
							id:     id,
							o:      o,
							fields: fields,
							noLock: true,
						})
					},
				)
			}
		}
	}

	sc.writeFoot()

	return nil
}

func (s *Server) luaTile38Get(ls *lua.LState, evalcmd, key, id string) (result lua.LValue, err error) {
	// Acquire a lock if we don't already have one
	switch evalcmd {
	case "evalna", "evalnasha":
		s.mu.RLock()
		defer s.mu.RUnlock()
	}

	// Ensure fully up to date
	if s.config.followHost() != "" && !s.fcuponce {
		return lua.LNil, errCatchingUp
	}

	col := s.getCol(key)
	if col == nil {
		return lua.LNil, nil
	}

	o, fields, ok := col.Get(id)
	ok = ok && !s.hasExpired(key, id)
	if !ok {
		return lua.LNil, nil
	}

	itemmt := ls.GetTypeMetatable(luaItemTypeName)
	ud := ls.NewUserData()
	ud.Value = luaCollectionItem{
		id:     id,
		col:    col,
		fields: fields,
		o:      o,
	}
	ud.Metatable = itemmt
	return ud, nil
}

const luaGeoJSONObjectTypeName = "geojsonObject"
const luaScanIteratorTypeName = "scanIterator"
const luaItemTypeName = "collectionItem"

type luaScanIterator struct {
	sc            *scanner
	currentParams ScanObjectParams
	gomt          lua.LValue
}

type luaCollectionItem struct {
	id     string
	o      geojson.Object
	fields []float64
	col    *collection.Collection
}

func registerLuaResultTypes(ls *lua.LState) {
	assertGObject := func(ls *lua.LState, idx int) geojson.Object {
		ud := ls.CheckUserData(idx)
		if v, ok := ud.Value.(geojson.Object); ok {
			return v
		}
		ls.ArgError(idx, "geojsonObject expected")
		return nil
	}

	gomt := ls.NewTypeMetatable(luaGeoJSONObjectTypeName)
	ls.SetFuncs(gomt, map[string]lua.LGFunction{
		"__tostring": func(ls *lua.LState) int {
			obj := assertGObject(ls, 1)
			ls.Push(lua.LString(obj.String()))
			return 1
		},
		"__index": func(ls *lua.LState) int {
			obj := assertGObject(ls, 1)
			v := ls.CheckString(2)
			switch v {
			case "empty":
				ls.Push(lua.LBool(obj.Empty()))
				return 1
			case "valid":
				ls.Push(lua.LBool(obj.Valid()))
				return 1
			case "rect":
				ud := ls.NewUserData()
				ud.Metatable = gomt
				rect := obj.Rect()
				ud.Value = geojson.NewRect(rect)
				ls.Push(ud)
				return 1
			case "center":
				ud := ls.NewUserData()
				ud.Metatable = gomt
				point := obj.Center()
				ud.Value = geojson.NewPoint(point)
				ls.Push(ud)
				return 1
			case "contains":
				other := assertGObject(ls, 2)
				ls.Push(lua.LBool(obj.Contains(other)))
				return 1
			case "within":
				other := assertGObject(ls, 2)
				ls.Push(lua.LBool(obj.Within(other)))
				return 1
			case "intersects":
				other := assertGObject(ls, 2)
				ls.Push(lua.LBool(obj.Intersects(other)))
				return 1
			case "json":
				ls.Push(lua.LString(obj.JSON()))
				return 1
			case "distance":
				other := assertGObject(ls, 2)
				ls.Push(lua.LNumber(obj.Distance(other)))
				return 1
			case "num_points":
				ls.Push(lua.LNumber(obj.NumPoints()))
				return 1
			case "x":
				if pt, ok := obj.(*geojson.Point); ok {
					ls.Push(lua.LNumber(pt.Base().X))
					return 1
				}
			case "y":
				if pt, ok := obj.(*geojson.Point); ok {
					ls.Push(lua.LNumber(pt.Base().Y))
					return 1
				}
			}
			ls.RaiseError("unknown property %s", v)
			return 0
		},
	})

	assertIterator := func(ls *lua.LState, idx int) *luaScanIterator {
		ud := ls.CheckUserData(idx)
		if v, ok := ud.Value.(*luaScanIterator); ok {
			return v
		}
		ls.ArgError(idx, "iterator expected")
		return nil
	}

	readIteratorFields := ls.NewFunction(func(ls *lua.LState) int {
		itr := assertIterator(ls, 1)
		nargs := ls.GetTop()

		for i := 2; i <= nargs; i++ {
			v := ls.CheckAny(i)
			var fieldValue float64
			if v.Type() == lua.LTNumber {
				fi := int(v.(lua.LNumber)) - 1
				if fi < len(itr.currentParams.fields) {
					fieldValue = itr.currentParams.fields[fi]
				}
			} else {
				fn := v.String()
				fi, ok := itr.sc.fmap[fn]
				if !ok {
					ls.RaiseError("invalid field %s", fn)
				}
				if fi < len(itr.currentParams.fields) {
					fieldValue = itr.currentParams.fields[fi]
				}
			}
			ls.Push(lua.LNumber(fieldValue))
		}
		return nargs - 1
	})

	itrmt := ls.NewTypeMetatable(luaScanIteratorTypeName)
	ls.SetField(itrmt, "__tostring", ls.NewFunction(func(ls *lua.LState) int {
		ls.Push(lua.LString("[scanIterator object]"))
		return 1
	}))
	ls.SetFuncs(itrmt, map[string]lua.LGFunction{
		"__index": func(ls *lua.LState) int {
			itr := assertIterator(ls, 1)
			v := ls.CheckString(2)
			switch v {
			case "id":
				ls.Push(lua.LString(itr.currentParams.id))
				return 1
			case "object":
				gobj := ls.NewUserData()
				gobj.Metatable = itr.gomt
				gobj.Value = itr.currentParams.o
				ls.Push(gobj)
				return 1
			case "distance":
				ls.Push(lua.LNumber(itr.currentParams.distance))
				return 1
			case "read_fields":
				ls.Push(readIteratorFields)
				return 1
			}
			ls.RaiseError("unknown property %s", v)
			return 0
		},
	})

	assertCollectionItem := func(ls *lua.LState, idx int) luaCollectionItem {
		ud := ls.CheckUserData(idx)
		if v, ok := ud.Value.(luaCollectionItem); ok {
			return v
		}
		ls.ArgError(idx, "collection item expected")
		return luaCollectionItem{}
	}

	readItemFields := ls.NewFunction(func(ls *lua.LState) int {
		item := assertCollectionItem(ls, 1)
		nargs := ls.GetTop()

		for i := 2; i <= nargs; i++ {
			v := ls.CheckAny(i)
			var fieldValue float64
			if v.Type() == lua.LTNumber {
				fi := int(v.(lua.LNumber)) - 1
				if fi < len(item.fields) {
					fieldValue = item.fields[fi]
				}
			} else {
				fn := v.String()
				fi, ok := item.col.FieldMap()[fn]
				if !ok {
					ls.RaiseError("invalid field %s", fn)
				}
				if fi < len(item.fields) {
					fieldValue = item.fields[fi]
				}
			}
			ls.Push(lua.LNumber(fieldValue))
		}
		return nargs - 1
	})

	itemmt := ls.NewTypeMetatable(luaItemTypeName)
	ls.SetField(itrmt, "__tostring", ls.NewFunction(func(ls *lua.LState) int {
		obj := assertCollectionItem(ls, 1)
		ls.Push(lua.LString(obj.id))
		return 1
	}))
	ls.SetFuncs(itemmt, map[string]lua.LGFunction{
		"__index": func(ls *lua.LState) int {
			item := assertCollectionItem(ls, 1)
			v := ls.CheckString(2)
			switch v {
			case "id":
				ls.Push(lua.LString(item.id))
				return 1
			case "object":
				gomt := ls.GetTypeMetatable(luaGeoJSONObjectTypeName)
				gobj := ls.NewUserData()
				gobj.Metatable = gomt
				gobj.Value = item.o
				ls.Push(gobj)
				return 1
			case "read_fields":
				ls.Push(readItemFields)
				return 1
			}
			ls.RaiseError("unknown property %s", v)
			return 0
		},
	})
}

type luaScanCollector struct {
	ls     *lua.LState
	f      *lua.LFunction
	itr    lua.LValue
	cursor uint64
}

var _ scanCollector = (*luaScanCollector)(nil)

func (coll *luaScanCollector) Init(sc *scanner) {
}

func (coll *luaScanCollector) ProcessItem(sc *scanner, opts ScanObjectParams) bool {
	ls := coll.ls

	itr := coll.itr.(*lua.LUserData).Value.(*luaScanIterator)
	itr.sc = sc
	itr.currentParams = opts

	// Function to call
	ls.Push(coll.f)
	ls.Push(coll.itr)
	ls.Call(1, 1)

	result := ls.ToBool(-1)
	ls.Pop(1)
	return result
}

func (coll *luaScanCollector) Complete(sc *scanner, cursor uint64) {
	coll.cursor = cursor
}
