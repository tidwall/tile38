package controller

import (
	"sync"

	"github.com/tidwall/tile38/controller/log"
	"github.com/yuin/gopher-lua"
)

const (
	INI_LUA_POOL_SIZE = 5
	MAX_LUA_POOL_SIZE = 1000
)


type lStatePool struct {
	m     sync.Mutex
	c     *Controller
	saved []*lua.LState
	total int
}

func (c *Controller) InitPool() *lStatePool {
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

	Tile38Call := func(ls *lua.LState) int {
		// Trying to work with unknown number of args.  When we see empty arg we call it enough.
		var args []string
		for i := 1; ; i++ {
			if arg := ls.ToString(i); arg == "" {
				break
			} else {
				args = append(args, arg)
			}
		}
		log.Debugf("ARGS %s\n", args)
		if res, err := pl.c.handleCommandInScript(args[0], args[1:]...); err != nil {
			log.Debugf("RES type: %s value: %s ERR %s\n", res.Type(), res.String(), err);
			ls.RaiseError("ERR %s", err.Error())
			return 0
		} else {
			log.Debugf("RES type: %s value: %s\n", res.Type(), res.String());
			ls.Push(ConvertToLua(ls, res))
			return 1
		}
	}
	var exports = map[string]lua.LGFunction {
		"call": Tile38Call,
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

func (c *Controller) InitScriptMap() *lScriptMap {
	return &lScriptMap{
		scripts: make(map[string]*lua.FunctionProto),
	}
}
