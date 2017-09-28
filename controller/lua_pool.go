package controller

import (
	"sync"

	"github.com/tidwall/tile38/controller/log"
	"github.com/yuin/gopher-lua"
)

type lStatePool struct {
	m     sync.Mutex
	saved []*lua.LState
	c     *Controller
}


func (c *Controller) InitPool() *lStatePool {
	pl := &lStatePool{
		saved: make([]*lua.LState, 0),
		c: c,
	}
	// Fill the pool with 5 ready handlers
	for i := 0; i < 5; i++ {
		pl.Put(pl.New())
	}
	return pl
}


func (pl *lStatePool) Get() *lua.LState {
	pl.m.Lock()
	defer pl.m.Unlock()
	n := len(pl.saved)
	if n == 0 {
		return pl.New()
	}
	x := pl.saved[n-1]
	pl.saved = pl.saved[0 : n-1]
	return x
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
	defer pl.m.Unlock()
	pl.saved = append(pl.saved, L)
}

func (pl *lStatePool) Shutdown() {
	for _, L := range pl.saved {
		L.Close()
	}
}
