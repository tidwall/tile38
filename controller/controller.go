package controller

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/collection"
	"github.com/tidwall/tile38/controller/endpoint"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/geojson"
	"github.com/yuin/gopher-lua"
)

var errOOM = errors.New("OOM command not allowed when used memory > 'maxmemory'")

const hookLogPrefix = "hook:log:"

type collectionT struct {
	Key        string
	Collection *collection.Collection
}

type commandDetailsT struct {
	command   string
	key, id   string
	field     string
	value     float64
	obj       geojson.Object
	fields    []float64
	fmap      map[string]int
	oldObj    geojson.Object
	oldFields []float64
	updated   bool
	timestamp time.Time

	parent   bool               // when true, only children are forwarded
	pattern  string             // PDEL key pattern
	children []*commandDetailsT // for multi actions such as "PDEL"
}

func (col *collectionT) Less(item btree.Item, ctx interface{}) bool {
	return col.Key < item.(*collectionT).Key
}

type clientConn struct {
	id     uint64
	name   string
	opened time.Time
	last   time.Time
	conn   *server.Conn
}

// Controller is a tile38 controller
type Controller struct {
	mu        sync.RWMutex
	host      string
	port      int
	f         *os.File
	qdb       *buntdb.DB // hook queue log
	qidx      uint64     // hook queue log last idx
	cols      *btree.BTree
	aofsz     int
	dir       string
	config    Config
	followc   uint64 // counter increases when follow property changes
	follows   map[*bytes.Buffer]bool
	fcond     *sync.Cond
	lstack    []*commandDetailsT
	lives     map[*liveBuffer]bool
	lcond     *sync.Cond
	fcup      bool                        // follow caught up
	fcuponce  bool                        // follow caught up once
	shrinking bool                        // aof shrinking flag
	shrinklog [][]string                  // aof shrinking log
	hooks     map[string]*Hook            // hook name
	hookcols  map[string]map[string]*Hook // col key
	aofconnM  map[net.Conn]bool
	expires   map[string]map[string]time.Time
	exlist    []exitem
	conns     map[*server.Conn]*clientConn
	started   time.Time
	http      bool

	luascripts map[string]*lua.FunctionProto
	luastate   *lua.LState

	epc *endpoint.EndpointManager

	statsTotalConns    int
	statsTotalCommands int
	statsExpired       int

	lastShrinkDuration time.Duration
	currentShrinkStart time.Time

	stopBackgroundExpiring bool
	stopWatchingMemory     bool
	stopWatchingAutoGC     bool
	outOfMemory            bool
}

// ListenAndServe starts a new tile38 server
func ListenAndServe(host string, port int, dir string, http bool) error {
	return ListenAndServeEx(host, port, dir, nil, http)
}
func ListenAndServeEx(host string, port int, dir string, ln *net.Listener, http bool) error {
	log.Infof("Server started, Tile38 version %s, git %s", core.Version, core.GitSHA)
	c := &Controller{
		host:     host,
		port:     port,
		dir:      dir,
		cols:     btree.New(16, 0),
		follows:  make(map[*bytes.Buffer]bool),
		fcond:    sync.NewCond(&sync.Mutex{}),
		lives:    make(map[*liveBuffer]bool),
		lcond:    sync.NewCond(&sync.Mutex{}),
		hooks:    make(map[string]*Hook),
		hookcols: make(map[string]map[string]*Hook),
		aofconnM: make(map[net.Conn]bool),
		expires:  make(map[string]map[string]time.Time),
		started:  time.Now(),
		conns:    make(map[*server.Conn]*clientConn),
		epc:      endpoint.NewEndpointManager(),
		http:     http,
		luascripts: make(map[string]*lua.FunctionProto),
		luastate: lua.NewState(),
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err := c.loadConfig(); err != nil {
		return err
	}
	// load the queue before the aof
	qdb, err := buntdb.Open(path.Join(dir, "queue.db"))
	if err != nil {
		return err
	}
	var qidx uint64
	if err := qdb.View(func(tx *buntdb.Tx) error {
		val, err := tx.Get("hook:idx")
		if err != nil {
			if err == buntdb.ErrNotFound {
				return nil
			}
			return err
		}
		qidx = stringToUint64(val)
		return nil
	}); err != nil {
		return err
	}
	err = qdb.CreateIndex("hooks", hookLogPrefix+"*", buntdb.IndexJSONCaseSensitive("hook"))
	if err != nil {
		return err
	}
	c.qdb = qdb
	c.qidx = qidx
	if err := c.migrateAOF(); err != nil {
		return err
	}
	f, err := os.OpenFile(path.Join(dir, "appendonly.aof"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	c.f = f
	if err := c.loadAOF(); err != nil {
		return err
	}
	c.mu.Lock()
	c.fillExpiresList()
	if c.config.FollowHost != "" {
		go c.follow(c.config.FollowHost, c.config.FollowPort, c.followc)
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.followc++ // this will force any follow communication to die
		c.mu.Unlock()
	}()
	go c.processLives()
	go c.watchMemory()
	go c.watchGC()
	go c.backgroundExpiring()
	defer func() {
		c.mu.Lock()
		c.stopBackgroundExpiring = true
		c.stopWatchingMemory = true
		c.stopWatchingAutoGC = true
		c.mu.Unlock()
	}()
	defer c.luastate.Close()
	handler := func(conn *server.Conn, msg *server.Message, rd *server.AnyReaderWriter, w io.Writer, websocket bool) error {
		c.mu.Lock()
		if cc, ok := c.conns[conn]; ok {
			cc.last = time.Now()
		}
		c.statsTotalCommands++
		c.mu.Unlock()
		err := c.handleInputCommand(conn, msg, w)
		if err != nil {
			if err.Error() == "going live" {
				return c.goLive(err, conn, rd, msg, websocket)
			}
			return err
		}
		return nil
	}
	protected := func() bool {
		if core.ProtectedMode == "no" {
			// --protected-mode no
			return false
		}
		if host != "" && host != "127.0.0.1" && host != "::1" && host != "localhost" {
			// -h address
			return false
		}
		c.mu.RLock()
		is := c.config.ProtectedMode != "no" && c.config.RequirePass == ""
		c.mu.RUnlock()
		return is
	}
	var clientId uint64

	opened := func(conn *server.Conn) {
		c.mu.Lock()
		if c.config.KeepAlive > 0 {
			err := conn.SetKeepAlive(
				time.Duration(c.config.KeepAlive) * time.Second)
			if err != nil {
				log.Warnf("could not set keepalive for connection: %v",
					conn.RemoteAddr().String())
			}
		}
		clientId++
		c.conns[conn] = &clientConn{
			id:     clientId,
			opened: time.Now(),
			conn:   conn,
		}
		c.statsTotalConns++
		c.mu.Unlock()
	}
	closed := func(conn *server.Conn) {
		c.mu.Lock()
		delete(c.conns, conn)
		c.mu.Unlock()
	}
	return server.ListenAndServe(host, port, protected, handler, opened, closed, ln, http)
}

func (c *Controller) watchGC() {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	s := time.Now()
	for range t.C {
		c.mu.RLock()

		if c.stopWatchingAutoGC {
			c.mu.RUnlock()
			return
		}

		autoGC := c.config.AutoGC
		c.mu.RUnlock()

		if autoGC == 0 {
			continue
		}

		if time.Now().Sub(s) < time.Second*time.Duration(autoGC) {
			continue
		}

		var mem1, mem2 runtime.MemStats
		runtime.ReadMemStats(&mem1)
		log.Debugf("autogc(before): "+
			"alloc: %v, heap_alloc: %v, heap_released: %v",
			mem1.Alloc, mem1.HeapAlloc, mem1.HeapReleased)

		runtime.GC()
		debug.FreeOSMemory()

		runtime.ReadMemStats(&mem2)
		log.Debugf("autogc(after): "+
			"alloc: %v, heap_alloc: %v, heap_released: %v",
			mem2.Alloc, mem2.HeapAlloc, mem2.HeapReleased)
		s = time.Now()
	}
}

func (c *Controller) watchMemory() {
	t := time.NewTicker(time.Second * 2)
	defer t.Stop()
	var mem runtime.MemStats
	for range t.C {
		func() {
			c.mu.RLock()
			if c.stopWatchingMemory {
				c.mu.RUnlock()
				return
			}
			maxmem := c.config.MaxMemory
			oom := c.outOfMemory
			c.mu.RUnlock()
			if maxmem == 0 {
				if oom {
					c.mu.Lock()
					c.outOfMemory = false
					c.mu.Unlock()
				}
				return
			}
			if oom {
				runtime.GC()
			}
			runtime.ReadMemStats(&mem)
			c.mu.Lock()
			c.outOfMemory = int(mem.HeapAlloc) > maxmem
			c.mu.Unlock()
		}()
	}
}

func (c *Controller) setCol(key string, col *collection.Collection) {
	c.cols.ReplaceOrInsert(&collectionT{Key: key, Collection: col})
}

func (c *Controller) getCol(key string) *collection.Collection {
	item := c.cols.Get(&collectionT{Key: key})
	if item == nil {
		return nil
	}
	return item.(*collectionT).Collection
}

func (c *Controller) scanGreaterOrEqual(key string, iterator func(key string, col *collection.Collection) bool) {
	c.cols.AscendGreaterOrEqual(&collectionT{Key: key}, func(item btree.Item) bool {
		col := item.(*collectionT)
		return iterator(col.Key, col.Collection)
	})
}

func (c *Controller) deleteCol(key string) *collection.Collection {
	i := c.cols.Delete(&collectionT{Key: key})
	if i == nil {
		return nil
	}
	return i.(*collectionT).Collection
}

func isReservedFieldName(field string) bool {
	switch field {
	case "z", "lat", "lon":
		return true
	}
	return false
}

func (c *Controller) handleInputCommand(conn *server.Conn, msg *server.Message, w io.Writer) error {
	var words []string
	for _, v := range msg.Values {
		words = append(words, v.String())
	}
	start := time.Now()
	writeOutput := func(res string) error {
		switch msg.ConnType {
		default:
			err := fmt.Errorf("unsupported conn type: %v", msg.ConnType)
			log.Error(err)
			return err
		case server.WebSocket:
			return server.WriteWebSocketMessage(w, []byte(res))
		case server.HTTP:
			_, err := fmt.Fprintf(w, "HTTP/1.1 200 OK\r\n"+
				"Connection: close\r\n"+
				"Content-Length: %d\r\n"+
				"Content-Type: application/json; charset=utf-8\r\n"+
				"\r\n", len(res)+2)
			if err != nil {
				return err
			}
			_, err = io.WriteString(w, res)
			if err != nil {
				return err
			}
			_, err = io.WriteString(w, "\r\n")
			return err
		case server.RESP:
			var err error
			if msg.OutputType == server.JSON {
				_, err = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(res), res)
			} else {
				_, err = io.WriteString(w, res)
			}
			return err
		case server.Native:
			_, err := fmt.Fprintf(w, "$%d %s\r\n", len(res), res)
			return err
		}
	}
	// Ping. Just send back the response. No need to put through the pipeline.
	if msg.Command == "ping" || msg.Command == "echo" {
		switch msg.OutputType {
		case server.JSON:
			if len(msg.Values) > 1 {
				return writeOutput(`{"ok":true,"` + msg.Command + `":` + jsonString(msg.Values[1].String()) + `,"elapsed":"` + time.Now().Sub(start).String() + `"}`)
			}
			return writeOutput(`{"ok":true,"` + msg.Command + `":"pong","elapsed":"` + time.Now().Sub(start).String() + `"}`)
		case server.RESP:
			if len(msg.Values) > 1 {
				data, _ := msg.Values[1].MarshalRESP()
				return writeOutput(string(data))
			}
			return writeOutput("+PONG\r\n")
		}
		return nil
	}
	writeErr := func(err error) error {
		switch msg.OutputType {
		case server.JSON:
			return writeOutput(`{"ok":false,"err":` + jsonString(err.Error()) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		case server.RESP:
			if err == errInvalidNumberOfArguments {
				return writeOutput("-ERR wrong number of arguments for '" + msg.Command + "' command\r\n")
			}
			v, _ := resp.ErrorValue(errors.New("ERR " + err.Error())).MarshalRESP()
			return writeOutput(string(v))
		}
		return nil
	}

	var write bool

	if !conn.Authenticated || msg.Command == "auth" {
		c.mu.RLock()
		requirePass := c.config.RequirePass
		c.mu.RUnlock()
		if requirePass != "" {
			password := ""
			// This better be an AUTH command or the Message should contain an Auth
			if msg.Command != "auth" && msg.Auth == "" {
				// Just shut down the pipeline now. The less the client connection knows the better.
				return writeErr(errors.New("authentication required"))
			}
			if msg.Auth != "" {
				password = msg.Auth
			} else {
				if len(msg.Values) > 1 {
					password = msg.Values[1].String()
				}
			}
			if requirePass != strings.TrimSpace(password) {
				return writeErr(errors.New("invalid password"))
			}
			conn.Authenticated = true
			if msg.ConnType != server.HTTP {
				return writeOutput(server.OKMessage(msg, start))
			}
		} else if msg.Command == "auth" {
			return writeErr(errors.New("invalid password"))
		}
	}
	// choose the locking strategy
	switch msg.Command {
	default:
		c.mu.RLock()
		defer c.mu.RUnlock()
	case "set", "del", "drop", "fset", "flushdb", "sethook", "pdelhook", "delhook",
		"expire", "persist", "jset", "pdel":
		// write operations
		write = true
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.config.FollowHost != "" {
			return writeErr(errors.New("not the leader"))
		}
		if c.config.ReadOnly {
			return writeErr(errors.New("read only"))
		}
	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks", "search",
		"ttl", "bounds", "server", "info", "type", "jget":
		// read operations
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.config.FollowHost != "" && !c.fcuponce {
			return writeErr(errors.New("catching up to leader"))
		}
	case "follow", "readonly", "config":
		// system operations
		// does not write to aof, but requires a write lock.
		c.mu.Lock()
		defer c.mu.Unlock()
	case "output":
		// this is local connection operation. Locks not needed.
	case "echo":
	case "massinsert":
		// dev operation
		c.mu.Lock()
		defer c.mu.Unlock()
	case "shutdown":
		// dev operation
		c.mu.Lock()
		defer c.mu.Unlock()
	case "aofshrink":
		c.mu.RLock()
		defer c.mu.RUnlock()
	case "client":
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	res, d, err := c.command(msg, w, conn)
	if err != nil {
		if err.Error() == "going live" {
			return err
		}
		return writeErr(err)
	}
	if write {
		if err := c.writeAOF(resp.ArrayValue(msg.Values), &d); err != nil {
			if _, ok := err.(errAOFHook); ok {
				return writeErr(err)
			}
			log.Fatal(err)
			return err
		}
	}
	if res != "" {
		if err := writeOutput(res); err != nil {
			return err
		}
	}
	return nil
}

func randomKey(n int) string {
	b := make([]byte, n)
	nn, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	if nn != n {
		panic("random failed")
	}
	return fmt.Sprintf("%x", b)
}

func (c *Controller) reset() {
	c.aofsz = 0
	c.cols = btree.New(16, 0)
}

func (c *Controller) command(
	msg *server.Message, w io.Writer, conn *server.Conn,
) (
	res string, d commandDetailsT, err error,
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
	case "flushdb":
		res, d, err = c.cmdFlushDB(msg)
	case "sethook":
		res, d, err = c.cmdSetHook(msg)
	case "delhook":
		res, d, err = c.cmdDelHook(msg)
	case "pdelhook":
		res, d, err = c.cmdPDelHook(msg)
	case "expire":
		res, d, err = c.cmdExpire(msg)
	case "persist":
		res, d, err = c.cmdPersist(msg)
	case "ttl":
		res, err = c.cmdTTL(msg)
	case "hooks":
		res, err = c.cmdHooks(msg)
	case "shutdown":
		if !core.DevMode {
			err = fmt.Errorf("unknown command '%s'", msg.Values[0])
			return
		}
		log.Fatal("shutdown requested by developer")
	case "massinsert":
		if !core.DevMode {
			err = fmt.Errorf("unknown command '%s'", msg.Values[0])
			return
		}
		res, err = c.cmdMassInsert(msg)
	case "follow":
		res, err = c.cmdFollow(msg)
	case "readonly":
		res, err = c.cmdReadOnly(msg)
	case "stats":
		res, err = c.cmdStats(msg)
	case "server":
		res, err = c.cmdServer(msg)
	case "info":
		res, err = c.cmdInfo(msg)
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
	case "output":
		res, err = c.cmdOutput(msg)
	case "aof":
		res, err = c.cmdAOF(msg)
	case "aofmd5":
		res, err = c.cmdAOFMD5(msg)
	case "gc":
		runtime.GC()
		debug.FreeOSMemory()
		res = server.OKMessage(msg, time.Now())
	case "aofshrink":
		go c.aofshrink()
		res = server.OKMessage(msg, time.Now())
	case "config get":
		res, err = c.cmdConfigGet(msg)
	case "config set":
		res, err = c.cmdConfigSet(msg)
	case "config rewrite":
		res, err = c.cmdConfigRewrite(msg)
	case "config":
		err = fmt.Errorf("unknown command '%s'", msg.Values[0])
		if len(msg.Values) > 1 {
			command := msg.Values[0].String() + " " + msg.Values[1].String()
			msg.Values[1] = resp.StringValue(command)
			msg.Values = msg.Values[1:]
			msg.Command = strings.ToLower(command)
			return c.command(msg, w, conn)
		}
	case "client":
		res, err = c.cmdClient(msg, conn)
	case "eval":
		res, err = c.cmdEval(msg)
	case "evalsha":
		res, err = c.cmdEvalSha(msg)
	case "script load":
		res, err = c.cmdScriptLoad(msg)
	case "script exists":
		res, err = c.cmdScriptExists(msg)
	case "script flush":
		res, err = c.cmdScriptFlush(msg)
	}
	return
}
