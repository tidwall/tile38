package server

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/rbang"
	"github.com/tidwall/redcon"
	"github.com/tidwall/resp"
	"github.com/tidwall/rhh"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/deadline"
	"github.com/tidwall/tile38/internal/endpoint"
	"github.com/tidwall/tile38/internal/expire"
	"github.com/tidwall/tile38/internal/log"
	"github.com/tidwall/tinybtree"
)

var errOOM = errors.New("OOM command not allowed when used memory > 'maxmemory'")

func errTimeoutOnCmd(cmd string) error {
	return fmt.Errorf("timeout not supported for '%s'", cmd)
}

const (
	goingLive     = "going live"
	hookLogPrefix = "hook:log:"
)

// commandDetails is detailed information about a mutable command. It's used
// for geofence formulas.
type commandDetails struct {
	command   string            // client command, like "SET" or "DEL"
	key, id   string            // collection key and object id of object
	newKey    string            // new key, for RENAME command
	fmap      map[string]int    // map of field names to value indexes
	obj       geojson.Object    // new object
	fields    []float64         // array of field values
	oldObj    geojson.Object    // previous object, if any
	oldFields []float64         // previous object field values
	updated   bool              // object was updated
	timestamp time.Time         // timestamp when the update occured
	parent    bool              // when true, only children are forwarded
	pattern   string            // PDEL key pattern
	children  []*commandDetails // for multi actions such as "PDEL"
}

// Server is a tile38 controller
type Server struct {
	// static values
	host    string
	port    int
	http    bool
	dir     string
	started time.Time
	config  *Config
	epc     *endpoint.Manager

	// env opts
	geomParseOpts geojson.ParseOptions

	// atomics
	followc            aint // counter increases when follow property changes
	statsTotalConns    aint // counter for total connections
	statsTotalCommands aint // counter for total commands
	statsTotalMsgsSent aint // counter for total sent webhook messages
	statsExpired       aint // item expiration counter
	lastShrinkDuration aint
	stopServer         abool
	outOfMemory        abool

	connsmu sync.RWMutex
	conns   map[int]*Client

	mu       sync.RWMutex
	aof      *os.File        // active aof file
	aofdirty int32           // mark the aofbuf as having data
	aofbuf   []byte          // prewrite buffer
	aofsz    int             // active size of the aof file
	qdb      *buntdb.DB      // hook queue log
	qidx     uint64          // hook queue log last idx
	cols     tinybtree.BTree // data collections
	expires  *rhh.Map        // map[string]map[string]time.Time

	follows    map[*bytes.Buffer]bool
	fcond      *sync.Cond
	lstack     []*commandDetails
	lives      map[*liveBuffer]bool
	lcond      *sync.Cond
	fcup       bool             // follow caught up
	fcuponce   bool             // follow caught up once
	shrinking  bool             // aof shrinking flag
	shrinklog  [][]string       // aof shrinking log
	hooks      map[string]*Hook // hook name
	hookTree   rbang.RTree      // hook spatial tree containing all
	hooksOut   map[string]*Hook // hooks with "outside" detection
	aofconnM   map[net.Conn]bool
	luascripts *lScriptMap
	luapool    *lStatePool

	pubsub *pubsub
	hookex expire.List
}

// Serve starts a new tile38 server
func Serve(host string, port int, dir string, http bool) error {
	if core.AppendFileName == "" {
		core.AppendFileName = path.Join(dir, "appendonly.aof")
	}
	if core.QueueFileName == "" {
		core.QueueFileName = path.Join(dir, "queue.db")
	}
	log.Infof("Server started, Tile38 version %s, git %s", core.Version, core.GitSHA)

	// Initialize the server
	server := &Server{
		host:     host,
		port:     port,
		dir:      dir,
		follows:  make(map[*bytes.Buffer]bool),
		fcond:    sync.NewCond(&sync.Mutex{}),
		lives:    make(map[*liveBuffer]bool),
		lcond:    sync.NewCond(&sync.Mutex{}),
		hooks:    make(map[string]*Hook),
		hooksOut: make(map[string]*Hook),
		aofconnM: make(map[net.Conn]bool),
		expires:  rhh.New(0),
		started:  time.Now(),
		conns:    make(map[int]*Client),
		http:     http,
		pubsub:   newPubsub(),
	}

	server.hookex.Expired = func(item expire.Item) {
		switch v := item.(type) {
		case *Hook:
			server.possiblyExpireHook(v.Name)
		}
	}
	server.epc = endpoint.NewManager(server)
	server.luascripts = server.newScriptMap()
	server.luapool = server.newPool()
	defer server.luapool.Shutdown()

	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	var err error
	server.config, err = loadConfig(filepath.Join(dir, "config"))
	if err != nil {
		return err
	}

	// Allow for geometry indexing options through environment variables:
	// T38IDXGEOMKIND -- None, RTree, QuadTree
	// T38IDXGEOM -- Min number of points in a geometry for indexing.
	// T38IDXMULTI -- Min number of object in a Multi/Collection for indexing.
	server.geomParseOpts = *geojson.DefaultParseOptions
	n, err := strconv.ParseUint(os.Getenv("T38IDXGEOM"), 10, 32)
	if err == nil {
		server.geomParseOpts.IndexGeometry = int(n)
	}
	n, err = strconv.ParseUint(os.Getenv("T38IDXMULTI"), 10, 32)
	if err == nil {
		server.geomParseOpts.IndexChildren = int(n)
	}
	requireValid := os.Getenv("REQUIREVALID")
	if requireValid != "" {
		server.geomParseOpts.RequireValid = true
	}
	indexKind := os.Getenv("T38IDXGEOMKIND")
	switch indexKind {
	default:
		log.Errorf("Unknown index kind: %s", indexKind)
	case "":
	case "None":
		server.geomParseOpts.IndexGeometryKind = geometry.None
		geometry.DefaultIndexOptions.Kind = geometry.None
	case "RTree":
		server.geomParseOpts.IndexGeometryKind = geometry.RTree
		geometry.DefaultIndexOptions.Kind = geometry.RTree
	case "QuadTree":
		server.geomParseOpts.IndexGeometryKind = geometry.QuadTree
		geometry.DefaultIndexOptions.Kind = geometry.QuadTree
	}
	if server.geomParseOpts.IndexGeometryKind == geometry.None {
		log.Debugf("Geom indexing: %s",
			server.geomParseOpts.IndexGeometryKind,
		)
	} else {
		log.Debugf("Geom indexing: %s (%d points)",
			server.geomParseOpts.IndexGeometryKind,
			server.geomParseOpts.IndexGeometry,
		)
	}
	log.Debugf("Multi indexing: RTree (%d points)", server.geomParseOpts.IndexChildren)

	// Load the queue before the aof
	qdb, err := buntdb.Open(core.QueueFileName)
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

	server.qdb = qdb
	server.qidx = qidx
	if err := server.migrateAOF(); err != nil {
		return err
	}
	if core.AppendOnly == true {
		f, err := os.OpenFile(core.AppendFileName, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		server.aof = f
		if err := server.loadAOF(); err != nil {
			return err
		}
		defer func() {
			server.flushAOF(false)
			server.aof.Sync()
		}()
	}
	// server.fillExpiresList()

	// Start background routines
	if server.config.followHost() != "" {
		go server.follow(server.config.followHost(), server.config.followPort(),
			server.followc.get())
	}
	go server.processLives()
	go server.watchOutOfMemory()
	go server.watchLuaStatePool()
	go server.watchAutoGC()
	go server.backgroundExpiring()
	go server.backgroundSyncAOF()
	defer func() {
		// Stop background routines
		server.followc.add(1) // this will force any follow communication to die
		server.stopServer.set(true)

		// notify the live geofence connections that we are stopping.
		server.lcond.L.Lock()
		server.lcond.Wait()
		server.lcond.L.Lock()
	}()

	// Start the network server
	return server.netServe()
}

func (server *Server) isProtected() bool {
	if core.ProtectedMode == "no" {
		// --protected-mode no
		return false
	}
	if server.host != "" && server.host != "127.0.0.1" &&
		server.host != "::1" && server.host != "localhost" {
		// -h address
		return false
	}
	is := server.config.protectedMode() != "no" && server.config.requirePass() == ""
	return is
}

func (server *Server) netServe() error {
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", server.host, server.port))
	if err != nil {
		return err
	}
	defer ln.Close()
	log.Infof("Ready to accept connections at %s", ln.Addr())
	var clientID int64
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}

		go func(conn net.Conn) {
			// open connection
			// create the client
			client := new(Client)
			client.id = int(atomic.AddInt64(&clientID, 1))
			client.opened = time.Now()
			client.remoteAddr = conn.RemoteAddr().String()

			// add client to server map
			server.connsmu.Lock()
			server.conns[client.id] = client
			server.connsmu.Unlock()
			server.statsTotalConns.add(1)

			// set the client keep-alive, if needed
			if server.config.keepAlive() > 0 {
				if conn, ok := conn.(*net.TCPConn); ok {
					conn.SetKeepAlive(true)
					conn.SetKeepAlivePeriod(
						time.Duration(server.config.keepAlive()) * time.Second,
					)
				}
			}
			log.Debugf("Opened connection: %s", client.remoteAddr)

			defer func() {
				// close connection
				// delete from server map
				server.connsmu.Lock()
				delete(server.conns, client.id)
				server.connsmu.Unlock()
				log.Debugf("Closed connection: %s", client.remoteAddr)
				conn.Close()
			}()

			var lastConnType Type
			var lastOutputType Type

			// check if the connection is protected
			if !strings.HasPrefix(client.remoteAddr, "127.0.0.1:") &&
				!strings.HasPrefix(client.remoteAddr, "[::1]:") {
				if server.isProtected() {
					// This is a protected server. Only loopback is allowed.
					conn.Write(deniedMessage)
					return // close connection
				}
			}
			packet := make([]byte, 0xFFFF)
			for {
				var close bool
				n, err := conn.Read(packet)
				if err != nil {
					return
				}
				in := packet[:n]

				// read the payload packet from the client input stream.
				packet := client.in.Begin(in)

				// load the pipeline reader
				pr := &client.pr
				rdbuf := bytes.NewBuffer(packet)
				pr.rd = rdbuf
				pr.wr = client

				msgs, err := pr.ReadMessages()
				for _, msg := range msgs {
					// Just closing connection if we have deprecated HTTP or WS connection,
					// And --http-transport = false
					if !server.http && (msg.ConnType == WebSocket ||
						msg.ConnType == HTTP) {
						close = true // close connection
						break
					}
					if msg != nil && msg.Command() != "" {
						if client.outputType != Null {
							msg.OutputType = client.outputType
						}
						if msg.Command() == "quit" {
							if msg.OutputType == RESP {
								io.WriteString(client, "+OK\r\n")
							}
							close = true // close connection
							break
						}

						// increment last used
						client.mu.Lock()
						client.last = time.Now()
						client.mu.Unlock()

						// update total command count
						server.statsTotalCommands.add(1)

						// handle the command
						err := server.handleInputCommand(client, msg)
						if err != nil {
							if err.Error() == goingLive {
								client.goLiveErr = err
								client.goLiveMsg = msg
								// detach
								var rwc io.ReadWriteCloser = conn
								client.conn = rwc
								if len(client.out) > 0 {
									client.conn.Write(client.out)
									client.out = nil
								}
								client.in = InputStream{}
								client.pr.rd = rwc
								client.pr.wr = rwc
								log.Debugf("Detached connection: %s", client.remoteAddr)

								var wg sync.WaitGroup
								wg.Add(1)
								go func() {
									defer wg.Done()
									err := server.goLive(
										client.goLiveErr,
										&liveConn{conn.RemoteAddr(), rwc},
										&client.pr,
										client.goLiveMsg,
										client.goLiveMsg.ConnType == WebSocket,
									)
									if err != nil {
										log.Error(err)
									}
								}()
								wg.Wait()
								return // close connection
							}
							log.Error(err)
							return // close connection, NOW
						}

						client.outputType = msg.OutputType
					} else {
						client.Write([]byte("HTTP/1.1 500 Bad Request\r\nConnection: close\r\n\r\n"))
						break
					}
					if msg.ConnType == HTTP || msg.ConnType == WebSocket {
						close = true // close connection
						break
					}
					lastOutputType = msg.OutputType
					lastConnType = msg.ConnType
				}

				packet = packet[len(packet)-rdbuf.Len():]
				client.in.End(packet)

				// write to client
				if len(client.out) > 0 {
					if atomic.LoadInt32(&server.aofdirty) != 0 {
						func() {
							// prewrite
							server.mu.Lock()
							defer server.mu.Unlock()
							server.flushAOF(false)
						}()
						atomic.StoreInt32(&server.aofdirty, 0)
					}
					conn.Write(client.out)
					client.out = nil
				}
				if close {
					break
				}
				if err != nil {
					log.Error(err)
					if lastConnType == RESP {
						var value resp.Value
						switch lastOutputType {
						case JSON:
							value = resp.StringValue(`{"ok":false,"err":` +
								jsonString(err.Error()) + "}")
						case RESP:
							value = resp.ErrorValue(err)
						}
						bytes, _ := value.MarshalRESP()
						conn.Write(bytes)
					}
					break // close connection
				}
			}
		}(conn)
	}
}

type liveConn struct {
	remoteAddr net.Addr
	rwc        io.ReadWriteCloser
}

func (conn *liveConn) Close() error {
	return conn.rwc.Close()
}

func (conn *liveConn) LocalAddr() net.Addr {
	panic("not supported")
}

func (conn *liveConn) RemoteAddr() net.Addr {
	return conn.remoteAddr
}
func (conn *liveConn) Read(b []byte) (n int, err error) {
	return conn.rwc.Read(b)
}

func (conn *liveConn) Write(b []byte) (n int, err error) {
	return conn.rwc.Write(b)
}

func (conn *liveConn) SetDeadline(deadline time.Time) error {
	panic("not supported")
}

func (conn *liveConn) SetReadDeadline(deadline time.Time) error {
	panic("not supported")
}

func (conn *liveConn) SetWriteDeadline(deadline time.Time) error {
	panic("not supported")
}

func (server *Server) watchAutoGC() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	s := time.Now()
	for range t.C {
		if server.stopServer.on() {
			return
		}
		autoGC := server.config.autoGC()
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

func (server *Server) watchOutOfMemory() {
	t := time.NewTicker(time.Second * 2)
	defer t.Stop()
	var mem runtime.MemStats
	for range t.C {
		func() {
			if server.stopServer.on() {
				return
			}
			oom := server.outOfMemory.on()
			if server.config.maxMemory() == 0 {
				if oom {
					server.outOfMemory.set(false)
				}
				return
			}
			if oom {
				runtime.GC()
			}
			runtime.ReadMemStats(&mem)
			server.outOfMemory.set(int(mem.HeapAlloc) > server.config.maxMemory())
		}()
	}
}

func (server *Server) watchLuaStatePool() {
	t := time.NewTicker(time.Second * 10)
	defer t.Stop()
	for range t.C {
		func() {
			server.luapool.Prune()
		}()
	}
}

// backgroundSyncAOF ensures that the aof buffer is does not grow too big.
func (server *Server) backgroundSyncAOF() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		if server.stopServer.on() {
			return
		}
		func() {
			server.mu.Lock()
			defer server.mu.Unlock()
			server.flushAOF(true)
		}()
	}
}

func (server *Server) setCol(key string, col *collection.Collection) {
	server.cols.Set(key, col)
}

func (server *Server) getCol(key string) *collection.Collection {
	if value, ok := server.cols.Get(key); ok {
		return value.(*collection.Collection)
	}
	return nil
}

func (server *Server) scanGreaterOrEqual(
	key string, iterator func(key string, col *collection.Collection) bool,
) {
	server.cols.Ascend(key, func(ikey string, ivalue interface{}) bool {
		return iterator(ikey, ivalue.(*collection.Collection))
	})
}

func (server *Server) deleteCol(key string) *collection.Collection {
	if prev, ok := server.cols.Delete(key); ok {
		return prev.(*collection.Collection)
	}
	return nil
}

func isReservedFieldName(field string) bool {
	switch field {
	case "z", "lat", "lon":
		return true
	}
	return false
}

func rewriteTimeoutMsg(msg *Message) (err error) {
	vs := msg.Args[1:]
	var valStr string
	var ok bool
	if vs, valStr, ok = tokenval(vs); !ok || valStr == "" || len(vs) == 0 {
		err = errInvalidNumberOfArguments
		return
	}
	timeoutSec, _err := strconv.ParseFloat(valStr, 64)
	if _err != nil || timeoutSec < 0 {
		err = errInvalidArgument(valStr)
		return
	}
	msg.Args = vs[:]
	msg._command = ""
	msg.Deadline = deadline.New(
		time.Now().Add(time.Duration(timeoutSec * float64(time.Second))))
	return
}

func (server *Server) handleInputCommand(client *Client, msg *Message) error {
	start := time.Now()
	serializeOutput := func(res resp.Value) (string, error) {
		var resStr string
		var err error
		switch msg.OutputType {
		case JSON:
			resStr = res.String()
		case RESP:
			var resBytes []byte
			resBytes, err = res.MarshalRESP()
			resStr = string(resBytes)
		}
		return resStr, err
	}
	writeOutput := func(res string) error {
		switch msg.ConnType {
		default:
			err := fmt.Errorf("unsupported conn type: %v", msg.ConnType)
			log.Error(err)
			return err
		case WebSocket:
			return WriteWebSocketMessage(client, []byte(res))
		case HTTP:
			_, err := fmt.Fprintf(client, "HTTP/1.1 200 OK\r\n"+
				"Connection: close\r\n"+
				"Content-Length: %d\r\n"+
				"Content-Type: application/json; charset=utf-8\r\n"+
				"\r\n", len(res)+2)
			if err != nil {
				return err
			}
			_, err = io.WriteString(client, res)
			if err != nil {
				return err
			}
			_, err = io.WriteString(client, "\r\n")
			return err
		case RESP:
			var err error
			if msg.OutputType == JSON {
				_, err = fmt.Fprintf(client, "$%d\r\n%s\r\n", len(res), res)
			} else {
				_, err = io.WriteString(client, res)
			}
			return err
		case Native:
			_, err := fmt.Fprintf(client, "$%d %s\r\n", len(res), res)
			return err
		}
	}

	// Ping. Just send back the response. No need to put through the pipeline.
	if msg.Command() == "ping" || msg.Command() == "echo" {
		switch msg.OutputType {
		case JSON:
			if len(msg.Args) > 1 {
				return writeOutput(`{"ok":true,"` + msg.Command() + `":` + jsonString(msg.Args[1]) + `,"elapsed":"` + time.Now().Sub(start).String() + `"}`)
			}
			return writeOutput(`{"ok":true,"` + msg.Command() + `":"pong","elapsed":"` + time.Now().Sub(start).String() + `"}`)
		case RESP:
			if len(msg.Args) > 1 {
				data := redcon.AppendBulkString(nil, msg.Args[1])
				return writeOutput(string(data))
			}
			return writeOutput("+PONG\r\n")
		}
		return nil
	}

	writeErr := func(errMsg string) error {
		switch msg.OutputType {
		case JSON:
			return writeOutput(`{"ok":false,"err":` + jsonString(errMsg) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		case RESP:
			if errMsg == errInvalidNumberOfArguments.Error() {
				return writeOutput("-ERR wrong number of arguments for '" + msg.Command() + "' command\r\n")
			}
			v, _ := resp.ErrorValue(errors.New("ERR " + errMsg)).MarshalRESP()
			return writeOutput(string(v))
		}
		return nil
	}

	if msg.Command() == "timeout" {
		if err := rewriteTimeoutMsg(msg); err != nil {
			return writeErr(err.Error())
		}
	}

	var write bool

	if !client.authd || msg.Command() == "auth" {
		if server.config.requirePass() != "" {
			password := ""
			// This better be an AUTH command or the Message should contain an Auth
			if msg.Command() != "auth" && msg.Auth == "" {
				// Just shut down the pipeline now. The less the client connection knows the better.
				return writeErr("authentication required")
			}
			if msg.Auth != "" {
				password = msg.Auth
			} else {
				if len(msg.Args) > 1 {
					password = msg.Args[1]
				}
			}
			if server.config.requirePass() != strings.TrimSpace(password) {
				return writeErr("invalid password")
			}
			client.authd = true
			if msg.ConnType != HTTP {
				resStr, _ := serializeOutput(OKMessage(msg, start))
				return writeOutput(resStr)
			}
		} else if msg.Command() == "auth" {
			return writeErr("invalid password")
		}
	}

	// choose the locking strategy
	switch msg.Command() {
	default:
		server.mu.RLock()
		defer server.mu.RUnlock()
	case "set", "del", "drop", "fset", "flushdb",
		"setchan", "pdelchan", "delchan",
		"sethook", "pdelhook", "delhook",
		"expire", "persist", "jset", "pdel", "rename", "renamenx":
		// write operations
		write = true
		server.mu.Lock()
		defer server.mu.Unlock()
		if server.config.followHost() != "" {
			return writeErr("not the leader")
		}
		if server.config.readOnly() {
			return writeErr("read only")
		}
	case "eval", "evalsha":
		// write operations (potentially) but no AOF for the script command itself
		server.mu.Lock()
		defer server.mu.Unlock()
		if server.config.followHost() != "" {
			return writeErr("not the leader")
		}
		if server.config.readOnly() {
			return writeErr("read only")
		}
	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks",
		"chans", "search", "ttl", "bounds", "server", "info", "type", "jget",
		"evalro", "evalrosha":
		// read operations

		server.mu.RLock()
		defer server.mu.RUnlock()
		if server.config.followHost() != "" && !server.fcuponce {
			return writeErr("catching up to leader")
		}
	case "follow", "slaveof", "replconf", "readonly", "config":
		// system operations
		// does not write to aof, but requires a write lock.
		server.mu.Lock()
		defer server.mu.Unlock()
	case "output":
		// this is local connection operation. Locks not needed.
	case "echo":
	case "massinsert":
		// dev operation
	case "sleep":
		// dev operation
		server.mu.RLock()
		defer server.mu.RUnlock()
	case "shutdown":
		// dev operation
		server.mu.Lock()
		defer server.mu.Unlock()
	case "aofshrink":
		server.mu.RLock()
		defer server.mu.RUnlock()
	case "client":
		server.mu.Lock()
		defer server.mu.Unlock()
	case "evalna", "evalnasha":
		// No locking for scripts, otherwise writes cannot happen within scripts
	case "subscribe", "psubscribe", "publish":
		// No locking for pubsub
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
					err = writeErr("timeout")
				}
			}()
		}
		return server.command(msg, client)
	}()
	if res.Type() == resp.Error {
		return writeErr(res.String())
	}
	if err != nil {
		if err.Error() == goingLive {
			return err
		}
		return writeErr(err.Error())
	}
	if write {
		if err := server.writeAOF(msg.Args, &d); err != nil {
			if _, ok := err.(errAOFHook); ok {
				return writeErr(err.Error())
			}
			log.Fatal(err)
			return err
		}
	}
	if !isRespValueEmptyString(res) {
		var resStr string
		resStr, err := serializeOutput(res)
		if err != nil {
			return err
		}
		if err := writeOutput(resStr); err != nil {
			return err
		}
	}
	return nil
}

func isRespValueEmptyString(val resp.Value) bool {
	return !val.IsNull() && (val.Type() == resp.SimpleString || val.Type() == resp.BulkString) && len(val.Bytes()) == 0
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

func (server *Server) reset() {
	server.aofsz = 0
	server.cols = tinybtree.BTree{}
	server.expires = rhh.New(0)
}

func (server *Server) command(msg *Message, client *Client) (
	res resp.Value, d commandDetails, err error,
) {
	switch msg.Command() {
	default:
		err = fmt.Errorf("unknown command '%s'", msg.Args[0])
	case "set":
		res, d, err = server.cmdSet(msg, true)
	case "fset":
		res, d, err = server.cmdFset(msg)
	case "del":
		res, d, err = server.cmdDel(msg)
	case "pdel":
		res, d, err = server.cmdPdel(msg)
	case "drop":
		res, d, err = server.cmdDrop(msg)
	case "flushdb":
		res, d, err = server.cmdFlushDB(msg)
	case "rename":
		res, d, err = server.cmdRename(msg, false)
	case "renamenx":
		res, d, err = server.cmdRename(msg, true)
	case "sethook":
		res, d, err = server.cmdSetHook(msg, false)
	case "delhook":
		res, d, err = server.cmdDelHook(msg, false)
	case "pdelhook":
		res, d, err = server.cmdPDelHook(msg, false)
	case "hooks":
		res, err = server.cmdHooks(msg, false)
	case "setchan":
		res, d, err = server.cmdSetHook(msg, true)
	case "delchan":
		res, d, err = server.cmdDelHook(msg, true)
	case "pdelchan":
		res, d, err = server.cmdPDelHook(msg, true)
	case "chans":
		res, err = server.cmdHooks(msg, true)
	case "expire":
		res, d, err = server.cmdExpire(msg)
	case "persist":
		res, d, err = server.cmdPersist(msg)
	case "ttl":
		res, err = server.cmdTTL(msg)
	case "shutdown":
		if !core.DevMode {
			err = fmt.Errorf("unknown command '%s'", msg.Args[0])
			return
		}
		log.Fatal("shutdown requested by developer")
	case "massinsert":
		if !core.DevMode {
			err = fmt.Errorf("unknown command '%s'", msg.Args[0])
			return
		}
		res, err = server.cmdMassInsert(msg)
	case "sleep":
		if !core.DevMode {
			err = fmt.Errorf("unknown command '%s'", msg.Args[0])
			return
		}
		res, err = server.cmdSleep(msg)
	case "follow", "slaveof":
		res, err = server.cmdFollow(msg)
	case "replconf":
		res, err = server.cmdReplConf(msg, client)
	case "readonly":
		res, err = server.cmdReadOnly(msg)
	case "stats":
		res, err = server.cmdStats(msg)
	case "server":
		res, err = server.cmdServer(msg)
	case "info":
		res, err = server.cmdInfo(msg)
	case "scan":
		res, err = server.cmdScan(msg)
	case "nearby":
		res, err = server.cmdNearby(msg)
	case "within":
		res, err = server.cmdWithin(msg)
	case "intersects":
		res, err = server.cmdIntersects(msg)
	case "search":
		res, err = server.cmdSearch(msg)
	case "bounds":
		res, err = server.cmdBounds(msg)
	case "get":
		res, err = server.cmdGet(msg)
	case "jget":
		res, err = server.cmdJget(msg)
	case "jset":
		res, d, err = server.cmdJset(msg)
	case "jdel":
		res, d, err = server.cmdJdel(msg)
	case "type":
		res, err = server.cmdType(msg)
	case "keys":
		res, err = server.cmdKeys(msg)
	case "output":
		res, err = server.cmdOutput(msg)
	case "aof":
		res, err = server.cmdAOF(msg)
	case "aofmd5":
		res, err = server.cmdAOFMD5(msg)
	case "gc":
		runtime.GC()
		debug.FreeOSMemory()
		res = OKMessage(msg, time.Now())
	case "aofshrink":
		go server.aofshrink()
		res = OKMessage(msg, time.Now())
	case "config get":
		res, err = server.cmdConfigGet(msg)
	case "config set":
		res, err = server.cmdConfigSet(msg)
	case "config rewrite":
		res, err = server.cmdConfigRewrite(msg)
	case "config", "script":
		// These get rewritten into "config foo" and "script bar"
		err = fmt.Errorf("unknown command '%s'", msg.Args[0])
		if len(msg.Args) > 1 {
			msg.Args[1] = msg.Args[0] + " " + msg.Args[1]
			msg.Args = msg.Args[1:]
			msg._command = ""
			return server.command(msg, client)
		}
	case "client":
		res, err = server.cmdClient(msg, client)
	case "eval", "evalro", "evalna":
		res, err = server.cmdEvalUnified(false, msg)
	case "evalsha", "evalrosha", "evalnasha":
		res, err = server.cmdEvalUnified(true, msg)
	case "script load":
		res, err = server.cmdScriptLoad(msg)
	case "script exists":
		res, err = server.cmdScriptExists(msg)
	case "script flush":
		res, err = server.cmdScriptFlush(msg)
	case "subscribe":
		res, err = server.cmdSubscribe(msg)
	case "psubscribe":
		res, err = server.cmdPsubscribe(msg)
	case "publish":
		res, err = server.cmdPublish(msg)
	case "test":
		res, err = server.cmdTest(msg)
	}
	return
}

// This phrase is copied nearly verbatim from Redis.
var deniedMessage = []byte(strings.Replace(strings.TrimSpace(`
-DENIED Tile38 is running in protected mode because protected mode is enabled,
no bind address was specified, no authentication password is requested to
clients. In this mode connections are only accepted from the loopback
interface. If you want to connect from external computers to Tile38 you may
adopt one of the following solutions: 1) Just disable protected mode sending
the command 'CONFIG SET protected-mode no' from the loopback interface by
connecting to Tile38 from the same host the server is running, however MAKE
SURE Tile38 is not publicly accessible from internet if you do so. Use CONFIG
REWRITE to make this change permanent. 2) Alternatively you can just disable
the protected mode by editing the Tile38 configuration file, and setting the
protected mode option to 'no', and then restarting the server. 3) If you
started the server manually just for testing, restart it with the
'--protected-mode no' option. 4) Setup a bind address or an authentication
password. NOTE: You only need to do one of the above things in order for the
server to start accepting connections from the outside.
`), "\n", " ", -1) + "\r\n")

// SetKeepAlive sets the connection keepalive
func setKeepAlive(conn net.Conn, period time.Duration) error {
	if tcp, ok := conn.(*net.TCPConn); ok {
		if err := tcp.SetKeepAlive(true); err != nil {
			return err
		}
		return tcp.SetKeepAlivePeriod(period)
	}
	return nil
}

var errCloseHTTP = errors.New("close http")

// WriteWebSocketMessage write a websocket message to an io.Writer.
func WriteWebSocketMessage(w io.Writer, data []byte) error {
	var msg []byte
	buf := make([]byte, 10+len(data))
	buf[0] = 129 // FIN + TEXT
	if len(data) <= 125 {
		buf[1] = byte(len(data))
		copy(buf[2:], data)
		msg = buf[:2+len(data)]
	} else if len(data) <= 0xFFFF {
		buf[1] = 126
		binary.BigEndian.PutUint16(buf[2:], uint16(len(data)))
		copy(buf[4:], data)
		msg = buf[:4+len(data)]
	} else {
		buf[1] = 127
		binary.BigEndian.PutUint64(buf[2:], uint64(len(data)))
		copy(buf[10:], data)
		msg = buf[:10+len(data)]
	}
	_, err := w.Write(msg)
	return err
}

// OKMessage returns a default OK message in JSON or RESP.
func OKMessage(msg *Message, start time.Time) resp.Value {
	switch msg.OutputType {
	case JSON:
		return resp.StringValue(`{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case RESP:
		return resp.SimpleStringValue("OK")
	}
	return resp.SimpleStringValue("")
}

// NOMessage is no message
var NOMessage = resp.SimpleStringValue("")

var errInvalidHTTP = errors.New("invalid HTTP request")

// Type is resp type
type Type byte

// Protocol Types
const (
	Null Type = iota
	RESP
	Telnet
	Native
	HTTP
	WebSocket
	JSON
)

// Message is a resp message
type Message struct {
	_command   string
	Args       []string
	ConnType   Type
	OutputType Type
	Auth       string
	Deadline   *deadline.Deadline
}

// Command returns the first argument as a lowercase string
func (msg *Message) Command() string {
	if msg._command == "" {
		msg._command = strings.ToLower(msg.Args[0])
	}
	return msg._command
}

// PipelineReader ...
type PipelineReader struct {
	rd     io.Reader
	wr     io.Writer
	packet [0xFFFF]byte
	buf    []byte
}

const kindHTTP redcon.Kind = 9999

// NewPipelineReader ...
func NewPipelineReader(rd io.ReadWriter) *PipelineReader {
	return &PipelineReader{rd: rd, wr: rd}
}

func readcrlfline(packet []byte) (line string, leftover []byte, ok bool) {
	for i := 1; i < len(packet); i++ {
		if packet[i] == '\n' && packet[i-1] == '\r' {
			return string(packet[:i-1]), packet[i+1:], true
		}
	}
	return "", packet, false
}

func readNextHTTPCommand(packet []byte, argsIn [][]byte, msg *Message, wr io.Writer) (
	complete bool, args [][]byte, kind redcon.Kind, leftover []byte, err error,
) {
	args = argsIn[:0]
	msg.ConnType = HTTP
	msg.OutputType = JSON
	opacket := packet

	ready, err := func() (bool, error) {
		var line string
		var ok bool

		// read header
		var headers []string
		for {
			line, packet, ok = readcrlfline(packet)
			if !ok {
				return false, nil
			}
			if line == "" {
				break
			}
			headers = append(headers, line)
		}
		parts := strings.Split(headers[0], " ")
		if len(parts) != 3 {
			return false, errInvalidHTTP
		}
		method := parts[0]
		path := parts[1]
		if len(path) == 0 || path[0] != '/' {
			return false, errInvalidHTTP
		}
		path, err = url.QueryUnescape(path[1:])
		if err != nil {
			return false, errInvalidHTTP
		}
		if method != "GET" && method != "POST" {
			return false, errInvalidHTTP
		}
		contentLength := 0
		websocket := false
		websocketVersion := 0
		websocketKey := ""
		for _, header := range headers[1:] {
			if header[0] == 'a' || header[0] == 'A' {
				if strings.HasPrefix(strings.ToLower(header), "authorization:") {
					msg.Auth = strings.TrimSpace(header[len("authorization:"):])
				}
			} else if header[0] == 'u' || header[0] == 'U' {
				if strings.HasPrefix(strings.ToLower(header), "upgrade:") && strings.ToLower(strings.TrimSpace(header[len("upgrade:"):])) == "websocket" {
					websocket = true
				}
			} else if header[0] == 's' || header[0] == 'S' {
				if strings.HasPrefix(strings.ToLower(header), "sec-websocket-version:") {
					var n uint64
					n, err = strconv.ParseUint(strings.TrimSpace(header[len("sec-websocket-version:"):]), 10, 64)
					if err != nil {
						return false, err
					}
					websocketVersion = int(n)
				} else if strings.HasPrefix(strings.ToLower(header), "sec-websocket-key:") {
					websocketKey = strings.TrimSpace(header[len("sec-websocket-key:"):])
				}
			} else if header[0] == 'c' || header[0] == 'C' {
				if strings.HasPrefix(strings.ToLower(header), "content-length:") {
					var n uint64
					n, err = strconv.ParseUint(strings.TrimSpace(header[len("content-length:"):]), 10, 64)
					if err != nil {
						return false, err
					}
					contentLength = int(n)
				}
			}
		}
		if websocket && websocketVersion >= 13 && websocketKey != "" {
			msg.ConnType = WebSocket
			if wr == nil {
				return false, errors.New("connection is nil")
			}
			sum := sha1.Sum([]byte(websocketKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			accept := base64.StdEncoding.EncodeToString(sum[:])
			wshead := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n"
			if _, err = wr.Write([]byte(wshead)); err != nil {
				return false, err
			}
		} else if contentLength > 0 {
			msg.ConnType = HTTP
			if len(packet) < contentLength {
				return false, nil
			}
			path += string(packet[:contentLength])
			packet = packet[contentLength:]
		}
		if path == "" {
			return true, nil
		}
		nmsg, err := readNativeMessageLine([]byte(path))
		if err != nil {
			return false, err
		}

		msg.OutputType = JSON
		msg.Args = nmsg.Args
		return true, nil
	}()
	if err != nil || !ready {
		return false, args[:0], kindHTTP, opacket, err
	}
	return true, args[:0], kindHTTP, packet, nil
}
func readNextCommand(packet []byte, argsIn [][]byte, msg *Message, wr io.Writer) (
	complete bool, args [][]byte, kind redcon.Kind, leftover []byte, err error,
) {
	if packet[0] == 'G' || packet[0] == 'P' {
		// could be an HTTP request
		var line []byte
		for i := 1; i < len(packet); i++ {
			if packet[i] == '\n' {
				if packet[i-1] == '\r' {
					line = packet[:i+1]
					break
				}
			}
		}
		if len(line) == 0 {
			return false, argsIn[:0], redcon.Redis, packet, nil
		}
		if len(line) > 11 && string(line[len(line)-11:len(line)-5]) == " HTTP/" {
			return readNextHTTPCommand(packet, argsIn, msg, wr)
		}
	}
	return redcon.ReadNextCommand(packet, args)
}

// ReadMessages ...
func (rd *PipelineReader) ReadMessages() ([]*Message, error) {
	var msgs []*Message
moreData:
	n, err := rd.rd.Read(rd.packet[:])
	if err != nil {
		return nil, err
	}
	if n == 0 {
		// need more data
		goto moreData
	}
	data := rd.packet[:n]
	if len(rd.buf) > 0 {
		data = append(rd.buf, data...)
	}
	for len(data) > 0 {
		msg := &Message{}
		complete, args, kind, leftover, err2 :=
			readNextCommand(data, nil, msg, rd.wr)
		if err2 != nil {
			err = err2
			break
		}
		if !complete {
			break
		}
		if kind == kindHTTP {
			if len(msg.Args) == 0 {
				return nil, errInvalidHTTP
			}
			msgs = append(msgs, msg)
		} else if len(args) > 0 {
			for i := 0; i < len(args); i++ {
				msg.Args = append(msg.Args, string(args[i]))
			}
			switch kind {
			case redcon.Redis:
				msg.ConnType = RESP
				msg.OutputType = RESP
			case redcon.Tile38:
				msg.ConnType = Native
				msg.OutputType = JSON
			case redcon.Telnet:
				msg.ConnType = RESP
				msg.OutputType = RESP
			}
			msgs = append(msgs, msg)
		}
		data = leftover
	}
	if len(data) > 0 {
		rd.buf = append(rd.buf[:0], data...)
	} else if len(rd.buf) > 0 {
		rd.buf = rd.buf[:0]
	}
	return msgs, err
}

func readNativeMessageLine(line []byte) (*Message, error) {
	var args []string
reading:
	for len(line) != 0 {
		if line[0] == '{' {
			// The native protocol cannot understand json boundaries so it assumes that
			// a json element must be at the end of the line.
			args = append(args, string(line))
			break
		}
		if line[0] == '"' && line[len(line)-1] == '"' {
			if len(args) > 0 &&
				strings.ToLower(args[0]) == "set" &&
				strings.ToLower(args[len(args)-1]) == "string" {
				// Setting a string value that is contained inside double quotes.
				// This is only because of the boundary issues of the native protocol.
				args = append(args, string(line[1:len(line)-1]))
				break
			}
		}
		i := 0
		for ; i < len(line); i++ {
			if line[i] == ' ' {
				arg := string(line[:i])
				if arg != "" {
					args = append(args, arg)
				}
				line = line[i+1:]
				continue reading
			}
		}
		args = append(args, string(line))
		break
	}
	return &Message{Args: args, ConnType: Native, OutputType: JSON}, nil
}

// InputStream is a helper type for managing input streams from inside
// the Data event.
type InputStream struct{ b []byte }

// Begin accepts a new packet and returns a working sequence of
// unprocessed bytes.
func (is *InputStream) Begin(packet []byte) (data []byte) {
	data = packet
	if len(is.b) > 0 {
		is.b = append(is.b, data...)
		data = is.b
	}
	return data
}

// End shifts the stream to match the unprocessed data.
func (is *InputStream) End(data []byte) {
	if len(data) > 0 {
		if len(data) != len(is.b) {
			is.b = append(is.b[:0], data...)
		}
	} else if len(is.b) > 0 {
		is.b = is.b[:0]
	}
}
