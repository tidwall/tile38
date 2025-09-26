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
	"net/http"
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tidwall/btree"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/gjson"
	"github.com/tidwall/redcon"
	"github.com/tidwall/resp"
	"github.com/tidwall/rtree"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/deadline"
	"github.com/tidwall/tile38/internal/endpoint"
	"github.com/tidwall/tile38/internal/log"
	"github.com/tidwall/tile38/internal/object"
	"github.com/tidwall/tile38/internal/viewer"
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
	command string // client command, like "SET" or "DEL"
	key     string // collection key
	newKey  string // new key, for RENAME command

	obj *object.Object // target object
	old *object.Object // previous object, if any

	updated   bool              // object was updated
	timestamp time.Time         // timestamp when the update occurred
	parent    bool              // when true, only children are forwarded
	pattern   string            // PDEL key pattern
	children  []*commandDetails // for multi actions such as "PDEL"
}

type rwlocker interface {
	LockLowPriority()
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

// rwspinlock is the same as a RWLock, but uses spinlocks instead of mutexes.
type rwspinlock struct {
	state atomic.Int32
}

func (l *rwspinlock) Lock() {
	for {
		state := l.state.Load()
		if state == 0 && l.state.CompareAndSwap(state, -1) {
			return
		}
		runtime.Gosched()
	}
}
func (l *rwspinlock) LockLowPriority() {
	// All write locks are low priority and unfair. First come first serve.
	l.Lock()
}
func (l *rwspinlock) Unlock() {
	if l.state.Add(1) > 0 {
		panic("Unlock of unlocked rwspinlock")
	}
}
func (l *rwspinlock) RLock() {
	for {
		state := l.state.Load()
		if state >= 0 && l.state.CompareAndSwap(state, state+1) {
			return
		}
		runtime.Gosched()
	}
}
func (l *rwspinlock) RUnlock() {
	if l.state.Add(-1) < 0 {
		panic("RUnlock of unlocked rwspinlock")
	}
}

// rwmutex is the same as a RWLock but includes a LockLowPriority for
// performing write locks that do not block the queue for readers.
type rwmutex struct {
	mu sync.RWMutex
}

func (l *rwmutex) Lock() {
	start := time.Now()
	for {
		for range 100 {
			if l.mu.TryLock() {
				return
			}
			runtime.Gosched()
		}
		if time.Since(start) > time.Millisecond*50 {
			break
		}
	}
	l.mu.Lock()
}
func (l *rwmutex) LockLowPriority() {
	for !l.mu.TryLock() {
		runtime.Gosched()
	}
}
func (l *rwmutex) Unlock() {
	l.mu.Unlock()
}
func (l *rwmutex) RLock() {
	l.mu.RLock()
}
func (l *rwmutex) RUnlock() {
	l.mu.RUnlock()
}

// Server is a tile38 controller
type Server struct {
	// user defined options
	opts Options

	// static values
	unix    string
	host    string
	port    int
	http    bool
	dir     string
	started time.Time
	config  *Config
	epc     *endpoint.Manager
	epool   *exprPool

	lnmu sync.Mutex
	ln   net.Listener // server listener

	// env opts
	geomParseOpts geojson.ParseOptions
	geomIndexOpts geometry.IndexOptions
	http500Errors bool

	// atomics
	followc            atomic.Int64 // counter when follow property changes
	statsTotalConns    atomic.Int64 // counter for total connections
	statsTotalCommands atomic.Int64 // counter for total commands
	statsTotalMsgsSent atomic.Int64 // counter for total sent webhook messages
	statsExpired       atomic.Int64 // item expiration counter
	lastShrinkDuration atomic.Int64
	stopServer         atomic.Bool
	outOfMemory        atomic.Bool
	loadedAndReady     atomic.Bool // server is loaded and ready for commands

	connsmu sync.RWMutex
	conns   map[int]*Client

	mu rwlocker // sync.RWMutex

	// aof
	aof       *os.File    // active aof file
	aofdirty  atomic.Bool // mark the aofbuf as having data
	aofbuf    []byte      // prewrite buffer
	aofsz     int         // active size of the aof file
	shrinking bool        // aof shrinking flag
	shrinklog [][]string  // aof shrinking log

	// database
	qdb  *buntdb.DB // hook queue log
	qidx uint64     // hook queue log last idx

	cols *btree.Map[string, *collection.Collection] // data collections

	hooks        *btree.BTree // hook name -- [string]*Hook
	hookCross    *rtree.RTree // hook spatial tree for "cross" geofences
	hookTree     *rtree.RTree // hook spatial tree for all
	hooksOut     *btree.BTree // hooks with "outside" detection -- [string]*Hook
	groupHooks   *btree.BTree // hooks that are connected to objects
	groupObjects *btree.BTree // objects that are connected to hooks
	hookExpires  *btree.BTree // queue of all hooks marked for expiration

	// followers (external aof readers)
	follows   map[*bytes.Buffer]bool
	fcond     *sync.Cond
	lstack    []*commandDetails
	lives     map[*liveBuffer]bool
	lcond     *sync.Cond   // live geofence signal
	faofsz    int          // last reported aofsize
	fcupflags atomic.Int32 // follow caught up (caughtUp and caughtUpOnce)
	aofconnM  map[net.Conn]io.Closer
	pubq      pubQueue

	// lua scripts
	luascripts *lScriptMap
	luapool    *lStatePool

	// pubsub system (SUBSCRIBE, PUBLISH, and SETCHAN)
	pubsub *pubsub

	// monitor connections (using the MONITOR command)
	monconnsMu sync.RWMutex
	monconns   map[net.Conn]bool
}

// Options for Serve()
type Options struct {
	Host           string
	Port           int
	Dir            string
	UseHTTP        bool
	MetricsAddr    string
	UnixSocketPath string // path for unix socket

	// DevMode puts application in to dev mode
	DevMode bool

	// ShowDebugMessages allows for log.Debug to print to console.
	ShowDebugMessages bool

	// ProtectedMode forces Tile38 to default in protected mode.
	ProtectedMode string

	// AppendOnly allows for disabling the appendonly file.
	AppendOnly bool

	// AppendFileName allows for custom appendonly file path
	AppendFileName string

	// QueueFileName allows for custom queue.db file path
	QueueFileName string

	// Shutdown allows for shutting down the server.
	Shutdown <-chan bool

	// Spinlock uses a spinlock instead of a mutex
	Spinlock bool
}

// Serve starts a new tile38 server
func Serve(opts Options) error {
	if opts.AppendFileName == "" {
		opts.AppendFileName = path.Join(opts.Dir, "appendonly.aof")
	}
	if opts.QueueFileName == "" {
		opts.QueueFileName = path.Join(opts.Dir, "queue.db")
	}
	if opts.ProtectedMode == "" {
		opts.ProtectedMode = "no"
	}

	log.Infof("Server started, Tile38 version %s, git %s", core.Version, core.GitSHA)
	defer func() {
		log.Warn("Server has shutdown, bye now")
		if false {
			// prints the stack, looking for running goroutines.
			buf := make([]byte, 10000)
			n := runtime.Stack(buf, true)
			println(string(buf[:n]))
		}
	}()

	var lock rwlocker

	if opts.Spinlock {
		lock = new(rwspinlock)
	} else {
		lock = new(rwmutex)
	}

	// Initialize the s
	s := &Server{
		mu:        lock,
		unix:      opts.UnixSocketPath,
		host:      opts.Host,
		port:      opts.Port,
		dir:       opts.Dir,
		follows:   make(map[*bytes.Buffer]bool),
		fcond:     sync.NewCond(&sync.Mutex{}),
		lives:     make(map[*liveBuffer]bool),
		lcond:     sync.NewCond(&sync.Mutex{}),
		hooks:     btree.NewNonConcurrent(byHookName),
		hooksOut:  btree.NewNonConcurrent(byHookName),
		hookCross: &rtree.RTree{},
		hookTree:  &rtree.RTree{},
		aofconnM:  make(map[net.Conn]io.Closer),
		started:   time.Now(),
		conns:     make(map[int]*Client),
		http:      opts.UseHTTP,
		pubsub:    newPubsub(),
		pubq:      pubQueue{cond: sync.NewCond(&sync.Mutex{})},
		monconns:  make(map[net.Conn]bool),
		cols:      &btree.Map[string, *collection.Collection]{},

		groupHooks:   btree.NewNonConcurrent(byGroupHook),
		groupObjects: btree.NewNonConcurrent(byGroupObject),
		hookExpires:  btree.NewNonConcurrent(byHookExpires),
		opts:         opts,
	}
	s.epool = newExprPool(s)
	s.epc = endpoint.NewManager(s)
	defer s.epc.Shutdown()
	s.luascripts = s.newScriptMap()
	s.luapool = s.newPool()
	defer s.luapool.Shutdown()

	if err := os.MkdirAll(opts.Dir, 0700); err != nil {
		return err
	}
	var err error
	s.config, err = loadConfig(filepath.Join(opts.Dir, "config"))
	if err != nil {
		return err
	}

	// Send "500 Internal Server" error instead of "200 OK" for json responses
	// with `"ok":false`. T38HTTP500ERRORS=1
	s.http500Errors, _ = strconv.ParseBool(os.Getenv("T38HTTP500ERRORS"))

	// Allow for geometry indexing options through environment variables:
	// T38IDXGEOMKIND -- None, RTree, QuadTree
	// T38IDXGEOM -- Min number of points in a geometry for indexing.
	// T38IDXMULTI -- Min number of object in a Multi/Collection for indexing.
	s.geomParseOpts = *geojson.DefaultParseOptions
	s.geomIndexOpts = *geometry.DefaultIndexOptions
	n, err := strconv.ParseUint(os.Getenv("T38IDXGEOM"), 10, 32)
	if err == nil {
		s.geomParseOpts.IndexGeometry = int(n)
		s.geomIndexOpts.MinPoints = int(n)
	}
	n, err = strconv.ParseUint(os.Getenv("T38IDXMULTI"), 10, 32)
	if err == nil {
		s.geomParseOpts.IndexChildren = int(n)
	}
	requireValid := os.Getenv("REQUIREVALID")
	if requireValid != "" {
		s.geomParseOpts.RequireValid = true
	}
	indexKind := os.Getenv("T38IDXGEOMKIND")
	switch indexKind {
	default:
		log.Errorf("Unknown index kind: %s", indexKind)
	case "":
	case "None":
		s.geomParseOpts.IndexGeometryKind = geometry.None
		s.geomIndexOpts.Kind = geometry.None
	case "RTree":
		s.geomParseOpts.IndexGeometryKind = geometry.RTree
		s.geomIndexOpts.Kind = geometry.RTree
	case "QuadTree":
		s.geomParseOpts.IndexGeometryKind = geometry.QuadTree
		s.geomIndexOpts.Kind = geometry.QuadTree
	}
	if s.geomParseOpts.IndexGeometryKind == geometry.None {
		log.Debugf("Geom indexing: %s",
			s.geomParseOpts.IndexGeometryKind,
		)
	} else {
		log.Debugf("Geom indexing: %s (%d points)",
			s.geomParseOpts.IndexGeometryKind,
			s.geomParseOpts.IndexGeometry,
		)
	}
	log.Debugf("Multi indexing: RTree (%d points)", s.geomParseOpts.IndexChildren)

	nerr := make(chan error)
	go func() {
		// Start the server in the background
		nerr <- s.netServe()
	}()

	var fstop atomic.Bool
	go func() {
		for !fstop.Load() {
			s.fcond.Broadcast()
			time.Sleep(time.Second / 4)
		}
	}()

	go func() {
		<-opts.Shutdown
		s.stopServer.Store(true)
		log.Warnf("Shutting down...")
		fstop.Store(true)
		s.lnmu.Lock()
		ln := s.ln
		s.ln = nil
		s.lnmu.Unlock()
		if ln != nil {
			ln.Close()
		}
		for conn, f := range s.aofconnM {
			conn.Close()
			f.Close()
		}
	}()

	// Load the queue before the aof
	qdb, err := buntdb.Open(opts.QueueFileName)
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

	s.qdb = qdb
	s.qidx = qidx
	if err := s.migrateAOF(); err != nil {
		return err
	}
	if opts.AppendOnly {
		f, err := os.OpenFile(opts.AppendFileName, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		s.aof = f
		if err := s.loadAOF(); err != nil {
			return err
		}
		defer func() {
			s.flushAOF(false)
			s.aof.Sync()
		}()
	}

	// Start background routines
	var bgwg sync.WaitGroup

	if s.config.followHost() != "" {
		bgwg.Add(1)
		go func() {
			defer bgwg.Done()
			s.follow(s.config.followHost(), s.config.followPort(),
				int(s.followc.Load()))
		}()
	}

	var mln net.Listener
	if opts.MetricsAddr != "" {
		log.Infof("Listening for metrics at: %s", opts.MetricsAddr)
		mln, err = net.Listen("tcp", opts.MetricsAddr)
		if err != nil {
			return err
		}
		bgwg.Add(1)
		go func() {
			defer bgwg.Done()
			smux := http.NewServeMux()
			smux.HandleFunc("/", s.MetricsIndexHandler)
			smux.HandleFunc("/metrics", s.MetricsHandler)
			err := http.Serve(mln, smux)
			if err != nil {
				if !s.stopServer.Load() {
					log.Fatalf("metrics server: %s", err)
				}
			}
		}()
	}

	bgwg.Add(1)
	go s.processLives(&bgwg)
	bgwg.Add(1)
	go s.watchOutOfMemory(&bgwg)
	bgwg.Add(1)
	go s.watchLuaStatePool(&bgwg)
	bgwg.Add(1)
	go s.watchAutoGC(&bgwg)
	bgwg.Add(1)
	go s.backgroundExpiring(&bgwg)
	bgwg.Add(1)
	go s.backgroundSyncAOF(&bgwg)
	bgwg.Add(1)
	go s.startPublishQueue(&bgwg)
	defer func() {
		log.Debug("Stopping background routines")
		// Stop background routines
		s.stopPublishQueue()
		s.followc.Add(1) // this will force any follow communication to die
		s.stopServer.Store(true)
		if mln != nil {
			mln.Close() // Stop the metrics server
		}
		bgwg.Wait()
	}()

	// Server is now loaded and ready. Wait for network error messages.
	s.loadedAndReady.Store(true)
	return <-nerr
}

func (s *Server) isProtected() bool {
	if s.opts.ProtectedMode == "no" {
		// --protected-mode no
		return false
	}
	if s.host != "" && s.host != "127.0.0.1" &&
		s.host != "::1" && s.host != "localhost" {
		// -h address
		return false
	}
	is := s.config.protectedMode() != "no" && s.config.requirePass() == ""
	return is
}

func (s *Server) netServe() error {
	var ln net.Listener
	var err error
	if s.unix != "" {
		os.RemoveAll(s.unix)
		ln, err = net.Listen("unix", s.unix)
	} else {
		tcpAddr := fmt.Sprintf("%s:%d", s.host, s.port)
		ln, err = net.Listen("tcp", tcpAddr)
	}
	if err != nil {
		return err
	}
	s.lnmu.Lock()
	s.ln = ln
	s.lnmu.Unlock()

	var wg sync.WaitGroup
	defer func() {
		log.Debug("Closing client connections...")
		s.connsmu.RLock()
		for _, c := range s.conns {
			c.closer.Close()
		}
		s.connsmu.RUnlock()
		wg.Wait()
		ln.Close()
		log.Debug("Client connection closed")
	}()

	log.Infof("Ready to accept connections at %s", ln.Addr())
	var clientID int64
	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.stopServer.Load() {
				return nil
			}
			log.Warn(err)
			time.Sleep(time.Second / 5)
			continue
		}
		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()

			// open connection
			// create the client
			client := new(Client)
			client.id = int(atomic.AddInt64(&clientID, 1))
			client.opened = time.Now()
			client.remoteAddr = conn.RemoteAddr().String()
			client.closer = conn

			// add client to server map
			s.connsmu.Lock()
			s.conns[client.id] = client
			s.connsmu.Unlock()
			s.statsTotalConns.Add(1)

			// set the client keep-alive, if needed
			if s.config.keepAlive() > 0 {
				if conn, ok := conn.(*net.TCPConn); ok {
					conn.SetKeepAlive(true)
					conn.SetKeepAlivePeriod(
						time.Duration(s.config.keepAlive()) * time.Second,
					)
				}
			}
			log.Debugf("Opened connection: %s", client.remoteAddr)

			defer func() {
				// close connection
				// delete from server map
				s.connsmu.Lock()
				delete(s.conns, client.id)
				s.connsmu.Unlock()
				log.Debugf("Closed connection: %s", client.remoteAddr)
				conn.Close()
			}()

			var lastConnType Type
			var lastOutputType Type

			// check if the connection is protected
			if !strings.HasPrefix(client.remoteAddr, "127.0.0.1:") &&
				!strings.HasPrefix(client.remoteAddr, "[::1]:") {
				if s.isProtected() {
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
					if !s.http && (msg.ConnType == WebSocket ||
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
						s.statsTotalCommands.Add(1)

						// handle the command
						err := s.handleInputCommand(client, msg)
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
									err := s.goLive(
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
					if s.aofdirty.Load() {
						func() {
							// prewrite
							s.mu.Lock()
							defer s.mu.Unlock()
							s.flushAOF(false)
						}()
						s.aofdirty.Store(false)
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

func (s *Server) watchAutoGC(wg *sync.WaitGroup) {
	defer wg.Done()
	start := time.Now()
	s.loopUntilServerStops(time.Second, func() {
		autoGC := s.config.autoGC()
		if autoGC == 0 {
			return
		}
		if time.Since(start) < time.Second*time.Duration(autoGC) {
			return
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
		start = time.Now()
	})
}

func (s *Server) checkOutOfMemory() {
	if s.stopServer.Load() {
		return
	}
	oom := s.outOfMemory.Load()
	var mem runtime.MemStats
	if s.config.maxMemory() == 0 {
		if oom {
			s.outOfMemory.Store(false)
		}
		return
	}
	if oom {
		runtime.GC()
	}
	runtime.ReadMemStats(&mem)
	s.outOfMemory.Store(int(mem.HeapAlloc) > s.config.maxMemory())
}

func (s *Server) loopUntilServerStops(dur time.Duration, op func()) {
	var last time.Time
	for {
		if s.stopServer.Load() {
			return
		}
		now := time.Now()
		if now.Sub(last) > dur {
			op()
			last = now
		}
		time.Sleep(time.Second / 5)
	}
}

func (s *Server) watchOutOfMemory(wg *sync.WaitGroup) {
	defer wg.Done()
	s.loopUntilServerStops(time.Second*4, func() {
		s.checkOutOfMemory()
	})
}

func (s *Server) watchLuaStatePool(wg *sync.WaitGroup) {
	defer wg.Done()
	s.loopUntilServerStops(time.Second*10, func() {
		s.luapool.Prune()
	})
}

// backgroundSyncAOF ensures that the aof buffer is does not grow too big.
func (s *Server) backgroundSyncAOF(wg *sync.WaitGroup) {
	defer wg.Done()
	s.loopUntilServerStops(time.Second, func() {
		s.mu.LockLowPriority()
		defer s.mu.Unlock()
		s.flushAOF(true)
	})
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

func (s *Server) handleInputCommand(client *Client, msg *Message) error {
	start := time.Now()
	var mvt bool
	if msg.ConnType == HTTP && len(msg.Args) == 1 {
		var query string
		if i := strings.IndexByte(msg.Args[0], '?'); i != -1 {
			query = msg.Args[0][i+1:]
			msg.Args[0] = msg.Args[0][:i]
		}
		if strings.HasSuffix(msg.Args[0], ".mvt") ||
			strings.HasSuffix(msg.Args[0], ".pbf") {
			mvt = mvtFilterHTTPArgs(msg, query)
		} else if strings.HasPrefix(msg.Args[0], "viewer/") ||
			msg.Args[0] == "viewer" {
			return viewer.HandleHTTP(client, "/"+strings.Join(msg.Args, "/"),
				s.opts.DevMode)
		}
	}
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
			extraNL := 2
			contentType := "application/json; charset=utf-8"
			status := "200 OK"
			if (s.http500Errors || msg._command == "healthz") &&
				!gjson.Get(res, "ok").Bool() {
				status = "500 Internal Server Error"
			} else if mvt {
				v := gjson.Get(res, "mvt")
				if !v.Exists() {
					status = "500 Internal Server Error"
				} else {
					res = v.String()
					out, err := base64.RawStdEncoding.DecodeString(res)
					if err != nil {
						status = "500 Internal Server Error"
					} else {
						res = string(out)
						contentType = "application/vnd.mapbox-vector-tile"
					}
				}
			}
			_, err := fmt.Fprintf(client, ""+
				"HTTP/1.1 %s\r\n"+
				"Connection: close\r\n"+
				"Content-Type: %s\r\n"+
				"Content-Length: %d\r\n"+
				"Access-Control-Allow-Origin: *\r\n"+
				"\r\n", status, contentType, len(res)+extraNL)
			if err != nil {
				return err
			}
			_, err = io.WriteString(client, res)
			if err != nil {
				return err
			}
			if extraNL == 2 {
				_, err = io.WriteString(client, "\r\n")
				if err != nil {
					return err
				}
			}
			return nil
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

	cmd := msg.Command()
	defer func() {
		took := time.Since(start).Seconds()
		cmdDurations.With(prometheus.Labels{"cmd": cmd}).Observe(took)
	}()

	// Ping. Just send back the response. No need to put through the pipeline.
	if cmd == "ping" || cmd == "echo" {
		switch msg.OutputType {
		case JSON:
			if len(msg.Args) > 1 {
				return writeOutput(`{"ok":true,"` + cmd + `":` + jsonString(msg.Args[1]) + `,"elapsed":"` + time.Since(start).String() + `"}`)
			}
			return writeOutput(`{"ok":true,"` + cmd + `":"pong","elapsed":"` + time.Since(start).String() + `"}`)
		case RESP:
			if len(msg.Args) > 1 {
				data := redcon.AppendBulkString(nil, msg.Args[1])
				return writeOutput(string(data))
			}
			return writeOutput("+PONG\r\n")
		}
		s.sendMonitor(nil, msg, client, false)
		return nil
	}

	writeErr := func(errMsg string) error {
		switch msg.OutputType {
		case JSON:
			return writeOutput(`{"ok":false,"err":` + jsonString(errMsg) + `,"elapsed":"` + time.Since(start).String() + "\"}")
		case RESP:
			if errMsg == errInvalidNumberOfArguments.Error() {
				return writeOutput("-ERR wrong number of arguments for '" + cmd + "' command\r\n")
			}
			var ucprefix bool
			word := strings.Split(errMsg, " ")[0]
			if len(word) > 0 {
				ucprefix = true
				for i := 0; i < len(word); i++ {
					if word[i] < 'A' || word[i] > 'Z' {
						ucprefix = false
						break
					}
				}
			}
			if !ucprefix {
				errMsg = "ERR " + errMsg
			}
			v, _ := resp.ErrorValue(errors.New(errMsg)).MarshalRESP()
			return writeOutput(string(v))
		}
		return nil
	}

	if !s.loadedAndReady.Load() {
		switch msg.Command() {
		case "output", "ping", "echo", "auth":
		default:
			return writeErr("LOADING Tile38 is loading the dataset in memory")
		}
	}

	if cmd == "hello" {
		// Not Supporting RESP3+, returns an ERR instead.
		return writeErr("unknown command '" + msg.Args[0] + "'")
	}

	if cmd == "timeout" {
		if err := rewriteTimeoutMsg(msg); err != nil {
			return writeErr(err.Error())
		}
	}

	var write bool

	if (!client.authd || cmd == "auth") && cmd != "output" && cmd != "healthz" {
		if s.config.requirePass() != "" {
			password := ""
			// This better be an AUTH command or the Message should contain an Auth
			if cmd != "auth" && msg.Auth == "" {
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
			if s.config.requirePass() != strings.TrimSpace(password) {
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
		s.mu.RLock()
		defer s.mu.RUnlock()
	case "set", "del", "drop", "fset", "flushdb",
		"setchan", "pdelchan", "delchan",
		"sethook", "pdelhook", "delhook",
		"expire", "persist", "jset", "pdel", "rename", "renamenx":
		// write operations
		write = true
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.config.followHost() != "" {
			return writeErr("not the leader")
		}
		if s.config.readOnly() {
			return writeErr("read only")
		}
	case "eval", "evalsha":
		// write operations (potentially) but no AOF for the script command itself
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.config.followHost() != "" {
			return writeErr("not the leader")
		}
		if s.config.readOnly() {
			return writeErr("read only")
		}
	case "get", "keys", "scan", "nearby", "within", "intersects", "hooks",
		"chans", "search", "ttl", "bounds", "server", "info", "type", "jget",
		"evalro", "evalrosha", "role", "fget", "exists", "fexists":
		// read operations
		s.mu.RLock()
		defer s.mu.RUnlock()
		// fallthrough to perform a "catching up to leader" check
		fallthrough
	case "healthz":
		// healthz operation does not require a read lock. It only needs
		// a caughtup once check to leader, which is atomic.
		if s.config.followHost() != "" && !s.caughtUpOnce() {
			return writeErr("catching up to leader")
		}
	case "follow", "slaveof", "replconf", "readonly", "config":
		// system operations
		// does not write to aof, but requires a write lock.
		s.mu.Lock()
		defer s.mu.Unlock()
	case "output":
		// this is local connection operation. Locks not needed.
	case "echo":
	case "massinsert":
		// dev operation
	case "sleep":
		// dev operation
		s.mu.RLock()
		defer s.mu.RUnlock()
	case "shutdown":
		// dev operation
		s.mu.Lock()
		defer s.mu.Unlock()
	case "aofshrink":
		s.mu.RLock()
		defer s.mu.RUnlock()
	case "client":
		s.mu.Lock()
		defer s.mu.Unlock()
	case "evalna", "evalnasha":
		// No locking for scripts, otherwise writes cannot happen within scripts
	case "subscribe", "psubscribe", "publish":
		// No locking for pubsub
	case "monitor":
		// No locking for monitor
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
		res, d, err = s.command(msg, client)
		if msg.Deadline != nil {
			msg.Deadline.Check()
		}
		return res, d, err
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
		if err := s.writeAOF(msg.Args, &d); err != nil {
			if _, ok := err.(errAOFHook); ok {
				return writeErr(err.Error())
			}
			log.Fatal(err)
			return err
		}
	}
	var resStr string
	resStr, err = serializeOutput(res)
	if err != nil {
		return err
	}
	if err := writeOutput(resStr); err != nil {
		return err
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

func (s *Server) reset() {
	s.aofsz = 0
	s.cols.Clear()
}

func (s *Server) command(msg *Message, client *Client) (
	res resp.Value, d commandDetails, err error,
) {
	switch msg.Command() {
	default:
		err = fmt.Errorf("unknown command '%s'", msg.Args[0])
	case "set":
		res, d, err = s.cmdSET(msg)
	case "fset":
		res, d, err = s.cmdFSET(msg)
	case "del":
		res, d, err = s.cmdDEL(msg)
	case "pdel":
		res, d, err = s.cmdPDEL(msg)
	case "drop":
		res, d, err = s.cmdDROP(msg)
	case "flushdb":
		res, d, err = s.cmdFLUSHDB(msg)
	case "rename":
		res, d, err = s.cmdRENAME(msg)
	case "renamenx":
		res, d, err = s.cmdRENAME(msg)
	case "sethook":
		res, d, err = s.cmdSetHook(msg)
	case "delhook":
		res, d, err = s.cmdDelHook(msg)
	case "pdelhook":
		res, d, err = s.cmdPDelHook(msg)
	case "hooks":
		res, err = s.cmdHooks(msg)
	case "setchan":
		res, d, err = s.cmdSetHook(msg)
	case "delchan":
		res, d, err = s.cmdDelHook(msg)
	case "pdelchan":
		res, d, err = s.cmdPDelHook(msg)
	case "chans":
		res, err = s.cmdHooks(msg)
	case "expire":
		res, d, err = s.cmdEXPIRE(msg)
	case "persist":
		res, d, err = s.cmdPERSIST(msg)
	case "ttl":
		res, err = s.cmdTTL(msg)
	case "shutdown":
		if !s.opts.DevMode {
			err = fmt.Errorf("unknown command '%s'", msg.Args[0])
			return
		}
		log.Fatal("shutdown requested by developer")
	case "massinsert":
		if !s.opts.DevMode {
			err = fmt.Errorf("unknown command '%s'", msg.Args[0])
			return
		}
		res, err = s.cmdMassInsert(msg)
	case "sleep":
		if !s.opts.DevMode {
			err = fmt.Errorf("unknown command '%s'", msg.Args[0])
			return
		}
		res, err = s.cmdSleep(msg)
	case "follow", "slaveof":
		res, err = s.cmdFollow(msg)
	case "replconf":
		res, err = s.cmdReplConf(msg, client)
	case "readonly":
		res, err = s.cmdREADONLY(msg)
	case "stats":
		res, err = s.cmdSTATS(msg)
	case "server":
		res, err = s.cmdSERVER(msg)
	case "healthz":
		res, err = s.cmdHEALTHZ(msg)
	case "info":
		res, err = s.cmdINFO(msg)
	case "role":
		res, err = s.cmdROLE(msg)
	case "scan":
		res, err = s.cmdScan(msg)
	case "nearby":
		res, err = s.cmdNearby(msg)
	case "within":
		res, err = s.cmdWITHIN(msg)
	case "intersects":
		res, err = s.cmdINTERSECTS(msg)
	case "search":
		res, err = s.cmdSearch(msg)
	case "bounds":
		res, err = s.cmdBOUNDS(msg)
	case "get":
		res, err = s.cmdGET(msg)
	case "fget":
		res, err = s.cmdFGET(msg)
	case "jget":
		res, err = s.cmdJget(msg)
	case "jset":
		res, d, err = s.cmdJset(msg)
	case "jdel":
		res, d, err = s.cmdJdel(msg)
	case "type":
		res, err = s.cmdTYPE(msg)
	case "keys":
		res, err = s.cmdKEYS(msg)
	case "exists":
		res, err = s.cmdEXISTS(msg)
	case "fexists":
		res, err = s.cmdFEXISTS(msg)
	case "output":
		res, err = s.cmdOUTPUT(msg)
	case "aof":
		res, err = s.cmdAOF(msg)
	case "aofmd5":
		res, err = s.cmdAOFMD5(msg)
	case "gc":
		runtime.GC()
		debug.FreeOSMemory()
		res = OKMessage(msg, time.Now())
	case "aofshrink":
		go s.aofshrink()
		res = OKMessage(msg, time.Now())
	case "config get":
		res, err = s.cmdConfigGet(msg)
	case "config set":
		res, err = s.cmdConfigSet(msg)
	case "config rewrite":
		res, err = s.cmdConfigRewrite(msg)
	case "config", "script":
		// These get rewritten into "config foo" and "script bar"
		err = fmt.Errorf("unknown command '%s'", msg.Args[0])
		if len(msg.Args) > 1 {
			msg.Args[1] = msg.Args[0] + " " + msg.Args[1]
			msg.Args = msg.Args[1:]
			msg._command = ""
			return s.command(msg, client)
		}
	case "client":
		res, err = s.cmdCLIENT(msg, client)
	case "eval", "evalro", "evalna":
		res, err = s.cmdEvalUnified(false, msg)
	case "evalsha", "evalrosha", "evalnasha":
		res, err = s.cmdEvalUnified(true, msg)
	case "script load":
		res, err = s.cmdScriptLoad(msg)
	case "script exists":
		res, err = s.cmdScriptExists(msg)
	case "script flush":
		res, err = s.cmdScriptFlush(msg)
	case "subscribe":
		res, err = s.cmdSubscribe(msg)
	case "psubscribe":
		res, err = s.cmdPsubscribe(msg)
	case "publish":
		res, err = s.cmdPublish(msg)
	case "test":
		res, err = s.cmdTEST(msg)
	case "monitor":
		res, err = s.cmdMonitor(msg)
	}

	s.sendMonitor(err, msg, client, false)
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
		return resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
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
	_command       string
	Args           []string
	ConnType       Type
	OutputType     Type
	Auth           string
	AcceptEncoding string
	Deadline       *deadline.Deadline
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

func headerValue(header, name string) int {
	i := 0
	for ; i < len(name) && i < len(header); i++ {
		a := header[i]
		if a >= 'A' && a <= 'Z' {
			a += 32
		}
		b := name[i]
		if b >= 'A' && b <= 'Z' {
			b += 32
		}
		if a != b {
			return -1
		}
	}
	if i != len(name) || i == len(header) || header[i] != ':' {
		return -1
	}
	i++
	for ; i < len(header); i++ {
		if header[i] != ' ' && header[i] != '\t' {
			break
		}
	}
	return i
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
		// Handle CORS request for allowed origins
		if method == "OPTIONS" {
			if wr == nil {
				return false, errors.New("connection is nil")
			}
			corshead := "HTTP/1.1 204 No Content\r\n" +
				"Connection: close\r\n" +
				"Access-Control-Allow-Origin: *\r\n" +
				"Access-Control-Allow-Headers: *, Authorization\r\n" +
				"Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n\r\n"

			if _, err = wr.Write([]byte(corshead)); err != nil {
				return false, err
			}
			return false, nil
		}
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
		acceptEncoding := ""
		for _, hdr := range headers[1:] {
			var i int
			if i = headerValue(hdr, "Accept-Encoding"); i != -1 {
				acceptEncoding = strings.TrimSpace(hdr[i:])
			} else if i = headerValue(hdr, "Authorization"); i != -1 {
				msg.Auth = strings.TrimSpace(hdr[i:])
			} else if i = headerValue(hdr, "Upgrade"); i != -1 {
				val := strings.TrimSpace(hdr[i:])
				if strings.ToLower(val) == "websocket" {
					websocket = true
				}
			} else if i = headerValue(hdr, "Sec-Websocket-Version"); i != -1 {
				val := strings.TrimSpace(hdr[i:])
				n, err := strconv.ParseUint(strings.TrimSpace(val), 10, 64)
				if err != nil {
					return false, err
				}
				websocketVersion = int(n)
			} else if i = headerValue(hdr, "Sec-Websocket-Key"); i != -1 {
				websocketKey = strings.TrimSpace(hdr[i:])
			} else if i = headerValue(hdr, "Content-Length"); i != -1 {
				val := strings.TrimSpace(hdr[i:])
				n, err := strconv.ParseUint(strings.TrimSpace(val), 10, 64)
				if err != nil {
					return false, err
				}
				contentLength = int(n)
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
		msg.AcceptEncoding = acceptEncoding
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
	if packet[0] == 'G' || packet[0] == 'P' || packet[0] == 'O' {
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

// clientErrorf is the same as fmt.Errorf, but is intented for errors that are
// sent back to the client. This allows for the Go static checker to ignore
// throwing warning for certain error strings.
// https://staticcheck.io/docs/checks#ST1005
func clientErrorf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}
