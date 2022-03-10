package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/internal/collection"
)

var memStats runtime.MemStats
var memStatsMu sync.Mutex
var memStatsBG bool

// ReadMemStats returns the latest memstats. It provides an instant response.
func readMemStats() runtime.MemStats {
	memStatsMu.Lock()
	if !memStatsBG {
		runtime.ReadMemStats(&memStats)
		go func() {
			var ms runtime.MemStats
			for {
				runtime.ReadMemStats(&ms)
				memStatsMu.Lock()
				memStats = ms
				memStatsMu.Unlock()
				time.Sleep(time.Second / 5)
			}
		}()
		memStatsBG = true
	}
	ms := memStats
	memStatsMu.Unlock()
	return ms
}

func (s *Server) cmdStats(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ms = []map[string]interface{}{}

	if len(vs) == 0 {
		return NOMessage, errInvalidNumberOfArguments
	}
	var vals []resp.Value
	var key string
	var ok bool
	for {
		vs, key, ok = tokenval(vs)
		if !ok {
			break
		}
		col := s.getCol(key)
		if col != nil {
			m := make(map[string]interface{})
			m["num_points"] = col.PointCount()
			m["in_memory_size"] = col.TotalWeight()
			m["num_objects"] = col.Count()
			m["num_strings"] = col.StringCount()
			switch msg.OutputType {
			case JSON:
				ms = append(ms, m)
			case RESP:
				vals = append(vals, resp.ArrayValue(respValuesSimpleMap(m)))
			}
		} else {
			switch msg.OutputType {
			case JSON:
				ms = append(ms, nil)
			case RESP:
				vals = append(vals, resp.NullValue())
			}
		}
	}
	switch msg.OutputType {
	case JSON:

		data, err := json.Marshal(ms)
		if err != nil {
			return NOMessage, err
		}
		res = resp.StringValue(`{"ok":true,"stats":` + string(data) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case RESP:
		res = resp.ArrayValue(vals)
	}
	return res, nil
}

func (s *Server) cmdServer(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	m := make(map[string]interface{})
	args := msg.Args[1:]

	// Switch on the type of stats requested
	switch len(args) {
	case 0:
		s.basicStats(m)
	case 1:
		if strings.ToLower(args[0]) == "ext" {
			s.extStats(m)
		}
	default:
		return NOMessage, errInvalidNumberOfArguments
	}

	switch msg.OutputType {
	case JSON:
		data, err := json.Marshal(m)
		if err != nil {
			return NOMessage, err
		}
		res = resp.StringValue(`{"ok":true,"stats":` + string(data) + `,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		vals := respValuesSimpleMap(m)
		res = resp.ArrayValue(vals)
	}
	return res, nil
}

// basicStats populates the passed map with basic system/go/tile38 statistics
func (s *Server) basicStats(m map[string]interface{}) {
	m["id"] = s.config.serverID()
	if s.config.followHost() != "" {
		m["following"] = fmt.Sprintf("%s:%d", s.config.followHost(),
			s.config.followPort())
		m["caught_up"] = s.fcup
		m["caught_up_once"] = s.fcuponce
	}
	m["http_transport"] = s.http
	m["pid"] = os.Getpid()
	m["aof_size"] = s.aofsz
	m["num_collections"] = s.cols.Len()
	m["num_hooks"] = len(s.hooks)
	sz := 0
	s.cols.Scan(func(key string, value interface{}) bool {
		col := value.(*collection.Collection)
		sz += col.TotalWeight()
		return true
	})
	m["in_memory_size"] = sz
	points := 0
	objects := 0
	strings := 0
	s.cols.Scan(func(key string, value interface{}) bool {
		col := value.(*collection.Collection)
		points += col.PointCount()
		objects += col.Count()
		strings += col.StringCount()
		return true
	})
	m["num_points"] = points
	m["num_objects"] = objects
	m["num_strings"] = strings
	mem := readMemStats()
	avgsz := 0
	if points != 0 {
		avgsz = int(mem.HeapAlloc) / points
	}
	m["mem_alloc"] = mem.Alloc
	m["heap_size"] = mem.HeapAlloc
	m["heap_released"] = mem.HeapReleased
	m["max_heap_size"] = s.config.maxMemory()
	m["avg_item_size"] = avgsz
	m["version"] = core.Version
	m["pointer_size"] = (32 << uintptr(uint64(^uintptr(0))>>63)) / 8
	m["read_only"] = s.config.readOnly()
	m["cpus"] = runtime.NumCPU()
	n, _ := runtime.ThreadCreateProfile(nil)
	m["threads"] = float64(n)
}

// extStats populates the passed map with extended system/go/tile38 statistics
func (s *Server) extStats(m map[string]interface{}) {
	n, _ := runtime.ThreadCreateProfile(nil)
	mem := readMemStats()

	// Go/Memory Stats

	// Number of goroutines that currently exist
	m["go_goroutines"] = runtime.NumGoroutine()
	// Number of OS threads created
	m["go_threads"] = float64(n)
	// A summary of the GC invocation durations
	m["go_version"] = runtime.Version()
	// Number of bytes allocated and still in use
	m["alloc_bytes"] = mem.Alloc
	// Total number of bytes allocated, even if freed
	m["alloc_bytes_total"] = mem.TotalAlloc
	// Number of CPUS available on the system
	m["sys_cpus"] = runtime.NumCPU()
	// Number of bytes obtained from system
	m["sys_bytes"] = mem.Sys
	// Total number of pointer lookups
	m["lookups_total"] = mem.Lookups
	// Total number of mallocs
	m["mallocs_total"] = mem.Mallocs
	// Total number of frees
	m["frees_total"] = mem.Frees
	// Number of heap bytes allocated and still in use
	m["heap_alloc_bytes"] = mem.HeapAlloc
	// Number of heap bytes obtained from system
	m["heap_sys_bytes"] = mem.HeapSys
	// Number of heap bytes waiting to be used
	m["heap_idle_bytes"] = mem.HeapIdle
	// Number of heap bytes that are in use
	m["heap_inuse_bytes"] = mem.HeapInuse
	// Number of heap bytes released to OS
	m["heap_released_bytes"] = mem.HeapReleased
	// Number of allocated objects
	m["heap_objects"] = mem.HeapObjects
	// Number of bytes in use by the stack allocator
	m["stack_inuse_bytes"] = mem.StackInuse
	// Number of bytes obtained from system for stack allocator
	m["stack_sys_bytes"] = mem.StackSys
	// Number of bytes in use by mspan structures
	m["mspan_inuse_bytes"] = mem.MSpanInuse
	// Number of bytes used for mspan structures obtained from system
	m["mspan_sys_bytes"] = mem.MSpanSys
	// Number of bytes in use by mcache structures
	m["mcache_inuse_bytes"] = mem.MCacheInuse
	// Number of bytes used for mcache structures obtained from system
	m["mcache_sys_bytes"] = mem.MCacheSys
	// Number of bytes used by the profiling bucket hash table
	m["buck_hash_sys_bytes"] = mem.BuckHashSys
	// Number of bytes used for garbage collection system metadata
	m["gc_sys_bytes"] = mem.GCSys
	// Number of bytes used for other system allocations
	m["other_sys_bytes"] = mem.OtherSys
	// Number of heap bytes when next garbage collection will take place
	m["next_gc_bytes"] = mem.NextGC
	// Number of seconds since 1970 of last garbage collection
	m["last_gc_time_seconds"] = float64(mem.LastGC) / 1e9
	// The fraction of this program's available CPU time used by the GC since
	// the program started
	m["gc_cpu_fraction"] = mem.GCCPUFraction

	// Tile38 Stats

	// ID of the server
	m["tile38_id"] = s.config.serverID()
	// The process ID of the server
	m["tile38_pid"] = os.Getpid()
	// Version of Tile38 running
	m["tile38_version"] = core.Version
	// Maximum heap size allowed
	m["tile38_max_heap_size"] = s.config.maxMemory()
	// Type of instance running
	if s.config.followHost() == "" {
		m["tile38_type"] = "leader"
	} else {
		m["tile38_type"] = "follower"
	}
	// Whether or not the server is read-only
	m["tile38_read_only"] = s.config.readOnly()
	// Size of pointer
	m["tile38_pointer_size"] = (32 << uintptr(uint64(^uintptr(0))>>63)) / 8
	// Uptime of the Tile38 server in seconds
	m["tile38_uptime_in_seconds"] = time.Since(s.started).Seconds()
	// Number of currently connected Tile38 clients
	s.connsmu.RLock()
	m["tile38_connected_clients"] = len(s.conns)
	s.connsmu.RUnlock()
	// Whether or not a cluster is enabled
	m["tile38_cluster_enabled"] = false
	// Whether or not the Tile38 AOF is enabled
	m["tile38_aof_enabled"] = core.AppendOnly
	// Whether or not an AOF shrink is currently in progress
	m["tile38_aof_rewrite_in_progress"] = s.shrinking
	// Length of time the last AOF shrink took
	m["tile38_aof_last_rewrite_time_sec"] = s.lastShrinkDuration.get() / int(time.Second)
	// Duration of the on-going AOF rewrite operation if any
	var currentShrinkStart time.Time
	if currentShrinkStart.IsZero() {
		m["tile38_aof_current_rewrite_time_sec"] = 0
	} else {
		m["tile38_aof_current_rewrite_time_sec"] = time.Since(currentShrinkStart).Seconds()
	}
	// Total size of the AOF in bytes
	m["tile38_aof_size"] = s.aofsz
	// Whether or no the HTTP transport is being served
	m["tile38_http_transport"] = s.http
	// Number of connections accepted by the server
	m["tile38_total_connections_received"] = s.statsTotalConns.get()
	// Number of commands processed by the server
	m["tile38_total_commands_processed"] = s.statsTotalCommands.get()
	// Number of webhook messages sent by server
	m["tile38_total_messages_sent"] = s.statsTotalMsgsSent.get()
	// Number of key expiration events
	m["tile38_expired_keys"] = s.statsExpired.get()
	// Number of connected slaves
	m["tile38_connected_slaves"] = len(s.aofconnM)

	points := 0
	objects := 0
	strings := 0
	s.cols.Scan(func(key string, value interface{}) bool {
		col := value.(*collection.Collection)
		points += col.PointCount()
		objects += col.Count()
		strings += col.StringCount()
		return true
	})

	// Number of points in the database
	m["tile38_num_points"] = points
	// Number of objects in the database
	m["tile38_num_objects"] = objects
	// Number of string in the database
	m["tile38_num_strings"] = strings
	// Number of collections in the database
	m["tile38_num_collections"] = s.cols.Len()
	// Number of hooks in the database
	m["tile38_num_hooks"] = len(s.hooks)

	avgsz := 0
	if points != 0 {
		avgsz = int(mem.HeapAlloc) / points
	}

	// Average point size in bytes
	m["tile38_avg_point_size"] = avgsz

	sz := 0
	s.cols.Scan(func(key string, value interface{}) bool {
		col := value.(*collection.Collection)
		sz += col.TotalWeight()
		return true
	})

	// Total in memory size of all collections
	m["tile38_in_memory_size"] = sz
}

func (s *Server) writeInfoServer(w *bytes.Buffer) {
	fmt.Fprintf(w, "tile38_version:%s\r\n", core.Version)
	fmt.Fprintf(w, "redis_version:%s\r\n", core.Version)                             // Version of the Redis server
	fmt.Fprintf(w, "uptime_in_seconds:%d\r\n", int(time.Since(s.started).Seconds())) // Number of seconds since Redis server start
}
func (s *Server) writeInfoClients(w *bytes.Buffer) {
	s.connsmu.RLock()
	fmt.Fprintf(w, "connected_clients:%d\r\n", len(s.conns)) // Number of client connections (excluding connections from slaves)
	s.connsmu.RUnlock()
}
func (s *Server) writeInfoMemory(w *bytes.Buffer) {
	mem := readMemStats()
	fmt.Fprintf(w, "used_memory:%d\r\n", mem.Alloc) // total number of bytes allocated by Redis using its allocator (either standard libc, jemalloc, or an alternative allocator such as tcmalloc
}
func boolInt(t bool) int {
	if t {
		return 1
	}
	return 0
}
func (s *Server) writeInfoPersistence(w *bytes.Buffer) {
	fmt.Fprintf(w, "aof_enabled:%d\r\n", boolInt(core.AppendOnly))
	fmt.Fprintf(w, "aof_rewrite_in_progress:%d\r\n", boolInt(s.shrinking))                          // Flag indicating a AOF rewrite operation is on-going
	fmt.Fprintf(w, "aof_last_rewrite_time_sec:%d\r\n", s.lastShrinkDuration.get()/int(time.Second)) // Duration of the last AOF rewrite operation in seconds

	var currentShrinkStart time.Time // c.currentShrinkStart.get()
	if currentShrinkStart.IsZero() {
		fmt.Fprintf(w, "aof_current_rewrite_time_sec:0\r\n") // Duration of the on-going AOF rewrite operation if any
	} else {
		fmt.Fprintf(w, "aof_current_rewrite_time_sec:%d\r\n", time.Now().Sub(currentShrinkStart)/time.Second) // Duration of the on-going AOF rewrite operation if any
	}
}

func (s *Server) writeInfoStats(w *bytes.Buffer) {
	fmt.Fprintf(w, "total_connections_received:%d\r\n", s.statsTotalConns.get())  // Total number of connections accepted by the server
	fmt.Fprintf(w, "total_commands_processed:%d\r\n", s.statsTotalCommands.get()) // Total number of commands processed by the server
	fmt.Fprintf(w, "total_messages_sent:%d\r\n", s.statsTotalMsgsSent.get())      // Total number of commands processed by the server
	fmt.Fprintf(w, "expired_keys:%d\r\n", s.statsExpired.get())                   // Total number of key expiration events
}

// writeInfoReplication writes all replication data to the 'info' response
func (s *Server) writeInfoReplication(w *bytes.Buffer) {
	if s.config.followHost() != "" {
		fmt.Fprintf(w, "role:slave\r\n")
		fmt.Fprintf(w, "master_host:%s\r\n", s.config.followHost())
		fmt.Fprintf(w, "master_port:%v\r\n", s.config.followPort())
	} else {
		fmt.Fprintf(w, "role:master\r\n")
		var i int
		s.connsmu.RLock()
		for _, cc := range s.conns {
			if cc.replPort != 0 {
				fmt.Fprintf(w, "slave%v:ip=%s,port=%v,state=online\r\n", i,
					strings.Split(cc.remoteAddr, ":")[0], cc.replPort)
				i++
			}
		}
		s.connsmu.RUnlock()
	}
	fmt.Fprintf(w, "connected_slaves:%d\r\n", len(s.aofconnM)) // Number of connected slaves
}

func (s *Server) writeInfoCluster(w *bytes.Buffer) {
	fmt.Fprintf(w, "cluster_enabled:0\r\n")
}

func (s *Server) cmdInfo(msg *Message) (res resp.Value, err error) {
	start := time.Now()

	sections := []string{"server", "clients", "memory", "persistence", "stats", "replication", "cpu", "cluster", "keyspace"}
	switch len(msg.Args) {
	default:
		return NOMessage, errInvalidNumberOfArguments
	case 1:
	case 2:
		section := strings.ToLower(msg.Args[1])
		switch section {
		default:
			sections = []string{section}
		case "all":
			sections = []string{"server", "clients", "memory", "persistence", "stats", "replication", "cpu", "commandstats", "cluster", "keyspace"}
		case "default":
		}
	}

	w := &bytes.Buffer{}
	for i, section := range sections {
		if i > 0 {
			w.WriteString("\r\n")
		}
		switch strings.ToLower(section) {
		default:
			continue
		case "server":
			w.WriteString("# Server\r\n")
			s.writeInfoServer(w)
		case "clients":
			w.WriteString("# Clients\r\n")
			s.writeInfoClients(w)
		case "memory":
			w.WriteString("# Memory\r\n")
			s.writeInfoMemory(w)
		case "persistence":
			w.WriteString("# Persistence\r\n")
			s.writeInfoPersistence(w)
		case "stats":
			w.WriteString("# Stats\r\n")
			s.writeInfoStats(w)
		case "replication":
			w.WriteString("# Replication\r\n")
			s.writeInfoReplication(w)
		case "cpu":
			w.WriteString("# CPU\r\n")
			s.writeInfoCPU(w)
		case "cluster":
			w.WriteString("# Cluster\r\n")
			s.writeInfoCluster(w)
		}
	}

	switch msg.OutputType {
	case JSON:
		// Create a map of all key/value info fields
		m := make(map[string]interface{})
		for _, kv := range strings.Split(w.String(), "\r\n") {
			kv = strings.TrimSpace(kv)
			if !strings.HasPrefix(kv, "#") {
				if split := strings.SplitN(kv, ":", 2); len(split) == 2 {
					m[split[0]] = tryParseType(split[1])
				}
			}
		}

		// Marshal the map and use the output in the JSON response
		data, err := json.Marshal(m)
		if err != nil {
			return NOMessage, err
		}
		res = resp.StringValue(`{"ok":true,"info":` + string(data) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case RESP:
		res = resp.BytesValue(w.Bytes())
	}
	return res, nil
}

// tryParseType attempts to parse the passed string as an integer, float64 and
// a bool returning any successful parsed values. It returns the passed string
// if all tries fail
func tryParseType(str string) interface{} {
	if v, err := strconv.ParseInt(str, 10, 64); err == nil {
		return v
	}
	if v, err := strconv.ParseFloat(str, 64); err == nil {
		return v
	}
	if v, err := strconv.ParseBool(str); err == nil {
		return v
	}
	return str
}

func respValuesSimpleMap(m map[string]interface{}) []resp.Value {
	var keys []string
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var vals []resp.Value
	for _, key := range keys {
		val := m[key]
		vals = append(vals, resp.StringValue(key))
		vals = append(vals, resp.StringValue(fmt.Sprintf("%v", val)))
	}
	return vals
}

func (s *Server) statsCollections(line string) (string, error) {
	start := time.Now()
	var key string
	var ms = []map[string]interface{}{}
	for len(line) > 0 {
		line, key = token(line)
		col := s.getCol(key)
		if col != nil {
			m := make(map[string]interface{})
			points := col.PointCount()
			m["num_points"] = points
			m["in_memory_size"] = col.TotalWeight()
			m["num_objects"] = col.Count()
			ms = append(ms, m)
		} else {
			ms = append(ms, nil)
		}
	}
	data, err := json.Marshal(ms)
	if err != nil {
		return "", err
	}
	return `{"ok":true,"stats":` + string(data) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}", nil
}

type prometheusStats struct {
	requests prometheus.ObserverVec
}

func (s *Server) EnablePrometheusStats(registry prometheus.Registerer) {
	prometheus.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_server_info", "", nil, prometheus.Labels{
			"id":      s.config.serverID(),
			"pid":     strconv.Itoa(os.Getpid()),
			"version": core.Version,
		}),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, 1)
		},
	})

	prometheus.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_collection_size_bytes", "", []string{"collection"}, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			defer s.ReaderLock()()
			s.cols.Scan(func(key string, value interface{}) bool {
				col := value.(*collection.Collection)
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(col.TotalWeight()), key)
				return true
			})
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_collection_items_count", "", []string{"collection", "type"}, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			defer s.ReaderLock()()
			s.cols.Scan(func(key string, value interface{}) bool {
				col := value.(*collection.Collection)
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(col.PointCount()), key, "point")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(col.Count()), key, "object")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(col.StringCount()), key, "string")
				return true
			})
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_server_followers_count", "", nil, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			defer s.ReaderLock()()
			obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(len(s.aofconnM)))
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_server_connected_clients_count", "", nil, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			s.connsmu.RLock()
			defer s.connsmu.RUnlock()
			obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(len(s.conns)))
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_server_uptime_seconds_total", "", nil, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			s.connsmu.RLock()
			defer s.connsmu.RUnlock()
			obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, time.Since(s.started).Seconds())
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_server_max_heap_size", "", nil, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(s.config.maxMemory()))
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_aof_enabled", "", nil, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(boolInt(core.AppendOnly)))
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_aof_rewrite_in_progress", "", nil, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			defer s.ReaderLock()()
			obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(boolInt(s.shrinking)))
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_aof_size_bytes", "", nil, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			defer s.ReaderLock()()
			obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(s.aofsz))
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_server_connections_received_total", "", nil, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(s.statsTotalConns.get()))
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_server_commands_processed_total", "", nil, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(s.statsTotalCommands.get()))
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_server_messages_sent_total", "", nil, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(s.statsTotalMsgsSent.get()))
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_server_expired_keys_total", "", nil, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(s.statsExpired.get()))
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_collection_operations_total", "", []string{"collection", "operation"}, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			defer s.ReaderLock()()
			s.cols.Scan(func(key string, value interface{}) bool {
				col := value.(*collection.Collection)
				stats := col.Stats()
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Get.Count()), key, "get")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Set.Count()), key, "set")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Delete.Count()), key, "delete")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.SetField.Count()), key, "set_field")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.SetFields.Count()), key, "set_fields")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Scan.Count()), key, "scan")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.ScanRange.Count()), key, "scan_range")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.SearchValues.Count()), key, "search_values")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.SearchValuesRange.Count()), key, "search_values_range")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.ScanGreaterOrEqual.Count()), key, "scan_greater_or_equal")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Within.Count()), key, "within")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Intersects.Count()), key, "intersects")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Nearby.Count()), key, "nearby")
				return true
			})
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_collection_operations_duration_seconds_total", "", []string{"collection", "operation"}, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			defer s.ReaderLock()()
			s.cols.Scan(func(key string, value interface{}) bool {
				col := value.(*collection.Collection)
				stats := col.Stats()
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Get.TotalDuration().Seconds()), key, "get")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Set.TotalDuration().Seconds()), key, "set")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Delete.TotalDuration().Seconds()), key, "delete")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.SetField.TotalDuration().Seconds()), key, "set_field")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.SetFields.TotalDuration().Seconds()), key, "set_fields")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Scan.TotalDuration().Seconds()), key, "scan")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.ScanRange.TotalDuration().Seconds()), key, "scan_range")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.SearchValues.TotalDuration().Seconds()), key, "search_values")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.SearchValuesRange.TotalDuration().Seconds()), key, "search_values_range")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.ScanGreaterOrEqual.TotalDuration().Seconds()), key, "scan_greater_or_equal")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Within.TotalDuration().Seconds()), key, "within")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Intersects.TotalDuration().Seconds()), key, "intersects")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(stats.Nearby.TotalDuration().Seconds()), key, "nearby")
				return true
			})
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_collection_operations_duration_seconds_min", "", []string{"collection", "operation"}, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			defer s.ReaderLock()()
			s.cols.Scan(func(key string, value interface{}) bool {
				col := value.(*collection.Collection)
				stats := col.Stats()
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Get.MinDuration().Seconds()), key, "get")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Set.MinDuration().Seconds()), key, "set")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Delete.MinDuration().Seconds()), key, "delete")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.SetField.MinDuration().Seconds()), key, "set_field")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.SetFields.MinDuration().Seconds()), key, "set_fields")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Scan.MinDuration().Seconds()), key, "scan")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.ScanRange.MinDuration().Seconds()), key, "scan_range")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.SearchValues.MinDuration().Seconds()), key, "search_values")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.SearchValuesRange.MinDuration().Seconds()), key, "search_values_range")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.ScanGreaterOrEqual.MinDuration().Seconds()), key, "scan_greater_or_equal")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Within.MinDuration().Seconds()), key, "within")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Intersects.MinDuration().Seconds()), key, "intersects")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Nearby.MinDuration().Seconds()), key, "nearby")
				return true
			})
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_collection_tree_depth", "", []string{"collection"}, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			defer s.ReaderLock()()
			s.cols.Scan(func(key string, value interface{}) bool {
				col := value.(*collection.Collection)

				stats := col.TreeStats()

				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Height.Count()), key, "height")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Join.Count()), key, "join")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Split.Count()), key, "split")

				return true
			})
		},
	})

	registry.MustRegister(&simpleCollector{
		desc: prometheus.NewDesc("tile38_collection_operations_duration_seconds_max", "", []string{"collection", "operation"}, nil),
		collect: func(desc *prometheus.Desc, obs chan<- prometheus.Metric) {
			defer s.ReaderLock()()
			s.cols.Scan(func(key string, value interface{}) bool {
				col := value.(*collection.Collection)
				stats := col.Stats()
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Get.MaxDuration().Seconds()), key, "get")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Set.MaxDuration().Seconds()), key, "set")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Delete.MaxDuration().Seconds()), key, "delete")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.SetField.MaxDuration().Seconds()), key, "set_field")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.SetFields.MaxDuration().Seconds()), key, "set_fields")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Scan.MaxDuration().Seconds()), key, "scan")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.ScanRange.MaxDuration().Seconds()), key, "scan_range")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.SearchValues.MaxDuration().Seconds()), key, "search_values")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.SearchValuesRange.MaxDuration().Seconds()), key, "search_values_range")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.ScanGreaterOrEqual.MaxDuration().Seconds()), key, "scan_greater_or_equal")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Within.MaxDuration().Seconds()), key, "within")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Intersects.MaxDuration().Seconds()), key, "intersects")
				obs <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(stats.Nearby.MaxDuration().Seconds()), key, "nearby")
				return true
			})
		},
	})

	requests := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tile38",
		Name:      "request_duration_seconds",
	}, []string{"command"})

	registry.MustRegister(requests)

	stats := &prometheusStats{
		requests: requests,
	}
	s.prometheusStats = stats
}

func (s *prometheusStats) RequestComplete(command string, elapsed time.Duration) {
	if s == nil {
		return
	}

	s.requests.WithLabelValues(command).Observe(elapsed.Seconds())
}

type simpleCollector struct {
	desc    *prometheus.Desc
	collect func(desc *prometheus.Desc, obs chan<- prometheus.Metric)
}

func (c *simpleCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- c.desc
}

func (c *simpleCollector) Collect(obs chan<- prometheus.Metric) {
	c.collect(c.desc, obs)
}
