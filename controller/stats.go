package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/core"
)

func (c *Controller) cmdStats(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var ms = []map[string]interface{}{}
	empty_response := resp.SimpleStringValue("")
	if len(vs) == 0 {
		return empty_response, errInvalidNumberOfArguments
	}
	var vals []resp.Value
	var key string
	var ok bool
	for {
		vs, key, ok = tokenval(vs)
		if !ok {
			break
		}
		col := c.getCol(key)
		if col != nil {
			m := make(map[string]interface{})
			m["num_points"] = col.PointCount()
			m["in_memory_size"] = col.TotalWeight()
			m["num_objects"] = col.Count()
			m["num_strings"] = col.StringCount()
			switch msg.OutputType {
			case server.JSON:
				ms = append(ms, m)
			case server.RESP:
				vals = append(vals, resp.ArrayValue(respValuesSimpleMap(m)))
			}
		} else {
			switch msg.OutputType {
			case server.JSON:
				ms = append(ms, nil)
			case server.RESP:
				vals = append(vals, resp.NullValue())
			}
		}
	}
	switch msg.OutputType {
	case server.JSON:

		data, err := json.Marshal(ms)
		if err != nil {
			return empty_response, err
		}
		res = resp.StringValue(`{"ok":true,"stats":` + string(data) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case server.RESP:
		res = resp.ArrayValue(vals)
	}
	return res, nil
}
func (c *Controller) cmdServer(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	empty_response := resp.SimpleStringValue("")
	if len(msg.Values) != 1 {
		return empty_response, errInvalidNumberOfArguments
	}
	m := make(map[string]interface{})
	m["id"] = c.config.ServerID
	if c.config.FollowHost != "" {
		m["following"] = fmt.Sprintf("%s:%d", c.config.FollowHost, c.config.FollowPort)
		m["caught_up"] = c.fcup
		m["caught_up_once"] = c.fcuponce
	}
	m["http_transport"] = c.http
	m["pid"] = os.Getpid()
	m["aof_size"] = c.aofsz
	m["num_collections"] = c.cols.Len()
	m["num_hooks"] = len(c.hooks)
	sz := 0
	c.cols.Ascend(func(item btree.Item) bool {
		col := item.(*collectionT).Collection
		sz += col.TotalWeight()
		return true
	})
	m["in_memory_size"] = sz
	points := 0
	objects := 0
	strings := 0
	c.cols.Ascend(func(item btree.Item) bool {
		col := item.(*collectionT).Collection
		points += col.PointCount()
		objects += col.Count()
		strings += col.StringCount()
		return true
	})
	m["num_points"] = points
	m["num_objects"] = objects
	m["num_strings"] = strings
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	avgsz := 0
	if points != 0 {
		avgsz = int(mem.HeapAlloc) / points
	}
	m["mem_alloc"] = mem.Alloc
	m["heap_size"] = mem.HeapAlloc
	m["heap_released"] = mem.HeapReleased
	m["max_heap_size"] = c.config.MaxMemory
	m["avg_item_size"] = avgsz
	m["pointer_size"] = (32 << uintptr(uint64(^uintptr(0))>>63)) / 8
	m["read_only"] = c.config.ReadOnly

	switch msg.OutputType {
	case server.JSON:
		data, err := json.Marshal(m)
		if err != nil {
			return empty_response, err
		}
		res = resp.StringValue(`{"ok":true,"stats":` + string(data) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case server.RESP:
		vals := respValuesSimpleMap(m)
		res = resp.ArrayValue(vals)
	}
	return res, nil
}

func (c *Controller) writeInfoServer(w *bytes.Buffer) {
	fmt.Fprintf(w, "tile38_version:%s\r\n", core.Version)
	fmt.Fprintf(w, "redis_version:%s\r\n", core.Version)                              //Version of the Redis server
	fmt.Fprintf(w, "uptime_in_seconds:%d\r\n", time.Now().Sub(c.started)/time.Second) //Number of seconds since Redis server start
}
func (c *Controller) writeInfoClients(w *bytes.Buffer) {
	fmt.Fprintf(w, "connected_clients:%d\r\n", len(c.conns)) // Number of client connections (excluding connections from slaves)
}
func (c *Controller) writeInfoMemory(w *bytes.Buffer) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Fprintf(w, "used_memory:%d\r\n", mem.Alloc) // total number of bytes allocated by Redis using its allocator (either standard libc, jemalloc, or an alternative allocator such as tcmalloc
}
func boolInt(t bool) int {
	if t {
		return 1
	}
	return 0
}
func (c *Controller) writeInfoPersistence(w *bytes.Buffer) {
	fmt.Fprintf(w, "aof_enabled:1\r\n")
	fmt.Fprintf(w, "aof_rewrite_in_progress:%d\r\n", boolInt(c.shrinking))               // Flag indicating a AOF rewrite operation is on-going
	fmt.Fprintf(w, "aof_last_rewrite_time_sec:%d\r\n", c.lastShrinkDuration/time.Second) // Duration of the last AOF rewrite operation in seconds
	if c.currentShrinkStart.IsZero() {
		fmt.Fprintf(w, "aof_current_rewrite_time_sec:0\r\n") // Duration of the on-going AOF rewrite operation if any
	} else {
		fmt.Fprintf(w, "aof_current_rewrite_time_sec:%d\r\n", time.Now().Sub(c.currentShrinkStart)/time.Second) // Duration of the on-going AOF rewrite operation if any
	}
}

func (c *Controller) writeInfoStats(w *bytes.Buffer) {
	fmt.Fprintf(w, "total_connections_received:%d\r\n", c.statsTotalConns)  // Total number of connections accepted by the server
	fmt.Fprintf(w, "total_commands_processed:%d\r\n", c.statsTotalCommands) // Total number of commands processed by the server
	fmt.Fprintf(w, "expired_keys:%d\r\n", c.statsExpired)                   // Total number of key expiration events
}
func (c *Controller) writeInfoReplication(w *bytes.Buffer) {
	fmt.Fprintf(w, "connected_slaves:%d\r\n", len(c.aofconnM)) // Number of connected slaves
}
func (c *Controller) writeInfoCluster(w *bytes.Buffer) {
	fmt.Fprintf(w, "cluster_enabled:0\r\n")
}

func (c *Controller) cmdInfo(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	empty_response := resp.SimpleStringValue("")
	sections := []string{"server", "clients", "memory", "persistence", "stats", "replication", "cpu", "cluster", "keyspace"}
	switch len(msg.Values) {
	default:
		return empty_response, errInvalidNumberOfArguments
	case 1:
	case 2:
		section := strings.ToLower(msg.Values[1].String())
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
			c.writeInfoServer(w)
		case "clients":
			w.WriteString("# Clients\r\n")
			c.writeInfoClients(w)
		case "memory":
			w.WriteString("# Memory\r\n")
			c.writeInfoMemory(w)
		case "persistence":
			w.WriteString("# Persistence\r\n")
			c.writeInfoPersistence(w)
		case "stats":
			w.WriteString("# Stats\r\n")
			c.writeInfoStats(w)
		case "replication":
			w.WriteString("# Replication\r\n")
			c.writeInfoReplication(w)
		case "cpu":
			w.WriteString("# CPU\r\n")
			c.writeInfoCPU(w)
		case "cluster":
			w.WriteString("# Cluster\r\n")
			c.writeInfoCluster(w)
		}
	}

	switch msg.OutputType {
	case server.JSON:
		data, err := json.Marshal(w.String())
		if err != nil {
			return empty_response, err
		}
		res = resp.StringValue(`{"ok":true,"info":` + string(data) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case server.RESP:
		res = resp.BytesValue(w.Bytes())
	}

	return res, nil
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

func (c *Controller) statsCollections(line string) (string, error) {
	start := time.Now()
	var key string
	var ms = []map[string]interface{}{}
	for len(line) > 0 {
		line, key = token(line)
		col := c.getCol(key)
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
