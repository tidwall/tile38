package tests

import (
	"errors"
	"fmt"
	"math/rand"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/gjson"
)

func subTestKeys(t *testing.T, mc *mockServer) {
	runStep(t, mc, "BOUNDS", keys_BOUNDS_test)
	runStep(t, mc, "DEL", keys_DEL_test)
	runStep(t, mc, "DROP", keys_DROP_test)
	runStep(t, mc, "RENAME", keys_RENAME_test)
	runStep(t, mc, "RENAMENX", keys_RENAMENX_test)
	runStep(t, mc, "EXPIRE", keys_EXPIRE_test)
	runStep(t, mc, "FSET", keys_FSET_test)
	runStep(t, mc, "GET", keys_GET_test)
	runStep(t, mc, "KEYS", keys_KEYS_test)
	runStep(t, mc, "PERSIST", keys_PERSIST_test)
	runStep(t, mc, "SET", keys_SET_test)
	runStep(t, mc, "STATS", keys_STATS_test)
	runStep(t, mc, "TTL", keys_TTL_test)
	runStep(t, mc, "SET EX", keys_SET_EX_test)
	runStep(t, mc, "PDEL", keys_PDEL_test)
	runStep(t, mc, "FIELDS", keys_FIELDS_test)
	runStep(t, mc, "WHEREIN", keys_WHEREIN_test)
	runStep(t, mc, "WHEREEVAL", keys_WHEREEVAL_test)
}

func keys_BOUNDS_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid1", "POINT", 33, -115}, {"OK"},
		{"BOUNDS", "mykey"}, {"[[-115 33] [-115 33]]"},
		{"SET", "mykey", "myid2", "POINT", 34, -112}, {"OK"},
		{"BOUNDS", "mykey"}, {"[[-115 33] [-112 34]]"},
		{"DEL", "mykey", "myid2"}, {1},
		{"BOUNDS", "mykey"}, {"[[-115 33] [-115 33]]"},
		{"SET", "mykey", "myid3", "OBJECT", `{"type":"Point","coordinates":[-130,38,10]}`}, {"OK"},
		{"SET", "mykey", "myid4", "OBJECT", `{"type":"Point","coordinates":[-110,25,-8]}`}, {"OK"},
		{"BOUNDS", "mykey"}, {"[[-130 25] [-110 38]]"},
	})
}
func keys_DEL_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid", "POINT", 33, -115}, {"OK"},
		{"GET", "mykey", "myid", "POINT"}, {"[33 -115]"},
		{"DEL", "mykey", "myid"}, {"1"},
		{"GET", "mykey", "myid"}, {nil},
	})
}
func keys_DROP_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid1", "HASH", "9my5xp7"}, {"OK"},
		{"SET", "mykey", "myid2", "HASH", "9my5xp8"}, {"OK"},
		{"SCAN", "mykey", "COUNT"}, {2},
		{"DROP", "mykey"}, {1},
		{"SCAN", "mykey", "COUNT"}, {0},
		{"DROP", "mykey"}, {0},
		{"SCAN", "mykey", "COUNT"}, {0},
	})
}
func keys_RENAME_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid1", "HASH", "9my5xp7"}, {"OK"},
		{"SET", "mykey", "myid2", "HASH", "9my5xp8"}, {"OK"},
		{"SCAN", "mykey", "COUNT"}, {2},
		{"RENAME", "mykey", "mynewkey"}, {"OK"},
		{"SCAN", "mykey", "COUNT"}, {0},
		{"SCAN", "mynewkey", "COUNT"}, {2},
		{"SET", "mykey", "myid3", "HASH", "9my5xp7"}, {"OK"},
		{"RENAME", "mykey", "mynewkey"}, {"OK"},
		{"SCAN", "mykey", "COUNT"}, {0},
		{"SCAN", "mynewkey", "COUNT"}, {1},
		{"RENAME", "foo", "mynewkey"}, {"ERR key not found"},
		{"SCAN", "mynewkey", "COUNT"}, {1},
	})
}
func keys_RENAMENX_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid1", "HASH", "9my5xp7"}, {"OK"},
		{"SET", "mykey", "myid2", "HASH", "9my5xp8"}, {"OK"},
		{"SCAN", "mykey", "COUNT"}, {2},
		{"RENAMENX", "mykey", "mynewkey"}, {1},
		{"SCAN", "mykey", "COUNT"}, {0},
		{"DROP", "mykey"}, {0},
		{"SCAN", "mykey", "COUNT"}, {0},
		{"SCAN", "mynewkey", "COUNT"}, {2},
		{"SET", "mykey", "myid3", "HASH", "9my5xp7"}, {"OK"},
		{"RENAMENX", "mykey", "mynewkey"}, {0},
		{"SCAN", "mykey", "COUNT"}, {1},
		{"SCAN", "mynewkey", "COUNT"}, {2},
		{"RENAMENX", "foo", "mynewkey"}, {"ERR key not found"},
		{"SCAN", "mynewkey", "COUNT"}, {2},
	})
}
func keys_EXPIRE_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid", "STRING", "value"}, {"OK"},
		{"EXPIRE", "mykey", "myid", 1}, {1},
		{time.Second / 4}, {}, // sleep
		{"GET", "mykey", "myid"}, {"value"},
		{time.Second}, {}, // sleep
		{"GET", "mykey", "myid"}, {nil},
	})
}
func keys_FSET_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid", "HASH", "9my5xp7"}, {"OK"},
		{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7]"},
		{"FSET", "mykey", "myid", "f1", 105.6}, {1},
		{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7 [f1 105.6]]"},
		{"FSET", "mykey", "myid", "f1", 1.1, "f2", 2.2}, {2},
		{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7 [f1 1.1 f2 2.2]]"},
		{"FSET", "mykey", "myid", "f1", 1.1, "f2", 22.22}, {1},
		{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7 [f1 1.1 f2 22.22]]"},
		{"FSET", "mykey", "myid", "f1", 0}, {1},
		{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7 [f2 22.22]]"},
		{"FSET", "mykey", "myid", "f2", 0}, {1},
		{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7]"},
		{"FSET", "mykey", "myid2", "xx", "f1", 1.1, "f2", 2.2}, {0},
		{"GET", "mykey", "myid2"}, {nil},
		{"DEL", "mykey", "myid"}, {"1"},
		{"GET", "mykey", "myid"}, {nil},
	})
}
func keys_GET_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid", "STRING", "value"}, {"OK"},
		{"GET", "mykey", "myid"}, {"value"},
		{"SET", "mykey", "myid", "STRING", "value2"}, {"OK"},
		{"GET", "mykey", "myid"}, {"value2"},
		{"DEL", "mykey", "myid"}, {"1"},
		{"GET", "mykey", "myid"}, {nil},
	})
}
func keys_KEYS_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey11", "myid4", "STRING", "value"}, {"OK"},
		{"SET", "mykey22", "myid2", "HASH", "9my5xp7"}, {"OK"},
		{"SET", "mykey22", "myid1", "OBJECT", `{"type":"Point","coordinates":[-130,38,10]}`}, {"OK"},
		{"SET", "mykey11", "myid3", "OBJECT", `{"type":"Point","coordinates":[-110,25,-8]}`}, {"OK"},
		{"SET", "mykey42", "myid2", "HASH", "9my5xp7"}, {"OK"},
		{"SET", "mykey31", "myid4", "STRING", "value"}, {"OK"},
		{"SET", "mykey310", "myid5", "STRING", "value"}, {"OK"},
		{"KEYS", "*"}, {"[mykey11 mykey22 mykey31 mykey310 mykey42]"},
		{"KEYS", "*key*"}, {"[mykey11 mykey22 mykey31 mykey310 mykey42]"},
		{"KEYS", "mykey*"}, {"[mykey11 mykey22 mykey31 mykey310 mykey42]"},
		{"KEYS", "mykey4*"}, {"[mykey42]"},
		{"KEYS", "mykey*1"}, {"[mykey11 mykey31]"},
		{"KEYS", "mykey*1*"}, {"[mykey11 mykey31 mykey310]"},
		{"KEYS", "mykey*10"}, {"[mykey310]"},
		{"KEYS", "mykey*2"}, {"[mykey22 mykey42]"},
		{"KEYS", "*2"}, {"[mykey22 mykey42]"},
		{"KEYS", "*1*"}, {"[mykey11 mykey31 mykey310]"},
		{"KEYS", "mykey"}, {"[]"},
		{"KEYS", "mykey31"}, {"[mykey31]"},
		{"KEYS", "mykey[^3]*"}, {"[mykey11 mykey22 mykey42]"},
	})
}
func keys_PERSIST_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid", "STRING", "value"}, {"OK"},
		{"EXPIRE", "mykey", "myid", 2}, {1},
		{"PERSIST", "mykey", "myid"}, {1},
		{"PERSIST", "mykey", "myid"}, {0},
	})
}
func keys_SET_test(mc *mockServer) error {
	return mc.DoBatch(
		"point", [][]interface{}{
			{"SET", "mykey", "myid", "POINT", 33, -115}, {"OK"},
			{"GET", "mykey", "myid", "POINT"}, {"[33 -115]"},
			{"GET", "mykey", "myid", "BOUNDS"}, {"[[33 -115] [33 -115]]"},
			{"GET", "mykey", "myid", "OBJECT"}, {`{"type":"Point","coordinates":[-115,33]}`},
			{"GET", "mykey", "myid", "HASH", 7}, {"9my5xp7"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
		},
		"object", [][]interface{}{
			{"SET", "mykey", "myid", "OBJECT", `{"type":"Point","coordinates":[-115,33]}`}, {"OK"},
			{"GET", "mykey", "myid", "POINT"}, {"[33 -115]"},
			{"GET", "mykey", "myid", "BOUNDS"}, {"[[33 -115] [33 -115]]"},
			{"GET", "mykey", "myid", "OBJECT"}, {`{"type":"Point","coordinates":[-115,33]}`},
			{"GET", "mykey", "myid", "HASH", 7}, {"9my5xp7"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
		},
		"bounds", [][]interface{}{
			{"SET", "mykey", "myid", "BOUNDS", 33, -115, 33, -115}, {"OK"},
			{"GET", "mykey", "myid", "POINT"}, {"[33 -115]"},
			{"GET", "mykey", "myid", "BOUNDS"}, {"[[33 -115] [33 -115]]"},
			{"GET", "mykey", "myid", "OBJECT"}, {`{"type":"Polygon","coordinates":[[[-115,33],[-115,33],[-115,33],[-115,33],[-115,33]]]}`},
			{"GET", "mykey", "myid", "HASH", 7}, {"9my5xp7"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
		},
		"hash", [][]interface{}{
			{"SET", "mykey", "myid", "HASH", "9my5xp7"}, {"OK"},
			{"GET", "mykey", "myid", "HASH", 7}, {"9my5xp7"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
		},
		"field", [][]interface{}{
			{"SET", "mykey", "myid", "FIELD", "f1", 33, "FIELD", "a2", 44.5, "HASH", "9my5xp7"}, {"OK"},
			{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7 [a2 44.5 f1 33]]"},
			{"FSET", "mykey", "myid", "f1", 0}, {1},
			{"FSET", "mykey", "myid", "f1", 0}, {0},
			{"GET", "mykey", "myid", "WITHFIELDS", "HASH", 7}, {"[9my5xp7 [a2 44.5]]"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
		},
		"string", [][]interface{}{
			{"SET", "mykey", "myid", "STRING", "value"}, {"OK"},
			{"GET", "mykey", "myid"}, {"value"},
			{"SET", "mykey", "myid", "STRING", "value2"}, {"OK"},
			{"GET", "mykey", "myid"}, {"value2"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
		},
	)
}

func keys_STATS_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"STATS", "mykey"}, {"[nil]"},
		{"SET", "mykey", "myid", "STRING", "value"}, {"OK"},
		{"STATS", "mykey"}, {"[[in_memory_size 9 num_objects 1 num_points 0 num_strings 1]]"},
		{"SET", "mykey", "myid2", "STRING", "value"}, {"OK"},
		{"STATS", "mykey"}, {"[[in_memory_size 19 num_objects 2 num_points 0 num_strings 2]]"},
		{"SET", "mykey", "myid3", "OBJECT", `{"type":"Point","coordinates":[-115,33]}`}, {"OK"},
		{"STATS", "mykey"}, {"[[in_memory_size 40 num_objects 3 num_points 1 num_strings 2]]"},
		{"DEL", "mykey", "myid"}, {1},
		{"STATS", "mykey"}, {"[[in_memory_size 31 num_objects 2 num_points 1 num_strings 1]]"},
		{"DEL", "mykey", "myid3"}, {1},
		{"STATS", "mykey"}, {"[[in_memory_size 10 num_objects 1 num_points 0 num_strings 1]]"},
		{"STATS", "mykey", "mykey2"}, {"[[in_memory_size 10 num_objects 1 num_points 0 num_strings 1] nil]"},
		{"DEL", "mykey", "myid2"}, {1},
		{"STATS", "mykey"}, {"[nil]"},
		{"STATS", "mykey", "mykey2"}, {"[nil nil]"},
	})
}
func keys_TTL_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid", "STRING", "value"}, {"OK"},
		{"EXPIRE", "mykey", "myid", 2}, {1},
		{time.Second / 4}, {}, // sleep
		{"TTL", "mykey", "myid"}, {1},
	})
}

type PSAUX struct {
	User    string
	PID     int
	CPU     float64
	Mem     float64
	VSZ     int
	RSS     int
	TTY     string
	Stat    string
	Start   string
	Time    string
	Command string
}

func atoi(s string) int {
	n, _ := strconv.ParseInt(s, 10, 64)
	return int(n)
}
func atof(s string) float64 {
	n, _ := strconv.ParseFloat(s, 64)
	return float64(n)
}
func psaux(pid int) PSAUX {
	var res []byte
	res, err := exec.Command("ps", "aux").CombinedOutput()
	if err != nil {
		return PSAUX{}
	}
	pids := strconv.FormatInt(int64(pid), 10)
	for _, line := range strings.Split(string(res), "\n") {
		var words []string
		for _, word := range strings.Split(line, " ") {
			if word != "" {
				words = append(words, word)
			}
			if len(words) > 11 {
				if words[1] == pids {
					return PSAUX{
						User:    words[0],
						PID:     atoi(words[1]),
						CPU:     atof(words[2]),
						Mem:     atof(words[3]),
						VSZ:     atoi(words[4]),
						RSS:     atoi(words[5]),
						TTY:     words[6],
						Stat:    words[7],
						Start:   words[8],
						Time:    words[9],
						Command: words[10],
					}
				}
			}
		}
	}
	return PSAUX{}
}
func keys_SET_EX_test(mc *mockServer) (err error) {
	rand.Seed(time.Now().UnixNano())

	// add a bunch of points
	for i := 0; i < 20000; i++ {
		val := fmt.Sprintf("val:%d", i)
		var resp string
		var lat, lon float64
		lat = rand.Float64()*180 - 90
		lon = rand.Float64()*360 - 180
		resp, err = redis.String(mc.conn.Do("SET",
			fmt.Sprintf("mykey%d", i%3), val,
			"EX", 1+rand.Float64(),
			"POINT", lat, lon))
		if err != nil {
			return
		}
		if resp != "OK" {
			err = fmt.Errorf("expected 'OK', got '%s'", resp)
			return
		}
		time.Sleep(time.Nanosecond)
	}
	time.Sleep(time.Second * 3)
	mc.conn.Do("OUTPUT", "json")
	json, _ := redis.String(mc.conn.Do("SERVER"))
	if !gjson.Get(json, "ok").Bool() {
		return errors.New("not ok")
	}
	if gjson.Get(json, "stats.num_objects").Int() > 0 {
		return errors.New("items left in database")
	}
	mc.conn.Do("FLUSHDB")
	return nil
}

func keys_FIELDS_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid1a", "FIELD", "a", 1, "POINT", 33, -115}, {"OK"},
		{"GET", "mykey", "myid1a", "WITHFIELDS"}, {`[{"type":"Point","coordinates":[-115,33]} [a 1]]`},
		{"SET", "mykey", "myid1a", "FIELD", "a", "a", "POINT", 33, -115}, {"ERR invalid argument 'a'"},
		{"GET", "mykey", "myid1a", "WITHFIELDS"}, {`[{"type":"Point","coordinates":[-115,33]} [a 1]]`},
		{"SET", "mykey", "myid1a", "FIELD", "a", 1, "FIELD", "b", 2, "POINT", 33, -115}, {"OK"},
		{"GET", "mykey", "myid1a", "WITHFIELDS"}, {`[{"type":"Point","coordinates":[-115,33]} [a 1 b 2]]`},
		{"SET", "mykey", "myid1a", "FIELD", "b", 2, "POINT", 33, -115}, {"OK"},
		{"GET", "mykey", "myid1a", "WITHFIELDS"}, {`[{"type":"Point","coordinates":[-115,33]} [a 1 b 2]]`},
		{"SET", "mykey", "myid1a", "FIELD", "b", 2, "FIELD", "a", "1", "FIELD", "c", 3, "POINT", 33, -115}, {"OK"},
		{"GET", "mykey", "myid1a", "WITHFIELDS"}, {`[{"type":"Point","coordinates":[-115,33]} [a 1 b 2 c 3]]`},
	})
}

func keys_PDEL_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid1a", "POINT", 33, -115}, {"OK"},
		{"SET", "mykey", "myid1b", "POINT", 33, -115}, {"OK"},
		{"SET", "mykey", "myid2a", "POINT", 33, -115}, {"OK"},
		{"SET", "mykey", "myid2b", "POINT", 33, -115}, {"OK"},
		{"SET", "mykey", "myid3a", "POINT", 33, -115}, {"OK"},
		{"SET", "mykey", "myid3b", "POINT", 33, -115}, {"OK"},
		{"SET", "mykey", "myid4a", "POINT", 33, -115}, {"OK"},
		{"SET", "mykey", "myid4b", "POINT", 33, -115}, {"OK"},
		{"PDEL", "mykeyNA", "*"}, {0},
		{"PDEL", "mykey", "myid1a"}, {1},
		{"PDEL", "mykey", "myid1a"}, {0},
		{"PDEL", "mykey", "myid1*"}, {1},
		{"PDEL", "mykey", "myid2*"}, {2},
		{"PDEL", "mykey", "*b"}, {2},
		{"PDEL", "mykey", "*"}, {2},
		{"PDEL", "mykey", "*"}, {0},
	})
}

func keys_WHEREIN_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid_a1", "FIELD", "a", 1, "POINT", 33, -115}, {"OK"},
		{"WITHIN", "mykey", "WHEREIN", "a", 3, 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`},
		{"WITHIN", "mykey", "WHEREIN", "a", "a", 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR invalid argument 'a'"},
		{"WITHIN", "mykey", "WHEREIN", "a", 1, 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR wrong number of arguments for 'within' command"},
		{"WITHIN", "mykey", "WHEREIN", "a", 3, 0, "a", 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR invalid argument 'a'"},
		{"SET", "mykey", "myid_a2", "FIELD", "a", 2, "POINT", 32.99, -115}, {"OK"},
		{"SET", "mykey", "myid_a3", "FIELD", "a", 3, "POINT", 33, -115.02}, {"OK"},
		{"WITHIN", "mykey", "WHEREIN", "a", 3, 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]] [myid_a2 {"type":"Point","coordinates":[-115,32.99]} [a 2]]]]`},
	})
}

func keys_WHEREEVAL_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid_a1", "FIELD", "a", 1, "POINT", 33, -115}, {"OK"},
		{"WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1])", 1, 0.5, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`},
		{"WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1])", "a", 0.5, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR invalid argument 'a'"},
		{"WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1])", 1, 0.5, 4, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR wrong number of arguments for 'within' command"},
		{"SET", "mykey", "myid_a2", "FIELD", "a", 2, "POINT", 32.99, -115}, {"OK"},
		{"SET", "mykey", "myid_a3", "FIELD", "a", 3, "POINT", 33, -115.02}, {"OK"},
		{"WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1]) and FIELDS.a ~= tonumber(ARGV[2])", 2, 0.5, 3, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]] [myid_a2 {"type":"Point","coordinates":[-115,32.99]} [a 2]]]]`},
	})
}
