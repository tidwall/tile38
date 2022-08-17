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
		"circle", [][]interface{}{
			{"SET", "mykey", "myid", "CIRCLE", 33, -115, 1000}, {"OK"},
			{"GET", "mykey", "myid", "OBJECT"}, {`{"type":"Polygon","coordinates":[[[-114.98927681932362,33],[-114.98932845436953,33.000881489320484],[-114.9894828622335,33.001754489416946],[-114.98973855588343,33.00261059282122],[-114.99009307284967,33.00344155478953],[-114.99054299894006,33.00423937270387],[-114.99108400112036,33.0049963631416],[-114.99171086924386,33.005705235870984],[-114.99241756622784,33.00635916406013],[-114.99319728619403,33.0069518500231],[-114.99404252001291,33.007477585870085],[-114.99494512762075,33.00793130847748],[-114.99589641641289,33.00830864824854],[-114.99688722495821,33.008605971194996],[-114.9979080112288,33.00882041393434],[-114.99894894449487,33.008949911265766],[-115,33.00899321605919],[-115.00105105550513,33.008949911265766],[-115.0020919887712,33.00882041393434],[-115.00311277504179,33.008605971194996],[-115.00410358358711,33.00830864824854],[-115.00505487237925,33.00793130847748],[-115.00595747998709,33.007477585870085],[-115.00680271380597,33.0069518500231],[-115.00758243377216,33.00635916406013],[-115.00828913075614,33.005705235870984],[-115.00891599887964,33.0049963631416],[-115.00945700105994,33.00423937270387],[-115.00990692715033,33.00344155478953],[-115.01026144411657,33.00261059282122],[-115.0105171377665,33.001754489416946],[-115.01067154563047,33.000881489320484],[-115.01072318067638,33],[-115.01067154563047,32.999118510679516],[-115.0105171377665,32.998245510583054],[-115.01026144411657,32.99738940717878],[-115.00990692715033,32.99655844521047],[-115.00945700105994,32.99576062729613],[-115.00891599887964,32.9950036368584],[-115.00828913075614,32.994294764129016],[-115.00758243377216,32.99364083593987],[-115.00680271380597,32.9930481499769],[-115.00595747998709,32.992522414129915],[-115.00505487237925,32.99206869152252],[-115.00410358358711,32.99169135175146],[-115.00311277504179,32.991394028805004],[-115.0020919887712,32.99117958606566],[-115.00105105550513,32.991050088734234],[-115,32.99100678394081],[-114.99894894449487,32.991050088734234],[-114.9979080112288,32.99117958606566],[-114.99688722495821,32.991394028805004],[-114.99589641641289,32.99169135175146],[-114.99494512762075,32.99206869152252],[-114.99404252001291,32.992522414129915],[-114.99319728619403,32.9930481499769],[-114.99241756622784,32.99364083593987],[-114.99171086924386,32.994294764129016],[-114.99108400112036,32.9950036368584],[-114.99054299894006,32.99576062729613],[-114.99009307284967,32.99655844521047],[-114.98973855588343,32.99738940717878],[-114.9894828622335,32.998245510583054],[-114.98932845436953,32.999118510679516],[-114.98927681932362,33],[-114.98927681932362,33]]]}`},
			{"GET", "mykey", "myid", "BOUNDS"}, {"[[32.99100678394081 -115.01072318067638] [33.00899321605919 -114.98927681932362]]"},
			{"GET", "mykey", "myid", "POINT"}, {"[33 -115]"},
			{"DEL", "mykey", "myid"}, {"1"},
			{"GET", "mykey", "myid"}, {nil},
			{"SET", "mykey", "myid", "CIRCLE", 33, -115, 0}, {"OK"},
			{"GET", "mykey", "myid", "OBJECT"}, {`{"type":"Point","coordinates":[-115,33]}`},
			{"GET", "mykey", "myid", "BOUNDS"}, {"[[33 -115] [33 -115]]"},
			{"GET", "mykey", "myid", "POINT"}, {"[33 -115]"},
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
		{"WITHIN", "mykey", "WHEREIN", "a", 1, 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR invalid argument '1'"},
		{"WITHIN", "mykey", "WHEREIN", "a", 3, 0, "a", 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR invalid argument 'a'"},
		{"SET", "mykey", "myid_a2", "FIELD", "a", 2, "POINT", 32.99, -115}, {"OK"},
		{"SET", "mykey", "myid_a3", "FIELD", "a", 3, "POINT", 33, -115.02}, {"OK"},
		{"WITHIN", "mykey", "WHEREIN", "a", 3, 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]] [myid_a2 {"type":"Point","coordinates":[-115,32.99]} [a 2]]]]`},
		// zero value should not match 1 and 2
		{"SET", "mykey", "myid_a0", "FIELD", "a", 0, "POINT", 33, -115.02}, {"OK"},
		{"WITHIN", "mykey", "WHEREIN", "a", 2, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]] [myid_a2 {"type":"Point","coordinates":[-115,32.99]} [a 2]]]]`},
	})
}

func keys_WHEREEVAL_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid_a1", "FIELD", "a", 1, "POINT", 33, -115}, {"OK"},
		{"WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1])", 1, 0.5, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`},
		{"WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1])", "a", 0.5, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR invalid argument 'a'"},
		{"WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1])", 1, 0.5, 4, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {"ERR invalid argument '4'"},
		{"SET", "mykey", "myid_a2", "FIELD", "a", 2, "POINT", 32.99, -115}, {"OK"},
		{"SET", "mykey", "myid_a3", "FIELD", "a", 3, "POINT", 33, -115.02}, {"OK"},
		{"WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1]) and FIELDS.a ~= tonumber(ARGV[2])", 2, 0.5, 3, "BOUNDS", 32.8, -115.2, 33.2, -114.8}, {`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]] [myid_a2 {"type":"Point","coordinates":[-115,32.99]} [a 2]]]]`},
	})
}
