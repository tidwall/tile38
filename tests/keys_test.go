package tests

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/gjson"
)

func subTestKeys(g *testGroup) {
	g.regSubTest("BOUNDS", keys_BOUNDS_test)
	g.regSubTest("DEL", keys_DEL_test)
	g.regSubTest("DROP", keys_DROP_test)
	g.regSubTest("RENAME", keys_RENAME_test)
	g.regSubTest("RENAMENX", keys_RENAMENX_test)
	g.regSubTest("EXPIRE", keys_EXPIRE_test)
	g.regSubTest("FSET", keys_FSET_test)
	g.regSubTest("GET", keys_GET_test)
	g.regSubTest("KEYS", keys_KEYS_test)
	g.regSubTest("PERSIST", keys_PERSIST_test)
	g.regSubTest("SET", keys_SET_test)
	g.regSubTest("SET CIRCLE", keys_SET_CIRCLE_test)
	g.regSubTest("STATS", keys_STATS_test)
	g.regSubTest("TTL", keys_TTL_test)
	g.regSubTest("SET EX", keys_SET_EX_test)
	g.regSubTest("PDEL", keys_PDEL_test)
	g.regSubTest("FIELDS", keys_FIELDS_test)
	g.regSubTest("WHEREIN", keys_WHEREIN_test)
	g.regSubTest("WHEREEVAL", keys_WHEREEVAL_test)
	g.regSubTest("TYPE", keys_TYPE_test)
	g.regSubTest("FLUSHDB", keys_FLUSHDB_test)
	g.regSubTest("HEALTHZ", keys_HEALTHZ_test)
	g.regSubTest("SERVER", keys_SERVER_test)
	g.regSubTest("INFO", keys_INFO_test)
}

func keys_BOUNDS_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("BOUNDS", "mykey").Str("<nil>"),
		Do("BOUNDS", "mykey").JSON().Err("key not found"),
		Do("SET", "mykey", "myid1", "POINT", 33, -115).OK(),
		Do("BOUNDS", "mykey").Str("[[-115 33] [-115 33]]"),
		Do("BOUNDS", "mykey").JSON().Str(`{"ok":true,"bounds":{"type":"Polygon","coordinates":[[[-115,33],[-115,33],[-115,33],[-115,33],[-115,33]]]}}`),
		Do("SET", "mykey", "myid2", "POINT", 34, -112).OK(),
		Do("BOUNDS", "mykey").Str("[[-115 33] [-112 34]]"),
		Do("DEL", "mykey", "myid2").Str("1"),
		Do("BOUNDS", "mykey").Str("[[-115 33] [-115 33]]"),
		Do("SET", "mykey", "myid3", "OBJECT", `{"type":"Point","coordinates":[-130,38,10]}`).OK(),
		Do("SET", "mykey", "myid4", "OBJECT", `{"type":"Point","coordinates":[-110,25,-8]}`).OK(),
		Do("BOUNDS", "mykey").Str("[[-130 25] [-110 38]]"),
		Do("BOUNDS", "mykey", "hello").Err("wrong number of arguments for 'bounds' command"),
		Do("BOUNDS", "nada").Str("<nil>"),
		Do("BOUNDS", "nada").JSON().Err("key not found"),
		Do("BOUNDS", "").Str("<nil>"),
		Do("BOUNDS", "mykey").JSON().Str(`{"ok":true,"bounds":{"type":"Polygon","coordinates":[[[-130,25],[-110,25],[-110,38],[-130,38],[-130,25]]]}}`),
	)
}

func keys_DEL_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid", "POINT", 33, -115).OK(),
		Do("GET", "mykey", "myid", "POINT").Str("[33 -115]"),
		Do("DEL", "mykey", "myid2", "ERRON404").Err("id not found"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("DEL", "mykey", "myid").Str("0"),
		Do("DEL", "mykey").Err("wrong number of arguments for 'del' command"),
		Do("GET", "mykey", "myid").Str("<nil>"),
		Do("DEL", "mykey", "myid", "ERRON404").Err("key not found"),
		Do("DEL", "mykey", "myid", "invalid-arg").Err("invalid argument 'invalid-arg'"),
		Do("SET", "mykey", "myid", "POINT", 33, -115).OK(),
		Do("DEL", "mykey", "myid2", "ERRON404").JSON().Err("id not found"),
		Do("DEL", "mykey", "myid").JSON().OK(),
		Do("DEL", "mykey", "myid").JSON().OK(),
		Do("DEL", "mykey", "myid", "ERRON404").JSON().Err("key not found"),
	)
}

func keys_DROP_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("SET", "mykey", "myid2", "HASH", "9my5xp8").OK(),
		Do("SCAN", "mykey", "COUNT").Str("2"),
		Do("DROP").Err("wrong number of arguments for 'drop' command"),
		Do("DROP", "mykey", "arg3").Err("wrong number of arguments for 'drop' command"),
		Do("DROP", "mykey").Str("1"),
		Do("SCAN", "mykey", "COUNT").Str("0"),
		Do("DROP", "mykey").Str("0"),
		Do("SCAN", "mykey", "COUNT").Str("0"),
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("DROP", "mykey").JSON().OK(),
		Do("DROP", "mykey").JSON().OK(),
	)
}
func keys_RENAME_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("SET", "mykey", "myid2", "HASH", "9my5xp8").OK(),
		Do("SCAN", "mykey", "COUNT").Str("2"),
		Do("RENAME", "foo", "mynewkey", "arg3").Err("wrong number of arguments for 'rename' command"),
		Do("RENAME", "mykey", "mynewkey").OK(),
		Do("SCAN", "mykey", "COUNT").Str("0"),
		Do("SCAN", "mynewkey", "COUNT").Str("2"),
		Do("SET", "mykey", "myid3", "HASH", "9my5xp7").OK(),
		Do("RENAME", "mykey", "mynewkey").OK(),
		Do("SCAN", "mykey", "COUNT").Str("0"),
		Do("SCAN", "mynewkey", "COUNT").Str("1"),
		Do("RENAME", "foo", "mynewkey").Err("key not found"),
		Do("SCAN", "mynewkey", "COUNT").Str("1"),
		Do("SETCHAN", "mychan", "INTERSECTS", "mynewkey", "BOUNDS", 10, 10, 20, 20).Str("1"),
		Do("RENAME", "mynewkey", "foo2").Err("key has hooks set"),
		Do("RENAMENX", "mynewkey", "foo2").Err("key has hooks set"),
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("RENAME", "mykey", "foo2").OK(),
		Do("RENAMENX", "foo2", "foo3").Str("1"),
		Do("RENAMENX", "foo2", "foo3").Err("key not found"),
		Do("RENAME", "foo2", "foo3").JSON().Err("key not found"),
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("RENAMENX", "mykey", "foo3").Str("0"),
		Do("RENAME", "foo3", "foo4").JSON().OK(),
	)
}
func keys_RENAMENX_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("SET", "mykey", "myid2", "HASH", "9my5xp8").OK(),
		Do("SCAN", "mykey", "COUNT").Str("2"),
		Do("RENAMENX", "mykey", "mynewkey").Str("1"),
		Do("SCAN", "mykey", "COUNT").Str("0"),
		Do("DROP", "mykey").Str("0"),
		Do("SCAN", "mykey", "COUNT").Str("0"),
		Do("SCAN", "mynewkey", "COUNT").Str("2"),
		Do("SET", "mykey", "myid3", "HASH", "9my5xp7").OK(),
		Do("RENAMENX", "mykey", "mynewkey").Str("0"),
		Do("SCAN", "mykey", "COUNT").Str("1"),
		Do("SCAN", "mynewkey", "COUNT").Str("2"),
		Do("RENAMENX", "foo", "mynewkey").Str("ERR key not found"),
		Do("SCAN", "mynewkey", "COUNT").Str("2"),
	)
}
func keys_EXPIRE_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid", "STRING", "value").OK(),
		Do("EXPIRE", "mykey", "myid").Err("wrong number of arguments for 'expire' command"),
		Do("EXPIRE", "mykey", "myid", "y").Err("invalid argument 'y'"),
		Do("EXPIRE", "mykey", "myid", 1).Str("1"),
		Do("EXPIRE", "mykey", "myid", 1).JSON().OK(),
		Sleep(time.Second/4),
		Do("GET", "mykey", "myid").Str("value"),
		Sleep(time.Second),
		Do("GET", "mykey", "myid").Str("<nil>"),
		Do("EXPIRE", "mykey", "myid", 1).JSON().Err("key not found"),
		Do("SET", "mykey", "myid1", "STRING", "value1").OK(),
		Do("SET", "mykey", "myid2", "STRING", "value2").OK(),
		Do("EXPIRE", "mykey", "myid1", 1).Str("1"),
		Sleep(time.Second/4),
		Do("GET", "mykey", "myid1").Str("value1"),
		Sleep(time.Second),
		Do("EXPIRE", "mykey", "myid1", 1).Str("0"),
		Do("EXPIRE", "mykey", "myid1", 1).JSON().Err("id not found"),
	)
}
func keys_FSET_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid", "HASH", "9my5xp7").OK(),
		Do("GET", "mykey", "myid", "WITHFIELDS", "HASH", 7).Str("[9my5xp7]"),
		Do("FSET", "mykey", "myid", "f1", 105.6).Str("1"),
		Do("GET", "mykey", "myid", "WITHFIELDS", "HASH", 7).Str("[9my5xp7 [f1 105.6]]"),
		Do("FSET", "mykey", "myid", "f1", 1.1, "f2", 2.2).Str("2"),
		Do("GET", "mykey", "myid", "WITHFIELDS", "HASH", 7).Str("[9my5xp7 [f1 1.1 f2 2.2]]"),
		Do("FSET", "mykey", "myid", "f1", 1.1, "f2", 22.22).Str("1"),
		Do("GET", "mykey", "myid", "WITHFIELDS", "HASH", 7).Str("[9my5xp7 [f1 1.1 f2 22.22]]"),
		Do("FSET", "mykey", "myid", "f1", 0).Str("1"),
		Do("GET", "mykey", "myid", "WITHFIELDS", "HASH", 7).Str("[9my5xp7 [f2 22.22]]"),
		Do("FSET", "mykey", "myid", "f2", 0).Str("1"),
		Do("GET", "mykey", "myid", "WITHFIELDS", "HASH", 7).Str("[9my5xp7]"),
		Do("FSET", "mykey", "myid2", "xx", "f1", 1.1, "f2", 2.2).Str("0"),
		Do("GET", "mykey", "myid2").Str("<nil>"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),
		Do("SET", "mykey", "myid", "POINT", 1, 2).OK(),
		Do("GET", "mykey", "myid", "WITHFIELDS").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[2,1]}}`),
		Do("FSET", "mykey", "myid", "f2", 1).JSON().OK(),
		Do("GET", "mykey", "myid", "WITHFIELDS").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[2,1]},"fields":{"f2":1}}`),
		Do("SET", "mykey", "myid", "POINT", 3, 4).OK(),
		Do("GET", "mykey", "myid", "WITHFIELDS").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[4,3]},"fields":{"f2":1}}`),
		Do("SET", "mykey", "myid", "HASH", "9my5xp7").OK(),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),
		Do("SET", "mykey", "myid", "HASH", "9my5xp7").OK(),
		Do("CONFIG", "SET", "maxmemory", "1").OK(),
		Do("FSET", "mykey", "myid", "xx", "f1", 1.1, "f2", 2.2).Err(`OOM command not allowed when used memory > 'maxmemory'`),
		Do("CONFIG", "SET", "maxmemory", "0").OK(),
		Do("FSET", "mykey", "myid", "xx").Err("wrong number of arguments for 'fset' command"),
		Do("FSET", "mykey", "myid", "f1", "a", "f2").Err("wrong number of arguments for 'fset' command"),
		Do("FSET", "mykey", "myid", "z", "a").Err("invalid argument 'z'"),
		Do("FSET", "mykey2", "myid", "a", "b").Err("key not found"),
		Do("FSET", "mykey", "myid2", "a", "b").Err("id not found"),
		Do("FSET", "mykey", "myid", "f2", 0).JSON().OK(),
	)
}
func keys_GET_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid", "STRING", "value").OK(),
		Do("GET", "mykey", "myid").Str("value"),
		Do("SET", "mykey", "myid", "STRING", "value2").OK(),
		Do("GET", "mykey", "myid").Str("value2"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),
		Do("GET", "mykey").Err("wrong number of arguments for 'get' command"),
		Do("GET", "mykey", "myid", "hash").Err("wrong number of arguments for 'get' command"),
		Do("GET", "mykey", "myid", "hash", "0").Err("invalid argument '0'"),
		Do("GET", "mykey", "myid", "hash", "-1").Err("invalid argument '-1'"),
		Do("GET", "mykey", "myid", "hash", "13").Err("invalid argument '13'"),
		Do("SET", "mykey", "myid", "field", "hello", "world", "field", "hiya", 55, "point", 33, -112).OK(),
		Do("GET", "mykey", "myid", "hash", "1").Str("9"),
		Do("GET", "mykey", "myid", "point").Str("[33 -112]"),
		Do("GET", "mykey", "myid", "bounds").Str("[[33 -112] [33 -112]]"),
		Do("GET", "mykey", "myid", "object").Str(`{"type":"Point","coordinates":[-112,33]}`),
		Do("GET", "mykey", "myid", "object").Str(`{"type":"Point","coordinates":[-112,33]}`),
		Do("GET", "mykey", "myid", "withfields", "point").Str(`[[33 -112] [hello world hiya 55]]`),
		Do("GET", "mykey", "myid", "joint").Err("wrong number of arguments for 'get' command"),
		Do("GET", "mykey2", "myid").Str("<nil>"),
		Do("GET", "mykey2", "myid").JSON().Err("key not found"),
		Do("GET", "mykey", "myid2").Str("<nil>"),
		Do("GET", "mykey", "myid2").JSON().Err("id not found"),
		Do("GET", "mykey", "myid", "point").JSON().Str(`{"ok":true,"point":{"lat":33,"lon":-112}}`),
		Do("GET", "mykey", "myid", "object").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[-112,33]}}`),
		Do("GET", "mykey", "myid", "hash", "1").JSON().Str(`{"ok":true,"hash":"9"}`),
		Do("GET", "mykey", "myid", "bounds").JSON().Str(`{"ok":true,"bounds":{"sw":{"lat":33,"lon":-112},"ne":{"lat":33,"lon":-112}}}`),
		Do("SET", "mykey", "myid2", "point", 33, -112, 55).OK(),
		Do("GET", "mykey", "myid2", "point").Str("[33 -112 55]"),
		Do("GET", "mykey", "myid2", "point").JSON().Str(`{"ok":true,"point":{"lat":33,"lon":-112,"z":55}}`),
		Do("GET", "mykey", "myid", "withfields").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[-112,33]},"fields":{"hello":"world","hiya":55}}`),
	)
}
func keys_KEYS_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey11", "myid4", "STRING", "value").OK(),
		Do("SET", "mykey22", "myid2", "HASH", "9my5xp7").OK(),
		Do("SET", "mykey22", "myid1", "OBJECT", `{"type":"Point","coordinates":[-130,38,10]}`).OK(),
		Do("SET", "mykey11", "myid3", "OBJECT", `{"type":"Point","coordinates":[-110,25,-8]}`).OK(),
		Do("SET", "mykey42", "myid2", "HASH", "9my5xp7").OK(),
		Do("SET", "mykey31", "myid4", "STRING", "value").OK(),
		Do("SET", "mykey310", "myid5", "STRING", "value").OK(),
		Do("KEYS", "*").Str("[mykey11 mykey22 mykey31 mykey310 mykey42]"),
		Do("KEYS", "*key*").Str("[mykey11 mykey22 mykey31 mykey310 mykey42]"),
		Do("KEYS", "mykey*").Str("[mykey11 mykey22 mykey31 mykey310 mykey42]"),
		Do("KEYS", "mykey4*").Str("[mykey42]"),
		Do("KEYS", "mykey*1").Str("[mykey11 mykey31]"),
		Do("KEYS", "mykey*1*").Str("[mykey11 mykey31 mykey310]"),
		Do("KEYS", "mykey*10").Str("[mykey310]"),
		Do("KEYS", "mykey*2").Str("[mykey22 mykey42]"),
		Do("KEYS", "*2").Str("[mykey22 mykey42]"),
		Do("KEYS", "*1*").Str("[mykey11 mykey31 mykey310]"),
		Do("KEYS", "mykey").Str("[]"),
		Do("KEYS", "mykey31").Str("[mykey31]"),
		Do("KEYS", "mykey[^3]*").Str("[mykey11 mykey22 mykey42]"),
		Do("KEYS").Err("wrong number of arguments for 'keys' command"),
		Do("KEYS", "*").JSON().Str(`{"ok":true,"keys":["mykey11","mykey22","mykey31","mykey310","mykey42"]}`),
	)
}
func keys_PERSIST_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid", "STRING", "value").OK(),
		Do("EXPIRE", "mykey", "myid", 2).Str("1"),
		Do("PERSIST", "mykey", "myid").Str("1"),
		Do("PERSIST", "mykey", "myid").Str("0"),
		Do("PERSIST", "mykey").Err("wrong number of arguments for 'persist' command"),
		Do("PERSIST", "mykey2", "myid").Str("0"),
		Do("PERSIST", "mykey2", "myid").JSON().Err("key not found"),
		Do("PERSIST", "mykey", "myid2").Str("0"),
		Do("PERSIST", "mykey", "myid2").JSON().Err("id not found"),
		Do("EXPIRE", "mykey", "myid", 2).Str("1"),
		Do("PERSIST", "mykey", "myid").JSON().OK(),
	)
}
func keys_SET_test(mc *mockServer) error {
	return mc.DoBatch(
		// Section: point
		Do("SET", "mykey", "myid", "POINT", 33, -115).OK(),
		Do("GET", "mykey", "myid", "POINT").Str("[33 -115]"),
		Do("GET", "mykey", "myid", "BOUNDS").Str("[[33 -115] [33 -115]]"),
		Do("GET", "mykey", "myid", "OBJECT").Str(`{"type":"Point","coordinates":[-115,33]}`),
		Do("GET", "mykey", "myid", "HASH", 7).Str("9my5xp7"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),
		Do("SET", "mykey", "myid", "point", "33", "-112", "99").OK(),

		// Section: object
		Do("SET", "mykey", "myid", "OBJECT", `{"type":"Point","coordinates":[-115,33]}`).OK(),
		Do("GET", "mykey", "myid", "POINT").Str("[33 -115]"),
		Do("GET", "mykey", "myid", "BOUNDS").Str("[[33 -115] [33 -115]]"),
		Do("GET", "mykey", "myid", "OBJECT").Str(`{"type":"Point","coordinates":[-115,33]}`),
		Do("GET", "mykey", "myid", "HASH", 7).Str("9my5xp7"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),

		// Section: bounds
		Do("SET", "mykey", "myid", "BOUNDS", 33, -115, 33, -115).OK(),
		Do("GET", "mykey", "myid", "POINT").Str("[33 -115]"),
		Do("GET", "mykey", "myid", "BOUNDS").Str("[[33 -115] [33 -115]]"),
		Do("GET", "mykey", "myid", "OBJECT").Str(`{"type":"Polygon","coordinates":[[[-115,33],[-115,33],[-115,33],[-115,33],[-115,33]]]}`),
		Do("GET", "mykey", "myid", "HASH", 7).Str("9my5xp7"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),

		// Section: hash
		Do("SET", "mykey", "myid", "HASH", "9my5xp7").OK(),
		Do("GET", "mykey", "myid", "HASH", 7).Str("9my5xp7"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),
		Do("SET", "mykey", "myid", "HASH", "9my5xp7").JSON().OK(),

		// Section: field
		Do("SET", "mykey", "myid", "FIELD", "f1", 33, "FIELD", "a2", 44.5, "HASH", "9my5xp7").OK(),
		Do("GET", "mykey", "myid", "WITHFIELDS", "HASH", 7).Str("[9my5xp7 [a2 44.5 f1 33]]"),
		Do("FSET", "mykey", "myid", "f1", 0).Str("1"),
		Do("FSET", "mykey", "myid", "f1", 0).Str("0"),
		Do("GET", "mykey", "myid", "WITHFIELDS", "HASH", 7).Str("[9my5xp7 [a2 44.5]]"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),

		// Section: string
		Do("SET", "mykey", "myid", "STRING", "value").OK(),
		Do("GET", "mykey", "myid").Str("value"),
		Do("SET", "mykey", "myid", "STRING", "value2").OK(),
		Do("GET", "mykey", "myid").Str("value2"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),

		// Test error conditions
		Do("CONFIG", "SET", "maxmemory", "1").OK(),
		Do("SET", "mykey", "myid", "STRING", "value2").Err("OOM command not allowed when used memory > 'maxmemory'"),
		Do("CONFIG", "SET", "maxmemory", "0").OK(),
		Do("SET").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "myid", "FIELD", "f1").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "myid", "FIELD", "z", "1").Err("invalid argument 'z'"),
		Do("SET", "mykey", "myid", "EX").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "myid", "EX", "yyy").Err("invalid argument 'yyy'"),
		Do("SET", "mykey", "myid", "EX", "123").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "myid", "nx").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "myid", "nx", "xx").Err("invalid argument 'xx'"),
		Do("SET", "mykey", "myid", "xx", "nx").Err("invalid argument 'nx'"),
		Do("SET", "mykey", "myid", "string").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "myid", "point").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "myid", "point", "33").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "myid", "point", "33f", "-112").Err("invalid argument '33f'"),
		Do("SET", "mykey", "myid", "point", "33", "-112f").Err("invalid argument '-112f'"),
		Do("SET", "mykey", "myid", "point", "33", "-112f", "99").Err("invalid argument '-112f'"),
		Do("SET", "mykey", "myid", "bounds").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "myid", "bounds", "fff", "1", "2", "3").Err("invalid argument 'fff'"),
		Do("SET", "mykey", "myid", "hash").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "myid", "object").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "myid", "object", "asd").Err("invalid data"),
		Do("SET", "mykey", "myid", "joint").Err("invalid argument 'joint'"),
		Do("SET", "mykey", "myid", "XX", "HASH", "9my5xp7").Err("<nil>"),
		Do("SET", "mykey", "myid", "XX", "HASH", "9my5xp7").JSON().Err("id not found"),
		Do("SET", "mykey", "myid1", "HASH", "9my5xp7").OK(),
		Do("SET", "mykey", "myid", "XX", "HASH", "9my5xp7").Err("<nil>"),
		Do("SET", "mykey", "myid", "NX", "HASH", "9my5xp7").OK(),
		Do("SET", "mykey", "myid", "XX", "HASH", "9my5xp7").OK(),
		Do("SET", "mykey", "myid", "NX", "HASH", "9my5xp7").Err("<nil>"),
		Do("SET", "mykey", "myid", "NX", "HASH", "9my5xp7").JSON().Err("id already exists"),
	)
}

func keys_SET_CIRCLE_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid", "CIRCLE", 33, -115, 100000).OK(),
		Do("GET", "mykey", "myid", "POINT").Str("[33 -115]"),
		Do("GET", "mykey", "myid", "OBJECT").Str(`{"type":"Feature","geometry":{"type":"Point","coordinates":[-115,33]},"properties":{"type":"Circle","radius":100000,"radius_units":"m"}}`),
		Do("GET", "mykey", "myid", "BOUNDS").Str("[[32.10067839408127 -116.072280937208] [33.89932160591873 -113.927719062792]]"),
		Do("GET", "mykey", "myid", "HASH", 7).Str("9my5xp7"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("GET", "mykey", "myid").Str("<nil>"),
	)
}

func keys_STATS_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("STATS", "mykey").Str("[nil]"),
		Do("SET", "mykey", "myid", "STRING", "value").OK(),
		Do("STATS", "mykey").Str("[[in_memory_size 9 num_objects 1 num_points 0 num_strings 1]]"),
		Do("STATS", "mykey", "hello").JSON().Str(`{"ok":true,"stats":[{"in_memory_size":9,"num_objects":1,"num_points":0,"num_strings":1},null]}`),
		Do("SET", "mykey", "myid2", "STRING", "value").OK(),
		Do("STATS", "mykey").Str("[[in_memory_size 19 num_objects 2 num_points 0 num_strings 2]]"),
		Do("SET", "mykey", "myid3", "OBJECT", `{"type":"Point","coordinates":[-115,33]}`).OK(),
		Do("STATS", "mykey").Str("[[in_memory_size 40 num_objects 3 num_points 1 num_strings 2]]"),
		Do("DEL", "mykey", "myid").Str("1"),
		Do("STATS", "mykey").Str("[[in_memory_size 31 num_objects 2 num_points 1 num_strings 1]]"),
		Do("DEL", "mykey", "myid3").Str("1"),
		Do("STATS", "mykey").Str("[[in_memory_size 10 num_objects 1 num_points 0 num_strings 1]]"),
		Do("STATS", "mykey", "mykey2").Str("[[in_memory_size 10 num_objects 1 num_points 0 num_strings 1] nil]"),
		Do("DEL", "mykey", "myid2").Str("1"),
		Do("STATS", "mykey").Str("[nil]"),
		Do("STATS", "mykey", "mykey2").Str("[nil nil]"),
		Do("STATS", "mykey").Str("[nil]"),
		Do("STATS").Err(`wrong number of arguments for 'stats' command`),
	)
}
func keys_TTL_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid", "STRING", "value").OK(),
		Do("EXPIRE", "mykey", "myid", 2).Str("1"),
		Do("EXPIRE", "mykey", "myid", 2).JSON().OK(),
		Sleep(time.Millisecond*10),
		Do("TTL", "mykey", "myid").Str("1"),
		Do("EXPIRE", "mykey", "myid", 1).Str("1"),
		Sleep(time.Millisecond*10),
		Do("TTL", "mykey", "myid").Str("0"),
		Do("TTL", "mykey", "myid").JSON().Str(`{"ok":true,"ttl":0}`),
		Do("TTL", "mykey2", "myid").Str("-2"),
		Do("TTL", "mykey", "myid2").Str("-2"),
		Do("TTL", "mykey").Err("wrong number of arguments for 'ttl' command"),
		Do("SET", "mykey", "myid", "STRING", "value").OK(),
		Do("TTL", "mykey", "myid").Str("-1"),
		Do("TTL", "mykey2", "myid").JSON().Err("key not found"),
		Do("TTL", "mykey", "myid2").JSON().Err("id not found"),
	)
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
	return mc.DoBatch(
		Do("SET", "mykey", "myid1a", "FIELD", "a", 1, "POINT", 33, -115).OK(),
		Do("GET", "mykey", "myid1a", "WITHFIELDS").Str(`[{"type":"Point","coordinates":[-115,33]} [a 1]]`),
		Do("SET", "mykey", "myid1a", "FIELD", "a", "a", "POINT", 33, -115).OK(),
		Do("GET", "mykey", "myid1a", "WITHFIELDS").Str(`[{"type":"Point","coordinates":[-115,33]} [a a]]`),
		Do("SET", "mykey", "myid1a", "FIELD", "a", 1, "FIELD", "b", 2, "POINT", 33, -115).OK(),
		Do("GET", "mykey", "myid1a", "WITHFIELDS").Str(`[{"type":"Point","coordinates":[-115,33]} [a 1 b 2]]`),
		Do("SET", "mykey", "myid1a", "FIELD", "b", 2, "POINT", 33, -115).OK(),
		Do("GET", "mykey", "myid1a", "WITHFIELDS").Str(`[{"type":"Point","coordinates":[-115,33]} [a 1 b 2]]`),
		Do("SET", "mykey", "myid1a", "FIELD", "b", 2, "FIELD", "a", "1", "FIELD", "c", 3, "POINT", 33, -115).OK(),
		Do("GET", "mykey", "myid1a", "WITHFIELDS").Str(`[{"type":"Point","coordinates":[-115,33]} [a 1 b 2 c 3]]`),

		Do("GET", "fleet", "truck1", "WITHFIELDS").JSON().Err("key not found"),
		Do("SET", "fleet", "truck1", "FIELD", "speed", "0", "POINT", "-112", "33").JSON().OK(),
		Do("GET", "fleet", "truck1", "WITHFIELDS").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[33,-112]}}`),
		Do("SET", "fleet", "truck1", "FIELD", "speed", "1", "POINT", "-112", "33").JSON().OK(),
		Do("GET", "fleet", "truck1", "WITHFIELDS").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[33,-112]},"fields":{"speed":1}}`),
		Do("SET", "fleet", "truck1", "FIELD", "speed", "0", "POINT", "-112", "33").JSON().OK(),
		Do("GET", "fleet", "truck1", "WITHFIELDS").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[33,-112]}}`),
		Do("SET", "fleet", "truck1", "FIELD", "speed", "2", "POINT", "-112", "33").JSON().OK(),
		Do("GET", "fleet", "truck1", "WITHFIELDS").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[33,-112]},"fields":{"speed":2}}`),

		// Do some GJSON queries.
		Do("SET", "fleet", "truck2", "FIELD", "hello", `{"world":"tom"}`, "POINT", "-112", "33").JSON().OK(),
		Do("SCAN", "fleet", "WHERE", "hello", `{"world":"tom"}`, `{"world":"tom"}`, "COUNT").JSON().Str(`{"ok":true,"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "hello.world == 'tom'", "COUNT").JSON().Str(`{"ok":true,"count":1,"cursor":0}`),
		// The next scan does not match on anything, but since we're matching
		// on zeros, which is the default, then all (two) objects are returned.
		Do("SCAN", "fleet", "WHERE", "hello.world.1", `0`, `0`, "IDS").JSON().Str(`{"ok":true,"ids":["truck1","truck2"],"count":2,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "hello.world", ">", `tom`, "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "hello.world", ">=", `Tom`, "IDS").JSON().Str(`{"ok":true,"ids":["truck2"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "hello.world", ">=", `tom`, "IDS").JSON().Str(`{"ok":true,"ids":["truck2"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "hello.world", "==", `tom`, "IDS").JSON().Str(`{"ok":true,"ids":["truck2"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "hello.world", "<", `tom`, "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "hello.world", "<=", `tom`, "IDS").JSON().Str(`{"ok":true,"ids":["truck1","truck2"],"count":2,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "hello.world", "<", `uom`, "IDS").JSON().Str(`{"ok":true,"ids":["truck1","truck2"],"count":2,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "hello.world", "!=", `tom`, "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),

		Do("SET", "fleet", "truck1", "OBJECT", `{"type":"Feature","geometry":{"type":"Point","coordinates":[-112,33]},"properties":{"speed":50},"asdf":"Adsf"}`).JSON().OK(),
		Do("SCAN", "fleet", "WHERE", "properties.speed", ">", 49, "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "properties.speed", ">", 50, "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "properties.speed", "<", 51, "IDS").JSON().Str(`{"ok":true,"ids":["truck1","truck2"],"count":2,"cursor":0}`),

		Do("DROP", "fleet").JSON().OK(),
		Do("SET", "fleet", "truck1", "FIELD", "speed", "50", "POINT", "-112", "33").JSON().OK(),
		Do("SET", "fleet", "truck1", "FIELD", "Speed", "51", "POINT", "-112", "33").JSON().OK(),
		Do("SET", "fleet", "truck1", "FIELD", "speeD", "52", "POINT", "-112", "33").JSON().OK(),
		Do("SET", "fleet", "truck1", "FIELD", "SpeeD", "53", "POINT", "-112", "33").JSON().OK(),
		Do("GET", "fleet", "truck1", "WITHFIELDS").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[33,-112]},"fields":{"SpeeD":53,"Speed":51,"speeD":52,"speed":50}}`),
		Do("SCAN", "fleet", "WHERE", "speed == 50", "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "Speed == 50", "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "Speed == 51", "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "speed", 50, 50, "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "Speed", 51, 51, "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "Speed", 50, 50, "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "speed", 51, 51, "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),

		Do("DROP", "fleet").JSON().OK(),
		Do("SET", "fleet", "truck1", "field", "props", `{"speed":50,"Speed":51}`, "point", "33", "-112").JSON().OK(),
		Do("SET", "fleet", "truck1", "field", "Props", `{"speed":52,"Speed":53}`, "point", "33", "-112").JSON().OK(),
		Do("GET", "fleet", "truck1", "WITHFIELDS").JSON().Str(`{"ok":true,"object":{"type":"Point","coordinates":[-112,33]},"fields":{"Props":{"speed":52,"Speed":53},"props":{"speed":50,"Speed":51}}}`),
		Do("SCAN", "fleet", "WHERE", "props.speed == 50", "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "props.Speed == 51", "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "Props.speed == 52", "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "Props.Speed == 53", "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "props.Speed == 52", "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "props.speed == 51", "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "Props.speed == 53", "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "Props.Speed == 50", "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "props.speed > 49", "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "props.Speed > 49", "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "Props.speed > 49", "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "Props.Speed > 49", "IDS").JSON().Str(`{"ok":true,"ids":["truck1"],"count":1,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "props.Speed > 53", "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "props.speed > 53", "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "Props.speed > 53", "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),
		Do("SCAN", "fleet", "WHERE", "Props.Speed > 53", "IDS").JSON().Str(`{"ok":true,"ids":[],"count":0,"cursor":0}`),
	)
}

func keys_PDEL_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid1a", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid1b", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid2a", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid2b", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid3a", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid3b", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid4a", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid4b", "POINT", 33, -115).OK(),
		Do("PDEL", "mykey").Err("wrong number of arguments for 'pdel' command"),
		Do("PDEL", "mykeyNA", "*").Str("0"),
		Do("PDEL", "mykey", "myid1a").Str("1"),
		Do("PDEL", "mykey", "myid1a").Str("0"),
		Do("PDEL", "mykey", "myid1*").Str("1"),
		Do("PDEL", "mykey", "myid2*").Str("2"),
		Do("PDEL", "mykey", "*b").Str("2"),
		Do("PDEL", "mykey", "*").Str("2"),
		Do("PDEL", "mykey", "*").Str("0"),
		Do("SET", "mykey", "myid1a", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid1b", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid2a", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid2b", "POINT", 33, -115).OK(),
		Do("SET", "mykey", "myid3a", "POINT", 33, -115).OK(),
		Do("PDEL", "mykey", "*").JSON().OK(),
	)
}

func keys_WHEREIN_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid_a1", "FIELD", "a", 1, "POINT", 33, -115).OK(),
		Do("WITHIN", "mykey", "WHEREIN", "a", 3, 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8).Str(`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`),
		Do("WITHIN", "mykey", "WHEREIN", "a", "a", 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8).Err("invalid argument 'a'"),
		Do("WITHIN", "mykey", "WHEREIN", "a", 1, 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8).Err("invalid argument '1'"),
		Do("WITHIN", "mykey", "WHEREIN", "a", 3, 0, "a", 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8).Str("[0 []]"),
		Do("WITHIN", "mykey", "WHEREIN", "a", 4, 0, "a", 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8).Str(`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`),
		Do("SET", "mykey", "myid_a2", "FIELD", "a", 2, "POINT", 32.99, -115).OK(),
		Do("SET", "mykey", "myid_a3", "FIELD", "a", 3, "POINT", 33, -115.02).OK(),
		Do("WITHIN", "mykey", "WHEREIN", "a", 3, 0, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8).Str(`[0 [[myid_a2 {"type":"Point","coordinates":[-115,32.99]} [a 2]] [myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`),
		// zero value should not match 1 and 2
		Do("SET", "mykey", "myid_a0", "FIELD", "a", 0, "POINT", 33, -115.02).OK(),
		Do("WITHIN", "mykey", "WHEREIN", "a", 2, 1, 2, "BOUNDS", 32.8, -115.2, 33.2, -114.8).Str(`[0 [[myid_a2 {"type":"Point","coordinates":[-115,32.99]} [a 2]] [myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`),
	)
}

func keys_WHEREEVAL_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid_a1", "FIELD", "a", 1, "POINT", 33, -115).OK(),
		Do("WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1])", 1, 0.5, "BOUNDS", 32.8, -115.2, 33.2, -114.8).Str(`[0 [[myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`),
		Do("WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1])", "a", 0.5, "BOUNDS", 32.8, -115.2, 33.2, -114.8).Err("invalid argument 'a'"),
		Do("WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1])", 1, 0.5, 4, "BOUNDS", 32.8, -115.2, 33.2, -114.8).Err("invalid argument '4'"),
		Do("SET", "mykey", "myid_a2", "FIELD", "a", 2, "POINT", 32.99, -115).OK(),
		Do("SET", "mykey", "myid_a3", "FIELD", "a", 3, "POINT", 33, -115.02).OK(),
		Do("WITHIN", "mykey", "WHEREEVAL", "return FIELDS.a > tonumber(ARGV[1]) and FIELDS.a ~= tonumber(ARGV[2])", 2, 0.5, 3, "BOUNDS", 32.8, -115.2, 33.2, -114.8).Str(`[0 [[myid_a2 {"type":"Point","coordinates":[-115,32.99]} [a 2]] [myid_a1 {"type":"Point","coordinates":[-115,33]} [a 1]]]]`),
	)
}

func keys_TYPE_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "myid1", "POINT", 33, -115).OK(),
		Do("TYPE", "mykey").Str("hash"),
		Do("TYPE", "mykey", "hello").Err("wrong number of arguments for 'type' command"),
		Do("TYPE", "mykey2").Str("none"),
		Do("TYPE", "mykey2").JSON().Err("key not found"),
		Do("TYPE", "mykey").JSON().Str(`{"ok":true,"type":"hash"}`),
	)
}

func keys_FLUSHDB_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey1", "myid1", "POINT", 33, -115).OK(),
		Do("SET", "mykey2", "myid1", "POINT", 33, -115).OK(),
		Do("SETCHAN", "mychan", "INTERSECTS", "mykey1", "BOUNDS", 10, 10, 10, 10).Str("1"),
		Do("KEYS", "*").Str("[mykey1 mykey2]"),
		Do("CHANS", "*").JSON().Func(func(s string) error {
			if gjson.Get(s, "chans.#").Int() != 1 {
				return fmt.Errorf("expected '%d', got '%d'", 1, gjson.Get(s, "chans.#").Int())
			}
			return nil
		}),
		Do("FLUSHDB", "arg2").Err("wrong number of arguments for 'flushdb' command"),
		Do("FLUSHDB").OK(),
		Do("KEYS", "*").Str("[]"),
		Do("CHANS", "*").Str("[]"),
		Do("SET", "mykey1", "myid1", "POINT", 33, -115).OK(),
		Do("SET", "mykey2", "myid1", "POINT", 33, -115).OK(),
		Do("SETCHAN", "mychan", "INTERSECTS", "mykey1", "BOUNDS", 10, 10, 10, 10).Str("1"),
		Do("FLUSHDB").JSON().OK(),
	)
}

func keys_HEALTHZ_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("HEALTHZ").OK(),
		Do("HEALTHZ").JSON().OK(),
		Do("HEALTHZ", "arg").Err(`wrong number of arguments for 'healthz' command`),
	)
}

func keys_SERVER_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SERVER").Func(func(s string) error {
			valid := strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") &&
				strings.Contains(s, "cpus") && strings.Contains(s, "mem_alloc")
			if !valid {
				return errors.New("looks invalid")
			}
			return nil
		}),
		Do("SERVER").JSON().Func(func(s string) error {
			if !gjson.Get(s, "ok").Bool() {
				return errors.New("not ok")
			}
			valid := gjson.Get(s, "stats.cpus").Exists() &&
				gjson.Get(s, "stats.mem_alloc").Exists()
			if !valid {
				return errors.New("looks invalid")
			}
			return nil
		}),
		Do("SERVER", "ext").Func(func(s string) error {
			valid := strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") &&
				strings.Contains(s, "sys_cpus") &&
				strings.Contains(s, "tile38_connected_clients")

			if !valid {
				return errors.New("looks invalid")
			}
			return nil
		}),
		Do("SERVER", "ext").JSON().Func(func(s string) error {
			if !gjson.Get(s, "ok").Bool() {
				return errors.New("not ok")
			}
			valid := gjson.Get(s, "stats.sys_cpus").Exists() &&
				gjson.Get(s, "stats.tile38_connected_clients").Exists()
			if !valid {
				return errors.New("looks invalid")
			}
			return nil
		}),
		Do("SERVER", "ett").Err(`invalid argument 'ett'`),
		Do("SERVER", "ett").JSON().Err(`invalid argument 'ett'`),
	)
}

func keys_INFO_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("INFO").Func(func(s string) error {
			if !strings.Contains(s, "# Clients") ||
				!strings.Contains(s, "# Stats") {
				return errors.New("looks invalid")
			}
			return nil
		}),
		Do("INFO", "all").Func(func(s string) error {
			if !strings.Contains(s, "# Clients") ||
				!strings.Contains(s, "# Stats") {
				return errors.New("looks invalid")
			}
			return nil
		}),
		Do("INFO", "default").Func(func(s string) error {
			if !strings.Contains(s, "# Clients") ||
				!strings.Contains(s, "# Stats") {
				return errors.New("looks invalid")
			}
			return nil
		}),
		Do("INFO", "cpu").Func(func(s string) error {
			if !strings.Contains(s, "# CPU") ||
				strings.Contains(s, "# Clients") ||
				strings.Contains(s, "# Stats") {
				return errors.New("looks invalid")
			}
			return nil
		}),
		Do("INFO", "cpu", "clients").Func(func(s string) error {
			if !strings.Contains(s, "# CPU") ||
				!strings.Contains(s, "# Clients") ||
				strings.Contains(s, "# Stats") {
				return errors.New("looks invalid")
			}
			return nil
		}),
		Do("INFO").JSON().Func(func(s string) error {
			if gjson.Get(s, "info.tile38_version").String() == "" {
				return errors.New("looks invalid")
			}
			return nil
		}),
	)
}
