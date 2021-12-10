package tests

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func subTestJSON(t *testing.T, mc *mockServer) {
	runStep(t, mc, "basic", json_JSET_basic_test)
	runStep(t, mc, "geojson", json_JSET_geojson_test)
	runStep(t, mc, "number", json_JSET_number_test)

}
func json_JSET_basic_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"JSET", "mykey", "myid1", "hello", "world"}, {"OK"},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"hello":"world"}`)},
		{"JSET", "mykey", "myid1", "hello", "planet"}, {"OK"},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"hello":"planet"}`)},
		{"JSET", "mykey", "myid1", "user.name.last", "tom"}, {"OK"},
		{"JSET", "mykey", "myid1", "user.name.first", "andrew"}, {"OK"},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"user":{"name":{"first":"andrew","last":"tom"}},"hello":"planet"}`)},
		{"JDEL", "mykey", "myid1", "user.name.last"}, {1},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"user":{"name":{"first":"andrew"}},"hello":"planet"}`)},
		{"JDEL", "mykey", "myid1", "user.name.last"}, {0},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"user":{"name":{"first":"andrew"}},"hello":"planet"}`)},
		{"JDEL", "mykey2", "myid1", "user.name.last"}, {0},
	})
}

func json_JSET_geojson_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "myid1", "POINT", 33, -115}, {"OK"},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"type":"Point","coordinates":[-115,33]}`)},
		{"JSET", "mykey", "myid1", "coordinates.1", 44}, {"OK"},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"type":"Point","coordinates":[-115,44]}`)},
		{"SET", "mykey", "myid1", "OBJECT", `{"type":"Feature","geometry":{"type":"Point","coordinates":[-115,44]}}`}, {"OK"},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"type":"Feature","geometry":{"type":"Point","coordinates":[-115,44]},"properties":{}}`)},
		{"JGET", "mykey", "myid1", "geometry.type"}, {"Point"},
		{"JSET", "mykey", "myid1", "properties.tags.-1", "southwest"}, {"OK"},
		{"JSET", "mykey", "myid1", "properties.tags.-1", "united states"}, {"OK"},
		{"JSET", "mykey", "myid1", "properties.tags.-1", "hot"}, {"OK"},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"type":"Feature","geometry":{"type":"Point","coordinates":[-115,44]},"properties":{"tags":["southwest","united states","hot"]}}`)},
		{"JDEL", "mykey", "myid1", "type"}, {"ERR missing type"},
	})
}

func json_JSET_number_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"JSET", "mykey", "myid1", "hello", "0"}, {"OK"},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"hello":0}`)},
		{"JSET", "mykey", "myid1", "hello", "0123"}, {"OK"},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"hello":"0123"}`)},
		{"JSET", "mykey", "myid1", "hello", "3.14"}, {"OK"},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"hello":3.14}`)},
		{"JSET", "mykey", "myid1", "hello", "1.0e10"}, {"OK"},
		{"JGET", "mykey", "myid1"}, {jsonEqual(`{"hello":1.0e10}`)},
	})
}

func jsonEqual(in string) func(v interface{}) (resp, expect interface{}) {
	return func(v interface{}) (resp, expect interface{}) {
		var want interface{}
		var got interface{}
		err := json.Unmarshal([]byte(in), &want)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal([]byte(fmt.Sprint(v)), &got)
		if err != nil {
			panic(err)
		}
		if reflect.DeepEqual(want, got) {
			return v, v
		}
		return v, in
	}
}
