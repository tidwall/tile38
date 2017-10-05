package grect

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func testGet(t *testing.T, s, expect string) {
	if Get(s).String() != expect {
		t.Fatalf("for '%v': expected '%v', got '%v'", s, expect, Get(s).String())
	}
}

func TestRect(t *testing.T) {
	testGet(t, "", "[]")
	testGet(t, "[]", "[]")
	testGet(t, "[],[]", "[]")
	testGet(t, "[],[],[]", "[]")
	testGet(t, "[10]", "[10]")
	testGet(t, "[10],[10]", "[10]")
	testGet(t, "[10 11],[10]", "[10 11]")
	testGet(t, "[10 11],[11 10]", "[10 10],[11 11]")
	testGet(t, ",[10]", "[10]")
	testGet(t, "[10 11]", "[10 11]")
	testGet(t, "[-3 -2 -1 0 1 2 3],[3 2 1 0 -1 -2 -3]", "[-3 -2 -1 0 -1 -2 -3],[3 2 1 0 1 2 3]")
}

func TestWKT(t *testing.T) {
	testGet(t, "POINT(1 2)", "[1 2]")
	testGet(t, "LINESTRING(3 4, -1 -3, (-20   15 18  ))", "[-20 -3 18],[3 15 18]")
	testGet(t, "POLYGON  (((1 2 0), 3 4 1, -1 -3 123))", "[-1 -3 0],[3 4 123]")
	testGet(t, "MULTIPOINT  (1 2, 3 4, -1 0)", "[-1 0],[3 4]")
	testGet(t, " mUltiLineString (  (1 2, 3 4),(3 4, (5 6)), (-1 -2 -3))  ", "[-1 -2 -3],[5 6 -3]")
	testGet(t, `	 MULTIPOLYGON  (  
							  ((1 2,2 3),(2 3,8 9)),
	                          ((4 5,6 7))
				)`, "[1 2],[8 9]")
	testGet(t, `  
	GEOMETRYCOLLECTION  (
		POLYGON EMPTY,
		POINT EMPTY,
		POINT(1000 2),
		POINT EMPTY,
		LINESTRING(3 4, -1 -3, (-20 15 18)),
		GEOMETRYCOLLECTION EMPTY,
		GEOMETRYCOLLECTION(POINT(-1000),POLYGON((10 20,-50 1500))),
	)`, "[-1000 -3 18],[1000 1500 18]")
}

func TestGeoJSON(t *testing.T) {
	testGet(t, `{"type":"Point","coordinates":[1,2]}`, "[1 2]")
	testGet(t, `{"type":"LineString","coordinates":[[3,4], [-1,-3], [-20,15,18]]}`, "[-20 -3 18],[3 15 18]")
	testGet(t, `{"type":"Polygon", "coordinates": [[[[1,2,0]],[ 3,4,1], [-1,-3,123]]]}`, "[-1 -3 0],[3 4 123]")
	testGet(t, `{"type":"MultiPoint", "coordinates":[[1,2], [3,4], [-1,0]]}`, "[-1 0],[3 4]")
	testGet(t, `{"type":"mUltiLineString","coordinates": [  [[1,2], [3,4]],[[3,4], [5,6]], [-1,-2,-3]]}  `, "[-1 -2 -3],[5 6 -3]")
	testGet(t, `{"type":"MULTIPOLYGON","coordinates":  [
								  [[[1 2],[2 3]],[[2 3],[8 9]]],
		                          [[[4 5],[6 7]]]
					)`, "[1 2],[8 9]")
	testGet(t, `{"type":"GeometryCollection", "geometries":[  
					{"type":"Feature","geometry":{"type":"Point","coordinates":[0 -10, 17]}},
					{"type":"FeatureCollection","features":[
						{"type":"Feature","geometry":{"type":"Point","coordinates":[0 -10]}},
						{"type":"Feature","geometry":{"type":"Point","coordinates":[0 -11]}}
					]},
					{"type":"POLYGON","coordinates":[]},
					{"type":"POINT","coordinates":[]},
					{"type":"POINT","coordinates":[1000,2]},
					{"type":"POINT","coordinates":[]},
					{"type":"LINESTRING","coordinates":[[3,4], [-1,-3], [[-20,15,18]]]},
					{"type":"GeometryCollection","geometries":[]},
					{"type":"GeometryCollection","geometries":[
						{"type":"Point","coordinates":[-1000]},
						{"type":"Polygon","coordinates":[[[10,20],[-50,1500]]]}
					],
		]}`, "[-1000 -11 17],[1000 1500 18]")
}

func TestRandom(t *testing.T) {
	buf := make([]byte, 50)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 10000000; i++ {
		rand.Read(buf)
		Get(string(buf))
	}
}

func ExampleGet() {
	r := Get(`{
      "type": "Polygon",
      "coordinates": [
        [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],
          [100.0, 1.0], [100.0, 0.0] ]
        ]
    }`)
	fmt.Printf("%v %v\n", r.Min, r.Max)
	// Output:
	// [100 0] [101 1]
}
