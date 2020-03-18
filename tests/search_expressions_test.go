package tests

import (
	"testing"
)

func subTestExpressions(t *testing.T, mc *mockServer) {
	runStep(t, mc, "WITHIN_CIRCLE", expressions_WITHIN_CIRCLE_test)
	runStep(t, mc, "INTERSECTS_CIRCLE", expressions_INTERSECTS_CIRCLE_test)
	runStep(t, mc, "WITHIN", expressions_WITHIN_test)
	runStep(t, mc, "INTERSECTS", expressions_INTERSECTS_test)
	runStep(t, mc, "FIELDS", expressions_FIELDS_search_test)
}


func expressions_WITHIN_test(mc *mockServer) error {
	poly := `{
				"type": "Polygon",
				"coordinates": [
					[
						[-122.44126439094543,37.732906137107],
						[-122.43980526924135,37.732906137107],
						[-122.43980526924135,37.73421283683962],
						[-122.44126439094543,37.73421283683962],
						[-122.44126439094543,37.732906137107]
					]
				]
			}`
	poly8 := `{"type":"Polygon","coordinates":[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]],[[-122.44060993194579,37.73345766902749],[-122.44044363498686,37.73345766902749],[-122.44044363498686,37.73355524732416],[-122.44060993194579,37.73355524732416],[-122.44060993194579,37.73345766902749]],[[-122.44060724973677,37.7336888869566],[-122.4402102828026,37.7336888869566],[-122.4402102828026,37.7339752567853],[-122.44060724973677,37.7339752567853],[-122.44060724973677,37.7336888869566]]]}`
	poly9 := `{"type": "Polygon","coordinates": [[[-122.44037926197052,37.73313523548048],[-122.44017541408539,37.73313523548048],[-122.44017541408539,37.73336857568778],[-122.44037926197052,37.73336857568778],[-122.44037926197052,37.73313523548048]]]}`

	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "line2", "OBJECT", `{"type":"Feature","properties":{},"geometry":{"type":"LineString","coordinates":[[-122.44110345840454,37.733383424585185],[-122.44110614061356,37.734043136878604]]}}`}, {"OK"},
		{"SET", "mykey", "line3", "OBJECT", `{"type":"LineString","coordinates":[[-122.4408378,37.7341129],[-122.4408378,37.733]]}`}, {"OK"},
		{"SET", "mykey", "poly8", "OBJECT", poly8}, {"OK"},
		{"SET", "mykey", "poly9", "OBJECT", poly9}, {"OK"},

		{"WITHIN", "mykey", "IDS", "OBJECT", poly8, "OR", "OBJECT", poly}, {"[0 [line2 line3 poly8 poly9]]"},
		{"WITHIN", "mykey", "IDS", "OBJECT", poly8, "AND", "OBJECT", poly}, {"[0 [line3 poly8 poly9]]"},
		{"WITHIN", "mykey", "IDS", "GET", "mykey", "line3"}, {"[0 [line3]]"},
		{"WITHIN", "mykey", "IDS", "GET", "mykey", "poly8", "AND",
			"(", "OBJECT", poly, "AND", "GET", "mykey", "line3", ")"}, {"[0 [line3]]"},
		{"WITHIN", "mykey", "IDS", "GET", "mykey", "poly8", "AND",
			"(", "OBJECT", poly, "OR", "GET", "mykey", "line3", ")"}, {"[0 [line2 line3 poly8 poly9]]"},
		{"WITHIN", "mykey", "IDS", "GET", "mykey", "poly8", "AND",
			"(", "OBJECT", poly, "AND", "NOT", "GET", "mykey", "line3", ")"}, {"[0 [line2 poly8 poly9]]"},
		{"WITHIN", "mykey", "IDS", "NOT", "GET", "mykey", "line3"}, {"[0 [line2 poly8 poly9]]"},
		// errors
		{"WITHIN", "mykey", "IDS", "NOT", "GET", "mykey1", "line1"}, {"ERR key not found"},
		{"WITHIN", "mykey", "IDS", "NOT", "GET", "mykey", "line1"}, {"ERR id not found"},
	})
}


func expressions_INTERSECTS_test(mc *mockServer) error {
	poly := `{
				"type": "Polygon",
				"coordinates": [
					[
						[-122.44126439094543,37.732906137107],
						[-122.43980526924135,37.732906137107],
						[-122.43980526924135,37.73421283683962],
						[-122.44126439094543,37.73421283683962],
						[-122.44126439094543,37.732906137107]
					]
				]
			}`
	poly8 := `{"type":"Polygon","coordinates":[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]],[[-122.44060993194579,37.73345766902749],[-122.44044363498686,37.73345766902749],[-122.44044363498686,37.73355524732416],[-122.44060993194579,37.73355524732416],[-122.44060993194579,37.73345766902749]],[[-122.44060724973677,37.7336888869566],[-122.4402102828026,37.7336888869566],[-122.4402102828026,37.7339752567853],[-122.44060724973677,37.7339752567853],[-122.44060724973677,37.7336888869566]]]}`
	poly9 := `{"type": "Polygon","coordinates": [[[-122.44037926197052,37.73313523548048],[-122.44017541408539,37.73313523548048],[-122.44017541408539,37.73336857568778],[-122.44037926197052,37.73336857568778],[-122.44037926197052,37.73313523548048]]]}`
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "line2", "OBJECT", `{"type":"Feature","properties":{},"geometry":{"type":"LineString","coordinates":[[-122.44110345840454,37.733383424585185],[-122.44110614061356,37.734043136878604]]}}`}, {"OK"},
		{"SET", "mykey", "line3", "OBJECT", `{"type":"LineString","coordinates":[[-122.4408378,37.7341129],[-122.4408378,37.733]]}`}, {"OK"},
		{"SET", "mykey", "poly8", "OBJECT", poly8}, {"OK"},
		{"SET", "mykey", "poly9", "OBJECT", poly9}, {"OK"},

		{"INTERSECTS", "mykey", "IDS", "NOT", "OBJECT", poly}, {"[0 []]"},
		{"INTERSECTS", "mykey", "IDS", "NOT", "NOT", "OBJECT", poly}, {"[0 [line2 line3 poly8 poly9]]"},
		{"INTERSECTS", "mykey", "IDS", "NOT", "NOT", "NOT", "OBJECT", poly}, {"[0 []]"},

		{"INTERSECTS", "mykey", "IDS", "OBJECT", poly8, "OR", "OBJECT", poly}, {"[0 [line2 line3 poly8 poly9]]"},
		{"INTERSECTS", "mykey", "IDS", "GET", "mykey", "poly8", "OR", "OBJECT", poly}, {"[0 [line2 line3 poly8 poly9]]"},
		{"INTERSECTS", "mykey", "IDS", "OBJECT", poly8, "AND", "OBJECT", poly}, {"[0 [line3 poly8 poly9]]"},

		{"INTERSECTS", "mykey", "IDS", "GET", "mykey", "line3"}, {"[0 [line3 poly8]]"},
		{"INTERSECTS", "mykey", "IDS", "GET", "mykey", "poly8", "AND",
			"(", "OBJECT", poly, "AND", "GET", "mykey", "line3", ")"}, {"[0 [line3 poly8]]"},
		{"INTERSECTS", "mykey", "IDS", "GET", "mykey", "poly8", "AND",
			"(", "OBJECT", poly, "OR", "GET", "mykey", "line3", ")"}, {"[0 [line2 line3 poly8 poly9]]"},
		{"INTERSECTS", "mykey", "IDS", "GET", "mykey", "poly8", "AND",
			"(", "OBJECT", poly, "AND", "NOT", "GET", "mykey", "line3", ")"}, {"[0 [line2 poly9]]"},
		{"TEST", "OBJECT", poly9, "INTERSECTS", "NOT", "GET", "mykey", "line3"}, {"1"},
		{"INTERSECTS", "mykey", "IDS", "GET", "mykey", "line3",
			"OR", "OBJECT", poly8, "AND", "OBJECT", poly}, {"[0 [line2 line3 poly8 poly9]]"},
		{"INTERSECTS", "mykey", "IDS", "OBJECT", poly8, "AND", "OBJECT", poly,
			"OR", "GET", "mykey", "line3"}, {"[0 [line3 poly8 poly9]]"},
		{"INTERSECTS", "mykey", "IDS", "GET", "mykey", "line3", "OR",
			"(", "OBJECT", poly8, "AND", "OBJECT", poly, ")"}, {"[0 [line3 poly8 poly9]]"},
		{"INTERSECTS", "mykey", "IDS", "(", "GET", "mykey", "line3",
			"OR", "OBJECT", poly8, ")", "AND", "OBJECT", poly}, {"[0 [line2 line3 poly8 poly9]]"},
		// errors
		{"INTERSECTS", "mykey", "IDS", "NOT", "GET", "mykey1", "line1"}, {"ERR key not found"},
		{"INTERSECTS", "mykey", "IDS", "NOT", "GET", "mykey", "line1"}, {"ERR id not found"},
	})
}

func expressions_WITHIN_CIRCLE_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "1", "POINT", 37.7335, -122.4412}, {"OK"},
		{"SET", "mykey", "2", "POINT", 37.7335, -122.44121}, {"OK"},
		{"SET", "mykey", "3", "OBJECT", `{"type":"LineString","coordinates":[[-122.4408378,37.7341129],[-122.4408378,37.733]]}`}, {"OK"},
		{"SET", "mykey", "4", "OBJECT", `{"type":"Polygon","coordinates":[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]]]}`}, {"OK"},
		{"SET", "mykey", "5", "OBJECT", `{"type":"MultiPolygon","coordinates":[[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]]]]}`}, {"OK"},
		{"SET", "mykey", "6", "POINT", -5, 5}, {"OK"},
		{"SET", "mykey", "7", "POINT", 33, 21}, {"OK"},

		{"WITHIN", "mykey", "IDS", "CIRCLE", 37.7335, -122.4412, 1000}, {"[0 [1 2 3 4 5]]"},
		{"WITHIN", "mykey", "IDS", "CIRCLE", 37.7335, -122.4412, 10}, {"[0 [1 2]]"},
		{"WITHIN", "mykey", "IDS", "CIRCLE", 37.7335, -122.4412, 1000, "AND", "GET", "mykey", "3"}, {"[0 [3]]"},
		{"WITHIN", "mykey", "IDS", "CIRCLE", 37.7335, -122.4412, 10, "OR", "GET", "mykey", "3"}, {"[0 [1 2 3]]"},

	})
}

func expressions_INTERSECTS_CIRCLE_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "1", "POINT", 37.7335, -122.4412}, {"OK"},
		{"SET", "mykey", "2", "POINT", 37.7335, -122.44121}, {"OK"},
		{"SET", "mykey", "3", "OBJECT", `{"type":"LineString","coordinates":[[-122.4408378,37.7341129],[-122.4408378,37.733]]}`}, {"OK"},
		{"SET", "mykey", "4", "OBJECT", `{"type":"Polygon","coordinates":[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]]]}`}, {"OK"},
		{"SET", "mykey", "5", "OBJECT", `{"type":"MultiPolygon","coordinates":[[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]]]]}`}, {"OK"},
		{"SET", "mykey", "6", "POINT", -5, 5}, {"OK"},
		{"SET", "mykey", "7", "POINT", 33, 21}, {"OK"},

		{"INTERSECTS", "mykey", "IDS", "CIRCLE", 37.7335, -122.4412, 70}, {"[0 [1 2 3 4 5]]"},
		{"INTERSECTS", "mykey", "IDS", "CIRCLE", 37.7335, -122.4412, 10}, {"[0 [1 2]]"},
		{"INTERSECTS", "mykey", "IDS", "CIRCLE", 37.7335, -122.4412, 70,
			"AND", "(", "GET", "mykey", "3", "OR", "GET", "mykey", "7", ")"}, {"[0 [3 4 5 7]]"},
	})
}

func expressions_FIELDS_search_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "1", "FIELD", "field1", 10, "FIELD", "field2", 11 /* field3 undefined */, "OBJECT", `{"type":"Point","coordinates":[-112.2791,33.5220]}`}, {"OK"},
		{"SET", "mykey", "2", "FIELD", "field1", 20, "FIELD", "field2", 10 /* field3 undefined */, "OBJECT", `{"type":"Point","coordinates":[-112.2793,33.5222]}`}, {"OK"},
		{"SET", "mykey", "3", "FIELD", "field1", 30, "FIELD", "field2", 13 /* field3 undefined */, "OBJECT", `{"type":"Point","coordinates":[-112.2795,33.5224]}`}, {"OK"},
		{"SET", "mykey", "4", "FIELD", "field1", 40, "FIELD", "field2", 14 /* field3 undefined */, "OBJECT", `{"type":"Point","coordinates":[-112.2797,33.5226]}`}, {"OK"},
		{"SET", "mykey", "5" /* field1 undefined */, "FIELD", "field2", 15, "FIELD", "field3", 28, "OBJECT", `{"type":"Point","coordinates":[-112.2799,33.5228]}`}, {"OK"},
		{"SET", "mykey", "6" /* field1 & field2 undefined               */, "FIELD", "field3", 29, "OBJECT", `{"type":"Point","coordinates":[-112.2801,33.5230]}`}, {"OK"},
		{"SET", "mykey", "7" /* field1, field2, & field3 undefined                             */, "OBJECT", `{"type":"Point","coordinates":[-112.2803,33.5232]}`}, {"OK"},

		{"WITHIN", "mykey", "WHERE", "field2", 11, "+inf", "CIRCLE", 33.462, -112.268, 60000,
			"AND", "(", "GET", "mykey", "3", "OR", "GET", "mykey", "5", ")"}, {
			`[0 [` +
				`[3 {"type":"Point","coordinates":[-112.2795,33.5224]} [field1 30 field2 13]] ` +
				`[5 {"type":"Point","coordinates":[-112.2799,33.5228]} [field2 15 field3 28]]]]`},
		{"WITHIN", "mykey", "WHERE", "field2", 0, 2, "CIRCLE", 33.462, -112.268, 60000,"AND", "GET", "mykey", "6"}, {
			`[0 [[6 {"type":"Point","coordinates":[-112.2801,33.523]} [field3 29]]]]`},
	})
}
