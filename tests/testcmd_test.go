package tests

func subTestTestCmd(g *testGroup) {
	g.regSubTest("WITHIN", testcmd_WITHIN_test)
	g.regSubTest("INTERSECTS", testcmd_INTERSECTS_test)
	g.regSubTest("INTERSECTS_CLIP", testcmd_INTERSECTS_CLIP_test)
	g.regSubTest("ExpressionErrors", testcmd_expressionErrors_test)
	g.regSubTest("Expressions", testcmd_expression_test)
}

func testcmd_WITHIN_test(mc *mockServer) error {
	poly := `{
				"type": "Polygon",
				"coordinates": [
					[
						[-122.44126439094543,37.72906137107],
						[-122.43980526924135,37.72906137107],
						[-122.43980526924135,37.73421283683962],
						[-122.44126439094543,37.73421283683962],
						[-122.44126439094543,37.72906137107]
					]
				]
			}`
	poly8 := `{"type":"Polygon","coordinates":[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]],[[-122.44060993194579,37.73345766902749],[-122.44044363498686,37.73345766902749],[-122.44044363498686,37.73355524732416],[-122.44060993194579,37.73355524732416],[-122.44060993194579,37.73345766902749]],[[-122.44060724973677,37.7336888869566],[-122.4402102828026,37.7336888869566],[-122.4402102828026,37.7339752567853],[-122.44060724973677,37.7339752567853],[-122.44060724973677,37.7336888869566]]]}`
	poly9 := `{"type":"Polygon","coordinates":[[[-122.44037926197052,37.73313523548048],[-122.44017541408539,37.73313523548048],[-122.44017541408539,37.73336857568778],[-122.44037926197052,37.73336857568778],[-122.44037926197052,37.73313523548048]]]}`
	poly10 := `{"type":"Polygon","coordinates":[[[-122.44040071964262,37.73359343010089],[-122.4402666091919,37.73359343010089],[-122.4402666091919,37.73373767596864],[-122.44040071964262,37.73373767596864],[-122.44040071964262,37.73359343010089]]]}`

	return mc.DoBatch(
		Do("SET", "mykey", "point1", "POINT", 37.7335, -122.4412).OK(),
		Do("SET", "mykey", "point2", "POINT", 37.7335, -122.44121).OK(),
		Do("SET", "mykey", "line3", "OBJECT", `{"type":"LineString","coordinates":[[-122.4408378,37.7341129],[-122.4408378,37.733]]}`).OK(),
		Do("SET", "mykey", "poly4", "OBJECT", `{"type":"Polygon","coordinates":[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]]]}`).OK(),
		Do("SET", "mykey", "multipoly5", "OBJECT", `{"type":"MultiPolygon","coordinates":[[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]]],[[[-122.44091033935547,37.731981251280985],[-122.43994474411011,37.731981251280985],[-122.43994474411011,37.73254976045042],[-122.44091033935547,37.73254976045042],[-122.44091033935547,37.731981251280985]]]]}`).OK(),
		Do("SET", "mykey", "point6", "POINT", -5, 5).OK(),
		Do("SET", "mykey", "point7", "POINT", 33, 21).OK(),
		Do("SET", "mykey", "poly8", "OBJECT", poly8).OK(),

		Do("TEST", "GET", "mykey", "point1", "WITHIN", "OBJECT", poly).Str("1"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "SECTOR", "37.72999", "-122.44760", "1000", "0", "90").Str("1"),
		Do("TEST", "GET", "mykey", "line3", "WITHIN", "OBJECT", poly).Str("1"),
		Do("TEST", "GET", "mykey", "poly4", "WITHIN", "OBJECT", poly).Str("1"),
		Do("TEST", "GET", "mykey", "multipoly5", "WITHIN", "OBJECT", poly).Str("1"),
		Do("TEST", "GET", "mykey", "poly8", "WITHIN", "OBJECT", poly).Str("1"),
		Do("TEST", "GET", "mykey", "poly8", "WITHIN", "SECTOR", "37.72999", "-122.44760", "1000", "0", "90").Str("1"),
		Do("TEST", "GET", "mykey", "poly8", "WITHIN", "SECTOR", "37.72999", "-122.44760", "1000", "0", "90").JSON().Str(`{"ok":true,"result":true}`),

		Do("TEST", "GET", "mykey", "point6", "WITHIN", "OBJECT", poly).Str("0"),
		Do("TEST", "GET", "mykey", "point6", "WITHIN", "OBJECT", poly).JSON().Str(`{"ok":true,"result":false}`),
		Do("TEST", "GET", "mykey", "point7", "WITHIN", "OBJECT", poly).Str("0"),

		Do("TEST", "OBJECT", poly9, "WITHIN", "OBJECT", poly8).Str("1"),
		Do("TEST", "OBJECT", poly10, "WITHIN", "OBJECT", poly8).Str("0"),

		Do("TEST", "GET", "mykey", "point1", "WITHIN", "SECTOR", "37.72999", "-122.44760", "1000", "0", "ff").Err("invalid argument 'ff'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "SECTOR", "37.72999", "-122.44760", "1000", "ee", "ff").Err("invalid argument 'ee'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "SECTOR", "37.72999", "-122.44760", "dd", "ee", "ff").Err("invalid argument 'dd'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "SECTOR", "37.72999", "cc", "dd", "ee", "ff").Err("invalid argument 'cc'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "SECTOR", "bb", "cc", "dd", "ee", "ff").Err("invalid argument 'bb'"),

		Do("TEST", "GET", "mykey", "point1", "WITHIN", "SECTOR", "37.72999", "-122.44760", "1000", "0").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "SECTOR", "37.72999", "-122.44760", "1000").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "SECTOR", "37.72999", "-122.44760").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "SECTOR", "37.72999").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "SECTOR").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "SECTOR", "37.72999", "-122.44760", "1000", "1", "1").Err("equal bearings (1 == 1), use CIRCLE instead"),

		Do("TEST", "GET", "mykey", "point1", "WITHIN", "CIRCLE", "37.72999", "-122.44760", "10000").Str("1"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "CIRCLE", "37.72999", "-122.44760", "10000", "10000").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "CIRCLE", "37.72999", "-122.44760").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "CIRCLE", "37.72999").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "CIRCLE").Err("wrong number of arguments for 'test' command"),

		Do("TEST", "GET", "mykey", "point1", "WITHIN", "CIRCLE", "37.72999", "-122.44760", "cc").Err("invalid argument 'cc'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "CIRCLE", "37.72999", "bb", "cc").Err("invalid argument 'bb'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "CIRCLE", "aa", "bb", "cc").Err("invalid argument 'aa'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "CIRCLE", "37.72999", "-122.44760", "-10000").Err("invalid argument '-10000'"),

		Do("TEST", "GET", "mykey", "point1", "WITHIN", "hash").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "hash", "123").Str("0"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "hash", "123", "asdf").Err("wrong number of arguments for 'test' command"),

		Do("TEST", "GET", "mykey", "point1", "WITHIN", "quadkey").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "quadkey", "123").Str("0"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "quadkey", "pqowie").Err("invalid argument 'pqowie'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "quadkey", "123", "asdf").Err("wrong number of arguments for 'test' command"),

		Do("TEST", "GET", "mykey", "point1", "WITHIN", "a5").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "a5", "51575d8000000000").Str("0"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "a5", "pqowie").Err("invalid argument 'pqowie'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "a5", "51575d8000000000", "asdf").Err("wrong number of arguments for 'test' command"),

		Do("TEST", "GET", "mykey", "point1", "WITHIN", "tile").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "tile", "1").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "tile", "1", "2").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "tile", "1", "2", "3").Str("0"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "tile", "1", "2", "cc").Err("invalid argument 'cc'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "tile", "1", "bb", "cc").Err("invalid argument 'bb'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "tile", "aa", "bb", "cc").Err("invalid argument 'aa'"),

		Do("TEST", "GET", "mykey", "point1", "WITHIN", "point", "1", "2").Str("0"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "point").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "point", "1").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "point", "1", "2", "3").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "point", "1", "bb").Err("invalid argument 'bb'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "point", "aa", "bb").Err("invalid argument 'aa'"),

		Do("TEST", "GET", "mykey", "point1", "WITHIN").Err("wrong number of arguments for 'test' command"),

		Do("TEST", "GET", "mykey", "point1", "WITHIN", "bounds", "1", "2", "3", "4").Str("0"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "bounds", "1", "2", "3", "4", "5").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "bounds", "1", "2", "3").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "bounds", "1", "2").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "bounds", "1").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "bounds").Err("wrong number of arguments for 'test' command"),

		Do("TEST", "GET", "mykey", "point1", "WITHIN", "bounds", "1", "2", "3", "dd").Err("invalid argument 'dd'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "bounds", "1", "2", "cc", "dd").Err("invalid argument 'cc'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "bounds", "1", "bb", "cc", "dd").Err("invalid argument 'bb'"),
		Do("TEST", "GET", "mykey", "point1", "WITHIN", "bounds", "aa", "bb", "cc", "dd").Err("invalid argument 'aa'"),

		Do("TEST", "GET", "mykey", "point6", "WITHIN", "GET").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point6", "WITHIN", "GET", "mykey").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "point6", "WITHIN", "GET", "mykey", "point6").Str("1"),
		Do("TEST", "GET", "mykey", "point6", "WITHIN", "GET", "mykey__", "point6").Err("key not found"),
		Do("TEST", "GET", "mykey", "point6", "WITHIN", "GET", "mykey", "point6__").Err("id not found"),
	)
}

func testcmd_INTERSECTS_test(mc *mockServer) error {
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
	poly10 := `{"type": "Polygon","coordinates": [[[-122.44040071964262,37.73359343010089],[-122.4402666091919,37.73359343010089],[-122.4402666091919,37.73373767596864],[-122.44040071964262,37.73373767596864],[-122.44040071964262,37.73359343010089]]]}`
	poly101 := `{"type":"Polygon","coordinates":[[[-122.44051605463028,37.73375464605226],[-122.44028002023695,37.73375464605226],[-122.44028002023695,37.733903134117966],[-122.44051605463028,37.733903134117966],[-122.44051605463028,37.73375464605226]]]}`

	return mc.DoBatch(
		Do("SET", "mykey", "point1", "POINT", 37.7335, -122.4412).OK(),
		Do("SET", "mykey", "point2", "POINT", 37.7335, -122.44121).OK(),
		Do("SET", "mykey", "line3", "OBJECT", `{"type":"LineString","coordinates":[[-122.4408378,37.7341129],[-122.4408378,37.733]]}`).OK(),
		Do("SET", "mykey", "poly4", "OBJECT", `{"type":"Polygon","coordinates":[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]]]}`).OK(),
		Do("SET", "mykey", "multipoly5", "OBJECT", `{"type":"MultiPolygon","coordinates":[[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]]],[[[-122.44091033935547,37.731981251280985],[-122.43994474411011,37.731981251280985],[-122.43994474411011,37.73254976045042],[-122.44091033935547,37.73254976045042],[-122.44091033935547,37.731981251280985]]]]}`).OK(),
		Do("SET", "mykey", "point6", "POINT", -5, 5).OK(),
		Do("SET", "mykey", "point7", "POINT", 33, 21).OK(),
		Do("SET", "mykey", "poly8", "OBJECT", poly8).OK(),

		Do("TEST", "GET", "mykey", "point1", "INTERSECTS", "OBJECT", poly).Str("1"),
		Do("TEST", "GET", "mykey", "point2", "INTERSECTS", "OBJECT", poly).Str("1"),
		Do("TEST", "GET", "mykey", "line3", "INTERSECTS", "OBJECT", poly).Str("1"),
		Do("TEST", "GET", "mykey", "poly4", "INTERSECTS", "OBJECT", poly).Str("1"),
		Do("TEST", "GET", "mykey", "multipoly5", "INTERSECTS", "OBJECT", poly).Str("1"),
		Do("TEST", "GET", "mykey", "poly8", "INTERSECTS", "OBJECT", poly).Str("1"),

		Do("TEST", "GET", "mykey", "point6", "INTERSECTS", "OBJECT", poly).Str("0"),
		Do("TEST", "GET", "mykey", "point7", "INTERSECTS", "OBJECT", poly).Str("0"),

		Do("TEST", "OBJECT", poly9, "INTERSECTS", "OBJECT", poly8).Str("1"),
		Do("TEST", "OBJECT", poly10, "INTERSECTS", "OBJECT", poly8).Str("1"),
		Do("TEST", "OBJECT", poly101, "INTERSECTS", "OBJECT", poly8).Str("0"),
	)
}

func testcmd_INTERSECTS_CLIP_test(mc *mockServer) error {
	poly8 := `{"type":"Polygon","coordinates":[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]],[[-122.44060993194579,37.73345766902749],[-122.44044363498686,37.73345766902749],[-122.44044363498686,37.73355524732416],[-122.44060993194579,37.73355524732416],[-122.44060993194579,37.73345766902749]],[[-122.44060724973677,37.7336888869566],[-122.4402102828026,37.7336888869566],[-122.4402102828026,37.7339752567853],[-122.44060724973677,37.7339752567853],[-122.44060724973677,37.7336888869566]]]}`
	poly9 := `{"type":"Polygon","coordinates":[[[-122.44037926197052,37.73313523548048],[-122.44017541408539,37.73313523548048],[-122.44017541408539,37.73336857568778],[-122.44037926197052,37.73336857568778],[-122.44037926197052,37.73313523548048]]]}`
	multipoly5 := `{"type":"MultiPolygon","coordinates":[[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]]],[[[-122.44091033935547,37.731981251280985],[-122.43994474411011,37.731981251280985],[-122.43994474411011,37.73254976045042],[-122.44091033935547,37.73254976045042],[-122.44091033935547,37.731981251280985]]]]}`
	poly101 := `{"type":"Polygon","coordinates":[[[-122.44051605463028,37.73375464605226],[-122.44028002023695,37.73375464605226],[-122.44028002023695,37.733903134117966],[-122.44051605463028,37.733903134117966],[-122.44051605463028,37.73375464605226]]]}`

	return mc.DoBatch(
		Do("SET", "mykey", "point1", "POINT", 37.7335, -122.4412).OK(),

		Do("TEST", "OBJECT", poly9, "INTERSECTS", "CLIP", "OBJECT", "{}").Err("invalid clip type 'OBJECT'"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "CLIP", "CIRCLE", "1", "2", "3").Err("invalid clip type 'CIRCLE'"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "CLIP", "GET", "mykey", "point1").Err("invalid clip type 'GET'"),
		Do("TEST", "OBJECT", poly9, "WITHIN", "CLIP", "BOUNDS", 10, 10, 20, 20).Err("invalid argument 'CLIP'"),

		Do("TEST", "OBJECT", poly9, "INTERSECTS", "CLIP", "SECTOR").Err("invalid clip type 'SECTOR'"),

		Do("TEST", "OBJECT", poly9, "INTERSECTS", "CLIP", "BOUNDS", 37.732906137107, -122.44126439094543, 37.73421283683962, -122.43980526924135).Str("[1 "+poly9+"]"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "CLIP", "BOUNDS", 37.732906137107, -122.44126439094543, 37.73421283683962, -122.43980526924135).JSON().Str(`{"ok":true,"result":true,"object":`+poly9+`}`),
		Do("TEST", "OBJECT", poly8, "INTERSECTS", "CLIP", "BOUNDS", 37.733, -122.4408378, 37.7341129, -122.44).Str("[1 "+poly8+"]"),
		Do("TEST", "OBJECT", multipoly5, "INTERSECTS", "CLIP", "BOUNDS", 37.73227823422744, -122.44120001792908, 37.73319038868677, -122.43955314159392).Str("[1 "+`{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[-122.4408378,37.73319038868677],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.73319038868677],[-122.4408378,37.73319038868677]]]},"properties":{}},{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[-122.44091033935547,37.73227823422744],[-122.43994474411011,37.73227823422744],[-122.43994474411011,37.73254976045042],[-122.44091033935547,37.73254976045042],[-122.44091033935547,37.73227823422744]]]},"properties":{}}]}`+"]"),
		Do("TEST", "OBJECT", poly101, "INTERSECTS", "CLIP", "BOUNDS", 37.73315644825698, -122.44054287672043, 37.73349585185455, -122.44008690118788).Str("0"),
	)
}

func testcmd_expressionErrors_test(mc *mockServer) error {
	return mc.DoBatch(
		Do("SET", "mykey", "foo", "OBJECT", `{"type":"LineString","coordinates":[[-122.4408378,37.7341129],[-122.4408378,37.733]]}`).OK(),
		Do("SET", "mykey", "bar", "OBJECT", `{"type":"LineString","coordinates":[[-122.4408378,37.7341129],[-122.4408378,37.733]]}`).OK(),
		Do("SET", "mykey", "baz", "OBJECT", `{"type":"LineString","coordinates":[[-122.4408378,37.7341129],[-122.4408378,37.733]]}`).OK(),

		Do("TEST", "GET", "mykey", "foo", "INTERSECTS", "(", "GET", "mykey", "bar").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "foo", "INTERSECTS", "GET", "mykey", "bar", ")").Err("invalid argument ')'"),

		Do("TEST", "GET", "mykey", "foo", "INTERSECTS", "OR", "GET", "mykey", "bar").Err("invalid argument 'or'"),
		Do("TEST", "GET", "mykey", "foo", "INTERSECTS", "AND", "GET", "mykey", "bar").Err("invalid argument 'and'"),
		Do("TEST", "GET", "mykey", "foo", "INTERSECTS", "GET", "mykey", "bar", "OR", "AND", "GET", "mykey", "baz").Err("invalid argument 'and'"),
		Do("TEST", "GET", "mykey", "foo", "INTERSECTS", "GET", "mykey", "bar", "AND", "OR", "GET", "mykey", "baz").Err("invalid argument 'or'"),
		Do("TEST", "GET", "mykey", "foo", "INTERSECTS", "GET", "mykey", "bar", "OR", "OR", "GET", "mykey", "baz").Err("invalid argument 'or'"),
		Do("TEST", "GET", "mykey", "foo", "INTERSECTS", "GET", "mykey", "bar", "AND", "AND", "GET", "mykey", "baz").Err("invalid argument 'and'"),
		Do("TEST", "GET", "mykey", "foo", "INTERSECTS", "GET", "mykey", "bar", "OR").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "foo", "INTERSECTS", "GET", "mykey", "bar", "AND").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "foo", "INTERSECTS", "GET", "mykey", "bar", "NOT").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "foo", "INTERSECTS", "GET", "mykey", "bar", "NOT", "AND", "GET", "mykey", "baz").Err("invalid argument 'and'"),

		Do("TEST").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "foo").Err("wrong number of arguments for 'test' command"),
		Do("TEST", "GET", "mykey", "foo", "jello").Err("invalid argument 'jello'"),
	)
}

func testcmd_expression_test(mc *mockServer) error {
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

	return mc.DoBatch(
		Do("SET", "mykey", "line3", "OBJECT", `{"type":"LineString","coordinates":[[-122.4408378,37.7341129],[-122.4408378,37.733]]}`).OK(),
		Do("SET", "mykey", "poly8", "OBJECT", poly8).OK(),

		Do("TEST", "OBJECT", poly9, "INTERSECTS", "NOT", "OBJECT", poly).Str("0"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "NOT", "NOT", "OBJECT", poly).Str("1"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "NOT", "NOT", "NOT", "OBJECT", poly).Str("0"),

		Do("TEST", "OBJECT", poly9, "INTERSECTS", "OBJECT", poly8, "OR", "OBJECT", poly).Str("1"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "OBJECT", poly8, "AND", "OBJECT", poly).Str("1"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "GET", "mykey", "poly8", "OR", "OBJECT", poly).Str("1"),

		Do("TEST", "OBJECT", poly9, "INTERSECTS", "GET", "mykey", "line3").Str("0"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "GET", "mykey", "poly8", "AND", "(", "OBJECT", poly, "AND", "GET", "mykey", "line3", ")").Str("0"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "GET", "mykey", "poly8", "AND", "(", "OBJECT", poly, "OR", "GET", "mykey", "line3", ")").Str("1"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "GET", "mykey", "poly8", "AND", "(", "OBJECT", poly, "AND", "NOT", "GET", "mykey", "line3", ")").Str("1"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "NOT", "GET", "mykey", "line3").Str("1"),
		Do("TEST", "NOT", "OBJECT", poly9, "INTERSECTS", "GET", "mykey", "line3").Str("1"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "GET", "mykey", "line3", "OR", "OBJECT", poly8, "AND", "OBJECT", poly).Str("1"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "OBJECT", poly8, "AND", "OBJECT", poly, "OR", "GET", "mykey", "line3").Str("1"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "GET", "mykey", "line3", "OR", "(", "OBJECT", poly8, "AND", "OBJECT", poly, ")").Str("1"),
		Do("TEST", "OBJECT", poly9, "INTERSECTS", "(", "GET", "mykey", "line3", "OR", "OBJECT", poly8, ")", "AND", "OBJECT", poly).Str("1"),

		Do("TEST", "OBJECT", poly9, "WITHIN", "OBJECT", poly8, "OR", "OBJECT", poly).Str("1"),
		Do("TEST", "OBJECT", poly9, "WITHIN", "OBJECT", poly8, "AND", "OBJECT", poly).Str("1"),

		Do("TEST", "OBJECT", poly9, "WITHIN", "GET", "mykey", "line3").Str("0"),
		Do("TEST", "OBJECT", poly9, "WITHIN", "GET", "mykey", "poly8", "AND", "(", "OBJECT", poly, "AND", "GET", "mykey", "line3", ")").Str("0"),
		Do("TEST", "OBJECT", poly9, "WITHIN", "GET", "mykey", "poly8", "AND", "(", "OBJECT", poly, "OR", "GET", "mykey", "line3", ")").Str("1"),
		Do("TEST", "OBJECT", poly9, "WITHIN", "GET", "mykey", "poly8", "AND", "(", "OBJECT", poly, "AND", "NOT", "GET", "mykey", "line3", ")").Str("1"),
		Do("TEST", "OBJECT", poly9, "WITHIN", "NOT", "GET", "mykey", "line3").Str("1"),
	)
}
