package tests

// A5 reference values (computed with github.com/a5geo/a5-go):
//   point lon=13 lat=52  -> res 0:  1200000000000000
//                           res 5:  63fe000000000000
//                           res 10: 51575d8000000000
//   cell 51575d8000000000 center re-encoded at res 10 -> 51575d8000000000
//   point lon=0  lat=0   -> res 10: 4f05dc8000000000

func keys_A5_test(mc *mockServer) error {
	return mc.DoBatch(
		// --- GET encoding (like HASH) ---
		Do("SET", "mykey", "p", "POINT", 52, 13).OK(),
		Do("GET", "mykey", "p", "A5", "10").Str("51575d8000000000"),
		Do("GET", "mykey", "p", "A5", "5").Str("63fe000000000000"),
		Do("GET", "mykey", "p", "A5", "0").Str("1200000000000000"),
		Do("GET", "mykey", "p", "A5", "10").JSON().Str(`{"ok":true,"a5":"51575d8000000000"}`),
		// GET A5 argument validation
		Do("GET", "mykey", "p", "a5").Err("wrong number of arguments for 'get' command"),
		Do("GET", "mykey", "p", "a5", "-1").Err("invalid argument '-1'"),
		Do("GET", "mykey", "p", "a5", "31").Err("invalid argument '31'"),
		Do("GET", "mykey", "p", "a5", "nope").Err("invalid argument 'nope'"),

		// --- SET encoding: decode a cell to its center point (like SET HASH) ---
		Do("SET", "mykey", "q", "A5", "51575d8000000000").OK(),
		// round-trip: the stored center re-encodes to the same cell
		Do("GET", "mykey", "q", "A5", "10").Str("51575d8000000000"),
		Do("SET", "mykey", "q", "A5").Err("wrong number of arguments for 'set' command"),
		Do("SET", "mykey", "q", "A5", "xyz").Err("invalid argument 'xyz'"),

		// --- Output format (like HASHES) ---
		Do("SET", "outkey", "a", "POINT", 52, 13).OK(),
		Do("SCAN", "outkey", "A5", "10").Str(`[0 [[a 51575d8000000000]]]`),
		Do("SCAN", "outkey", "A5", "0").Str(`[0 [[a 1200000000000000]]]`),
		Do("NEARBY", "outkey", "LIMIT", 10, "A5", "10", "POINT", 52, 13, 100000).
			Str(`[0 [[a 51575d8000000000]]]`),
		// output resolution validation
		Do("SCAN", "outkey", "A5", "31").Err("invalid argument '31'"),
		Do("SCAN", "outkey", "A5").Err("wrong number of arguments for 'scan' command"),

		// --- Query area (like QUADKEY) ---
		Do("SET", "areakey", "in", "POINT", 52, 13).OK(),
		Do("SET", "areakey", "out", "POINT", 0, 0).OK(),
		Do("INTERSECTS", "areakey", "IDS", "A5", "51575d8000000000").Str(`[0 [in]]`),
		Do("WITHIN", "areakey", "IDS", "A5", "51575d8000000000").Str(`[0 [in]]`),
		Do("INTERSECTS", "areakey", "IDS", "A5", "nothex").Err("invalid argument 'nothex'"),
		Do("INTERSECTS", "areakey", "A5").Err("wrong number of arguments for 'intersects' command"),
		// A5 names both an output and a search area, so the area must still
		// resolve when no output is given.
		Do("INTERSECTS", "areakey", "A5", "51575d8000000000").
			Str(`[0 [[in {"type":"Point","coordinates":[13,52]}]]]`),
		Do("INTERSECTS", "areakey", "A5", "nothex").Err("invalid argument 'nothex'"),
		// ...and the output form still wins when a resolution follows.
		Do("INTERSECTS", "areakey", "A5", "10", "A5", "51575d8000000000").
			Str(`[0 [[in 51575d8000000000]]]`),

		// --- TEST command (like QUADKEY) ---
		Do("TEST", "POINT", 52, 13, "WITHIN", "A5", "51575d8000000000").Str("1"),
		Do("TEST", "POINT", 0, 0, "WITHIN", "A5", "51575d8000000000").Str("0"),
		Do("TEST", "POINT", 52, 13, "INTERSECTS", "A5", "51575d8000000000").Str("1"),
		Do("TEST", "A5", "51575d8000000000", "INTERSECTS", "POINT", 52, 13).Str("1"),
		Do("TEST", "GET", "areakey", "in", "WITHIN", "A5", "51575d8000000000").Str("1"),
		Do("TEST", "GET", "areakey", "out", "WITHIN", "A5", "51575d8000000000").Str("0"),
		// a pentagon isn't a rectangle, so it can't be clipped against
		Do("TEST", "POINT", 52, 13, "INTERSECTS", "CLIP", "A5", "51575d8000000000").
			Err("invalid clip type 'A5'"),

		// --- Fences (hooks and channels) ---
		Do("SETCHAN", "a5chan", "WITHIN", "areakey", "FENCE", "A5",
			"51575d8000000000").Str("1"),
		Do("SETCHAN", "a5chanout", "INTERSECTS", "areakey", "FENCE", "A5", "10",
			"A5", "51575d8000000000").Str("1"),
		Do("SETCHAN", "a5chandetect", "WITHIN", "areakey", "FENCE", "DETECT",
			"enter,exit", "A5", "51575d8000000000").Str("1"),
		Do("SETHOOK", "a5hook", "http://127.0.0.1:12345/", "WITHIN", "areakey",
			"FENCE", "A5", "51575d8000000000").Str("1"),
		Do("SETHOOK", "a5hookout", "http://127.0.0.1:12345/", "INTERSECTS",
			"areakey", "FENCE", "A5", "10", "A5", "51575d8000000000").Str("1"),
		// the stored command round-trips through CHANS/HOOKS unchanged
		Do("CHANS", "a5chan").JSON().Str(`{"ok":true,"chans":[{"name":"a5chan",`+
			`"key":"areakey","ttl":-1,"command":["WITHIN","areakey","FENCE",`+
			`"A5","51575d8000000000"],"meta":{}}]}`),
		Do("HOOKS", "a5hook").JSON().Str(`{"ok":true,"hooks":[{"name":"a5hook",`+
			`"key":"areakey","ttl":-1,"endpoints":["http://127.0.0.1:12345/"],`+
			`"command":["WITHIN","areakey","FENCE","A5","51575d8000000000"],`+
			`"meta":{}}]}`),
		// fence argument validation
		Do("SETCHAN", "a5bad", "WITHIN", "areakey", "FENCE", "A5", "nothex").
			Err("invalid argument 'nothex'"),
		Do("SETCHAN", "a5bad", "WITHIN", "areakey", "FENCE", "A5").
			Err("wrong number of arguments for 'setchan' command"),
		Do("SETHOOK", "a5bad", "http://127.0.0.1:12345/", "WITHIN", "areakey",
			"FENCE", "A5", "nothex").Err("invalid argument 'nothex'"),
		// NEARBY only takes POINT areas, so A5 stays an output there
		Do("SETCHAN", "a5bad", "NEARBY", "areakey", "FENCE", "A5",
			"51575d8000000000").Err("invalid argument '51575d8000000000'"),
		Do("DELCHAN", "a5chan").Str("1"),
		Do("DELCHAN", "a5chanout").Str("1"),
		Do("DELCHAN", "a5chandetect").Str("1"),
		Do("DELHOOK", "a5hook").Str("1"),
		Do("DELHOOK", "a5hookout").Str("1"),
	)
}

// keys_A5_fence_reload_test replays an AOF holding A5 fences. The stored
// command is re-parsed on load, so a fence that only parses interactively
// would take the server down on the next restart.
func keys_A5_fence_reload_test(mc *mockServer) error {
	mc2, err := loadAOF("" +
		"SET fleet truck POINT 52 13\r\n" +
		"SETCHAN a5chan WITHIN fleet FENCE A5 51575d8000000000\r\n" +
		"SETHOOK a5hook http://127.0.0.1:12345/ INTERSECTS fleet FENCE " +
		"A5 10 A5 51575d8000000000\r\n")
	if mc2 != nil {
		defer mc2.Close()
	}
	if err != nil {
		return err
	}
	return mc2.DoBatch(
		Do("CHANS", "*").JSON().Str(`{"ok":true,"chans":[{"name":"a5chan",`+
			`"key":"fleet","ttl":-1,"command":["WITHIN","fleet","FENCE","A5",`+
			`"51575d8000000000"],"meta":{}}]}`),
		Do("HOOKS", "*").JSON().Str(`{"ok":true,"hooks":[{"name":"a5hook",`+
			`"key":"fleet","ttl":-1,"endpoints":["http://127.0.0.1:12345/"],`+
			`"command":["INTERSECTS","fleet","FENCE","A5","10","A5",`+
			`"51575d8000000000"],"meta":{}}]}`),
		// the reloaded fence still matches
		Do("INTERSECTS", "fleet", "IDS", "A5", "51575d8000000000").Str(`[0 [truck]]`),
	)
}
