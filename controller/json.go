package controller

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/resp"
	"github.com/tidwall/sjson"
	"github.com/tidwall/tile38/controller/collection"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/geojson"
)

func appendJSONString(b []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		if s[i] < ' ' || s[i] == '\\' || s[i] == '"' || s[i] > 126 {
			d, _ := json.Marshal(s)
			return append(b, string(d)...)
		}
	}
	b = append(b, '"')
	b = append(b, s...)
	b = append(b, '"')
	return b
}

func jsonString(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] < ' ' || s[i] == '\\' || s[i] == '"' || s[i] > 126 {
			d, _ := json.Marshal(s)
			return string(d)
		}
	}
	b := make([]byte, len(s)+2)
	b[0] = '"'
	copy(b[1:], s)
	b[len(b)-1] = '"'
	return string(b)
}

func (c *Controller) cmdJget(msg *server.Message) (resp.Value, error) {
	start := time.Now()
	empty_response := resp.SimpleStringValue("")
	if len(msg.Values) < 3 {
		return empty_response, errInvalidNumberOfArguments
	}
	if len(msg.Values) > 5 {
		return empty_response, errInvalidNumberOfArguments
	}
	key := msg.Values[1].String()
	id := msg.Values[2].String()
	var doget bool
	var path string
	var raw bool
	if len(msg.Values) > 3 {
		doget = true
		path = msg.Values[3].String()
		if len(msg.Values) == 5 {
			if strings.ToLower(msg.Values[4].String()) == "raw" {
				raw = true
			} else {
				return empty_response, errInvalidArgument(msg.Values[4].String())
			}
		}
	}
	col := c.getCol(key)
	if col == nil {
		if msg.OutputType == server.RESP {
			return resp.NullValue(), nil
		}
		return empty_response, errKeyNotFound
	}
	o, _, ok := col.Get(id)
	if !ok {
		if msg.OutputType == server.RESP {
			return resp.NullValue(), nil
		}
		return empty_response, errIDNotFound
	}
	var res gjson.Result
	if doget {
		res = gjson.Get(o.String(), path)
	} else {
		res = gjson.Parse(o.String())
	}
	var val string
	if raw {
		val = res.Raw
	} else {
		val = res.String()
	}
	var buf bytes.Buffer
	if msg.OutputType == server.JSON {
		buf.WriteString(`{"ok":true`)
	}
	switch msg.OutputType {
	case server.JSON:
		if res.Exists() {
			buf.WriteString(`,"value":` + jsonString(val))
		}
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case server.RESP:
		if !res.Exists() {
			return resp.NullValue(), nil
		}
		return resp.StringValue(val), nil
	}
	return empty_response, nil
}

func (c *Controller) cmdJset(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	// JSET key path value [RAW]
	start := time.Now()
	empty_response := resp.SimpleStringValue("")
	var raw, str bool
	switch len(msg.Values) {
	default:
		return empty_response, d, errInvalidNumberOfArguments
	case 5:
	case 6:
		switch strings.ToLower(msg.Values[5].String()) {
		default:
			return empty_response, d, errInvalidArgument(msg.Values[5].String())
		case "raw":
			raw = true
		case "str":
			str = true
		}
	}

	key := msg.Values[1].String()
	id := msg.Values[2].String()
	path := msg.Values[3].String()
	val := msg.Values[4].String()
	if !str && !raw {
		switch val {
		default:
			if len(val) > 0 {
				if (val[0] >= '0' && val[0] <= '9') || val[0] == '-' {
					if _, err := strconv.ParseFloat(val, 64); err == nil {
						raw = true
					}
				}
			}
		case "true", "false", "null":
			raw = true
		}
	}
	col := c.getCol(key)
	var createcol bool
	if col == nil {
		col = collection.New()
		createcol = true
	}
	var json string
	var geoobj bool
	o, _, ok := col.Get(id)
	if ok {
		if _, ok := o.(geojson.String); !ok {
			geoobj = true
		}
		json = o.String()
	}
	if raw {
		// set as raw block
		json, err = sjson.SetRaw(json, path, val)
	} else {
		// set as a string
		json, err = sjson.Set(json, path, val)
	}
	if err != nil {
		return empty_response, d, err
	}

	if geoobj {
		nmsg := *msg
		nmsg.Values = []resp.Value{
			resp.StringValue("SET"),
			resp.StringValue(key),
			resp.StringValue(id),
			resp.StringValue("OBJECT"),
			resp.StringValue(json),
		}
		// SET key id OBJECT json
		return c.cmdSet(&nmsg)
	}
	if createcol {
		c.setCol(key, col)
	}

	d.key = key
	d.id = id
	d.obj = geojson.String(json)
	d.timestamp = time.Now()
	d.updated = true

	c.clearIDExpires(key, id)
	col.ReplaceOrInsert(d.id, d.obj, nil, nil)
	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), d, nil
	case server.RESP:
		return resp.SimpleStringValue("OK"), d, nil
	}
	return empty_response, d, nil
}

func (c *Controller) cmdJdel(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	start := time.Now()
	empty_response := resp.SimpleStringValue("")
	if len(msg.Values) != 4 {
		return empty_response, d, errInvalidNumberOfArguments
	}
	key := msg.Values[1].String()
	id := msg.Values[2].String()
	path := msg.Values[3].String()

	col := c.getCol(key)
	if col == nil {
		if msg.OutputType == server.RESP {
			return resp.IntegerValue(0), d, nil
		}
		return empty_response, d, errKeyNotFound
	}

	var json string
	var geoobj bool
	o, _, ok := col.Get(id)
	if ok {
		if _, ok := o.(geojson.String); !ok {
			geoobj = true
		}
		json = o.String()
	}
	njson, err := sjson.Delete(json, path)
	if err != nil {
		return empty_response, d, err
	}
	if njson == json {
		switch msg.OutputType {
		case server.JSON:
			return empty_response, d, errPathNotFound
		case server.RESP:
			return resp.IntegerValue(0), d, nil
		}
		return empty_response, d, nil
	}
	json = njson
	if geoobj {
		nmsg := *msg
		nmsg.Values = []resp.Value{
			resp.StringValue("SET"),
			resp.StringValue(key),
			resp.StringValue(id),
			resp.StringValue("OBJECT"),
			resp.StringValue(json),
		}
		// SET key id OBJECT json
		return c.cmdSet(&nmsg)
	}

	d.key = key
	d.id = id
	d.obj = geojson.String(json)
	d.timestamp = time.Now()
	d.updated = true

	c.clearIDExpires(d.key, d.id)
	col.ReplaceOrInsert(d.id, d.obj, nil, nil)
	switch msg.OutputType {
	case server.JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), d, nil
	case server.RESP:
		return resp.IntegerValue(1), d, nil
	}
	return empty_response, d, nil
}
