package server

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/mmcloughlin/geohash"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/glob"
)

type fvt struct {
	field string
	value float64
}

func orderFields(fmap map[string]int, farr []string, fields []float64) []fvt {
	var fv fvt
	var idx int
	fvs := make([]fvt, 0, len(fmap))
	for _, field := range farr {
		idx = fmap[field]
		if idx < len(fields) {
			fv.field = field
			fv.value = fields[idx]
			if fv.value != 0 {
				fvs = append(fvs, fv)
			}
		}
	}
	return fvs
}
func (s *Server) cmdBounds(msg *Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Args[1:]

	var ok bool
	var key string
	if vs, key, ok = tokenval(vs); !ok || key == "" {
		return NOMessage, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return NOMessage, errInvalidNumberOfArguments
	}

	col, _ := s.cols.Get(key)
	if col == nil {
		if msg.OutputType == RESP {
			return resp.NullValue(), nil
		}
		return NOMessage, errKeyNotFound
	}

	vals := make([]resp.Value, 0, 2)
	var buf bytes.Buffer
	if msg.OutputType == JSON {
		buf.WriteString(`{"ok":true`)
	}
	minX, minY, maxX, maxY := col.Bounds()

	bbox := geojson.NewRect(geometry.Rect{
		Min: geometry.Point{X: minX, Y: minY},
		Max: geometry.Point{X: maxX, Y: maxY},
	})
	if msg.OutputType == JSON {
		buf.WriteString(`,"bounds":`)
		buf.WriteString(string(bbox.AppendJSON(nil)))
	} else {
		vals = append(vals, resp.ArrayValue([]resp.Value{
			resp.ArrayValue([]resp.Value{
				resp.FloatValue(minX),
				resp.FloatValue(minY),
			}),
			resp.ArrayValue([]resp.Value{
				resp.FloatValue(maxX),
				resp.FloatValue(maxY),
			}),
		}))
	}
	switch msg.OutputType {
	case JSON:
		buf.WriteString(`,"elapsed":"` + time.Since(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case RESP:
		return vals[0], nil
	}
	return NOMessage, nil
}

func (s *Server) cmdType(msg *Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Args[1:]

	var ok bool
	var key string
	if _, key, ok = tokenval(vs); !ok || key == "" {
		return NOMessage, errInvalidNumberOfArguments
	}

	col, _ := s.cols.Get(key)
	if col == nil {
		if msg.OutputType == RESP {
			return resp.SimpleStringValue("none"), nil
		}
		return NOMessage, errKeyNotFound
	}

	typ := "hash"

	switch msg.OutputType {
	case JSON:
		return resp.StringValue(`{"ok":true,"type":` + string(typ) + `,"elapsed":"` + time.Since(start).String() + "\"}"), nil
	case RESP:
		return resp.SimpleStringValue(typ), nil
	}
	return NOMessage, nil
}

func (s *Server) cmdGet(msg *Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Args[1:]

	var ok bool
	var key, id, typ, sprecision string
	if vs, key, ok = tokenval(vs); !ok || key == "" {
		return NOMessage, errInvalidNumberOfArguments
	}
	if vs, id, ok = tokenval(vs); !ok || id == "" {
		return NOMessage, errInvalidNumberOfArguments
	}

	withfields := false
	if _, peek, ok := tokenval(vs); ok && strings.ToLower(peek) == "withfields" {
		withfields = true
		vs = vs[1:]
	}

	col, _ := s.cols.Get(key)
	if col == nil {
		if msg.OutputType == RESP {
			return resp.NullValue(), nil
		}
		return NOMessage, errKeyNotFound
	}
	o, fields, _, ok := col.Get(id)
	if !ok {
		if msg.OutputType == RESP {
			return resp.NullValue(), nil
		}
		return NOMessage, errIDNotFound
	}

	vals := make([]resp.Value, 0, 2)
	var buf bytes.Buffer
	if msg.OutputType == JSON {
		buf.WriteString(`{"ok":true`)
	}
	vs, typ, ok = tokenval(vs)
	typ = strings.ToLower(typ)
	if !ok {
		typ = "object"
	}
	switch typ {
	default:
		return NOMessage, errInvalidArgument(typ)
	case "object":
		if msg.OutputType == JSON {
			buf.WriteString(`,"object":`)
			buf.WriteString(string(o.AppendJSON(nil)))
		} else {
			vals = append(vals, resp.StringValue(o.String()))
		}
	case "point":
		if msg.OutputType == JSON {
			buf.WriteString(`,"point":`)
			buf.Write(appendJSONSimplePoint(nil, o))
		} else {
			point := o.Center()
			z := extractZCoordinate(o)
			if z != 0 {
				vals = append(vals, resp.ArrayValue([]resp.Value{
					resp.StringValue(strconv.FormatFloat(point.Y, 'f', -1, 64)),
					resp.StringValue(strconv.FormatFloat(point.X, 'f', -1, 64)),
					resp.StringValue(strconv.FormatFloat(z, 'f', -1, 64)),
				}))
			} else {
				vals = append(vals, resp.ArrayValue([]resp.Value{
					resp.StringValue(strconv.FormatFloat(point.Y, 'f', -1, 64)),
					resp.StringValue(strconv.FormatFloat(point.X, 'f', -1, 64)),
				}))
			}
		}
	case "hash":
		if vs, sprecision, ok = tokenval(vs); !ok || sprecision == "" {
			return NOMessage, errInvalidNumberOfArguments
		}
		if msg.OutputType == JSON {
			buf.WriteString(`,"hash":`)
		}
		precision, err := strconv.ParseInt(sprecision, 10, 64)
		if err != nil || precision < 1 || precision > 12 {
			return NOMessage, errInvalidArgument(sprecision)
		}
		center := o.Center()
		p := geohash.EncodeWithPrecision(center.Y, center.X, uint(precision))
		if msg.OutputType == JSON {
			buf.WriteString(`"` + p + `"`)
		} else {
			vals = append(vals, resp.StringValue(p))
		}
	case "bounds":
		if msg.OutputType == JSON {
			buf.WriteString(`,"bounds":`)
			buf.Write(appendJSONSimpleBounds(nil, o))
		} else {
			bbox := o.Rect()
			vals = append(vals, resp.ArrayValue([]resp.Value{
				resp.ArrayValue([]resp.Value{
					resp.FloatValue(bbox.Min.Y),
					resp.FloatValue(bbox.Min.X),
				}),
				resp.ArrayValue([]resp.Value{
					resp.FloatValue(bbox.Max.Y),
					resp.FloatValue(bbox.Max.X),
				}),
			}))
		}
	}

	if len(vs) != 0 {
		return NOMessage, errInvalidNumberOfArguments
	}
	if withfields {
		fvs := orderFields(col.FieldMap(), col.FieldArr(), fields)
		if len(fvs) > 0 {
			fvals := make([]resp.Value, 0, len(fvs)*2)
			if msg.OutputType == JSON {
				buf.WriteString(`,"fields":{`)
			}
			for i, fv := range fvs {
				if msg.OutputType == JSON {
					if i > 0 {
						buf.WriteString(`,`)
					}
					buf.WriteString(jsonString(fv.field) + ":" + strconv.FormatFloat(fv.value, 'f', -1, 64))
				} else {
					fvals = append(fvals, resp.StringValue(fv.field), resp.StringValue(strconv.FormatFloat(fv.value, 'f', -1, 64)))
				}
				i++
			}
			if msg.OutputType == JSON {
				buf.WriteString(`}`)
			} else {
				vals = append(vals, resp.ArrayValue(fvals))
			}
		}
	}
	switch msg.OutputType {
	case JSON:
		buf.WriteString(`,"elapsed":"` + time.Since(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case RESP:
		var oval resp.Value
		if withfields {
			oval = resp.ArrayValue(vals)
		} else {
			oval = vals[0]
		}
		return oval, nil
	}
	return NOMessage, nil
}

func (s *Server) cmdDel(msg *Message) (res resp.Value, d commandDetails, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.id, ok = tokenval(vs); !ok || d.id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	erron404 := false
	if len(vs) > 0 {
		_, arg, ok := tokenval(vs)
		if ok && strings.ToLower(arg) == "erron404" {
			erron404 = true
			vs = vs[1:]
		} else {
			err = errInvalidArgument(arg)
			return
		}
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	found := false
	col, _ := s.cols.Get(d.key)
	if col != nil {
		d.obj, d.fields, ok = col.Delete(d.id)
		if ok {
			if col.Count() == 0 {
				s.cols.Delete(d.key)
			}
			found = true
		} else if erron404 {
			err = errIDNotFound
			return
		}
	} else if erron404 {
		err = errKeyNotFound
		return
	}
	s.groupDisconnectObject(d.key, d.id)
	d.command = "del"
	d.updated = found
	d.timestamp = time.Now()
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		if d.updated {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (s *Server) cmdPdel(msg *Message) (res resp.Value, d commandDetails, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.pattern, ok = tokenval(vs); !ok || d.pattern == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	now := time.Now()
	iter := func(id string, o geojson.Object, fields []float64) bool {
		if match, _ := glob.Match(d.pattern, id); match {
			d.children = append(d.children, &commandDetails{
				command:   "del",
				updated:   true,
				timestamp: now,
				key:       d.key,
				id:        id,
			})
		}
		return true
	}

	var expired int
	col, _ := s.cols.Get(d.key)
	if col != nil {
		g := glob.Parse(d.pattern, false)
		if g.Limits[0] == "" && g.Limits[1] == "" {
			col.Scan(false, nil, msg.Deadline, iter)
		} else {
			col.ScanRange(g.Limits[0], g.Limits[1], false, nil, msg.Deadline, iter)
		}
		var atLeastOneNotDeleted bool
		for i, dc := range d.children {
			dc.obj, dc.fields, ok = col.Delete(dc.id)
			if !ok {
				d.children[i].command = "?"
				atLeastOneNotDeleted = true
			} else {
				d.children[i] = dc
			}
			s.groupDisconnectObject(dc.key, dc.id)
		}
		if atLeastOneNotDeleted {
			var nchildren []*commandDetails
			for _, dc := range d.children {
				if dc.command == "del" {
					nchildren = append(nchildren, dc)
				}
			}
			d.children = nchildren
		}
		if col.Count() == 0 {
			s.cols.Delete(d.key)
		}
	}
	d.command = "pdel"
	d.updated = len(d.children) > 0
	d.timestamp = now
	d.parent = true
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		total := len(d.children) - expired
		if total < 0 {
			total = 0
		}
		res = resp.IntegerValue(total)
	}
	return
}

func (s *Server) cmdDrop(msg *Message) (res resp.Value, d commandDetails, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	col, _ := s.cols.Get(d.key)
	if col != nil {
		s.cols.Delete(d.key)
		d.updated = true
	} else {
		d.key = "" // ignore the details
		d.updated = false
	}
	s.groupDisconnectCollection(d.key)
	d.command = "drop"
	d.timestamp = time.Now()
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		if d.updated {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (s *Server) cmdRename(msg *Message) (res resp.Value, d commandDetails, err error) {
	nx := msg.Command() == "renamenx"
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.newKey, ok = tokenval(vs); !ok || d.newKey == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	col, _ := s.cols.Get(d.key)
	if col == nil {
		err = errKeyNotFound
		return
	}
	s.hooks.Ascend(nil, func(v interface{}) bool {
		h := v.(*Hook)
		if h.Key == d.key || h.Key == d.newKey {
			err = errKeyHasHooksSet
			return false
		}
		return true
	})
	d.command = "rename"
	newCol, _ := s.cols.Get(d.newKey)
	if newCol == nil {
		d.updated = true
	} else if nx {
		d.updated = false
	} else {
		s.cols.Delete(d.newKey)
		d.updated = true
	}
	if d.updated {
		s.cols.Delete(d.key)
		s.cols.Set(d.newKey, col)
	}
	d.timestamp = time.Now()
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		if !nx {
			res = resp.SimpleStringValue("OK")
		} else if d.updated {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (s *Server) cmdFlushDB(msg *Message) (res resp.Value, d commandDetails, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}

	// clear the entire database
	s.cols.Clear()
	s.groupHooks.Clear()
	s.groupObjects.Clear()
	s.hookExpires.Clear()
	s.hooks.Clear()
	s.hooksOut.Clear()
	s.hookTree.Clear()
	s.hookCross.Clear()

	d.command = "flushdb"
	d.updated = true
	d.timestamp = time.Now()
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		res = resp.SimpleStringValue("OK")
	}
	return
}

func (s *Server) parseSetArgs(vs []string) (
	d commandDetails, fields []string, values []float64,
	xx, nx bool,
	ex int64, etype []byte, evs []string, err error,
) {
	var ok bool
	var typ []byte
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.id, ok = tokenval(vs); !ok || d.id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	var arg []byte
	var nvs []string
	for {
		if nvs, arg, ok = tokenvalbytes(vs); !ok || len(arg) == 0 {
			err = errInvalidNumberOfArguments
			return
		}
		if lcb(arg, "field") {
			vs = nvs
			var name string
			var svalue string
			var value float64
			if vs, name, ok = tokenval(vs); !ok || name == "" {
				err = errInvalidNumberOfArguments
				return
			}
			if isReservedFieldName(name) {
				err = errInvalidArgument(name)
				return
			}
			if vs, svalue, ok = tokenval(vs); !ok || svalue == "" {
				err = errInvalidNumberOfArguments
				return
			}
			value, err = strconv.ParseFloat(svalue, 64)
			if err != nil {
				err = errInvalidArgument(svalue)
				return
			}
			fields = append(fields, name)
			values = append(values, value)
			continue
		}
		if lcb(arg, "ex") {
			vs = nvs
			if ex != 0 {
				err = errInvalidArgument(string(arg))
				return
			}
			var s string
			var v float64
			if vs, s, ok = tokenval(vs); !ok || s == "" {
				err = errInvalidNumberOfArguments
				return
			}
			v, err = strconv.ParseFloat(s, 64)
			if err != nil {
				err = errInvalidArgument(s)
				return
			}
			ex = time.Now().UnixNano() + int64(float64(time.Second)*v)
			continue
		}
		if lcb(arg, "xx") {
			vs = nvs
			if nx {
				err = errInvalidArgument(string(arg))
				return
			}
			xx = true
			continue
		}
		if lcb(arg, "nx") {
			vs = nvs
			if xx {
				err = errInvalidArgument(string(arg))
				return
			}
			nx = true
			continue
		}
		break
	}
	if vs, typ, ok = tokenvalbytes(vs); !ok || len(typ) == 0 {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) == 0 {
		err = errInvalidNumberOfArguments
		return
	}
	etype = typ
	evs = vs
	switch {
	default:
		err = errInvalidArgument(string(typ))
		return
	case lcb(typ, "string"):
		var str string
		if vs, str, ok = tokenval(vs); !ok {
			err = errInvalidNumberOfArguments
			return
		}
		d.obj = collection.String(str)
	case lcb(typ, "point"):
		var slat, slon, sz string
		if vs, slat, ok = tokenval(vs); !ok || slat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, slon, ok = tokenval(vs); !ok || slon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		vs, sz, ok = tokenval(vs)
		if !ok || sz == "" {
			var x, y float64
			y, err = strconv.ParseFloat(slat, 64)
			if err != nil {
				err = errInvalidArgument(slat)
				return
			}
			x, err = strconv.ParseFloat(slon, 64)
			if err != nil {
				err = errInvalidArgument(slon)
				return
			}
			d.obj = geojson.NewPoint(geometry.Point{X: x, Y: y})
		} else {
			var x, y, z float64
			y, err = strconv.ParseFloat(slat, 64)
			if err != nil {
				err = errInvalidArgument(slat)
				return
			}
			x, err = strconv.ParseFloat(slon, 64)
			if err != nil {
				err = errInvalidArgument(slon)
				return
			}
			z, err = strconv.ParseFloat(sz, 64)
			if err != nil {
				err = errInvalidArgument(sz)
				return
			}
			d.obj = geojson.NewPointZ(geometry.Point{X: x, Y: y}, z)
		}
	case lcb(typ, "bounds"):
		var sminlat, sminlon, smaxlat, smaxlon string
		if vs, sminlat, ok = tokenval(vs); !ok || sminlat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, sminlon, ok = tokenval(vs); !ok || sminlon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, smaxlat, ok = tokenval(vs); !ok || smaxlat == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if vs, smaxlon, ok = tokenval(vs); !ok || smaxlon == "" {
			err = errInvalidNumberOfArguments
			return
		}
		var minlat, minlon, maxlat, maxlon float64
		minlat, err = strconv.ParseFloat(sminlat, 64)
		if err != nil {
			err = errInvalidArgument(sminlat)
			return
		}
		minlon, err = strconv.ParseFloat(sminlon, 64)
		if err != nil {
			err = errInvalidArgument(sminlon)
			return
		}
		maxlat, err = strconv.ParseFloat(smaxlat, 64)
		if err != nil {
			err = errInvalidArgument(smaxlat)
			return
		}
		maxlon, err = strconv.ParseFloat(smaxlon, 64)
		if err != nil {
			err = errInvalidArgument(smaxlon)
			return
		}
		d.obj = geojson.NewRect(geometry.Rect{
			Min: geometry.Point{X: minlon, Y: minlat},
			Max: geometry.Point{X: maxlon, Y: maxlat},
		})
	case lcb(typ, "hash"):
		var shash string
		if vs, shash, ok = tokenval(vs); !ok || shash == "" {
			err = errInvalidNumberOfArguments
			return
		}
		lat, lon := geohash.Decode(shash)
		d.obj = geojson.NewPoint(geometry.Point{X: lon, Y: lat})
	case lcb(typ, "object"):
		var object string
		if vs, object, ok = tokenval(vs); !ok || object == "" {
			err = errInvalidNumberOfArguments
			return
		}
		d.obj, err = geojson.Parse(object, &s.geomParseOpts)
		if err != nil {
			return
		}
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
	}
	return
}

func (s *Server) cmdSet(msg *Message) (res resp.Value, d commandDetails, err error) {
	if s.config.maxMemory() > 0 && s.outOfMemory.on() {
		err = errOOM
		return
	}
	start := time.Now()
	vs := msg.Args[1:]
	var fmap map[string]int
	var fields []string
	var values []float64
	var xx, nx bool
	var ex int64
	d, fields, values, xx, nx, ex, _, _, err = s.parseSetArgs(vs)
	if err != nil {
		return
	}
	col, _ := s.cols.Get(d.key)
	if col == nil {
		if xx {
			goto notok
		}
		col = collection.New()
		s.cols.Set(d.key, col)
	}
	if xx || nx {
		_, _, _, ok := col.Get(d.id)
		if (nx && ok) || (xx && !ok) {
			goto notok
		}
	}
	d.oldObj, d.oldFields, d.fields = col.Set(d.id, d.obj, fields, values, ex)
	d.command = "set"
	d.updated = true // perhaps we should do a diff on the previous object?
	d.timestamp = time.Now()
	if msg.ConnType != Null || msg.OutputType != Null {
		// likely loaded from aof at server startup, ignore field remapping.
		fmap = col.FieldMap()
		d.fmap = make(map[string]int)
		for key, idx := range fmap {
			d.fmap[key] = idx
		}
	}
	// if ex != nil {
	// 	server.expireAt(d.key, d.id, d.timestamp.Add(time.Duration(float64(time.Second)*(*ex))))
	// }
	switch msg.OutputType {
	default:
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		res = resp.SimpleStringValue("OK")
	}
	return
notok:
	switch msg.OutputType {
	default:
	case JSON:
		if nx {
			err = errIDAlreadyExists
		} else {
			err = errIDNotFound
		}
		return
	case RESP:
		res = resp.NullValue()
	}
	return
}

func (s *Server) parseFSetArgs(vs []string) (
	d commandDetails, fields []string, values []float64, xx bool, err error,
) {
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.id, ok = tokenval(vs); !ok || d.id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	for len(vs) > 0 {
		var name string
		if vs, name, ok = tokenval(vs); !ok || name == "" {
			err = errInvalidNumberOfArguments
			return
		}
		if lc(name, "xx") {
			xx = true
			continue
		}
		if isReservedFieldName(name) {
			err = errInvalidArgument(name)
			return
		}
		var svalue string
		var value float64
		if vs, svalue, ok = tokenval(vs); !ok || svalue == "" {
			err = errInvalidNumberOfArguments
			return
		}
		value, err = strconv.ParseFloat(svalue, 64)
		if err != nil {
			err = errInvalidArgument(svalue)
			return
		}
		fields = append(fields, name)
		values = append(values, value)
	}
	return
}

func (s *Server) cmdFset(msg *Message) (res resp.Value, d commandDetails, err error) {
	if s.config.maxMemory() > 0 && s.outOfMemory.on() {
		err = errOOM
		return
	}
	start := time.Now()
	vs := msg.Args[1:]
	var fields []string
	var values []float64
	var xx bool
	var updateCount int
	d, fields, values, xx, err = s.parseFSetArgs(vs)

	col, _ := s.cols.Get(d.key)
	if col == nil {
		err = errKeyNotFound
		return
	}
	var ok bool
	d.obj, d.fields, updateCount, ok = col.SetFields(d.id, fields, values)
	if !(ok || xx) {
		err = errIDNotFound
		return
	}
	if ok {
		d.command = "fset"
		d.timestamp = time.Now()
		d.updated = updateCount > 0
		fmap := col.FieldMap()
		d.fmap = make(map[string]int)
		for key, idx := range fmap {
			d.fmap[key] = idx
		}
	}

	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		res = resp.IntegerValue(updateCount)
	}
	return
}

func (s *Server) cmdExpire(msg *Message) (res resp.Value, d commandDetails, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var key, id, svalue string
	var ok bool
	if vs, key, ok = tokenval(vs); !ok || key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, id, ok = tokenval(vs); !ok || id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, svalue, ok = tokenval(vs); !ok || svalue == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	var value float64
	value, err = strconv.ParseFloat(svalue, 64)
	if err != nil {
		err = errInvalidArgument(svalue)
		return
	}
	ok = false
	col, _ := s.cols.Get(key)
	if col != nil {
		ex := time.Now().Add(time.Duration(float64(time.Second) * value)).UnixNano()
		ok = col.SetExpires(id, ex)
	}
	if ok {
		d.updated = true
	}
	switch msg.OutputType {
	case JSON:
		if ok {
			res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
		} else {
			return resp.SimpleStringValue(""), d, errIDNotFound
		}
	case RESP:
		if ok {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (s *Server) cmdPersist(msg *Message) (res resp.Value, d commandDetails, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var key, id string
	var ok bool
	if vs, key, ok = tokenval(vs); !ok || key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, id, ok = tokenval(vs); !ok || id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	var cleared bool
	ok = false
	col, _ := s.cols.Get(key)
	if col != nil {
		var ex int64
		_, _, ex, ok = col.Get(id)
		if ok && ex != 0 {
			ok = col.SetExpires(id, 0)
			if ok {
				cleared = true
			}
		}
	}
	if !ok {
		if msg.OutputType == RESP {
			return resp.IntegerValue(0), d, nil
		}
		return resp.SimpleStringValue(""), d, errIDNotFound
	}
	d.command = "persist"
	d.updated = cleared
	d.timestamp = time.Now()
	switch msg.OutputType {
	case JSON:
		res = resp.SimpleStringValue(`{"ok":true,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		if cleared {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (s *Server) cmdTTL(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var key, id string
	var ok bool
	if vs, key, ok = tokenval(vs); !ok || key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, id, ok = tokenval(vs); !ok || id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	var v float64
	ok = false
	var ok2 bool
	col, _ := s.cols.Get(key)
	if col != nil {
		var ex int64
		_, _, ex, ok = col.Get(id)
		if ok {
			if ex != 0 {
				now := start.UnixNano()
				if now > ex {
					ok2 = false
				} else {
					v = float64(ex-now) / float64(time.Second)
					if v < 0 {
						v = 0
					}
					ok2 = true
				}
			}
		}
	}
	switch msg.OutputType {
	case JSON:
		if ok {
			var ttl string
			if ok2 {
				ttl = strconv.FormatFloat(v, 'f', -1, 64)
			} else {
				ttl = "-1"
			}
			res = resp.SimpleStringValue(
				`{"ok":true,"ttl":` + ttl + `,"elapsed":"` + time.Since(start).String() + "\"}")
		} else {
			return resp.SimpleStringValue(""), errIDNotFound
		}
	case RESP:
		if ok {
			if ok2 {
				res = resp.IntegerValue(int(v))
			} else {
				res = resp.IntegerValue(-1)
			}
		} else {
			res = resp.IntegerValue(-2)
		}
	}
	return
}
