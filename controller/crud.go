package controller

import (
	"bytes"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/collection"
	"github.com/tidwall/tile38/controller/glob"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/geojson"
	"github.com/tidwall/tile38/geojson/geohash"
)

type fvt struct {
	field string
	value float64
}

type byField []fvt

func (a byField) Len() int {
	return len(a)
}
func (a byField) Less(i, j int) bool {
	return a[i].field < a[j].field
}
func (a byField) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func orderFields(fmap map[string]int, fields []float64) []fvt {
	var fv fvt
	fvs := make([]fvt, 0, len(fmap))
	for field, idx := range fmap {
		if idx < len(fields) {
			fv.field = field
			fv.value = fields[idx]
			if fv.value != 0 {
				fvs = append(fvs, fv)
			}
		}
	}
	sort.Sort(byField(fvs))
	return fvs
}
func (c *Controller) cmdBounds(msg *server.Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var key string
	if vs, key, ok = tokenval(vs); !ok || key == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	col := c.getCol(key)
	if col == nil {
		if msg.OutputType == server.RESP {
			return resp.NullValue(), nil
		}
		return server.NOMessage, errKeyNotFound
	}

	vals := make([]resp.Value, 0, 2)
	var buf bytes.Buffer
	if msg.OutputType == server.JSON {
		buf.WriteString(`{"ok":true`)
	}
	minX, minY, maxX, maxY := col.Bounds()

	bbox := geojson.New2DBBox(minX, minY, maxX, maxY)
	if msg.OutputType == server.JSON {
		buf.WriteString(`,"bounds":`)
		buf.WriteString(bbox.ExternalJSON())
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
	case server.JSON:
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case server.RESP:
		return vals[0], nil
	}
	return server.NOMessage, nil
}

func (c *Controller) cmdType(msg *server.Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var key string
	if vs, key, ok = tokenval(vs); !ok || key == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	col := c.getCol(key)
	if col == nil {
		if msg.OutputType == server.RESP {
			return resp.SimpleStringValue("none"), nil
		}
		return server.NOMessage, errKeyNotFound
	}

	typ := "hash"

	switch msg.OutputType {
	case server.JSON:
		return resp.StringValue(`{"ok":true,"type":` + string(typ) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}"), nil
	case server.RESP:
		return resp.SimpleStringValue(typ), nil
	}
	return server.NOMessage, nil
}

func (c *Controller) cmdGet(msg *server.Message) (resp.Value, error) {
	start := time.Now()
	vs := msg.Values[1:]

	var ok bool
	var key, id, typ, sprecision string
	if vs, key, ok = tokenval(vs); !ok || key == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}
	if vs, id, ok = tokenval(vs); !ok || id == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	withfields := false
	if _, peek, ok := tokenval(vs); ok && strings.ToLower(peek) == "withfields" {
		withfields = true
		vs = vs[1:]
	}

	col := c.getCol(key)
	if col == nil {
		if msg.OutputType == server.RESP {
			return resp.NullValue(), nil
		}
		return server.NOMessage, errKeyNotFound
	}
	o, fields, ok := col.Get(id)
	ok = ok && !c.hasExpired(key, id)
	if !ok {
		if msg.OutputType == server.RESP {
			return resp.NullValue(), nil
		}
		return server.NOMessage, errIDNotFound
	}

	vals := make([]resp.Value, 0, 2)
	var buf bytes.Buffer
	if msg.OutputType == server.JSON {
		buf.WriteString(`{"ok":true`)
	}
	vs, typ, ok = tokenval(vs)
	typ = strings.ToLower(typ)
	if !ok {
		typ = "object"
	}
	switch typ {
	default:
		return server.NOMessage, errInvalidArgument(typ)
	case "object":
		if msg.OutputType == server.JSON {
			buf.WriteString(`,"object":`)
			buf.WriteString(o.JSON())
		} else {
			vals = append(vals, resp.StringValue(o.String()))
		}
	case "point":
		point := o.CalculatedPoint()
		if msg.OutputType == server.JSON {
			buf.WriteString(`,"point":`)
			buf.WriteString(point.ExternalJSON())
		} else {
			if point.Z != 0 {
				vals = append(vals, resp.ArrayValue([]resp.Value{
					resp.StringValue(strconv.FormatFloat(point.Y, 'f', -1, 64)),
					resp.StringValue(strconv.FormatFloat(point.X, 'f', -1, 64)),
					resp.StringValue(strconv.FormatFloat(point.Z, 'f', -1, 64)),
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
			return server.NOMessage, errInvalidNumberOfArguments
		}
		if msg.OutputType == server.JSON {
			buf.WriteString(`,"hash":`)
		}
		precision, err := strconv.ParseInt(sprecision, 10, 64)
		if err != nil || precision < 1 || precision > 64 {
			return server.NOMessage, errInvalidArgument(sprecision)
		}
		p, err := o.Geohash(int(precision))
		if err != nil {
			return server.NOMessage, err
		}
		if msg.OutputType == server.JSON {
			buf.WriteString(`"` + p + `"`)
		} else {
			vals = append(vals, resp.StringValue(p))
		}
	case "bounds":
		bbox := o.CalculatedBBox()
		if msg.OutputType == server.JSON {
			buf.WriteString(`,"bounds":`)
			buf.WriteString(bbox.ExternalJSON())
		} else {
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
		return server.NOMessage, errInvalidNumberOfArguments
	}
	if withfields {
		fvs := orderFields(col.FieldMap(), fields)
		if len(fvs) > 0 {
			fvals := make([]resp.Value, 0, len(fvs)*2)
			if msg.OutputType == server.JSON {
				buf.WriteString(`,"fields":{`)
			}
			for i, fv := range fvs {
				if msg.OutputType == server.JSON {
					if i > 0 {
						buf.WriteString(`,`)
					}
					buf.WriteString(jsonString(fv.field) + ":" + strconv.FormatFloat(fv.value, 'f', -1, 64))
				} else {
					fvals = append(fvals, resp.StringValue(fv.field), resp.StringValue(strconv.FormatFloat(fv.value, 'f', -1, 64)))
				}
				i++
			}
			if msg.OutputType == server.JSON {
				buf.WriteString(`}`)
			} else {
				vals = append(vals, resp.ArrayValue(fvals))
			}
		}
	}
	switch msg.OutputType {
	case server.JSON:
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case server.RESP:
		var oval resp.Value
		if withfields {
			oval = resp.ArrayValue(vals)
		} else {
			oval = vals[0]
		}
		return oval, nil
	}
	return server.NOMessage, nil
}

func (c *Controller) cmdDel(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.id, ok = tokenval(vs); !ok || d.id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	found := false
	col := c.getCol(d.key)
	if col != nil {
		d.obj, d.fields, ok = col.Remove(d.id)
		if ok {
			if col.Count() == 0 {
				c.deleteCol(d.key)
			}
			found = true
		}
	}
	c.clearIDExpires(d.key, d.id)
	d.command = "del"
	d.updated = found
	d.timestamp = time.Now()
	switch msg.OutputType {
	case server.JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case server.RESP:
		if d.updated {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (c *Controller) cmdPdel(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]
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
			d.children = append(d.children, &commandDetailsT{
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
	col := c.getCol(d.key)
	if col != nil {
		g := glob.Parse(d.pattern, false)
		if g.Limits[0] == "" && g.Limits[1] == "" {
			col.Scan(false, iter)
		} else {
			col.ScanRange(g.Limits[0], g.Limits[1], false, iter)
		}
		var atLeastOneNotDeleted bool
		for i, dc := range d.children {
			dc.obj, dc.fields, ok = col.Remove(dc.id)
			if !ok {
				d.children[i].command = "?"
				atLeastOneNotDeleted = true
			} else {
				d.children[i] = dc
			}
			c.clearIDExpires(d.key, dc.id)
		}
		if atLeastOneNotDeleted {
			var nchildren []*commandDetailsT
			for _, dc := range d.children {
				if dc.command == "del" {
					nchildren = append(nchildren, dc)
				}
			}
			d.children = nchildren
		}
		if col.Count() == 0 {
			c.deleteCol(d.key)
		}
	}
	d.command = "pdel"
	d.updated = len(d.children) > 0
	d.timestamp = now
	d.parent = true
	switch msg.OutputType {
	case server.JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case server.RESP:
		total := len(d.children) - expired
		if total < 0 {
			total = 0
		}
		res = resp.IntegerValue(total)
	}
	return
}

func (c *Controller) cmdDrop(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	col := c.getCol(d.key)
	if col != nil {
		c.deleteCol(d.key)
		d.updated = true
	} else {
		d.key = "" // ignore the details
		d.updated = false
	}
	d.command = "drop"
	d.timestamp = time.Now()
	c.clearKeyExpires(d.key)
	switch msg.OutputType {
	case server.JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case server.RESP:
		if d.updated {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (c *Controller) cmdFlushDB(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	c.cols = btree.New(16, 0)
	c.clearAllExpires()
	c.hooks = make(map[string]*Hook)
	c.hookcols = make(map[string]map[string]*Hook)
	d.command = "flushdb"
	d.updated = true
	d.timestamp = time.Now()
	switch msg.OutputType {
	case server.JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case server.RESP:
		res = resp.SimpleStringValue("OK")
	}
	return
}

func (c *Controller) parseSetArgs(vs []resp.Value) (
	d commandDetailsT, fields []string, values []float64,
	xx, nx bool,
	expires *float64, etype []byte, evs []resp.Value, err error,
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
	var nvs []resp.Value
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
			if expires != nil {
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
			expires = &v
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
		d.obj = geojson.String(str)
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
			var sp geojson.SimplePoint
			sp.Y, err = strconv.ParseFloat(slat, 64)
			if err != nil {
				err = errInvalidArgument(slat)
				return
			}
			sp.X, err = strconv.ParseFloat(slon, 64)
			if err != nil {
				err = errInvalidArgument(slon)
				return
			}
			d.obj = sp
		} else {
			var sp geojson.Point
			sp.Coordinates.Y, err = strconv.ParseFloat(slat, 64)
			if err != nil {
				err = errInvalidArgument(slat)
				return
			}
			sp.Coordinates.X, err = strconv.ParseFloat(slon, 64)
			if err != nil {
				err = errInvalidArgument(slon)
				return
			}
			sp.Coordinates.Z, err = strconv.ParseFloat(sz, 64)
			if err != nil {
				err = errInvalidArgument(sz)
				return
			}
			d.obj = sp
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
		g := geojson.Polygon{
			Coordinates: [][]geojson.Position{
				{
					{X: minlon, Y: minlat, Z: 0},
					{X: minlon, Y: maxlat, Z: 0},
					{X: maxlon, Y: maxlat, Z: 0},
					{X: maxlon, Y: minlat, Z: 0},
					{X: minlon, Y: minlat, Z: 0},
				},
			},
		}
		d.obj = g
	case lcb(typ, "hash"):
		var sp geojson.SimplePoint
		var shash string
		if vs, shash, ok = tokenval(vs); !ok || shash == "" {
			err = errInvalidNumberOfArguments
			return
		}
		var lat, lon float64
		lat, lon, err = geohash.Decode(shash)
		if err != nil {
			return
		}
		sp.X = lon
		sp.Y = lat
		d.obj = sp
	case lcb(typ, "object"):
		var object string
		if vs, object, ok = tokenval(vs); !ok || object == "" {
			err = errInvalidNumberOfArguments
			return
		}
		d.obj, err = geojson.ObjectJSON(object)
		if err != nil {
			return
		}
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
	}
	return
}

func (c *Controller) cmdSet(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	if c.config.MaxMemory > 0 && c.outOfMemory {
		err = errOOM
		return
	}
	start := time.Now()
	vs := msg.Values[1:]
	var fmap map[string]int
	var fields []string
	var values []float64
	var xx, nx bool
	var ex *float64
	d, fields, values, xx, nx, ex, _, _, err = c.parseSetArgs(vs)
	if err != nil {
		return
	}
	ex = ex
	col := c.getCol(d.key)
	if col == nil {
		if xx {
			goto notok
		}
		col = collection.New()
		c.setCol(d.key, col)
	}
	if xx || nx {
		_, _, ok := col.Get(d.id)
		if (nx && ok) || (xx && !ok) {
			goto notok
		}
	}
	c.clearIDExpires(d.key, d.id)
	d.oldObj, d.oldFields, d.fields = col.ReplaceOrInsert(d.id, d.obj, fields, values)
	d.command = "set"
	d.updated = true // perhaps we should do a diff on the previous object?
	d.timestamp = time.Now()
	if msg.ConnType != server.Null || msg.OutputType != server.Null {
		// likely loaded from aof at server startup, ignore field remapping.
		fmap = col.FieldMap()
		d.fmap = make(map[string]int)
		for key, idx := range fmap {
			d.fmap[key] = idx
		}
	}
	if ex != nil {
		c.expireAt(d.key, d.id, d.timestamp.Add(time.Duration(float64(time.Second)*(*ex))))
	}
	switch msg.OutputType {
	default:
	case server.JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case server.RESP:
		res = resp.SimpleStringValue("OK")
	}
	return
notok:
	switch msg.OutputType {
	default:
	case server.JSON:
		if nx {
			err = errIDAlreadyExists
		} else {
			err = errIDNotFound
		}
		return
	case server.RESP:
		res = resp.NullValue()
	}
	return
}

func (c *Controller) parseFSetArgs(vs []resp.Value) (d commandDetailsT, err error) {
	var svalue string
	var ok bool
	if vs, d.key, ok = tokenval(vs); !ok || d.key == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.id, ok = tokenval(vs); !ok || d.id == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if vs, d.field, ok = tokenval(vs); !ok || d.field == "" {
		err = errInvalidNumberOfArguments
		return
	}
	if isReservedFieldName(d.field) {
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
	d.value, err = strconv.ParseFloat(svalue, 64)
	if err != nil {
		err = errInvalidArgument(svalue)
		return
	}
	return
}

func (c *Controller) cmdFset(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	d, err = c.parseFSetArgs(vs)
	col := c.getCol(d.key)
	if col == nil {
		err = errKeyNotFound
		return
	}
	var ok bool
	d.obj, d.fields, d.updated, ok = col.SetField(d.id, d.field, d.value)
	if !ok {
		err = errIDNotFound
		return
	}
	d.command = "fset"
	d.timestamp = time.Now()
	fmap := col.FieldMap()
	d.fmap = make(map[string]int)
	for key, idx := range fmap {
		d.fmap[key] = idx
	}

	switch msg.OutputType {
	case server.JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case server.RESP:
		if d.updated {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (c *Controller) cmdExpire(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]
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
	col := c.getCol(key)
	if col != nil {
		_, _, ok = col.Get(id)
		ok = ok && !c.hasExpired(key, id)
	}
	if ok {
		c.expireAt(key, id, time.Now().Add(time.Duration(float64(time.Second)*value)))
		d.updated = true
	}
	switch msg.OutputType {
	case server.JSON:
		if ok {
			res = resp.StringValue(`{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		} else {
			return resp.SimpleStringValue(""), d, errIDNotFound
		}
	case server.RESP:
		if ok {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (c *Controller) cmdPersist(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]
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
	col := c.getCol(key)
	if col != nil {
		_, _, ok = col.Get(id)
		ok = ok && !c.hasExpired(key, id)
		if ok {
			cleared = c.clearIDExpires(key, id)
		}
	}
	if !ok {
		if msg.OutputType == server.RESP {
			return resp.IntegerValue(0), d, nil
		}
		return resp.SimpleStringValue(""), d, errIDNotFound
	}
	d.command = "persist"
	d.updated = cleared
	d.timestamp = time.Now()
	switch msg.OutputType {
	case server.JSON:
		res = resp.SimpleStringValue(`{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case server.RESP:
		if cleared {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return
}

func (c *Controller) cmdTTL(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]
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
	col := c.getCol(key)
	if col != nil {
		_, _, ok = col.Get(id)
		ok = ok && !c.hasExpired(key, id)
		if ok {
			var at time.Time
			at, ok2 = c.getExpires(key, id)
			if ok2 {
				if time.Now().After(at) {
					ok2 = false
				} else {
					v = float64(at.Sub(time.Now())) / float64(time.Second)
					if v < 0 {
						v = 0
					}
				}
			}
		}
	}
	switch msg.OutputType {
	case server.JSON:
		if ok {
			var ttl string
			if ok2 {
				ttl = strconv.FormatFloat(v, 'f', -1, 64)
			} else {
				ttl = "-1"
			}
			res = resp.SimpleStringValue(
				`{"ok":true,"ttl":` + ttl + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		} else {
			return resp.SimpleStringValue(""), errIDNotFound
		}
	case server.RESP:
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
