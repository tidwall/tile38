package server

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/mmcloughlin/geohash"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/field"
	"github.com/tidwall/tile38/internal/glob"
	"github.com/tidwall/tile38/internal/object"
)

// BOUNDS key
func (s *Server) cmdBOUNDS(msg *Message) (resp.Value, error) {
	start := time.Now()

	// >> Args

	args := msg.Args
	if len(args) != 2 {
		return retrerr(errInvalidNumberOfArguments)
	}
	key := args[1]

	// >> Operation

	col, _ := s.cols.Get(key)
	if col == nil {
		if msg.OutputType == RESP {
			return resp.NullValue(), nil
		}
		return retrerr(errKeyNotFound)
	}

	// >> Response

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
		buf.WriteString(`,"elapsed":"` + time.Since(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	}

	// RESP
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
	return vals[0], nil
}

// TYPE key
// undocumented return "none" or "hash"
func (s *Server) cmdTYPE(msg *Message) (resp.Value, error) {
	start := time.Now()

	// >> Args

	args := msg.Args
	if len(args) != 2 {
		return retrerr(errInvalidNumberOfArguments)
	}
	key := args[1]

	// >> Operation

	col, _ := s.cols.Get(key)
	if col == nil {
		if msg.OutputType == RESP {
			return resp.SimpleStringValue("none"), nil
		}
		return retrerr(errKeyNotFound)
	}

	// >> Response

	typ := "hash"

	if msg.OutputType == JSON {
		return resp.StringValue(`{"ok":true,"type":` + jsonString(typ) +
			`,"elapsed":"` + time.Since(start).String() + "\"}"), nil
	}
	return resp.SimpleStringValue(typ), nil
}

// GET key id [WITHFIELDS] [OBJECT|POINT|BOUNDS|(HASH geohash)]
func (s *Server) cmdGET(msg *Message) (resp.Value, error) {
	start := time.Now()

	// >> Args

	args := msg.Args

	if len(args) < 3 {
		return retrerr(errInvalidNumberOfArguments)
	}
	key, id := args[1], args[2]

	withfields := false
	kind := "object"
	var precision int64
	for i := 3; i < len(args); i++ {
		switch strings.ToLower(args[i]) {
		case "withfields":
			withfields = true
		case "object":
			kind = "object"
		case "point":
			kind = "point"
		case "bounds":
			kind = "bounds"
		case "hash":
			kind = "hash"
			i++
			if i == len(args) {
				return retrerr(errInvalidNumberOfArguments)
			}
			var err error
			precision, err = strconv.ParseInt(args[i], 10, 64)
			if err != nil || precision < 1 || precision > 12 {
				return retrerr(errInvalidArgument(args[i]))
			}
		default:
			return retrerr(errInvalidNumberOfArguments)
		}
	}

	// >> Operation

	col, _ := s.cols.Get(key)
	if col == nil {
		if msg.OutputType == RESP {
			return resp.NullValue(), nil
		}
		return retrerr(errKeyNotFound)
	}
	o := col.Get(id)
	if o == nil {
		if msg.OutputType == RESP {
			return resp.NullValue(), nil
		}
		return retrerr(errIDNotFound)
	}

	// >> Response

	vals := make([]resp.Value, 0, 2)
	var buf bytes.Buffer
	if msg.OutputType == JSON {
		buf.WriteString(`{"ok":true`)
	}
	switch kind {
	case "object":
		if msg.OutputType == JSON {
			buf.WriteString(`,"object":`)
			buf.WriteString(string(o.Geo().AppendJSON(nil)))
		} else {
			vals = append(vals, resp.StringValue(o.Geo().String()))
		}
	case "point":
		if msg.OutputType == JSON {
			buf.WriteString(`,"point":`)
			buf.Write(appendJSONSimplePoint(nil, o.Geo()))
		} else {
			point := o.Geo().Center()
			z := extractZCoordinate(o.Geo())
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
		if msg.OutputType == JSON {
			buf.WriteString(`,"hash":`)
		}
		center := o.Geo().Center()
		p := geohash.EncodeWithPrecision(center.Y, center.X, uint(precision))
		if msg.OutputType == JSON {
			buf.WriteString(`"` + p + `"`)
		} else {
			vals = append(vals, resp.StringValue(p))
		}
	case "bounds":
		if msg.OutputType == JSON {
			buf.WriteString(`,"bounds":`)
			buf.Write(appendJSONSimpleBounds(nil, o.Geo()))
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

	if withfields {
		nfields := o.Fields().Len()
		if nfields > 0 {
			fvals := make([]resp.Value, 0, nfields*2)
			if msg.OutputType == JSON {
				buf.WriteString(`,"fields":{`)
			}
			var i int
			o.Fields().Scan(func(f field.Field) bool {
				if msg.OutputType == JSON {
					if i > 0 {
						buf.WriteString(`,`)
					}
					buf.WriteString(jsonString(f.Name()) + ":" +
						f.Value().JSON())
				} else {
					fvals = append(fvals, resp.StringValue(f.Name()),
						resp.StringValue(f.Value().Data()))
				}
				i++
				return true
			})
			if msg.OutputType == JSON {
				buf.WriteString(`}`)
			} else {
				vals = append(vals, resp.ArrayValue(fvals))
			}
		}
	}
	if msg.OutputType == JSON {
		buf.WriteString(`,"elapsed":"` + time.Since(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	}
	var oval resp.Value
	if withfields {
		oval = resp.ArrayValue(vals)
	} else {
		oval = vals[0]
	}
	return oval, nil
}

// DEL key id [ERRON404]
func (s *Server) cmdDEL(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()

	// >> Args

	args := msg.Args
	if len(args) < 3 {
		return retwerr(errInvalidNumberOfArguments)
	}
	key := args[1]
	id := args[2]
	erron404 := false
	for i := 3; i < len(args); i++ {
		switch strings.ToLower(args[i]) {
		case "erron404":
			erron404 = true
		default:
			return retwerr(errInvalidArgument(args[i]))
		}
	}

	// >> Operation

	updated := false
	var old *object.Object
	col, _ := s.cols.Get(key)
	if col != nil {
		old = col.Delete(id)
		if old != nil {
			if col.Count() == 0 {
				s.cols.Delete(key)
			}
			updated = true
		} else if erron404 {
			return retwerr(errIDNotFound)
		}
	} else if erron404 {
		return retwerr(errKeyNotFound)
	}
	s.groupDisconnectObject(key, id)

	// >> Response

	var d commandDetails

	d.command = "del"
	d.key = key
	d.obj = old
	d.updated = updated
	d.timestamp = time.Now()

	var res resp.Value

	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` +
			time.Since(start).String() + "\"}")
	case RESP:
		if d.updated {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return res, d, nil
}

// PDEL key pattern
func (s *Server) cmdPDEL(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()

	// >> Args

	args := msg.Args
	if len(args) != 3 {
		return retwerr(errInvalidNumberOfArguments)
	}
	key := args[1]
	pattern := args[2]

	// >> Operation

	now := time.Now()
	var children []*commandDetails
	col, _ := s.cols.Get(key)
	if col != nil {
		g := glob.Parse(pattern, false)
		var ids []string
		iter := func(o *object.Object) bool {
			if match, _ := glob.Match(pattern, o.ID()); match {
				ids = append(ids, o.ID())
			}
			return true
		}
		if g.Limits[0] == "" && g.Limits[1] == "" {
			col.Scan(false, nil, msg.Deadline, iter)
		} else {
			col.ScanRange(g.Limits[0], g.Limits[1],
				false, nil, msg.Deadline, iter)
		}
		for _, id := range ids {
			obj := col.Delete(id)
			children = append(children, &commandDetails{
				command:   "del",
				updated:   true,
				timestamp: now,
				key:       key,
				obj:       obj,
			})
			s.groupDisconnectObject(key, id)
		}
		if col.Count() == 0 {
			s.cols.Delete(key)
		}
	}

	// >> Response

	var d commandDetails
	var res resp.Value

	d.command = "pdel"
	d.children = children
	d.key = key
	d.pattern = pattern
	d.updated = len(d.children) > 0
	d.timestamp = now
	d.parent = true
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` +
			time.Since(start).String() + "\"}")
	case RESP:
		res = resp.IntegerValue(len(d.children))
	}
	return res, d, nil
}

func (s *Server) cmdDROPop(key string) *collection.Collection {
	col, _ := s.cols.Get(key)
	if col != nil {
		s.cols.Delete(key)
	}
	s.groupDisconnectCollection(key)
	return col
}

// DROP key
func (s *Server) cmdDROP(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()

	// >> Args

	args := msg.Args
	if len(args) != 2 {
		return retwerr(errInvalidNumberOfArguments)
	}
	key := args[1]

	// >> Operation
	col := s.cmdDROPop(key)

	// >> Response

	var res resp.Value
	var d commandDetails
	d.key = key
	d.updated = col != nil
	d.command = "drop"
	d.timestamp = time.Now()
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` +
			time.Since(start).String() + "\"}")
	case RESP:
		if d.updated {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return res, d, nil
}

// RENAME key newkey
// RENAMENX key newkey
func (s *Server) cmdRENAME(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()

	// >> Args

	args := msg.Args
	if len(args) != 3 {
		return retwerr(errInvalidNumberOfArguments)
	}
	nx := strings.ToLower(args[0]) == "renamenx"
	key := args[1]
	newKey := args[2]

	// >> Operation

	col, _ := s.cols.Get(key)
	if col == nil {
		return retwerr(errKeyNotFound)
	}
	var hasHook, hasChannel bool
	s.hooks.Ascend(nil, func(v interface{}) bool {
		h := v.(*Hook)
		if h.Key == key || h.Key == newKey {
			if h.channel {
				hasChannel = true
			} else {
				hasHook = true
			}
		}
		return true
	})
	if hasHook {
		return retwerr(errKeyHasHooksSet)
	}
	if hasChannel {
		return retwerr(errKeyHasChannelsSet)
	}
	var updated bool
	newCol, _ := s.cols.Get(newKey)
	if newCol == nil {
		updated = true
	} else if !nx {
		s.cols.Delete(newKey)
		updated = true
	}
	if updated {
		s.cols.Delete(key)
		s.cols.Set(newKey, col)
	}

	// >> Response

	var d commandDetails
	var res resp.Value

	d.command = "rename"
	d.key = key
	d.newKey = newKey
	d.updated = updated
	d.timestamp = time.Now()

	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` +
			time.Since(start).String() + "\"}")
	case RESP:
		if !nx {
			res = resp.SimpleStringValue("OK")
		} else if d.updated {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return res, d, nil
}

// FLUSHDB
func (s *Server) cmdFLUSHDB(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()

	// >> Args

	args := msg.Args

	if len(args) != 1 {
		return retwerr(errInvalidNumberOfArguments)
	}

	// >> Operation

	// clear the entire database

	// drop each collection
	keys := s.cols.Keys()
	for _, key := range keys {
		s.cmdDROPop(key)
	}

	// delete all channels
	var names []string
	s.hooks.Ascend(nil, func(item any) bool {
		hook := item.(*Hook)
		if hook.channel {
			names = append(names, hook.Name)
		}
		return true
	})
	for _, name := range names {
		s.cmdDELHOOKop(name, true)
	}

	// delete all hooks
	names = names[:0]
	s.hooks.Ascend(nil, func(item any) bool {
		hook := item.(*Hook)
		if !hook.channel {
			names = append(names, hook.Name)
		}
		return true
	})
	for _, name := range names {
		s.cmdDELHOOKop(name, false)
	}

	s.cols.Clear()
	s.groupHooks.Clear()
	s.groupObjects.Clear()
	s.hookExpires.Clear()
	s.hooks.Clear()
	s.hooksOut.Clear()
	s.hookTree.Clear()
	s.hookCross.Clear()

	// >> Response

	var d commandDetails
	d.command = "flushdb"
	d.updated = true
	d.timestamp = time.Now()

	var res resp.Value
	if msg.OutputType == JSON {
		res = resp.StringValue(`{"ok":true,"elapsed":"` +
			time.Since(start).String() + "\"}")
	} else {
		res = resp.SimpleStringValue("OK")
	}
	return res, d, nil
}

// SET key id [FIELD name value ...] [EX seconds] [NX|XX]
// (OBJECT geojson)|(POINT lat lon z)|(BOUNDS minlat minlon maxlat maxlon)|
// (HASH geohash)|(STRING value)
func (s *Server) cmdSET(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()
	if s.config.maxMemory() > 0 && s.outOfMemory.Load() {
		return retwerr(errOOM)
	}

	// >> Args

	var key string
	var id string
	var fields []field.Field
	var ex int64
	var xx bool
	var nx bool
	var oobj geojson.Object

	args := msg.Args
	if len(args) < 3 {
		return retwerr(errInvalidNumberOfArguments)
	}

	key, id = args[1], args[2]

	for i := 3; i < len(args); i++ {
		switch strings.ToLower(args[i]) {
		case "field":
			if i+2 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			fkey := args[i+1]
			fval := args[i+2]
			i += 2
			if isReservedFieldName(fkey) {
				return retwerr(errInvalidArgument(fkey))
			}
			fields = append(fields, field.Make(fkey, fval))
		case "ex":
			if i+1 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			exval := args[i+1]
			i += 1
			x, err := strconv.ParseFloat(exval, 64)
			if err != nil {
				return retwerr(errInvalidArgument(exval))
			}
			ex = time.Now().UnixNano() + int64(float64(time.Second)*x)
		case "nx":
			if xx {
				return retwerr(errInvalidArgument(args[i]))
			}
			nx = true
		case "xx":
			if nx {
				return retwerr(errInvalidArgument(args[i]))
			}
			xx = true
		case "string":
			if i+1 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			str := args[i+1]
			i += 1
			oobj = collection.String(str)
		case "point":
			if i+2 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			slat := args[i+1]
			slon := args[i+2]
			i += 2
			var z float64
			var hasZ bool
			if i+1 < len(args) {
				// probe for possible z coordinate
				var err error
				z, err = strconv.ParseFloat(args[i+1], 64)
				if err == nil {
					hasZ = true
					i++
				}
			}
			y, err := strconv.ParseFloat(slat, 64)
			if err != nil {
				return retwerr(errInvalidArgument(slat))
			}
			x, err := strconv.ParseFloat(slon, 64)
			if err != nil {
				return retwerr(errInvalidArgument(slon))
			}
			if !hasZ {
				oobj = geojson.NewPoint(geometry.Point{X: x, Y: y})
			} else {
				oobj = geojson.NewPointZ(geometry.Point{X: x, Y: y}, z)
			}
		case "bounds":
			if i+4 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			var vals [4]float64
			for j := 0; j < 4; j++ {
				var err error
				vals[j], err = strconv.ParseFloat(args[i+1+j], 64)
				if err != nil {
					return retwerr(errInvalidArgument(args[i+1+j]))
				}
			}
			i += 4
			oobj = geojson.NewRect(geometry.Rect{
				Min: geometry.Point{X: vals[1], Y: vals[0]},
				Max: geometry.Point{X: vals[3], Y: vals[2]},
			})
		case "hash":
			if i+1 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			shash := args[i+1]
			i += 1
			lat, lon := geohash.Decode(shash)
			oobj = geojson.NewPoint(geometry.Point{X: lon, Y: lat})
		case "object":
			if i+1 >= len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			json := args[i+1]
			i += 1
			var err error
			oobj, err = geojson.Parse(json, &s.geomParseOpts)
			if err != nil {
				return retwerr(err)
			}
		default:
			return retwerr(errInvalidArgument(args[i]))
		}
	}
	if oobj == nil {
		return retwerr(errInvalidNumberOfArguments)
	}

	// >> Operation

	nada := func() (resp.Value, commandDetails, error) {
		// exclude operation due to 'xx' or 'nx' match
		if msg.OutputType == JSON {
			if nx {
				return retwerr(errIDAlreadyExists)
			} else {
				return retwerr(errIDNotFound)
			}
		}
		return resp.NullValue(), commandDetails{}, nil
	}

	col, ok := s.cols.Get(key)
	if !ok {
		if xx {
			return nada()
		}
		col = collection.New()
		s.cols.Set(key, col)
	}

	if xx || nx {
		if col.Get(id) == nil {
			if xx {
				return nada()
			}
		} else {
			if nx {
				return nada()
			}
		}
	}

	var flist field.List
	if old := col.Get(id); old != nil {
		flist = old.Fields()
	}
	for _, f := range fields {
		flist = flist.Set(f)
	}
	obj := object.New(id, oobj, ex, flist)
	old := col.Set(obj)

	// >> Response

	var d commandDetails
	d.command = "set"
	d.key = key
	d.obj = obj
	d.old = old
	d.updated = true // perhaps we should do a diff on the previous object?
	d.timestamp = time.Now()

	var res resp.Value
	switch msg.OutputType {
	default:
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` +
			time.Since(start).String() + "\"}")
	case RESP:
		res = resp.SimpleStringValue("OK")
	}
	return res, d, nil
}

func retwerr(err error) (resp.Value, commandDetails, error) {
	return resp.Value{}, commandDetails{}, err
}
func retrerr(err error) (resp.Value, error) {
	return resp.Value{}, err
}

// FSET key id [XX] field value [field value...]
func (s *Server) cmdFSET(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()
	if s.config.maxMemory() > 0 && s.outOfMemory.Load() {
		return retwerr(errOOM)
	}

	// >> Args

	var id string
	var key string
	var xx bool
	var fields []field.Field // raw fields

	args := msg.Args
	if len(args) < 5 {
		return retwerr(errInvalidNumberOfArguments)
	}
	key, id = args[1], args[2]
	for i := 3; i < len(args); i++ {
		arg := args[i]
		switch strings.ToLower(arg) {
		case "xx":
			xx = true
		default:
			fkey := arg
			i++
			if i == len(args) {
				return retwerr(errInvalidNumberOfArguments)
			}
			if isReservedFieldName(fkey) {
				return retwerr(errInvalidArgument(fkey))
			}
			fval := args[i]
			fields = append(fields, field.Make(fkey, fval))
		}
	}

	// >> Operation

	var d commandDetails
	var updateCount int

	col, ok := s.cols.Get(key)
	if !ok {
		return retwerr(errKeyNotFound)
	}
	o := col.Get(id)
	ok = o != nil
	if !(ok || xx) {
		return retwerr(errIDNotFound)
	}

	if ok {
		ofields := o.Fields()
		for _, f := range fields {
			prev := ofields.Get(f.Name())
			if !prev.Value().Equals(f.Value()) {
				ofields = ofields.Set(f)
				updateCount++
			}
		}
		obj := object.New(id, o.Geo(), o.Expires(), ofields)
		col.Set(obj)
		d.command = "fset"
		d.key = key
		d.obj = obj
		d.timestamp = time.Now()
		d.updated = updateCount > 0
	}

	// >> Response

	var res resp.Value

	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true,"elapsed":"` +
			time.Since(start).String() + "\"}")
	case RESP:
		res = resp.IntegerValue(updateCount)
	}

	return res, d, nil
}

// FGET key id field
func (s *Server) cmdFGET(msg *Message) (resp.Value, error) {
	start := time.Now()

	// >> Args

	args := msg.Args

	if len(args) < 4 {
		return retrerr(errInvalidNumberOfArguments)
	}
	key, id, field := args[1], args[2], args[3]

	// >> Operation

	col, _ := s.cols.Get(key)
	if col == nil {
		return retrerr(errKeyNotFound)
	}
	o := col.Get(id)
	if o == nil {
		return retrerr(errIDNotFound)
	}
	f := o.Fields().Get(field)

	// >> Response

	var buf bytes.Buffer
	switch msg.OutputType {
	case JSON:
		buf.WriteString(`{"ok":true`)
		buf.WriteString(`,"value":` + f.Value().JSON())
		buf.WriteString(`,"elapsed":"` + time.Since(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case RESP:
		return resp.StringValue(f.Value().Data()), nil
	}
	return NOMessage, nil
}

// EXPIRE key id seconds
func (s *Server) cmdEXPIRE(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()

	// >> Args

	args := msg.Args
	if len(args) != 4 {
		return retwerr(errInvalidNumberOfArguments)
	}
	key, id, svalue := args[1], args[2], args[3]
	value, err := strconv.ParseFloat(svalue, 64)
	if err != nil {
		return retwerr(errInvalidArgument(svalue))
	}

	// >> Operation

	var ok bool
	var obj *object.Object
	col, _ := s.cols.Get(key)
	if col != nil {
		// replace the expiration by getting the old object
		ex := time.Now().Add(
			time.Duration(float64(time.Second) * value)).UnixNano()
		o := col.Get(id)
		ok = o != nil
		if ok {
			obj = object.New(id, o.Geo(), ex, o.Fields())
			col.Set(obj)
		}
	}

	// >> Response

	var d commandDetails
	if ok {
		d.key = key
		d.obj = obj
		d.command = "expire"
		d.updated = true
		d.timestamp = time.Now()
	}
	var res resp.Value
	switch msg.OutputType {
	case JSON:
		if ok {
			res = resp.StringValue(`{"ok":true,"elapsed":"` +
				time.Since(start).String() + "\"}")
		} else if col == nil {
			return retwerr(errKeyNotFound)
		} else {
			return retwerr(errIDNotFound)
		}
	case RESP:
		if ok {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return res, d, nil
}

// PERSIST key id
func (s *Server) cmdPERSIST(msg *Message) (resp.Value, commandDetails, error) {
	start := time.Now()

	// >> Args

	args := msg.Args
	if len(args) != 3 {
		return retwerr(errInvalidNumberOfArguments)
	}
	key, id := args[1], args[2]

	// >> Operation

	col, _ := s.cols.Get(key)
	if col == nil {
		if msg.OutputType == RESP {
			return resp.IntegerValue(0), commandDetails{}, nil
		}
		return retwerr(errKeyNotFound)
	}
	o := col.Get(id)
	if o == nil {
		if msg.OutputType == RESP {
			return resp.IntegerValue(0), commandDetails{}, nil
		}
		return retwerr(errIDNotFound)
	}

	var obj *object.Object
	var cleared bool
	if o.Expires() != 0 {
		obj = object.New(id, o.Geo(), 0, o.Fields())
		col.Set(obj)
		cleared = true
	}

	// >> Response

	var res resp.Value

	var d commandDetails
	d.command = "persist"
	d.key = key
	d.obj = obj
	d.updated = cleared
	d.timestamp = time.Now()

	switch msg.OutputType {
	case JSON:
		res = resp.SimpleStringValue(`{"ok":true,"elapsed":"` +
			time.Since(start).String() + "\"}")
	case RESP:
		if cleared {
			res = resp.IntegerValue(1)
		} else {
			res = resp.IntegerValue(0)
		}
	}
	return res, d, nil
}

// TTL key id
func (s *Server) cmdTTL(msg *Message) (resp.Value, error) {
	start := time.Now()

	// >> Args

	args := msg.Args
	if len(args) != 3 {
		return retrerr(errInvalidNumberOfArguments)
	}
	key, id := args[1], args[2]

	// >> Operation

	col, _ := s.cols.Get(key)
	if col == nil {
		if msg.OutputType == JSON {
			return retrerr(errKeyNotFound)
		}
		return resp.IntegerValue(-2), nil
	}

	o := col.Get(id)
	if o == nil {
		if msg.OutputType == JSON {
			return retrerr(errIDNotFound)
		}
		return resp.IntegerValue(-2), nil
	}

	var ttl float64
	if o.Expires() == 0 {
		ttl = -1
	} else {
		now := start.UnixNano()
		ttl = math.Max(float64(o.Expires()-now)/float64(time.Second), 0)
	}

	// >> Response

	if msg.OutputType == JSON {
		return resp.SimpleStringValue(
			`{"ok":true,"ttl":` + strconv.Itoa(int(ttl)) + `,"elapsed":"` +
				time.Since(start).String() + "\"}"), nil
	}
	return resp.IntegerValue(int(ttl)), nil
}

// EXISTS key id
func (s *Server) cmdEXISTS(msg *Message) (resp.Value, error) {
	start := time.Now()

	// >> Args

	args := msg.Args
	if len(args) != 3 {
		return retrerr(errInvalidNumberOfArguments)
	}
	key, id := args[1], args[2]

	// >> Operation

	col, _ := s.cols.Get(key)
	if col == nil {
		return retrerr(errKeyNotFound)
	}

	o := col.Get(id)
	exists := o != nil

	// >> Response

	if msg.OutputType == JSON {
		return resp.SimpleStringValue(
			`{"ok":true,"exists":` + strconv.FormatBool(exists) + `,"elapsed":"` +
				time.Since(start).String() + "\"}"), nil
	}
	return resp.BoolValue(exists), nil
}

// FEXISTS key id field
func (s *Server) cmdFEXISTS(msg *Message) (resp.Value, error) {
	start := time.Now()

	// >> Args

	args := msg.Args
	if len(args) != 4 {
		return retrerr(errInvalidNumberOfArguments)
	}
	key, id, field := args[1], args[2], args[3]

	// >> Operation

	col, _ := s.cols.Get(key)
	if col == nil {
		return retrerr(errKeyNotFound)
	}

	o := col.Get(id)
	if o == nil {
		return retrerr(errIDNotFound)
	}

	f := o.Fields().Get(field)
	exists := f.Name() != ""

	// >> Response

	if msg.OutputType == JSON {
		return resp.SimpleStringValue(
			`{"ok":true,"exists":` + strconv.FormatBool(exists) + `,"elapsed":"` +
				time.Since(start).String() + "\"}"), nil
	}
	return resp.BoolValue(exists), nil
}
