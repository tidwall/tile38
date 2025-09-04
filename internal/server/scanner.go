package server

import (
	"bytes"
	"encoding/base64"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/mmcloughlin/geohash"
	"github.com/tidwall/btree"
	"github.com/tidwall/geojson"
	"github.com/tidwall/gjson"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/clip"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/field"
	"github.com/tidwall/tile38/internal/glob"
	"github.com/tidwall/tile38/internal/object"
)

const limitItems = 100

type outputT int

const (
	outputUnknown outputT = iota
	outputIDs
	outputObjects
	outputCount
	outputPoints
	outputHashes
	outputBounds
)

type scanWriter struct {
	s              *Server
	wr             *bytes.Buffer
	name           string
	msg            *Message
	col            *collection.Collection
	fkeys          btree.Set[string]
	output         outputT
	wheres         []whereT
	whereins       []whereinT
	whereevals     []whereevalT
	numberIters    uint64
	numberItems    uint64
	nofields       bool
	cursor         uint64
	limit          uint64
	hitLimit       bool
	once           bool
	count          uint64
	precision      uint64
	globs          []string
	globEverything bool
	fullFields     bool
	values         []resp.Value
	matchValues    bool
	respOut        resp.Value
	filled         []ScanWriterParams
	mvtObjs        []mvtObj
	mvt            bool
	tileX          int
	tileY          int
	tileZ          int
}

type ScanWriterParams struct {
	obj             *object.Object
	dist            float64
	distOutput      bool // query or fence requested distance output
	noTest          bool
	ignoreGlobMatch bool
	clip            geojson.Object
	skipTesting     bool
}

func (s *Server) newScanWriter(
	wr *bytes.Buffer, msg *Message, name string, output outputT,
	precision uint64, globs []string, matchValues bool,
	cursor, limit uint64, wheres []whereT, whereins []whereinT,
	whereevals []whereevalT, nofields, mvt bool, tileX, tileY, tileZ int,
) (
	*scanWriter, error,
) {
	switch output {
	default:
		return nil, errors.New("invalid output type")
	case outputIDs, outputObjects, outputCount, outputBounds, outputPoints,
		outputHashes:
	}
	if limit == 0 {
		if output == outputCount {
			limit = math.MaxUint64
		} else {
			limit = limitItems
		}
	}
	sw := &scanWriter{
		s:           s,
		wr:          wr,
		name:        name,
		msg:         msg,
		globs:       globs,
		limit:       limit,
		cursor:      cursor,
		output:      output,
		nofields:    nofields,
		precision:   precision,
		whereevals:  whereevals,
		matchValues: matchValues,
	}

	sw.mvt = mvt
	sw.tileX = tileX
	sw.tileY = tileY
	sw.tileZ = tileZ

	if len(globs) == 0 || (len(globs) == 1 && globs[0] == "*") {
		sw.globEverything = true
	}
	sw.wheres = wheres
	sw.whereins = whereins
	sw.col, _ = sw.s.cols.Get(sw.name)
	return sw, nil
}

func (sw *scanWriter) hasFieldsOutput() bool {
	switch sw.output {
	default:
		return false
	case outputObjects, outputPoints, outputHashes, outputBounds:
		return !sw.nofields
	}
}

func (sw *scanWriter) writeFoot() {
	if sw.mvt {
		sw.wr.WriteString(`,"mvt":"`)
	} else {
		switch sw.msg.OutputType {
		case JSON:
			if sw.fkeys.Len() > 0 && sw.hasFieldsOutput() {
				sw.wr.WriteString(`,"fields":[`)
				var i int
				sw.fkeys.Scan(func(name string) bool {
					if i > 0 {
						sw.wr.WriteByte(',')
					}
					sw.wr.WriteString(jsonString(name))
					i++
					return true
				})
				sw.wr.WriteByte(']')
			}
			switch sw.output {
			case outputIDs:
				sw.wr.WriteString(`,"ids":[`)
			case outputObjects:
				sw.wr.WriteString(`,"objects":[`)
			case outputPoints:
				sw.wr.WriteString(`,"points":[`)
			case outputBounds:
				sw.wr.WriteString(`,"bounds":[`)
			case outputHashes:
				sw.wr.WriteString(`,"hashes":[`)
			case outputCount:

			}
		case RESP:
		}
	}
	var mvtTile []byte
	if sw.mvt {
		mvtTile = mvtRender(sw.tileX, sw.tileY, sw.tileZ, sw.mvtObjs)
	} else {
		for _, opts := range sw.filled {
			sw.writeFilled(opts)
		}
	}
	cursor := sw.numberIters
	if !sw.hitLimit {
		cursor = 0
	}
	switch sw.msg.OutputType {
	case JSON:
		if sw.mvt {
			sw.wr.WriteString(base64.RawStdEncoding.EncodeToString(mvtTile))
			sw.wr.WriteByte('"')
		} else {
			switch sw.output {
			default:
				sw.wr.WriteByte(']')
			case outputCount:
			}
		}
		sw.wr.WriteString(`,"count":` + strconv.FormatUint(sw.count, 10))
		sw.wr.WriteString(`,"cursor":` + strconv.FormatUint(cursor, 10))
	case RESP:
		if sw.output == outputCount {
			sw.respOut = resp.IntegerValue(int(sw.count))
		} else {
			values := []resp.Value{resp.IntegerValue(int(cursor))}
			if sw.mvt {
				values = append(values, resp.BytesValue(mvtTile))
			} else {
				values = append(values, resp.ArrayValue(sw.values))
			}
			sw.respOut = resp.ArrayValue(values)
		}
	}
}

func extractZCoordinate(o geojson.Object) float64 {
	for {
		switch g := o.(type) {
		case *geojson.Point:
			return g.Z()
		case *geojson.Feature:
			o = g.Base()
		default:
			return 0
		}
	}
}

func isPathKey(s string, key string) bool {
	return strings.HasPrefix(s, key) && (s == key || s[len(key)] == '.')
}

func getFieldValue(o *object.Object, name string) field.Value {
	if isPathKey(name, "properties") {
		if g := o.Geo(); g != nil {
			if res := gjson.Get(g.Members(), "properties"); res.Exists() {
				if name != "properties" {
					// We have a dot path suffix.
					res = res.Get(name[len("properties")+1:])
				}
				if res.Exists() {
					return field.ValueOf(res.Raw)
				}
			}
		}
	}
	if name == "z" {
		z := extractZCoordinate(o.Geo())
		return field.ValueOf(strconv.FormatFloat(z, 'f', -1, 64))
	}
	return o.Fields().Get(name).Value()
}

func (sw *scanWriter) fieldMatch(o *object.Object) (bool, error) {
	for _, where := range sw.wheres {
		if where.expr {
			if !where.matchExpr(sw.s, o) {
				return false, nil
			}
		} else {
			if !where.matchField(getFieldValue(o, where.name)) {
				return false, nil
			}
		}
	}
	for _, wherein := range sw.whereins {
		if !wherein.match(getFieldValue(o, wherein.name)) {
			return false, nil
		}
	}
	if len(sw.whereevals) > 0 {
		fieldNames := make(map[string]field.Value)
		var props string
		if objIsSpatial(o.Geo()) {
			z := extractZCoordinate(o.Geo())
			fieldNames["z"] = field.ValueOf(strconv.FormatFloat(z, 'f', -1, 64))
			props = gjson.Get(o.Geo().Members(), "properties").Raw
		}
		o.Fields().Scan(func(f field.Field) bool {
			fieldNames[f.Name()] = f.Value()
			return true
		})
		for _, whereval := range sw.whereevals {
			match, err := whereval.match(fieldNames, o.ID(), props)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
	}
	return true, nil
}

func (sw *scanWriter) globMatch(o *object.Object) (ok, keepGoing bool) {
	if sw.globEverything {
		return true, true
	}
	var val string
	if sw.matchValues {
		val = o.String()
	} else {
		val = o.ID()
	}
	for _, pattern := range sw.globs {
		ok, _ := glob.Match(pattern, val)
		if ok {
			return true, true
		}
	}
	return false, true
}

// Increment cursor
func (sw *scanWriter) Offset() uint64 {
	return sw.cursor
}

func (sw *scanWriter) Step(n uint64) {
	sw.numberIters += n
}

// ok is whether the object passes the test and should be written
// keepGoing is whether there could be more objects to test
func (sw *scanWriter) testObject(o *object.Object,
) (ok, keepGoing bool, err error) {
	match, kg := sw.globMatch(o)
	if !match {
		return false, kg, nil
	}
	ok, err = sw.fieldMatch(o)
	if err != nil {
		return false, false, err
	}
	return ok, true, nil
}

func (sw *scanWriter) pushObject(opts ScanWriterParams) (keepGoing bool,
	err error,
) {
	keepGoing = true
	if !opts.noTest {
		var ok bool
		var err error
		ok, keepGoing, err = sw.testObject(opts.obj)
		if err != nil {
			return false, err
		}
		if !ok {
			return keepGoing, nil
		}
	}
	sw.count++
	if sw.output == outputCount {
		return sw.count < sw.limit, nil
	}
	if opts.clip != nil {
		// create a newly clipped object
		opts.obj = object.New(
			opts.obj.ID(),
			clip.Clip(opts.obj.Geo(), opts.clip, &sw.s.geomIndexOpts),
			opts.obj.Expires(),
			opts.obj.Fields(),
		)
	}
	if sw.mvt {
		sw.mvtObjs = append(sw.mvtObjs, mvtObj{opts.obj.ID(), opts.obj.Geo()})
	}
	if !sw.fullFields {
		opts.obj.Fields().Scan(func(f field.Field) bool {
			sw.fkeys.Insert(f.Name())
			return true
		})
	}
	sw.filled = append(sw.filled, opts)
	sw.numberItems++
	if sw.numberItems == sw.limit {
		sw.hitLimit = true
		return false, nil
	}
	return keepGoing, nil
}

func (sw *scanWriter) writeObject(opts ScanWriterParams) {
	n := len(sw.filled)
	sw.pushObject(opts)
	if len(sw.filled) > n {
		sw.writeFilled(sw.filled[len(sw.filled)-1])
		sw.filled = sw.filled[:n]
	}
}

func (sw *scanWriter) writeFilled(opts ScanWriterParams) {
	switch sw.msg.OutputType {
	case JSON:
		var wr bytes.Buffer
		var jsfields string
		if sw.once {
			wr.WriteByte(',')
		} else {
			sw.once = true
		}
		fieldsOutput := sw.hasFieldsOutput()
		if fieldsOutput && sw.fullFields {
			if opts.obj.Fields().Len() > 0 {
				jsfields = `,"fields":{`
				var i int
				opts.obj.Fields().Scan(func(f field.Field) bool {
					if !f.Value().IsZero() {
						if i > 0 {
							jsfields += `,`
						}
						jsfields += jsonString(f.Name()) + ":" + f.Value().JSON()
						i++
					}
					return true
				})
				jsfields += `}`
			}
		} else if fieldsOutput && sw.fkeys.Len() > 0 && !sw.fullFields {
			jsfields = `,"fields":[`
			var i int
			sw.fkeys.Scan(func(name string) bool {
				if i > 0 {
					jsfields += `,`
				}
				f := opts.obj.Fields().Get(name)
				jsfields += f.Value().JSON()
				i++
				return true
			})
			jsfields += `]`
		}
		if sw.output == outputIDs {
			if opts.distOutput || opts.dist > 0 {
				wr.WriteString(`{"id":` + jsonString(opts.obj.ID()) +
					`,"distance":` + strconv.FormatFloat(opts.dist, 'f', -1, 64) + "}")
			} else {
				wr.WriteString(jsonString(opts.obj.ID()))
			}
		} else {
			wr.WriteString(`{"id":` + jsonString(opts.obj.ID()))
			switch sw.output {
			case outputObjects:
				wr.WriteString(`,"object":` + string(opts.obj.Geo().AppendJSON(nil)))
			case outputPoints:
				wr.WriteString(`,"point":` + string(appendJSONSimplePoint(nil, opts.obj.Geo())))
			case outputHashes:
				center := opts.obj.Geo().Center()
				p := geohash.EncodeWithPrecision(center.Y, center.X, uint(sw.precision))
				wr.WriteString(`,"hash":"` + p + `"`)
			case outputBounds:
				wr.WriteString(`,"bounds":` + string(appendJSONSimpleBounds(nil, opts.obj.Geo())))
			}
			wr.WriteString(jsfields)
			if opts.distOutput || opts.dist > 0 {
				wr.WriteString(`,"distance":` + strconv.FormatFloat(opts.dist, 'f', -1, 64))
			}

			wr.WriteString(`}`)
		}
		sw.wr.Write(wr.Bytes())
	case RESP:
		vals := make([]resp.Value, 1, 3)
		vals[0] = resp.StringValue(opts.obj.ID())
		if sw.output == outputIDs {
			if opts.distOutput || opts.dist > 0 {
				vals = append(vals, resp.FloatValue(opts.dist))
				sw.values = append(sw.values, resp.ArrayValue(vals))
			} else {
				sw.values = append(sw.values, vals[0])
			}
		} else {
			switch sw.output {
			case outputObjects:
				vals = append(vals, resp.StringValue(opts.obj.String()))
			case outputPoints:
				point := opts.obj.Geo().Center()
				z := extractZCoordinate(opts.obj.Geo())
				if z != 0 {
					vals = append(vals, resp.ArrayValue([]resp.Value{
						resp.FloatValue(point.Y),
						resp.FloatValue(point.X),
						resp.FloatValue(z),
					}))
				} else {
					vals = append(vals, resp.ArrayValue([]resp.Value{
						resp.FloatValue(point.Y),
						resp.FloatValue(point.X),
					}))
				}
			case outputHashes:
				center := opts.obj.Geo().Center()
				p := geohash.EncodeWithPrecision(center.Y, center.X, uint(sw.precision))
				vals = append(vals, resp.StringValue(p))
			case outputBounds:
				bbox := opts.obj.Rect()
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
			if sw.hasFieldsOutput() {
				var fvals []resp.Value
				var i int
				opts.obj.Fields().Scan(func(f field.Field) bool {
					if !f.Value().IsZero() {
						fvals = append(fvals, resp.StringValue(f.Name()), resp.StringValue(f.Value().Data()))
						i++
					}
					return true
				})
				if len(fvals) > 0 {
					vals = append(vals, resp.ArrayValue(fvals))
				}
			}
			if opts.distOutput || opts.dist > 0 {
				vals = append(vals, resp.FloatValue(opts.dist))
			}
			sw.values = append(sw.values, resp.ArrayValue(vals))
		}
	}
}
