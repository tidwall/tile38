package server

import (
	"bytes"
	"encoding/base64"
	"errors"
	"math"
	"strconv"
	"sync"

	"github.com/mmcloughlin/geohash"
	"github.com/tidwall/geojson"
	"github.com/tidwall/mvt"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/clip"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/glob"
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
	mu             sync.Mutex
	s              *Server
	wr             *bytes.Buffer
	key            string
	msg            *Message
	col            *collection.Collection
	fmap           map[string]int
	farr           []string
	fvals          []float64
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
	mvtObjs        []geojson.Object
	matchValues    bool
	mvt            bool
	respOut        resp.Value
	orgWheres      []whereT
	orgWhereins    []whereinT
}

// ScanWriterParams ...
type ScanWriterParams struct {
	id              string
	o               geojson.Object
	fields          []float64
	distance        float64
	distOutput      bool // query or fence requested distance output
	noLock          bool
	noTest          bool
	ignoreGlobMatch bool
	clip            geojson.Object
	skipTesting     bool
}

func (s *Server) newScanWriter(
	wr *bytes.Buffer, msg *Message, key string, output outputT,
	precision uint64, globs []string, matchValues bool,
	cursor, limit uint64, wheres []whereT, whereins []whereinT,
	whereevals []whereevalT, nofields, mvt bool,
) (
	*scanWriter, error,
) {
	switch output {
	default:
		return nil, errors.New("invalid output type")
	case outputIDs, outputObjects, outputCount, outputBounds, outputPoints, outputHashes:
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
		key:         key,
		msg:         msg,
		mvt:         mvt,
		globs:       globs,
		limit:       limit,
		cursor:      cursor,
		output:      output,
		nofields:    nofields,
		precision:   precision,
		whereevals:  whereevals,
		matchValues: matchValues,
	}

	if len(globs) == 0 || (len(globs) == 1 && globs[0] == "*") {
		sw.globEverything = true
	}
	sw.orgWheres = wheres
	sw.orgWhereins = whereins
	sw.loadWheres()
	return sw, nil
}

func (sw *scanWriter) loadWheres() {
	sw.fmap = nil
	sw.farr = nil
	sw.wheres = nil
	sw.whereins = nil
	sw.fvals = nil
	sw.col, _ = sw.s.cols.Get(sw.key)
	if sw.col != nil {
		sw.fmap = sw.col.FieldMap()
		sw.farr = sw.col.FieldArr()
		// This fills index value in wheres/whereins
		// so we don't have to map string field names for each tested object
		var ok bool
		if len(sw.orgWheres) > 0 {
			sw.wheres = make([]whereT, len(sw.orgWheres))
			for i, where := range sw.orgWheres {
				if where.index, ok = sw.fmap[where.field]; !ok {
					where.index = math.MaxInt32
				}
				sw.wheres[i] = where
			}
		}
		if len(sw.orgWhereins) > 0 {
			sw.whereins = make([]whereinT, len(sw.orgWhereins))
			for i, wherein := range sw.orgWhereins {
				if wherein.index, ok = sw.fmap[wherein.field]; !ok {
					wherein.index = math.MaxInt32
				}
				sw.whereins[i] = wherein
			}
		}
		if len(sw.farr) > 0 {
			sw.fvals = make([]float64, len(sw.farr))
		}
	}

}

func (sw *scanWriter) hasFieldsOutput() bool {
	switch sw.output {
	default:
		return false
	case outputObjects, outputPoints, outputHashes, outputBounds:
		return !sw.nofields
	}
}

func (sw *scanWriter) writeHead() {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.mvt {
		sw.wr.WriteString(`,"mvt":"`)
	} else {
		switch sw.msg.OutputType {
		case JSON:
			if len(sw.farr) > 0 && sw.hasFieldsOutput() {
				sw.wr.WriteString(`,"fields":[`)
				for i, field := range sw.farr {
					if i > 0 {
						sw.wr.WriteByte(',')
					}
					sw.wr.WriteString(jsonString(field))
				}
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
}

func (sw *scanWriter) compileMVT() []byte {
	var tile mvt.Tile
	l := tile.AddLayer("default")
	l.SetExtent(4096)

	for _, g := range sw.mvtObjs {
		_ = g
		f := l.AddFeature(mvt.Polygon)
		// f.MoveTo(128, 96)
		// f.LineTo(148, 128)
		// f.LineTo(108, 128)
		// f.LineTo(128, 96)
		f.ClosePath()
	}

	// println(sw.mvtObjs)

	return tile.Render()
}

func (sw *scanWriter) writeFoot() {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	cursor := sw.numberIters
	if !sw.hitLimit {
		cursor = 0
	}

	var mvtTile []byte
	if sw.mvt {
		mvtTile = sw.compileMVT()
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

func (sw *scanWriter) fieldMatch(fields []float64, o geojson.Object) (fvals []float64, match bool) {
	var z float64
	var gotz bool
	fvals = sw.fvals
	if !sw.hasFieldsOutput() || sw.fullFields {
		for _, where := range sw.wheres {
			if where.field == "z" {
				if !gotz {
					z = extractZCoordinate(o)
				}
				if !where.match(z) {
					return
				}
				continue
			}
			var value float64
			if where.index < len(fields) {
				value = fields[where.index]
			}
			if !where.match(value) {
				return
			}
		}
		for _, wherein := range sw.whereins {
			var value float64
			if wherein.index < len(fields) {
				value = fields[wherein.index]
			}
			if !wherein.match(value) {
				return
			}
		}
		for _, whereval := range sw.whereevals {
			fieldsWithNames := make(map[string]float64)
			for field, idx := range sw.fmap {
				if idx < len(fields) {
					fieldsWithNames[field] = fields[idx]
				} else {
					fieldsWithNames[field] = 0
				}
			}
			if !whereval.match(fieldsWithNames) {
				return
			}
		}
	} else {
		copy(sw.fvals, fields)
		// fields might be shorter for this item, need to pad sw.fvals with zeros
		for i := len(fields); i < len(sw.fvals); i++ {
			sw.fvals[i] = 0
		}
		for _, where := range sw.wheres {
			if where.field == "z" {
				if !gotz {
					z = extractZCoordinate(o)
				}
				if !where.match(z) {
					return
				}
				continue
			}
			var value float64
			if where.index < len(sw.fvals) {
				value = sw.fvals[where.index]
			}
			if !where.match(value) {
				return
			}
		}
		for _, wherein := range sw.whereins {
			var value float64
			if wherein.index < len(sw.fvals) {
				value = sw.fvals[wherein.index]
			}
			if !wherein.match(value) {
				return
			}
		}
		for _, whereval := range sw.whereevals {
			fieldsWithNames := make(map[string]float64)
			for field, idx := range sw.fmap {
				if idx < len(fields) {
					fieldsWithNames[field] = fields[idx]
				} else {
					fieldsWithNames[field] = 0
				}
			}
			if !whereval.match(fieldsWithNames) {
				return
			}
		}
	}
	match = true
	return
}

func (sw *scanWriter) globMatch(id string, o geojson.Object) (ok, keepGoing bool) {
	if sw.globEverything {
		return true, true
	}
	var val string
	if sw.matchValues {
		val = o.String()
	} else {
		val = id
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
func (sw *scanWriter) testObject(id string, o geojson.Object, fields []float64) (
	ok, keepGoing bool, fieldVals []float64) {
	match, kg := sw.globMatch(id, o)
	if !match {
		return false, kg, fieldVals
	}
	nf, ok := sw.fieldMatch(fields, o)
	return ok, true, nf
}

// id string, o geojson.Object, fields []float64, noLock bool
func (sw *scanWriter) writeObject(opts ScanWriterParams) bool {
	if !opts.noLock {
		sw.mu.Lock()
		defer sw.mu.Unlock()
	}

	keepGoing := true
	if !opts.noTest {
		var ok bool
		ok, keepGoing, _ = sw.testObject(opts.id, opts.o, opts.fields)
		if !ok {
			return keepGoing
		}
	}
	sw.count++
	if sw.output == outputCount {
		return sw.count < sw.limit
	}
	if opts.clip != nil {
		opts.o = clip.Clip(opts.o, opts.clip, &sw.s.geomIndexOpts)
	}
	if sw.mvt {
		sw.mvtObjs = append(sw.mvtObjs, opts.o)
	} else {
		switch sw.msg.OutputType {
		case JSON:
			var wr bytes.Buffer
			var jsfields string
			if sw.once {
				wr.WriteByte(',')
			} else {
				sw.once = true
			}
			if sw.hasFieldsOutput() {
				if sw.fullFields {
					if len(sw.fmap) > 0 {
						jsfields = `,"fields":{`
						var i int
						for field, idx := range sw.fmap {
							if len(opts.fields) > idx {
								if opts.fields[idx] != 0 {
									if i > 0 {
										jsfields += `,`
									}
									jsfields += jsonString(field) + ":" + strconv.FormatFloat(opts.fields[idx], 'f', -1, 64)
									i++
								}
							}
						}
						jsfields += `}`
					}

				} else if len(sw.farr) > 0 {
					jsfields = `,"fields":[`
					for i, name := range sw.farr {
						if i > 0 {
							jsfields += `,`
						}
						j := sw.fmap[name]
						if j < len(opts.fields) {
							jsfields += strconv.FormatFloat(opts.fields[j], 'f', -1, 64)
						} else {
							jsfields += "0"
						}
					}
					jsfields += `]`
				}
			}
			if sw.output == outputIDs {
				wr.WriteString(jsonString(opts.id))
			} else {
				wr.WriteString(`{"id":` + jsonString(opts.id))
				switch sw.output {
				case outputObjects:
					wr.WriteString(`,"object":` + string(opts.o.AppendJSON(nil)))
				case outputPoints:
					wr.WriteString(`,"point":` + string(appendJSONSimplePoint(nil, opts.o)))
				case outputHashes:
					center := opts.o.Center()
					p := geohash.EncodeWithPrecision(center.Y, center.X, uint(sw.precision))
					wr.WriteString(`,"hash":"` + p + `"`)
				case outputBounds:
					wr.WriteString(`,"bounds":` + string(appendJSONSimpleBounds(nil, opts.o)))
				}

				wr.WriteString(jsfields)

				if opts.distOutput || opts.distance > 0 {
					wr.WriteString(`,"distance":` + strconv.FormatFloat(opts.distance, 'f', -1, 64))
				}

				wr.WriteString(`}`)
			}
			sw.wr.Write(wr.Bytes())
		case RESP:
			vals := make([]resp.Value, 1, 3)
			vals[0] = resp.StringValue(opts.id)
			if sw.output == outputIDs {
				sw.values = append(sw.values, vals[0])
			} else {
				switch sw.output {
				case outputObjects:
					vals = append(vals, resp.StringValue(opts.o.String()))
				case outputPoints:
					point := opts.o.Center()
					z := extractZCoordinate(opts.o)
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
					center := opts.o.Center()
					p := geohash.EncodeWithPrecision(center.Y, center.X, uint(sw.precision))
					vals = append(vals, resp.StringValue(p))
				case outputBounds:
					bbox := opts.o.Rect()
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
					fvs := orderFields(sw.fmap, sw.farr, opts.fields)
					if len(fvs) > 0 {
						fvals := make([]resp.Value, 0, len(fvs)*2)
						for i, fv := range fvs {
							fvals = append(fvals, resp.StringValue(fv.field), resp.StringValue(strconv.FormatFloat(fv.value, 'f', -1, 64)))
							i++
						}
						vals = append(vals, resp.ArrayValue(fvals))
					}
				}
				if opts.distOutput || opts.distance > 0 {
					vals = append(vals, resp.FloatValue(opts.distance))
				}

				sw.values = append(sw.values, resp.ArrayValue(vals))
			}
		}
	}
	sw.numberItems++
	if sw.numberItems == sw.limit {
		sw.hitLimit = true
		return false
	}
	return keepGoing
}
