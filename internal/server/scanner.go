package server

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"sync"

	"github.com/mmcloughlin/geohash"
	"github.com/tidwall/geojson"
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

type scanner struct {
	mu             sync.Mutex
	s              *Server
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
	limit          limitT
	earlyStop      bool
	once           bool
	count          uint64
	precision      uint64
	globPattern    string
	globEverything bool
	globSingle     bool
	fullFields     bool
	matchValues    bool
	collector      scanCollector
}

// ScanObjectParams ...
type ScanObjectParams struct {
	id       string
	o        geojson.Object
	fields   []float64
	distance float64
	noLock   bool
	clip     geojson.Object
}

func (s *Server) newScanner(
	collector scanCollector, key string, output outputT,
	precision uint64, globPattern string, matchValues bool,
	cursor uint64, limit limitT, wheres []whereT, whereins []whereinT, whereevals []whereevalT, nofields bool,
) (
	*scanner, error,
) {
	switch output {
	default:
		return nil, errors.New("invalid output type")
	case outputIDs, outputObjects, outputCount, outputBounds, outputPoints, outputHashes:
	}
	limitMatched := limit.matched
	if limitMatched == 0 {
		if output == outputCount {
			limitMatched = math.MaxUint64
		} else {
			limitMatched = limitItems
		}
	}

	limitScanned := limit.scanned
	if limitScanned == 0 {
		limitScanned = math.MaxUint64
	}
	sc := &scanner{
		s:      s,
		cursor: cursor,
		limit: limitT{
			matched: limitMatched,
			scanned: limitScanned,
		},
		whereevals:  whereevals,
		output:      output,
		nofields:    nofields,
		precision:   precision,
		globPattern: globPattern,
		matchValues: matchValues,
		collector:   collector,
	}
	if globPattern == "*" || globPattern == "" {
		sc.globEverything = true
	} else {
		if !glob.IsGlob(globPattern) {
			sc.globSingle = true
		}
	}
	sc.col = s.getCol(key)
	if sc.col != nil {
		sc.fmap = sc.col.FieldMap()
		sc.farr = sc.col.FieldArr()
		// This fills index value in wheres/whereins
		// so we don't have to map string field names for each tested object
		var ok bool
		if len(wheres) > 0 {
			sc.wheres = make([]whereT, len(wheres))
			for i, where := range wheres {
				if where.index, ok = sc.fmap[where.field]; !ok {
					where.index = math.MaxInt32
				}
				sc.wheres[i] = where
			}
		}
		if len(whereins) > 0 {
			sc.whereins = make([]whereinT, len(whereins))
			for i, wherein := range whereins {
				if wherein.index, ok = sc.fmap[wherein.field]; !ok {
					wherein.index = math.MaxInt32
				}
				sc.whereins[i] = wherein
			}
		}
	}
	sc.fvals = make([]float64, len(sc.farr))
	return sc, nil
}

func (sc *scanner) hasFieldsOutput() bool {
	switch sc.output {
	default:
		return false
	case outputObjects, outputPoints, outputHashes, outputBounds:
		return !sc.nofields
	}
}

func (sc *scanner) writeHead() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.collector.Init(sc)
}

func (sc *scanner) writeFoot() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	cursor := sc.numberIters
	if !sc.earlyStop {
		cursor = 0
	}
	sc.collector.Complete(sc, cursor)
}

func (sc *scanner) fieldMatch(id string, fields []float64, o geojson.Object) (fvals []float64, match bool) {
	var z float64
	var gotz bool
	fvals = sc.fvals
	if !sc.hasFieldsOutput() || sc.fullFields {
		for _, where := range sc.wheres {
			if where.field == "z" {
				if !gotz {
					if point, ok := o.(*geojson.Point); ok {
						z = point.Z()
					}
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
		for _, wherein := range sc.whereins {
			var value float64
			if wherein.index < len(fields) {
				value = fields[wherein.index]
			}
			if !wherein.match(value) {
				return
			}
		}
		for _, whereval := range sc.whereevals {
			fieldsWithNames := make(map[string]float64)
			for field, idx := range sc.fmap {
				if idx < len(fields) {
					fieldsWithNames[field] = fields[idx]
				} else {
					fieldsWithNames[field] = 0
				}
			}
			if !whereval.match(id, fieldsWithNames) {
				return
			}
		}
	} else {
		copy(sc.fvals, fields)
		// fields might be shorter for this item, need to pad sw.fvals with zeros
		for i := len(fields); i < len(sc.fvals); i++ {
			sc.fvals[i] = 0
		}
		for _, where := range sc.wheres {
			if where.field == "z" {
				if !gotz {
					if point, ok := o.(*geojson.Point); ok {
						z = point.Z()
					}
				}
				if !where.match(z) {
					return
				}
				continue
			}
			var value float64
			if where.index < len(sc.fvals) {
				value = sc.fvals[where.index]
			}
			if !where.match(value) {
				return
			}
		}
		for _, wherein := range sc.whereins {
			var value float64
			if wherein.index < len(sc.fvals) {
				value = sc.fvals[wherein.index]
			}
			if !wherein.match(value) {
				return
			}
		}
		for _, whereval := range sc.whereevals {
			fieldsWithNames := make(map[string]float64)
			for field, idx := range sc.fmap {
				if idx < len(fields) {
					fieldsWithNames[field] = fields[idx]
				} else {
					fieldsWithNames[field] = 0
				}
			}
			if !whereval.match(id, fieldsWithNames) {
				return
			}
		}
	}
	match = true
	return
}

func (sc *scanner) globMatch(id string, o geojson.Object) (ok, keepGoing bool) {
	if !sc.globEverything {
		if sc.globSingle {
			if sc.globPattern != id {
				return false, true
			}
			return true, false
		}
		var val string
		if sc.matchValues {
			val = o.String()
		} else {
			val = id
		}
		ok, _ := glob.Match(sc.globPattern, val)
		if !ok {
			return false, true
		}
	}
	return true, true
}

// Increment cursor
func (sc *scanner) Offset() uint64 {
	return sc.cursor
}

func (sc *scanner) Step(n uint64) {
	sc.numberIters += n
}

// ok is whether the object passes the test and should be written
// keepGoing is whether there could be more objects to test
func (sc *scanner) testObject(id string, o geojson.Object, fields []float64) (
	ok, keepGoing bool, fieldVals []float64) {
	match, kg := sc.globMatch(id, o)
	if !match {
		return false, kg, fieldVals
	}
	nf, ok := sc.fieldMatch(id, fields, o)
	return ok, true, nf
}

//id string, o geojson.Object, fields []float64, noLock bool
func (sc *scanner) writeObject(opts ScanObjectParams) bool {
	if !opts.noLock {
		sc.mu.Lock()
		defer sc.mu.Unlock()
	}
	atScanLimit := sc.numberIters-sc.cursor == sc.limit.scanned
	if atScanLimit {
		sc.earlyStop = true
	}
	ok, keepGoing, _ := sc.testObject(opts.id, opts.o, opts.fields)
	if !ok {
		return keepGoing && !atScanLimit
	}
	sc.count++
	if sc.output == outputCount {
		return sc.count < sc.limit.matched && !atScanLimit
	}
	if opts.clip != nil {
		opts.o = clip.Clip(opts.o, opts.clip, &sc.s.geomIndexOpts)
	}
	keepProcessing := sc.collector.ProcessItem(sc, opts)
	sc.numberItems++
	// Regular stop is when we exhausted all objects.
	// Early stop is when we either hit the limit or ProcessItem() returned false (scripts)
	if sc.numberItems == sc.limit.matched || !keepProcessing {
		sc.earlyStop = true
		return false
	}
	return keepProcessing && keepGoing && !atScanLimit
}

type scanCollector interface {
	Init(sc *scanner)
	ProcessItem(sc *scanner, opts ScanObjectParams) bool
	Complete(sc *scanner, cursor uint64)
}

func newScanCollector(msg *Message, buf *bytes.Buffer, respOut *resp.Value) scanCollector {
	switch msg.OutputType {
	case JSON:
		return &jsonScanCollector{
			buffer: buf,
		}
	case RESP:
		return &respScanCollector{
			respOut: respOut,
		}
	}
	return nil
}

type jsonScanCollector struct {
	buffer *bytes.Buffer
	once   bool
}

var _ scanCollector = (*jsonScanCollector)(nil)

func (coll *jsonScanCollector) Init(sc *scanner) {
	wr := coll.buffer
	if len(sc.farr) > 0 && sc.hasFieldsOutput() {
		wr.WriteString(`,"fields":[`)
		for i, field := range sc.farr {
			if i > 0 {
				wr.WriteByte(',')
			}
			wr.WriteString(jsonString(field))
		}
		wr.WriteByte(']')
	}
	switch sc.output {
	case outputIDs:
		wr.WriteString(`,"ids":[`)
	case outputObjects:
		wr.WriteString(`,"objects":[`)
	case outputPoints:
		wr.WriteString(`,"points":[`)
	case outputBounds:
		wr.WriteString(`,"bounds":[`)
	case outputHashes:
		wr.WriteString(`,"hashes":[`)
	case outputCount:

	}
}

func (coll *jsonScanCollector) ProcessItem(sc *scanner, opts ScanObjectParams) bool {
	wr := coll.buffer
	if coll.once {
		wr.WriteByte(',')
	} else {
		coll.once = true
	}
	if sc.output == outputIDs {
		wr.WriteString(jsonString(opts.id))
	} else {
		writeScannedObjectJSON(wr, sc, opts)
	}
	return true
}

func writeScannedObjectJSON(wr *bytes.Buffer, sc *scanner, opts ScanObjectParams) {
	var jsfields string
	if sc.hasFieldsOutput() {
		if sc.fullFields {
			if len(sc.fmap) > 0 {
				jsfields = `,"fields":{`
				var i int
				for field, idx := range sc.fmap {
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

		} else if len(sc.farr) > 0 {
			jsfields = `,"fields":[`
			for i := range sc.farr {
				if i > 0 {
					jsfields += `,`
				}
				if len(opts.fields) > i {
					jsfields += strconv.FormatFloat(opts.fields[i], 'f', -1, 64)
				} else {
					jsfields += "0"
				}
			}
			jsfields += `]`
		}
	}
	wr.WriteString(`{"id":` + jsonString(opts.id))
	switch sc.output {
	case outputObjects:
		wr.WriteString(`,"object":` + string(opts.o.AppendJSON(nil)))
	case outputPoints:
		wr.WriteString(`,"point":` + string(appendJSONSimplePoint(nil, opts.o)))
	case outputHashes:
		center := opts.o.Center()
		p := geohash.EncodeWithPrecision(center.Y, center.X, uint(sc.precision))
		wr.WriteString(`,"hash":"` + p + `"`)
	case outputBounds:
		wr.WriteString(`,"bounds":` + string(appendJSONSimpleBounds(nil, opts.o)))
	}

	wr.WriteString(jsfields)

	if opts.distance > 0 {
		wr.WriteString(`,"distance":` + strconv.FormatFloat(opts.distance, 'f', -1, 64))
	}

	wr.WriteString(`}`)
}

func (coll *jsonScanCollector) Complete(sc *scanner, cursor uint64) {
	wr := coll.buffer
	switch sc.output {
	default:
		wr.WriteByte(']')
	case outputCount:

	}
	wr.WriteString(`,"count":` + strconv.FormatUint(sc.count, 10))
	wr.WriteString(`,"cursor":` + strconv.FormatUint(cursor, 10))
}

type respScanCollector struct {
	values  []resp.Value
	respOut *resp.Value
}

var _ scanCollector = (*respScanCollector)(nil)

func (coll *respScanCollector) Init(sc *scanner) {

}

func (coll *respScanCollector) ProcessItem(sc *scanner, opts ScanObjectParams) bool {
	vals := make([]resp.Value, 1, 3)
	vals[0] = resp.StringValue(opts.id)
	if sc.output == outputIDs {
		coll.values = append(coll.values, vals[0])
	} else {
		switch sc.output {
		case outputObjects:
			vals = append(vals, resp.StringValue(opts.o.String()))
		case outputPoints:
			point := opts.o.Center()
			var z float64
			if point, ok := opts.o.(*geojson.Point); ok {
				z = point.Z()
			}
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
			p := geohash.EncodeWithPrecision(center.Y, center.X, uint(sc.precision))
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

		if sc.hasFieldsOutput() {
			fvs := orderFields(sc.fmap, sc.farr, opts.fields)
			if len(fvs) > 0 {
				fvals := make([]resp.Value, 0, len(fvs)*2)
				for _, fv := range fvs {
					fvals = append(fvals, resp.StringValue(fv.field), resp.StringValue(strconv.FormatFloat(fv.value, 'f', -1, 64)))
				}
				vals = append(vals, resp.ArrayValue(fvals))
			}
		}
		if opts.distance > 0 {
			vals = append(vals, resp.FloatValue(opts.distance))
		}

		coll.values = append(coll.values, resp.ArrayValue(vals))
	}
	return true
}

func (coll *respScanCollector) Complete(sc *scanner, cursor uint64) {
	if sc.output == outputCount {
		*coll.respOut = resp.IntegerValue(int(sc.count))
	} else {
		values := []resp.Value{
			resp.IntegerValue(int(cursor)),
			resp.ArrayValue(coll.values),
		}
		*coll.respOut = resp.ArrayValue(values)
	}
}
