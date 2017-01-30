package controller

import (
	"bytes"
	"errors"
	"strconv"
	"sync"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/collection"
	"github.com/tidwall/tile38/controller/glob"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/geojson"
)

const limitItems = 100
const capLimit = 100000

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
	c              *Controller
	wr             *bytes.Buffer
	msg            *server.Message
	col            *collection.Collection
	fmap           map[string]int
	farr           []string
	fvals          []float64
	output         outputT
	wheres         []whereT
	numberItems    uint64
	nofields       bool
	limit          uint64
	hitLimit       bool
	once           bool
	count          uint64
	precision      uint64
	globPattern    string
	globEverything bool
	globSingle     bool
	fullFields     bool
	values         []resp.Value
	matchValues    bool
	pool           ScanWriterPool
}

type ScanWriterParams struct {
	id       string
	o        geojson.Object
	fields   []float64
	distance float64
	noLock   bool
}

type ScanWriterPool []ScanWriterParams

func (s ScanWriterPool) Len() int           { return len(s) }
func (s ScanWriterPool) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ScanWriterPool) Less(i, j int) bool { return s[i].distance < s[j].distance }

func (c *Controller) newScanWriter(
	wr *bytes.Buffer, msg *server.Message, key string, output outputT,
	precision uint64, globPattern string, matchValues bool,
	limit uint64, wheres []whereT, nofields bool,
) (
	*scanWriter, error,
) {
	if limit == 0 {
		limit = limitItems
	} else if limit > capLimit {
		limit = capLimit
	}
	switch output {
	default:
		return nil, errors.New("invalid output type")
	case outputIDs, outputObjects, outputCount, outputBounds, outputPoints, outputHashes:
	}
	sw := &scanWriter{
		c:           c,
		wr:          wr,
		msg:         msg,
		limit:       limit,
		wheres:      wheres,
		output:      output,
		nofields:    nofields,
		precision:   precision,
		globPattern: globPattern,
		matchValues: matchValues,
	}
	if globPattern == "*" || globPattern == "" {
		sw.globEverything = true
	} else {
		if !glob.IsGlob(globPattern) {
			sw.globSingle = true
		}
	}
	sw.col = c.getCol(key)
	if sw.col != nil {
		sw.fmap = sw.col.FieldMap()
		sw.farr = sw.col.FieldArr()
	}
	sw.fvals = make([]float64, len(sw.farr))
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

func (sw *scanWriter) writeHead() {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	switch sw.msg.OutputType {
	case server.JSON:
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
	case server.RESP:
	}
}

func (sw *scanWriter) writeFoot(cursor uint64) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if !sw.hitLimit {
		cursor = 0
	}
	switch sw.msg.OutputType {
	case server.JSON:
		switch sw.output {
		default:
			sw.wr.WriteByte(']')
		case outputCount:

		}
		sw.wr.WriteString(`,"count":` + strconv.FormatUint(sw.count, 10))
		sw.wr.WriteString(`,"cursor":` + strconv.FormatUint(cursor, 10))
	case server.RESP:
		sw.wr.Reset()
		var data []byte
		var err error
		if sw.output == outputCount {
			data, err = resp.IntegerValue(int(sw.count)).MarshalRESP()
		} else {
			values := []resp.Value{
				resp.IntegerValue(int(cursor)),
				resp.ArrayValue(sw.values),
			}
			data, err = resp.ArrayValue(values).MarshalRESP()
		}
		if err != nil {
			panic("Eek this is bad. Marshal resp should not fail.")
		}
		sw.wr.Write(data)
	}
}

func (sw *scanWriter) fieldMatch(fields []float64, o geojson.Object) ([]float64, bool) {
	var z float64
	var gotz bool
	if !sw.hasFieldsOutput() || sw.fullFields {
		for _, where := range sw.wheres {
			if where.field == "z" {
				if !gotz {
					z = o.CalculatedPoint().Z
				}
				if !where.match(z) {
					return sw.fvals, false
				}
				continue
			}
			var value float64
			idx, ok := sw.fmap[where.field]
			if ok {
				if len(fields) > idx {
					value = fields[idx]
				}
			}
			if !where.match(value) {
				return sw.fvals, false
			}
		}
	} else {
		for idx := range sw.farr {
			var value float64
			if len(fields) > idx {
				value = fields[idx]
			}
			sw.fvals[idx] = value
		}
		for _, where := range sw.wheres {
			if where.field == "z" {
				if !gotz {
					z = o.CalculatedPoint().Z
				}
				if !where.match(z) {
					return sw.fvals, false
				}
				continue
			}
			var value float64
			idx, ok := sw.fmap[where.field]
			if ok {
				value = sw.fvals[idx]
			}
			if !where.match(value) {
				return sw.fvals, false
			}
		}
	}
	return sw.fvals, true
}

// TODO: This function is a hacky way of implementation sorted distance between points
func (sw *scanWriter) poolObject(opts ScanWriterParams) bool {
	sw.pool = append(sw.pool, opts)
	return true
}

//id string, o geojson.Object, fields []float64, noLock bool
func (sw *scanWriter) writeObject(opts ScanWriterParams) bool {
	if !opts.noLock {
		sw.mu.Lock()
		defer sw.mu.Unlock()
	}
	keepGoing := true
	if !sw.globEverything {
		if sw.globSingle {
			if sw.globPattern != opts.id {
				return true
			}
			keepGoing = false // return current object and stop iterating
		} else {
			var val string
			if sw.matchValues {
				val = opts.o.String()
			} else {
				val = opts.id
			}
			ok, _ := glob.Match(sw.globPattern, val)
			if !ok {
				return true
			}
		}
	}
	nfields, ok := sw.fieldMatch(opts.fields, opts.o)
	if !ok {
		return true
	}
	sw.count++
	if sw.output == outputCount {
		return true
	}

	switch sw.msg.OutputType {
	case server.JSON:
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
				for i, field := range nfields {
					if i > 0 {
						jsfields += ","
					}
					jsfields += strconv.FormatFloat(field, 'f', -1, 64)
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
				wr.WriteString(`,"object":` + opts.o.JSON())
			case outputPoints:
				wr.WriteString(`,"point":` + opts.o.CalculatedPoint().ExternalJSON())
			case outputHashes:
				p, err := opts.o.Geohash(int(sw.precision))
				if err != nil {
					p = ""
				}
				wr.WriteString(`,"hash":"` + p + `"`)
			case outputBounds:
				wr.WriteString(`,"bounds":` + opts.o.CalculatedBBox().ExternalJSON())
			}

			wr.WriteString(jsfields)

			if opts.distance > 0 {
				wr.WriteString(`,"distance":` + strconv.FormatFloat(opts.distance, 'f', 2, 64))
			}

			wr.WriteString(`}`)
		}
		sw.wr.Write(wr.Bytes())
	case server.RESP:
		vals := make([]resp.Value, 1, 3)
		vals[0] = resp.StringValue(opts.id)
		if sw.output == outputIDs {
			sw.values = append(sw.values, vals[0])
		} else {
			switch sw.output {
			case outputObjects:
				vals = append(vals, resp.StringValue(opts.o.String()))
			case outputPoints:
				point := opts.o.CalculatedPoint()
				if point.Z != 0 {
					vals = append(vals, resp.ArrayValue([]resp.Value{
						resp.FloatValue(point.Y),
						resp.FloatValue(point.X),
						resp.FloatValue(point.Z),
					}))
				} else {
					vals = append(vals, resp.ArrayValue([]resp.Value{
						resp.FloatValue(point.Y),
						resp.FloatValue(point.X),
					}))
				}
			case outputHashes:
				p, err := opts.o.Geohash(int(sw.precision))
				if err != nil {
					p = ""
				}
				vals = append(vals, resp.StringValue(p))
			case outputBounds:
				bbox := opts.o.CalculatedBBox()
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

			fvs := orderFields(sw.fmap, opts.fields)
			if len(fvs) > 0 {
				fvals := make([]resp.Value, 0, len(fvs)*2)
				for i, fv := range fvs {
					fvals = append(fvals, resp.StringValue(fv.field), resp.StringValue(strconv.FormatFloat(fv.value, 'f', -1, 64)))
					i++
				}
				vals = append(vals, resp.ArrayValue(fvals))
			}

			if opts.distance > 0 {
				vals = append(vals, resp.FloatValue(opts.distance))
			}

			sw.values = append(sw.values, resp.ArrayValue(vals))
		}
	}
	sw.numberItems++
	if sw.numberItems == sw.limit {
		sw.hitLimit = true
		return false
	}
	return keepGoing
}
