package server

import (
	"bytes"
	"errors"
	"sort"
	"time"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geo"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/deadline"
	"github.com/tidwall/tile38/internal/glob"
)

type liveFenceSwitches struct {
	searchScanBaseTokens
	area   *collection.AreaExpression
	cmd    string
	roam   collection.RoamSwitches
	groups map[string]string
}


type roamMatch struct {
	id     string
	obj    geojson.Object
	meters float64
}

func (s liveFenceSwitches) Error() string {
	return goingLive
}

func (s liveFenceSwitches) Close() {
	for _, whereeval := range s.whereevals {
		whereeval.Close()
	}
}

func (s liveFenceSwitches) usingLua() bool {
	return len(s.whereevals) > 0
}

func (server *Server) cmdSearchArgs(
	fromFenceCmd bool, cmd string, vs []string, types []string,
) (s liveFenceSwitches, err error) {
	var t searchScanBaseTokens
	vs, s.searchScanBaseTokens, err = server.parseSearchScanBaseTokens(cmd, t, vs)
	if err != nil {
		return
	}

	if cmd == "nearby" {
		vs, s.area, s.roam, err = collection.ParseNearbyAreaExpression(vs, s.searchScanBaseTokens.fence)
	} else {
		fixMissingBounds := s.searchScanBaseTokens.output == outputBounds
		var fixedBounds bool
		vs, s.area, fixedBounds, err = collection.ParseAreaExpression(
			vs, s.clip, server.getCol, server.geomParseOpts, fixMissingBounds)
		if fixedBounds {
			s.searchScanBaseTokens.output = defaultSearchOutput
		}
	}

	if err != nil {
		return
	}

	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	return
}

var nearbyTypes = []string{"point"}
var withinOrIntersectsTypes = []string{
	"geo", "bounds", "hash", "tile", "quadkey", "get", "object", "circle"}

func (server *Server) cmdNearby(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	wr := &bytes.Buffer{}
	s, err := server.cmdSearchArgs(false, "nearby", vs, nearbyTypes)
	if err != nil {
		return NOMessage, err
	}
	if s.usingLua() {
		defer s.Close()
		defer func() {
			if r := recover(); r != nil {
				res = NOMessage
				err = errors.New(r.(string))
				return
			}
		}()
	}
	s.cmd = "nearby"
	if s.fence {
		return NOMessage, s
	}
	sw, err := server.newScanWriter(
		wr, msg, s.key, s.output, s.precision, s.glob, false,
		s.cursor, s.limit, s.wheres, s.whereins, s.whereevals, s.nofields)
	if err != nil {
		return NOMessage, err
	}
	if msg.OutputType == JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		iter := func(id string, o geojson.Object, fields []float64, dist float64) bool {
			meters := 0.0
			if s.distance {
				meters = geo.DistanceFromHaversine(dist)
			}
			return sw.writeObject(ScanWriterParams{
				id:              id,
				o:               o,
				fields:          fields,
				distance:        meters,
				noLock:          true,
				ignoreGlobMatch: true,
				skipTesting:     true,
			})
		}
		server.nearestNeighbors(&s, sw, msg.Deadline, s.area.Obj().(*geojson.Circle), iter)
	}
	sw.writeFoot()
	if msg.OutputType == JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
}

type iterItem struct {
	id     string
	o      geojson.Object
	fields []float64
	dist   float64
}

func (server *Server) nearestNeighbors(
	s *liveFenceSwitches, sw *scanWriter, dl *deadline.Deadline,
	target *geojson.Circle,
	iter func(id string, o geojson.Object, fields []float64, dist float64,
	) bool) {
	maxDist := target.Haversine()
	var items []iterItem
	sw.col.Nearby(target, sw, dl, func(id string, o geojson.Object, fields []float64) bool {
		if server.hasExpired(s.key, id) {
			return true
		}
		ok, keepGoing, _ := sw.testObject(id, o, fields, false)
		if !ok {
			return true
		}
		dist := target.HaversineTo(o.Center())
		if maxDist > 0 && dist > maxDist {
			return false
		}
		items = append(items, iterItem{id: id, o: o, fields: fields, dist: dist})
		if !keepGoing {
			return false
		}
		return uint64(len(items)) < sw.limit
	})
	sort.Slice(items, func(i, j int) bool {
		return items[i].dist < items[j].dist
	})
	for _, item := range items {
		if !iter(item.id, item.o, item.fields, item.dist) {
			return
		}
	}
}

func (server *Server) cmdWithin(msg *Message) (res resp.Value, err error) {
	return server.cmdWithinOrIntersects("within", msg)
}

func (server *Server) cmdIntersects(msg *Message) (res resp.Value, err error) {
	return server.cmdWithinOrIntersects("intersects", msg)
}

func (server *Server) cmdWithinOrIntersects(cmd string, msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]

	wr := &bytes.Buffer{}
	s, err := server.cmdSearchArgs(false, cmd, vs, withinOrIntersectsTypes)
	if err != nil {
		return NOMessage, err
	} else if s.usingLua() {
		defer s.Close()
		defer func() {
			if r := recover(); r != nil {
				res = NOMessage
				err = errors.New(r.(string))
				return
			}
		}()
	}
	s.cmd = cmd
	if s.fence {
		return NOMessage, s
	}
	sw, err := server.newScanWriter(
		wr, msg, s.key, s.output, s.precision, s.glob, false,
		s.cursor, s.limit, s.wheres, s.whereins, s.whereevals, s.nofields)
	if err != nil {
		return NOMessage, err
	}
	if msg.OutputType == JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		if cmd == "within" {
			sw.col.WithinArea(s.area, s.sparse, sw, msg.Deadline, func(
				id string, o geojson.Object, fields []float64,
			) bool {
				if server.hasExpired(s.key, id) {
					return true
				}
				return sw.writeObject(ScanWriterParams{
					id:     id,
					o:      o,
					fields: fields,
					noLock: true,
				})
			})
		} else if cmd == "intersects" {
			sw.col.IntersectsArea(s.area, s.sparse, sw, msg.Deadline, func(
				id string,
				o geojson.Object,
				fields []float64,
			) bool {
				if server.hasExpired(s.key, id) {
					return true
				}
				params := ScanWriterParams{
					id:     id,
					o:      o,
					fields: fields,
					noLock: true,
				}
				if s.clip {
					params.clip = s.area.Obj()
				}
				return sw.writeObject(params)
			})
		}
	}
	sw.writeFoot()
	if msg.OutputType == JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
}

func (server *Server) cmdSeachValuesArgs(vs []string) (
	s liveFenceSwitches, err error,
) {
	var t searchScanBaseTokens
	vs, t, err = server.parseSearchScanBaseTokens("search", t, vs)
	if err != nil {
		return
	}
	s.searchScanBaseTokens = t
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	return
}

func (server *Server) cmdSearch(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]

	wr := &bytes.Buffer{}
	s, err := server.cmdSeachValuesArgs(vs)
	if err != nil {
		return NOMessage, err
	}
	if s.usingLua() {
		defer s.Close()
		defer func() {
			if r := recover(); r != nil {
				res = NOMessage
				err = errors.New(r.(string))
				return
			}
		}()
	}
	sw, err := server.newScanWriter(
		wr, msg, s.key, s.output, s.precision, s.glob, true,
		s.cursor, s.limit, s.wheres, s.whereins, s.whereevals, s.nofields)
	if err != nil {
		return NOMessage, err
	}
	if msg.OutputType == JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		if sw.output == outputCount && len(sw.wheres) == 0 && sw.globEverything == true {
			count := sw.col.Count() - int(s.cursor)
			if count < 0 {
				count = 0
			}
			sw.count = uint64(count)
		} else {
			g := glob.Parse(sw.globPattern, s.desc)
			if g.Limits[0] == "" && g.Limits[1] == "" {
				sw.col.SearchValues(s.desc, sw, msg.Deadline,
					func(id string, o geojson.Object, fields []float64) bool {
						return sw.writeObject(ScanWriterParams{
							id:     id,
							o:      o,
							fields: fields,
							noLock: true,
						})
					},
				)
			} else {
				// must disable globSingle for string value type matching because
				// globSingle is only for ID matches, not values.
				sw.globSingle = false
				sw.col.SearchValuesRange(g.Limits[0], g.Limits[1], s.desc, sw,
					msg.Deadline,
					func(id string, o geojson.Object, fields []float64) bool {
						return sw.writeObject(ScanWriterParams{
							id:     id,
							o:      o,
							fields: fields,
							noLock: true,
						})
					},
				)
			}
		}
	}
	sw.writeFoot()
	if msg.OutputType == JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
}
