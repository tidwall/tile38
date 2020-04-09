package server

import (
	"bytes"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geo"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/gjson"
	"github.com/tidwall/tile38/internal/glob"
)

// FenceMatch executes a fence match returns back json messages for fence detection.
func FenceMatch(hookName string, sc *scanner, fence *liveFenceSwitches, metas []FenceMeta, details *commandDetails) []string {
	msgs := fenceMatch(hookName, sc, fence, metas, details)
	if len(fence.accept) == 0 {
		return msgs
	}
	nmsgs := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		if fence.accept[gjson.Get(msg, "command").String()] {
			nmsgs = append(nmsgs, msg)
		}
	}
	return nmsgs
}
func appendHookDetails(b []byte, hookName string, metas []FenceMeta) []byte {
	if len(hookName) > 0 {
		b = append(b, `,"hook":`...)
		b = appendJSONString(b, hookName)
	}
	if len(metas) > 0 {
		b = append(b, `,"meta":{`...)
		for i, meta := range metas {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendJSONString(b, meta.Name)
			b = append(b, ':')
			b = appendJSONString(b, meta.Value)
		}
		b = append(b, '}')
	}
	return b
}

func objIsSpatial(obj geojson.Object) bool {
	_, ok := obj.(geojson.Spatial)
	return ok
}

func hookJSONString(hookName string, metas []FenceMeta) string {
	return string(appendHookDetails(nil, hookName, metas))
}
func fenceMatch(
	hookName string, sc *scanner, fence *liveFenceSwitches,
	metas []FenceMeta, details *commandDetails,
) []string {
	if details.command == "drop" {
		return []string{
			`{"command":"drop"` + hookJSONString(hookName, metas) +
				`,"key":` + jsonString(details.key) +
				`,"time":` + jsonTimeFormat(details.timestamp) + `}`,
		}
	}
	if len(fence.glob) > 0 && !(len(fence.glob) == 1 && fence.glob[0] == '*') {
		match, _ := glob.Match(fence.glob, details.id)
		if !match {
			return nil
		}
	}
	if details.obj == nil || !objIsSpatial(details.obj) {
		return nil
	}
	if details.command == "fset" {
		sc.mu.Lock()
		nofields := sc.nofields
		sc.mu.Unlock()
		if nofields {
			return nil
		}
	}
	if details.command == "del" {
		return []string{
			`{"command":"del"` + hookJSONString(hookName, metas) +
				`,"key":` + jsonString(details.key) +
				`,"id":` + jsonString(details.id) +
				`,"time":` + jsonTimeFormat(details.timestamp) + `}`,
		}
	}
	var roamNearbys, roamFaraways []roamMatch
	var detect = "outside"
	if fence != nil {
		if fence.roam.on {
			if details.command == "set" {
				roamNearbys, roamFaraways =
					fenceMatchRoam(sc.s, fence, details.id,
						details.oldObj, details.obj)
			}
			if len(roamNearbys) == 0 && len(roamFaraways) == 0 {
				return nil
			}
			detect = "roam"
		} else {
			// not using roaming
			match1 := fenceMatchObject(fence, details.oldObj)
			match2 := fenceMatchObject(fence, details.obj)
			if match1 && match2 {
				detect = "inside"
			} else if match1 && !match2 {
				detect = "exit"
			} else if !match1 && match2 {
				detect = "enter"
				if details.command == "fset" {
					detect = "inside"
				}
			} else {
				if details.command != "fset" {
					// Maybe the old object and new object create a line that crosses the fence.
					// Must detect for that possibility.
					if details.oldObj != nil {
						ls := geojson.NewLineString(geometry.NewLine(
							[]geometry.Point{
								details.oldObj.Center(),
								details.obj.Center(),
							}, nil))
						temp := false
						if fence.cmd == "within" {
							// because we are testing if the line croses the area we need to use
							// "intersects" instead of "within".
							fence.cmd = "intersects"
							temp = true
						}
						if fenceMatchObject(fence, ls) {
							detect = "cross"
						}
						if temp {
							fence.cmd = "within"
						}
					}
				}
			}
		}
	}

	if details.fmap == nil {
		return nil
	}
	for {
		if fence.detect != nil && !fence.detect[detect] {
			if detect == "enter" {
				detect = "inside"
				continue
			}
			if detect == "exit" {
				detect = "outside"
				continue
			}
			return nil
		}
		break
	}
	sc.mu.Lock()
	var distance float64
	if fence.distance && fence.obj != nil {
		distance = details.obj.Distance(fence.obj)
	}
	sc.fmap = details.fmap
	sc.fullFields = true
	sc.writeObject(ScanObjectParams{
		id:       details.id,
		o:        details.obj,
		fields:   details.fields,
		noLock:   true,
		distance: distance,
	})

	coll := sc.collector.(*fenceScanCollector)
	if coll.buffer.Len() == 0 {
		sc.mu.Unlock()
		return nil
	}

	res := coll.buffer.String()
	coll.buffer.Reset()
	sc.mu.Unlock()

	if fence.groups == nil {
		fence.groups = make(map[string]string)
	}
	groupkey := details.key + ":" + details.id
	var group string
	var ok bool
	if detect == "enter" {
		group = bsonID()
		fence.groups[groupkey] = group
	} else if detect == "cross" {
		group = bsonID()
		delete(fence.groups, groupkey)
	} else {
		group, ok = fence.groups[groupkey]
		if !ok {
			group = bsonID()
			fence.groups[groupkey] = group
		}
	}
	var msgs []string
	if fence.detect == nil || fence.detect[detect] {
		if len(res) > 0 && res[0] == '{' {
			msgs = append(msgs, makemsg(details.command, group, detect,
				hookName, metas, details.key, details.timestamp, res[1:]))
		} else {
			msgs = append(msgs, string(res))
		}
	}
	switch detect {
	case "enter":
		if fence.detect == nil || fence.detect["inside"] {
			msgs = append(msgs, makemsg(details.command, group, "inside", hookName, metas, details.key, details.timestamp, res[1:]))
		}
	case "exit", "cross":
		if fence.detect == nil || fence.detect["outside"] {
			msgs = append(msgs, makemsg(details.command, group, "outside", hookName, metas, details.key, details.timestamp, res[1:]))
		}
	case "roam":
		if len(msgs) > 0 {
			var nmsgs []string
			for i := range roamNearbys {
				nmsg := extendRoamMessage(sc, fence,
					"nearby", msgs[0], roamNearbys[i])
				nmsgs = append(nmsgs, string(nmsg))
			}
			for i := range roamFaraways {
				nmsg := extendRoamMessage(sc, fence,
					"faraway", msgs[0], roamFaraways[i])
				nmsgs = append(nmsgs, string(nmsg))
			}
			msgs = nmsgs
		}
	}
	return msgs
}

func extendRoamMessage(
	sc *scanner, fence *liveFenceSwitches,
	kind string, baseMsg string, match roamMatch,
) string {
	// hack off the last '}'
	nmsg := []byte(baseMsg[:len(baseMsg)-1])
	nmsg = append(nmsg, `,"`+kind+`":{"key":`...)
	nmsg = appendJSONString(nmsg, fence.roam.key)
	nmsg = append(nmsg, `,"id":`...)
	nmsg = appendJSONString(nmsg, match.id)
	nmsg = append(nmsg, `,"object":`...)
	nmsg = match.obj.AppendJSON(nmsg)
	nmsg = append(nmsg, `,"meters":`...)
	nmsg = strconv.AppendFloat(nmsg,
		math.Floor(match.meters*1000)/1000, 'f', -1, 64)
	if fence.roam.scan != "" {
		nmsg = append(nmsg, `,"scan":[`...)
		col := sc.s.getCol(fence.roam.key)
		if col != nil {
			obj, _, ok := col.Get(match.id)
			if ok {
				nmsg = append(nmsg, `{"id":`...)
				nmsg = appendJSONString(nmsg, match.id)
				nmsg = append(nmsg, `,"self":true,"object":`...)
				nmsg = obj.AppendJSON(nmsg)
				nmsg = append(nmsg, '}')
			}
			pattern := match.id + fence.roam.scan
			iterator := func(
				oid string, o geojson.Object, fields []float64,
			) bool {
				if oid == match.id {
					return true
				}
				if matched, _ := glob.Match(pattern, oid); matched {
					nmsg = append(nmsg, `,{"id":`...)
					nmsg = appendJSONString(nmsg, oid)
					nmsg = append(nmsg, `,"object":`...)
					nmsg = o.AppendJSON(nmsg)
					nmsg = append(nmsg, '}')
				}
				return true
			}
			g := glob.Parse(pattern, false)
			if g.Limits[0] == "" && g.Limits[1] == "" {
				col.Scan(false, nil, nil, iterator)
			} else {
				col.ScanRange(g.Limits[0], g.Limits[1],
					false, nil, nil, iterator)
			}
		}
		nmsg = append(nmsg, ']')
	}

	nmsg = append(nmsg, '}')

	// re-add the last '}'
	nmsg = append(nmsg, '}')
	return string(nmsg)
}

func makemsg(
	command, group, detect, hookName string,
	metas []FenceMeta, key string, t time.Time, tail string,
) string {
	var buf []byte
	buf = append(append(buf, `{"command":"`...), command...)
	buf = append(append(buf, `","group":"`...), group...)
	buf = append(append(buf, `","detect":"`...), detect...)
	buf = append(buf, '"')
	buf = appendHookDetails(buf, hookName, metas)
	buf = appendJSONString(append(buf, `,"key":`...), key)
	buf = appendJSONTimeFormat(append(buf, `,"time":`...), t)
	buf = append(append(buf, ','), tail...)
	return string(buf)
}

func fenceMatchObject(fence *liveFenceSwitches, obj geojson.Object) bool {
	gobj, _ := obj.(geojson.Object)
	if gobj == nil {
		return false
	}
	if fence.roam.on {
		// we need to check this object against
		return false
	}
	switch fence.cmd {
	case "nearby":
		// nearby is an INTERSECT on a Circle
		return gobj.Intersects(fence.obj)
	case "within":
		return gobj.Within(fence.obj)
	case "intersects":
		return gobj.Intersects(fence.obj)
	}
	return false
}

func fenceMatchNearbys(
	s *Server, fence *liveFenceSwitches,
	id string, obj geojson.Object,
) (nearbys []roamMatch) {
	if obj == nil {
		return nil
	}
	col := s.getCol(fence.roam.key)
	if col == nil {
		return nil
	}
	center := obj.Center()
	minLat, minLon, maxLat, maxLon :=
		geo.RectFromCenter(center.Y, center.X, fence.roam.meters)
	rect := geometry.Rect{
		Min: geometry.Point{X: minLon, Y: minLat},
		Max: geometry.Point{X: maxLon, Y: maxLat},
	}
	col.Intersects(geojson.NewRect(rect), 0, nil, nil, func(
		id2 string, obj2 geojson.Object, fields []float64,
	) bool {
		if s.hasExpired(fence.roam.key, id2) {
			return true // skip expired
		}
		var idMatch bool
		if id2 == id {
			return true // skip self
		}
		meters := obj.Distance(obj2)
		if meters > fence.roam.meters {
			return true // skip outside radius
		}
		if fence.roam.pattern {
			idMatch, _ = glob.Match(fence.roam.id, id2)
		} else {
			idMatch = fence.roam.id == id2
		}
		if !idMatch {
			return true // skip non-id match
		}
		match := roamMatch{
			id:     id2,
			obj:    obj2,
			meters: obj.Distance(obj2),
		}
		nearbys = append(nearbys, match)
		return true
	})
	return nearbys
}

func fenceMatchRoam(
	s *Server, fence *liveFenceSwitches,
	id string, old, obj geojson.Object,
) (nearbys, faraways []roamMatch) {
	oldNearbys := fenceMatchNearbys(s, fence, id, old)
	newNearbys := fenceMatchNearbys(s, fence, id, obj)
	// Go through all matching objects in new-nearbys and old-nearbys.
	for i := 0; i < len(oldNearbys); i++ {
		var match bool
		var j int
		for ; j < len(newNearbys); j++ {
			if newNearbys[i].id == oldNearbys[i].id {
				match = true
				break
			}
		}
		if match {
			// dwelling, more from old-nearbys
			oldNearbys[i] = oldNearbys[len(oldNearbys)-1]
			oldNearbys = oldNearbys[:len(oldNearbys)-1]
			i--
			if fence.nodwell {
				// no dwelling allowed, remove from both lists
				newNearbys[j] = newNearbys[len(newNearbys)-1]
				newNearbys = newNearbys[:len(newNearbys)-1]
			}
		}
	}
	faraways, nearbys = oldNearbys, newNearbys
	// ensure the faraways distances are to the new object
	for i := 0; i < len(faraways); i++ {
		faraways[i].meters = faraways[i].obj.Distance(obj)
	}
	sortRoamMatches(faraways)
	sortRoamMatches(nearbys)
	return nearbys, faraways
}

// sortRoamMatches stable sorts roam matches
func sortRoamMatches(matches []roamMatch) {
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].meters < matches[j].meters {
			return true
		}
		if matches[i].meters > matches[j].meters {
			return false
		}
		return matches[i].id < matches[j].id
	})
}

type fenceScanCollector struct {
	buffer *bytes.Buffer
}

var _ scanCollector = (*fenceScanCollector)(nil)

func (coll *fenceScanCollector) Init(sc *scanner) {
}

func (coll *fenceScanCollector) ProcessItem(sc *scanner, opts ScanObjectParams) bool {
	wr := coll.buffer
	if sc.output == outputIDs {
		wr.WriteString(`{"id":` + jsonString(opts.id) + "}")
	} else {
		writeScannedObjectJSON(wr, sc, opts)
	}
	return true
}

func (coll *fenceScanCollector) Complete(sc *scanner, cursor uint64) {
}
