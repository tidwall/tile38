package controller

import (
	"math"
	"strconv"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/tile38/controller/glob"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/geojson"
)

// FenceMatch executes a fence match returns back json messages for fence detection.
func FenceMatch(hookName string, sw *scanWriter, fence *liveFenceSwitches, metas []FenceMeta, details *commandDetailsT) [][]byte {
	msgs := fenceMatch(hookName, sw, fence, metas, details)
	if len(fence.accept) == 0 {
		return msgs
	}
	nmsgs := make([][]byte, 0, len(msgs))
	for _, msg := range msgs {
		if fence.accept[gjson.GetBytes(msg, "command").String()] {
			nmsgs = append(nmsgs, msg)
		}
	}
	return nmsgs
}
func appendJSONTimeFormat(b []byte, t time.Time) []byte {
	b = append(b, '"')
	b = t.AppendFormat(b, "2006-01-02T15:04:05.999999999Z07:00")
	b = append(b, '"')
	return b
}
func jsonTimeFormat(t time.Time) string {
	var b []byte
	b = appendJSONTimeFormat(b, t)
	return string(b)
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
func hookJSONString(hookName string, metas []FenceMeta) string {
	return string(appendHookDetails(nil, hookName, metas))
}
func fenceMatch(hookName string, sw *scanWriter, fence *liveFenceSwitches, metas []FenceMeta, details *commandDetailsT) [][]byte {
	if details.command == "drop" {
		return [][]byte{[]byte(`{"command":"drop"` + hookJSONString(hookName, metas) + `,"time":` + jsonTimeFormat(details.timestamp) + `}`)}
	}
	if len(fence.glob) > 0 && !(len(fence.glob) == 1 && fence.glob[0] == '*') {
		match, _ := glob.Match(fence.glob, details.id)
		if !match {
			return nil
		}
	}
	if details.obj == nil || !details.obj.IsGeometry() {
		return nil
	}
	if details.command == "fset" {
		sw.mu.Lock()
		nofields := sw.nofields
		sw.mu.Unlock()
		if nofields {
			return nil
		}
	}
	if details.command == "del" {
		return [][]byte{[]byte(`{"command":"del"` + hookJSONString(hookName, metas) + `,"id":` + jsonString(details.id) + `,"time":` + jsonTimeFormat(details.timestamp) + `}`)}
	}
	var roamkeys, roamids []string
	var roammeters []float64
	var detect = "outside"
	if fence != nil {
		if fence.roam.on {
			if details.command == "set" {
				roamkeys, roamids, roammeters = fenceMatchRoam(sw.c, fence, details.key, details.id, details.obj)
			}
			if len(roamids) == 0 || len(roamids) != len(roamkeys) {
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
						ls := geojson.LineString{
							Coordinates: []geojson.Position{
								details.oldObj.CalculatedPoint(),
								details.obj.CalculatedPoint(),
							},
						}
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
	sw.mu.Lock()
	var distance float64
	if fence.distance {
		distance = details.obj.CalculatedPoint().DistanceTo(geojson.Position{X: fence.lon, Y: fence.lat, Z: 0})
	}
	sw.fmap = details.fmap
	sw.fullFields = true
	sw.msg.OutputType = server.JSON
	sw.writeObject(ScanWriterParams{
		id:       details.id,
		o:        details.obj,
		fields:   details.fields,
		noLock:   true,
		distance: distance,
	})

	if sw.wr.Len() == 0 {
		sw.mu.Unlock()
		return nil
	}

	res := make([]byte, sw.wr.Len())
	copy(res, sw.wr.Bytes())
	sw.wr.Reset()
	if len(res) > 0 && res[0] == ',' {
		res = res[1:]
	}
	if sw.output == outputIDs {
		res = []byte(`{"id":` + string(res) + `}`)
	}
	sw.mu.Unlock()

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

	var msgs [][]byte
	if fence.detect == nil || fence.detect[detect] {
		if len(res) > 0 && res[0] == '{' {
			msgs = append(msgs, makemsg(details.command, group, detect, hookName, metas, details.key, details.timestamp, res[1:]))
		} else {
			msgs = append(msgs, res)
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
			var nmsgs [][]byte
			msg := msgs[0][:len(msgs[0])-1]
			for i, id := range roamids {

				nmsg := append([]byte(nil), msg...)
				nmsg = append(nmsg, `,"nearby":{"key":`...)
				nmsg = appendJSONString(nmsg, roamkeys[i])
				nmsg = append(nmsg, `,"id":`...)
				nmsg = appendJSONString(nmsg, id)
				nmsg = append(nmsg, `,"meters":`...)
				nmsg = append(nmsg, strconv.FormatFloat(roammeters[i], 'f', -1, 64)...)

				if fence.roam.scan != "" {
					nmsg = append(nmsg, `,"scan":[`...)

					func() {
						sw.c.mu.Lock()
						defer sw.c.mu.Unlock()
						col := sw.c.getCol(roamkeys[i])
						if col != nil {
							obj, _, ok := col.Get(id)
							if ok {
								nmsg = append(nmsg, `{"id":`+jsonString(id)+`,"self":true,"object":`+obj.JSON()+`}`...)
							}
							pattern := id + fence.roam.scan
							iterator := func(oid string, o geojson.Object, fields []float64) bool {
								if oid == id {
									return true
								}
								if matched, _ := glob.Match(pattern, oid); matched {
									nmsg = append(nmsg, `,{"id":`+jsonString(oid)+`,"object":`+o.JSON()+`}`...)
								}
								return true
							}
							g := glob.Parse(pattern, false)
							if g.Limits[0] == "" && g.Limits[1] == "" {
								col.Scan(false, iterator)
							} else {
								col.ScanRange(g.Limits[0], g.Limits[1], false, iterator)
							}
						}
					}()
					nmsg = append(nmsg, ']')
				}

				nmsg = append(nmsg, '}')
				nmsg = append(nmsg, '}')
				nmsgs = append(nmsgs, nmsg)
			}
			msgs = nmsgs
		}
	}
	return msgs
}

func makemsg(command, group, detect, hookName string, metas []FenceMeta, key string, t time.Time, tail []byte) []byte {
	var buf []byte
	buf = append(append(buf, `{"command":"`...), command...)
	buf = append(append(buf, `","group":"`...), group...)
	buf = append(append(buf, `","detect":"`...), detect...)
	buf = append(buf, '"')
	buf = appendHookDetails(buf, hookName, metas)
	buf = appendJSONString(append(buf, `,"key":`...), key)
	buf = appendJSONTimeFormat(append(buf, `,"time":`...), t)
	buf = append(append(buf, ','), tail...)
	return buf
}

func fenceMatchObject(fence *liveFenceSwitches, obj geojson.Object) bool {
	if obj == nil {
		return false
	}
	if fence.roam.on {
		// we need to check this object against
		return false
	}

	if fence.cmd == "nearby" {
		return obj.Nearby(geojson.Position{X: fence.lon, Y: fence.lat, Z: 0}, fence.meters)
	}
	if fence.cmd == "within" {
		if fence.o != nil {
			return obj.Within(fence.o)
		}
		return obj.WithinBBox(geojson.BBox{
			Min: geojson.Position{X: fence.minLon, Y: fence.minLat, Z: 0},
			Max: geojson.Position{X: fence.maxLon, Y: fence.maxLat, Z: 0},
		})
	}
	if fence.cmd == "intersects" {
		if fence.o != nil {
			return obj.Intersects(fence.o)
		}
		return obj.IntersectsBBox(geojson.BBox{
			Min: geojson.Position{X: fence.minLon, Y: fence.minLat, Z: 0},
			Max: geojson.Position{X: fence.maxLon, Y: fence.maxLat, Z: 0},
		})
	}
	return false
}

func fenceMatchRoam(c *Controller, fence *liveFenceSwitches, tkey, tid string, obj geojson.Object) (keys, ids []string, meterss []float64) {
	col := c.getCol(fence.roam.key)
	if col == nil {
		return
	}
	p := obj.CalculatedPoint()
	col.Nearby(0, p.Y, p.X, fence.roam.meters, math.Inf(-1), math.Inf(+1),
		func(id string, obj geojson.Object, fields []float64) bool {
			var match bool
			if id == tid {
				return true // skip self
			}
			if fence.roam.pattern {
				match, _ = glob.Match(fence.roam.id, id)
			} else {
				match = fence.roam.id == id
			}
			if match {
				keys = append(keys, fence.roam.key)
				ids = append(ids, id)
				meterss = append(meterss, obj.CalculatedPoint().DistanceTo(p))
			}
			return true
		},
	)
	return
}
