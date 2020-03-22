package server

// TEST command: spatial tests without walking the tree.

import (
	"bytes"
	"strings"
	"time"

	"github.com/tidwall/geojson"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/clip"
	"github.com/tidwall/tile38/internal/collection"
)


func (s *Server) cmdTest(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]

	var ok bool
	var test string
	var clipped geojson.Object
	var area1, area2 *collection.AreaExpression
	if vs, area1, _, err = collection.ParseAreaExpression(vs, false, s.getCol, s.geomParseOpts, false); err != nil {
		return
	}
	if vs, test, ok = tokenval(vs); !ok || test == "" {
		err = errInvalidNumberOfArguments
		return
	}
	lTest := strings.ToLower(test)
	if lTest != "within" && lTest != "intersects" {
		err = errInvalidArgument(test)
		return
	}
	var wtok string
	var nvs []string
	var doClip bool
	nvs, wtok, ok = tokenval(vs)
	if ok && len(wtok) > 0 {
		switch strings.ToLower(wtok) {
		case "clip":
			vs = nvs
			if lTest != "intersects" {
				err = errInvalidArgument(wtok)
				return
			}
			doClip = true
		}
	}
	if vs, area2, _, err = collection.ParseAreaExpression(vs, doClip, s.getCol, s.geomParseOpts, false); err != nil {
		return
	}
	if doClip && (area1.IsCompound() || area2.IsCompound()) {
		err = errInvalidArgument("clip")
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
	}

	var result int
	if lTest == "within" {
		if area1.WithinExpr(area2) {
			result = 1
		}
	} else if lTest == "intersects" {
		if area1.IntersectsExpr(area2) {
			result = 1
			if doClip {
				clipped = clip.Clip(area1.Obj(), area2.Obj())
			}
		}
	}
	switch msg.OutputType {
	case JSON:
		var buf bytes.Buffer
		buf.WriteString(`{"ok":true`)
		if result != 0 {
			buf.WriteString(`,"result":true`)
		} else {
			buf.WriteString(`,"result":false`)
		}
		if clipped != nil {
			buf.WriteString(`,"object":` + clipped.JSON())
		}
		buf.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case RESP:
		if clipped != nil {
			return resp.ArrayValue([]resp.Value{
				resp.IntegerValue(result),
				resp.StringValue(clipped.JSON())}), nil
		}
		return resp.IntegerValue(result), nil
	}
	return NOMessage, nil
}
