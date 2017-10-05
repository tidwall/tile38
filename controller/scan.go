package controller

import (
	"bytes"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/glob"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/tile38/geojson"
)

func cmdScanArgs(vs []resp.Value) (s liveFenceSwitches, err error) {
	if vs, s.searchScanBaseTokens, err = parseSearchScanBaseTokens("scan", vs); err != nil {
		return
	}
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	return
}

func (c *Controller) cmdScan(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	s, err := cmdScanArgs(vs)
	if err != nil {
		return server.NOMessage, err
	}
	wr := &bytes.Buffer{}
	sw, err := c.newScanWriter(wr, msg, s.key, s.output, s.precision, s.glob, false, s.cursor, s.limit, s.wheres, s.whereins, s.nofields)
	if err != nil {
		return server.NOMessage, err
	}
	if msg.OutputType == server.JSON {
		wr.WriteString(`{"ok":true`)
	}
	sw.writeHead()
	if sw.col != nil {
		if sw.output == outputCount && len(sw.wheres) == 0 &&
			len(sw.whereins) == 0 && sw.globEverything == true {
			count := sw.col.Count() - int(s.cursor)
			if count < 0 {
				count = 0
			}
			sw.count = uint64(count)
		} else {
			g := glob.Parse(sw.globPattern, s.desc)
			if g.Limits[0] == "" && g.Limits[1] == "" {
				sw.col.Scan(s.desc,
					func(id string, o geojson.Object, fields []float64) bool {
						return sw.writeObject(ScanWriterParams{
							id:     id,
							o:      o,
							fields: fields,
						})
					},
				)
			} else {
				sw.col.ScanRange(g.Limits[0], g.Limits[1], s.desc,
					func(id string, o geojson.Object, fields []float64) bool {
						return sw.writeObject(ScanWriterParams{
							id:     id,
							o:      o,
							fields: fields,
						})
					},
				)
			}
		}
	}
	sw.writeFoot()
	if msg.OutputType == server.JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return sw.respOut, nil
}
