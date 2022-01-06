package server

import (
	"bytes"
	"time"

	"github.com/tidwall/geojson"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/glob"
	"github.com/tidwall/tile38/internal/txn"
)

func (s *Server) cmdScanArgs(vs []string) (
	ls liveFenceSwitches, err error,
) {
	var t searchScanBaseTokens
	vs, t, err = s.parseSearchScanBaseTokens("scan", t, vs)
	if err != nil {
		return
	}
	ls.searchScanBaseTokens = t
	if len(vs) != 0 {
		err = errInvalidNumberOfArguments
		return
	}
	return
}

func (s *Server) cmdScan(msg *Message, ts *txn.Status) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]

	args, err := s.cmdScanArgs(vs)
	if args.usingLua() {
		defer args.Close()
		defer func() {
			if r := recover(); r != nil {
				res = NOMessage
				err = panicToError(r)
				return
			}
		}()
	}
	if err != nil {
		return NOMessage, err
	}
	wr := &bytes.Buffer{}
	var respOut resp.Value
	sc, err := s.newScanner(
		newScanCollector(msg, wr, &respOut), args.key, args.output, args.precision, args.glob, false,
		args.cursor, args.limit, args.wheres, args.whereins, args.whereevals,
		args.nofields)
	if err != nil {
		return NOMessage, err
	}
	if msg.OutputType == JSON {
		wr.WriteString(`{"ok":true`)
	}
	sc.writeHead()
	if sc.col != nil {
		if sc.output == outputCount && len(sc.wheres) == 0 &&
			len(sc.whereins) == 0 && sc.globEverything == true {
			count := sc.col.Count() - int(args.cursor)
			if count < 0 {
				count = 0
			}
			sc.count = uint64(count)
		} else {
			g := glob.Parse(sc.globPattern, args.desc)
			if g.Limits[0] == "" && g.Limits[1] == "" {
				sc.col.Scan(args.desc, sc,
					ts,
					func(id string, o geojson.Object, fields []float64) bool {
						return sc.writeObject(ScanObjectParams{
							id:     id,
							o:      o,
							fields: fields,
						})
					},
				)
			} else {
				sc.col.ScanRange(g.Limits[0], g.Limits[1], args.desc, sc,
					ts,
					func(id string, o geojson.Object, fields []float64) bool {
						return sc.writeObject(ScanObjectParams{
							id:     id,
							o:      o,
							fields: fields,
						})
					},
				)
			}
		}
	}
	sc.writeFoot()
	if msg.OutputType == JSON {
		wr.WriteString(`,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.BytesValue(wr.Bytes()), nil
	}
	return respOut, nil
}
