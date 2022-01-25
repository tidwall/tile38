package server

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/log"
)

// MASSINSERT num_keys num_points [minx miny maxx maxy]

func randMassInsertPosition(minLat, minLon, maxLat, maxLon float64) (float64, float64) {
	lat, lon := (rand.Float64()*(maxLat-minLat))+minLat, (rand.Float64()*(maxLon-minLon))+minLon
	return lat, lon
}

func (s *Server) cmdMassInsert(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]

	minLat, minLon, maxLat, maxLon := -90.0, -180.0, 90.0, 180.0 //37.10776, -122.67145, 38.19502, -121.62775

	var snumCols, snumPoints string
	var cols, objs int
	var ok bool
	if vs, snumCols, ok = tokenval(vs); !ok || snumCols == "" {
		return NOMessage, errInvalidNumberOfArguments
	}
	if vs, snumPoints, ok = tokenval(vs); !ok || snumPoints == "" {
		return NOMessage, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		var sminLat, sminLon, smaxLat, smaxLon string
		if vs, sminLat, ok = tokenval(vs); !ok || sminLat == "" {
			return NOMessage, errInvalidNumberOfArguments
		}
		if vs, sminLon, ok = tokenval(vs); !ok || sminLon == "" {
			return NOMessage, errInvalidNumberOfArguments
		}
		if vs, smaxLat, ok = tokenval(vs); !ok || smaxLat == "" {
			return NOMessage, errInvalidNumberOfArguments
		}
		if vs, smaxLon, ok = tokenval(vs); !ok || smaxLon == "" {
			return NOMessage, errInvalidNumberOfArguments
		}
		var err error
		if minLat, err = strconv.ParseFloat(sminLat, 64); err != nil {
			return NOMessage, err
		}
		if minLon, err = strconv.ParseFloat(sminLon, 64); err != nil {
			return NOMessage, err
		}
		if maxLat, err = strconv.ParseFloat(smaxLat, 64); err != nil {
			return NOMessage, err
		}
		if maxLon, err = strconv.ParseFloat(smaxLon, 64); err != nil {
			return NOMessage, err
		}
		if len(vs) != 0 {
			return NOMessage, errors.New("invalid number of arguments")
		}
	}
	n, err := strconv.ParseUint(snumCols, 10, 64)
	if err != nil {
		return NOMessage, errInvalidArgument(snumCols)
	}
	cols = int(n)
	n, err = strconv.ParseUint(snumPoints, 10, 64)
	if err != nil {
		return NOMessage, errInvalidArgument(snumPoints)
	}

	docmd := func(args []string) error {
		defer s.WriterLock()()
		var nmsg Message
		nmsg = *msg
		nmsg._command = ""
		nmsg.Args = args
		_, d, err := s.command(&nmsg, nil, nil)
		if err != nil {
			return err
		}
		return s.writeAOF(nmsg.Args, &d)

	}
	rand.Seed(time.Now().UnixNano())
	objs = int(n)
	var k uint64
	for i := 0; i < cols; i++ {
		key := "mi:" + strconv.FormatInt(int64(i), 10)
		func(key string) {
			// lock cycle
			for j := 0; j < objs; j++ {
				id := strconv.FormatInt(int64(j), 10)
				var values []string
				values = append(values, "set", key, id)
				fvals := []float64{
					1,            // one
					0,            // zero
					-1,           // negOne
					14,           // nibble
					20.5,         // tinyDiv10
					120,          // int8
					-120,         // int8
					20000,        // int16
					-20000,       // int16
					214748300,    // int32
					-214748300,   // int32
					2014748300,   // float64
					123.12312301, // float64
				}
				for i, fval := range fvals {
					values = append(values, "FIELD",
						fmt.Sprintf("fname:%d", i),
						strconv.FormatFloat(fval, 'f', -1, 64))
				}
				if j%8 == 0 {
					values = append(values, "STRING", fmt.Sprintf("str%v", j))
				} else {
					lat, lon := randMassInsertPosition(minLat, minLon, maxLat, maxLon)
					values = append(values, "POINT",
						strconv.FormatFloat(lat, 'f', -1, 64),
						strconv.FormatFloat(lon, 'f', -1, 64),
					)
				}
				err := docmd(values)
				if err != nil {
					log.Fatal(err)
					return
				}
				atomic.AddUint64(&k, 1)
				if j%1000 == 1000-1 {
					log.Debugf("massinsert: %s %d/%d",
						key, atomic.LoadUint64(&k), cols*objs)
				}
			}
		}(key)
	}
	log.Infof("massinsert: done %d objects", atomic.LoadUint64(&k))
	return OKMessage(msg, start), nil
}

func (s *Server) cmdSleep(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	if len(msg.Args) != 2 {
		return NOMessage, errInvalidNumberOfArguments
	}
	d, _ := strconv.ParseFloat(msg.Args[1], 64)
	time.Sleep(time.Duration(float64(time.Second) * d))
	return OKMessage(msg, start), nil
}
