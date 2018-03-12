package controller

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
)

// MASSINSERT num_keys num_points [minx miny maxx maxy]

const useRandField = true

func randMassInsertPosition(minLat, minLon, maxLat, maxLon float64) (float64, float64) {
	lat, lon := (rand.Float64()*(maxLat-minLat))+minLat, (rand.Float64()*(maxLon-minLon))+minLon
	return lat, lon
}

func (c *Controller) cmdMassInsert(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	minLat, minLon, maxLat, maxLon := -90.0, -180.0, 90.0, 180.0 //37.10776, -122.67145, 38.19502, -121.62775

	var snumCols, snumPoints string
	var cols, objs int
	var ok bool
	if vs, snumCols, ok = tokenval(vs); !ok || snumCols == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}
	if vs, snumPoints, ok = tokenval(vs); !ok || snumPoints == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		var sminLat, sminLon, smaxLat, smaxLon string
		if vs, sminLat, ok = tokenval(vs); !ok || sminLat == "" {
			return server.NOMessage, errInvalidNumberOfArguments
		}
		if vs, sminLon, ok = tokenval(vs); !ok || sminLon == "" {
			return server.NOMessage, errInvalidNumberOfArguments
		}
		if vs, smaxLat, ok = tokenval(vs); !ok || smaxLat == "" {
			return server.NOMessage, errInvalidNumberOfArguments
		}
		if vs, smaxLon, ok = tokenval(vs); !ok || smaxLon == "" {
			return server.NOMessage, errInvalidNumberOfArguments
		}
		var err error
		if minLat, err = strconv.ParseFloat(sminLat, 64); err != nil {
			return server.NOMessage, err
		}
		if minLon, err = strconv.ParseFloat(sminLon, 64); err != nil {
			return server.NOMessage, err
		}
		if maxLat, err = strconv.ParseFloat(smaxLat, 64); err != nil {
			return server.NOMessage, err
		}
		if maxLon, err = strconv.ParseFloat(smaxLon, 64); err != nil {
			return server.NOMessage, err
		}
		if len(vs) != 0 {
			return server.NOMessage, errors.New("invalid number of arguments")
		}
	}
	n, err := strconv.ParseUint(snumCols, 10, 64)
	if err != nil {
		return server.NOMessage, errInvalidArgument(snumCols)
	}
	cols = int(n)
	n, err = strconv.ParseUint(snumPoints, 10, 64)
	if err != nil {
		return server.NOMessage, errInvalidArgument(snumPoints)
	}
	docmd := func(values []resp.Value) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		nmsg := &server.Message{}
		*nmsg = *msg
		nmsg.Values = values
		nmsg.Command = strings.ToLower(values[0].String())
		var d commandDetailsT
		_, d, err = c.command(nmsg, nil, nil)
		if err != nil {
			return err
		}
		return c.writeAOF(resp.ArrayValue(nmsg.Values), &d)
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
				var values []resp.Value
				if j%8 == 0 {
					values = append(values, resp.StringValue("set"),
						resp.StringValue(key), resp.StringValue(id),
						resp.StringValue("STRING"), resp.StringValue(fmt.Sprintf("str%v", j)))
				} else {
					lat, lon := randMassInsertPosition(minLat, minLon, maxLat, maxLon)
					values = make([]resp.Value, 0, 16)
					values = append(values, resp.StringValue("set"), resp.StringValue(key), resp.StringValue(id))
					if useRandField {
						values = append(values, resp.StringValue("FIELD"), resp.StringValue("fname"), resp.FloatValue(rand.Float64()*10))
					}
					values = append(values, resp.StringValue("POINT"), resp.FloatValue(lat), resp.FloatValue(lon))
				}
				if err := docmd(values); err != nil {
					log.Fatal(err)
					return
				}
				atomic.AddUint64(&k, 1)
				if j%1000 == 1000-1 {
					log.Infof("massinsert: %s %d/%d", key, atomic.LoadUint64(&k), cols*objs)
				}
			}
		}(key)
	}
	log.Infof("massinsert: done %d objects", atomic.LoadUint64(&k))
	return server.OKMessage(msg, start), nil
}

func (c *Controller) cmdSleep(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	if len(msg.Values) != 2 {
		return server.NOMessage, errInvalidNumberOfArguments
	}
	d, _ := strconv.ParseFloat(msg.Values[1].String(), 64)
	time.Sleep(time.Duration(float64(time.Second) * d))
	return server.OKMessage(msg, start), nil
}
