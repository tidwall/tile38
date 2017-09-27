package controller

import (
	"bytes"
	"strings"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/glob"
	"github.com/tidwall/tile38/controller/server"
)

func (c *Controller) cmdKeys(msg *server.Message) (res resp.Value, err error) {
	var start = time.Now()
	vs := msg.Values[1:]

	empty_response := resp.SimpleStringValue("")
	var pattern string
	var ok bool
	if vs, pattern, ok = tokenval(vs); !ok || pattern == "" {
		return empty_response, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return empty_response, errInvalidNumberOfArguments
	}

	var wr = &bytes.Buffer{}
	var once bool
	if msg.OutputType == server.JSON {
		wr.WriteString(`{"ok":true,"keys":[`)
	}
	var everything bool
	var greater bool
	var greaterPivot string
	var vals []resp.Value

	iterator := func(item btree.Item) bool {
		key := item.(*collectionT).Key
		var match bool
		if everything {
			match = true
		} else if greater {
			if !strings.HasPrefix(key, greaterPivot) {
				return false
			}
			match = true
		} else {
			match, _ = glob.Match(pattern, key)
		}
		if match {
			if once {
				if msg.OutputType == server.JSON {
					wr.WriteByte(',')
				}
			} else {
				once = true
			}
			switch msg.OutputType {
			case server.JSON:
				wr.WriteString(jsonString(key))
			case server.RESP:
				vals = append(vals, resp.StringValue(key))
			}
		}
		return true
	}
	if pattern == "*" {
		everything = true
		c.cols.Ascend(iterator)
	} else {
		if strings.HasSuffix(pattern, "*") {
			greaterPivot = pattern[:len(pattern)-1]
			if glob.IsGlob(greaterPivot) {
				greater = false
				c.cols.Ascend(iterator)
			} else {
				greater = true
				c.cols.AscendGreaterOrEqual(&collectionT{Key: greaterPivot}, iterator)
			}
		} else if glob.IsGlob(pattern) {
			greater = false
			c.cols.Ascend(iterator)
		} else {
			greater = true
			greaterPivot = pattern
			c.cols.AscendGreaterOrEqual(&collectionT{Key: greaterPivot}, iterator)
		}
	}
	if msg.OutputType == server.JSON {
		wr.WriteString(`],"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(wr.String()), nil
	} else {
		return resp.ArrayValue(vals), nil
	}
}
