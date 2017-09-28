package controller

import (
	"strings"
	"time"

	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/resp"
)

func (c *Controller) cmdOutput(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var arg string
	var ok bool

	if len(vs) != 0 {
		if _, arg, ok = tokenval(vs); !ok || arg == "" {
			return server.NOMessage, errInvalidNumberOfArguments
		}
		// Setting the original message output type will be picked up by the
		// server prior to the next command being executed.
		switch strings.ToLower(arg) {
		default:
			return server.NOMessage, errInvalidArgument(arg)
		case "json":
			msg.OutputType = server.JSON
		case "resp":
			msg.OutputType = server.RESP
		}
		return server.OKMessage(msg, start), nil
	}
	// return the output
	switch msg.OutputType {
	default:
		return server.NOMessage, nil
	case server.JSON:
		return resp.StringValue(`{"ok":true,"output":"json","elapsed":` + time.Now().Sub(start).String() + `}`), nil
	case server.RESP:
		return resp.StringValue("resp"), nil
	}
}
