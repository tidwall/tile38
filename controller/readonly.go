package controller

import (
	"strings"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
)

func (c *Controller) cmdReadOnly(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var arg string
	var ok bool

	if vs, arg, ok = tokenval(vs); !ok || arg == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return server.NOMessage, errInvalidNumberOfArguments
	}
	update := false
	switch strings.ToLower(arg) {
	default:
		return server.NOMessage, errInvalidArgument(arg)
	case "yes":
		if !c.config.readOnly() {
			update = true
			c.config.setReadOnly(true)
			log.Info("read only")
		}
	case "no":
		if c.config.readOnly() {
			update = true
			c.config.setReadOnly(false)
			log.Info("read write")
		}
	}
	if update {
		c.config.write(false)
	}
	return server.OKMessage(msg, start), nil
}
