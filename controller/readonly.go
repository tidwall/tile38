package controller

import (
	"strings"
	"time"

	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
	"github.com/tidwall/resp"
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
	backup := c.config
	switch strings.ToLower(arg) {
	default:
		return server.NOMessage, errInvalidArgument(arg)
	case "yes":
		if !c.config.ReadOnly {
			update = true
			c.config.ReadOnly = true
			log.Info("read only")
		}
	case "no":
		if c.config.ReadOnly {
			update = true
			c.config.ReadOnly = false
			log.Info("read write")
		}
	}
	if update {
		err := c.writeConfig(false)
		if err != nil {
			c.config = backup
			return server.NOMessage, err
		}
	}
	return server.OKMessage(msg, start), nil
}
