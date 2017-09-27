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
	empty_response := resp.SimpleStringValue("")
	if vs, arg, ok = tokenval(vs); !ok || arg == "" {
		return empty_response, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return empty_response, errInvalidNumberOfArguments
	}
	update := false
	backup := c.config
	switch strings.ToLower(arg) {
	default:
		return empty_response, errInvalidArgument(arg)
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
			return empty_response, err
		}
	}
	return server.OKMessage(msg, start), nil
}
