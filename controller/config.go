package controller

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/glob"
	"github.com/tidwall/tile38/controller/server"
)

const (
	defaultKeepAlive     = 300 // seconds
	defaultProtectedMode = "yes"
)

const (
	RequirePass   = "requirepass"
	LeaderAuth    = "leaderauth"
	ProtectedMode = "protected-mode"
	MaxMemory     = "maxmemory"
	AutoGC        = "autogc"
	KeepAlive     = "keepalive"
)

var validProperties = []string{RequirePass, LeaderAuth, ProtectedMode, MaxMemory, AutoGC, KeepAlive}

// Config is a tile38 config
type Config struct {
	FollowHost string `json:"follow_host,omitempty"`
	FollowPort int    `json:"follow_port,omitempty"`
	FollowID   string `json:"follow_id,omitempty"`
	FollowPos  int    `json:"follow_pos,omitempty"`
	ServerID   string `json:"server_id,omitempty"`
	ReadOnly   bool   `json:"read_only,omitempty"`

	// Properties
	RequirePassP   string `json:"requirepass,omitempty"`
	RequirePass    string `json:"-"`
	LeaderAuthP    string `json:"leaderauth,omitempty"`
	LeaderAuth     string `json:"-"`
	ProtectedModeP string `json:"protected-mode,omitempty"`
	ProtectedMode  string `json:"-"`
	MaxMemoryP     string `json:"maxmemory,omitempty"`
	MaxMemory      int    `json:"-"`
	AutoGCP        string `json:"autogc,omitempty"`
	AutoGC         uint64 `json:"-"`
	KeepAliveP     string `json:"keepalive,omitempty"`
	KeepAlive      int    `json:"-"`
}

func (c *Controller) loadConfig() error {
	data, err := ioutil.ReadFile(c.dir + "/config")
	if err != nil {
		if os.IsNotExist(err) {
			return c.initConfig()
		}
		return err
	}
	err = json.Unmarshal(data, &c.config)
	if err != nil {
		return err
	}
	// load properties
	if err := c.setConfigProperty(RequirePass, c.config.RequirePassP, true); err != nil {
		return err
	}
	if err := c.setConfigProperty(LeaderAuth, c.config.LeaderAuthP, true); err != nil {
		return err
	}
	if err := c.setConfigProperty(ProtectedMode, c.config.ProtectedModeP, true); err != nil {
		return err
	}
	if err := c.setConfigProperty(MaxMemory, c.config.MaxMemoryP, true); err != nil {
		return err
	}
	if err := c.setConfigProperty(AutoGC, c.config.AutoGCP, true); err != nil {
		return err
	}
	if err := c.setConfigProperty(KeepAlive, c.config.KeepAliveP, true); err != nil {
		return err
	}
	return nil
}

func parseMemSize(s string) (bytes int, ok bool) {
	if s == "" {
		return 0, true
	}
	s = strings.ToLower(s)
	var n uint64
	var sz int
	var err error
	if strings.HasSuffix(s, "gb") {
		n, err = strconv.ParseUint(s[:len(s)-2], 10, 64)
		sz = int(n * 1024 * 1024 * 1024)
	} else if strings.HasSuffix(s, "mb") {
		n, err = strconv.ParseUint(s[:len(s)-2], 10, 64)
		sz = int(n * 1024 * 1024)
	} else if strings.HasSuffix(s, "kb") {
		n, err = strconv.ParseUint(s[:len(s)-2], 10, 64)
		sz = int(n * 1024)
	} else {
		n, err = strconv.ParseUint(s, 10, 64)
		sz = int(n)
	}
	if err != nil {
		return 0, false
	}
	return sz, true
}

func formatMemSize(sz int) string {
	if sz <= 0 {
		return ""
	}
	if sz < 1024 {
		return strconv.FormatInt(int64(sz), 10)
	}
	sz /= 1024
	if sz < 1024 {
		return strconv.FormatInt(int64(sz), 10) + "kb"
	}
	sz /= 1024
	if sz < 1024 {
		return strconv.FormatInt(int64(sz), 10) + "mb"
	}
	sz /= 1024
	return strconv.FormatInt(int64(sz), 10) + "gb"
}

func (c *Controller) setConfigProperty(name, value string, fromLoad bool) error {
	var invalid bool
	switch name {
	default:
		return fmt.Errorf("Unsupported CONFIG parameter: %s", name)
	case RequirePass:
		c.config.RequirePass = value
	case LeaderAuth:
		c.config.LeaderAuth = value
	case AutoGC:
		if value == "" {
			c.config.AutoGC = 0
		} else {
			gc, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return err
			}
			c.config.AutoGC = gc
		}
	case MaxMemory:
		sz, ok := parseMemSize(value)
		if !ok {
			return fmt.Errorf("Invalid argument '%s' for CONFIG SET '%s'", value, name)
		}
		c.config.MaxMemory = sz
	case ProtectedMode:
		switch strings.ToLower(value) {
		case "":
			if fromLoad {
				c.config.ProtectedMode = defaultProtectedMode
			} else {
				invalid = true
			}
		case "yes", "no":
			c.config.ProtectedMode = strings.ToLower(value)
		default:
			invalid = true
		}
	case KeepAlive:
		if value == "" {
			c.config.KeepAlive = defaultKeepAlive
		} else {
			keepalive, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				invalid = true
			} else {
				c.config.KeepAlive = int(keepalive)
			}
		}
	}

	if invalid {
		return fmt.Errorf("Invalid argument '%s' for CONFIG SET '%s'", value, name)
	}
	return nil
}

func (c *Controller) getConfigProperties(pattern string) map[string]interface{} {
	m := make(map[string]interface{})
	for _, name := range validProperties {
		matched, _ := glob.Match(pattern, name)
		if matched {
			m[name] = c.getConfigProperty(name)
		}
	}
	return m
}
func (c *Controller) getConfigProperty(name string) string {
	switch name {
	default:
		return ""
	case AutoGC:
		return strconv.FormatUint(c.config.AutoGC, 10)
	case RequirePass:
		return c.config.RequirePass
	case LeaderAuth:
		return c.config.LeaderAuth
	case ProtectedMode:
		return c.config.ProtectedMode
	case MaxMemory:
		return formatMemSize(c.config.MaxMemory)
	case KeepAlive:
		return strconv.FormatUint(uint64(c.config.KeepAlive), 10)
	}
}

func (c *Controller) initConfig() error {
	c.config = Config{ServerID: randomKey(16)}
	return c.writeConfig(true)
}

func (c *Controller) writeConfig(writeProperties bool) error {
	var err error
	bak := c.config
	defer func() {
		if err != nil {
			// revert changes
			c.config = bak
		}
	}()
	if writeProperties {
		// save properties
		c.config.RequirePassP = c.config.RequirePass
		c.config.LeaderAuthP = c.config.LeaderAuth
		if c.config.ProtectedMode == defaultProtectedMode {
			c.config.ProtectedModeP = ""
		} else {
			c.config.ProtectedModeP = c.config.ProtectedMode
		}
		c.config.MaxMemoryP = formatMemSize(c.config.MaxMemory)
		if c.config.AutoGC == 0 {
			c.config.AutoGCP = ""
		} else {
			c.config.AutoGCP = strconv.FormatUint(c.config.AutoGC, 10)
		}
		if c.config.KeepAlive == defaultKeepAlive {
			c.config.KeepAliveP = ""
		} else {
			c.config.KeepAliveP = strconv.FormatUint(uint64(c.config.KeepAlive), 10)
		}
	}
	var data []byte
	data, err = json.MarshalIndent(c.config, "", "\t")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	err = ioutil.WriteFile(c.dir+"/config", data, 0600)
	if err != nil {
		return err
	}
	return nil
}

func (c *Controller) cmdConfigGet(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var ok bool
	var name string
	empty_response := resp.SimpleStringValue("")
	if vs, name, ok = tokenval(vs); !ok {
		return empty_response, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return empty_response, errInvalidNumberOfArguments
	}
	m := c.getConfigProperties(name)
	switch msg.OutputType {
	case server.JSON:
		data, err := json.Marshal(m)
		if err != nil {
			return empty_response, err
		}
		res = resp.StringValue(`{"ok":true,"properties":` + string(data) + `,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case server.RESP:
		vals := respValuesSimpleMap(m)
		res = resp.ArrayValue(vals)
	}
	return
}
func (c *Controller) cmdConfigSet(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var ok bool
	var name string
	empty_response := resp.SimpleStringValue("")
	if vs, name, ok = tokenval(vs); !ok {
		return empty_response, errInvalidNumberOfArguments
	}
	var value string
	if vs, value, ok = tokenval(vs); !ok {
		if strings.ToLower(name) != RequirePass {
			return empty_response, errInvalidNumberOfArguments
		}
	}
	if len(vs) != 0 {
		return empty_response, errInvalidNumberOfArguments
	}
	if err := c.setConfigProperty(name, value, false); err != nil {
		return empty_response, err
	}
	return server.OKMessage(msg, start), nil
}
func (c *Controller) cmdConfigRewrite(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	empty_response := resp.SimpleStringValue("")
	if len(vs) != 0 {
		return empty_response, errInvalidNumberOfArguments
	}
	if err := c.writeConfig(true); err != nil {
		return empty_response, err
	}
	return server.OKMessage(msg, start), nil
}
