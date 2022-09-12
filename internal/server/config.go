package server

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/glob"
)

const (
	defaultKeepAlive     = 300 // seconds
	defaultProtectedMode = "yes"
)

// Config keys
const (
	FollowHost      = "follow_host"
	FollowPort      = "follow_port"
	FollowID        = "follow_id"
	FollowPos       = "follow_pos"
	ReplicaPriority = "replica-priority"
	ServerID        = "server_id"
	ReadOnly        = "read_only"
	RequirePass     = "requirepass"
	LeaderAuth      = "leaderauth"
	ProtectedMode   = "protected-mode"
	MaxMemory       = "maxmemory"
	AutoGC          = "autogc"
	KeepAlive       = "keepalive"
	LogConfig       = "logconfig"
)

var validProperties = []string{RequirePass, LeaderAuth, ProtectedMode, MaxMemory, AutoGC, KeepAlive, LogConfig, ReplicaPriority}

// Config is a tile38 config
type Config struct {
	path string

	mu sync.RWMutex

	_followHost      string
	_followPort      int64
	_followID        string
	_followPos       int64
	_replicaPriority int64
	_serverID        string
	_readOnly        bool

	_requirePassP   string
	_requirePass    string
	_leaderAuthP    string
	_leaderAuth     string
	_protectedModeP string
	_protectedMode  string
	_maxMemoryP     string
	_maxMemory      int64
	_autoGCP        string
	_autoGC         uint64
	_keepAliveP     string
	_keepAlive      int64
	_logConfigP     interface{}
	_logConfig      string
}

func loadConfig(path string) (*Config, error) {
	var json string
	data, err := ioutil.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	} else {
		json = string(data)
	}

	config := &Config{
		path:            path,
		_followHost:     gjson.Get(json, FollowHost).String(),
		_followPort:     gjson.Get(json, FollowPort).Int(),
		_followID:       gjson.Get(json, FollowID).String(),
		_followPos:      gjson.Get(json, FollowPos).Int(),
		_serverID:       gjson.Get(json, ServerID).String(),
		_readOnly:       gjson.Get(json, ReadOnly).Bool(),
		_requirePassP:   gjson.Get(json, RequirePass).String(),
		_leaderAuthP:    gjson.Get(json, LeaderAuth).String(),
		_protectedModeP: gjson.Get(json, ProtectedMode).String(),
		_maxMemoryP:     gjson.Get(json, MaxMemory).String(),
		_autoGCP:        gjson.Get(json, AutoGC).String(),
		_keepAliveP:     gjson.Get(json, KeepAlive).String(),
		_logConfig:      gjson.Get(json, LogConfig).String(),
	}

	if config._serverID == "" {
		config._serverID = randomKey(16)
	}

	// Need to be sure we look for existence vs not zero because zero is an intentional setting
	// anything less than zero will be considered default and will result in no slave_priority
	// being output when INFO is called.
	if gjson.Get(json, ReplicaPriority).Exists() {
		config._replicaPriority = gjson.Get(json, ReplicaPriority).Int()
	} else {
		config._replicaPriority = -1
	}

	// load properties
	if err := config.setProperty(RequirePass, config._requirePassP, true); err != nil {
		return nil, err
	}
	if err := config.setProperty(LeaderAuth, config._leaderAuthP, true); err != nil {
		return nil, err
	}
	if err := config.setProperty(ProtectedMode, config._protectedModeP, true); err != nil {
		return nil, err
	}
	if err := config.setProperty(MaxMemory, config._maxMemoryP, true); err != nil {
		return nil, err
	}
	if err := config.setProperty(AutoGC, config._autoGCP, true); err != nil {
		return nil, err
	}
	if err := config.setProperty(KeepAlive, config._keepAliveP, true); err != nil {
		return nil, err
	}
	if err := config.setProperty(LogConfig, config._logConfig, true); err != nil {
		return nil, err
	}
	config.write(false)
	return config, nil
}

func (config *Config) write(writeProperties bool) {
	config.mu.Lock()
	defer config.mu.Unlock()

	if writeProperties {
		// save properties
		config._requirePassP = config._requirePass
		config._leaderAuthP = config._leaderAuth
		if config._protectedMode == defaultProtectedMode {
			config._protectedModeP = ""
		} else {
			config._protectedModeP = config._protectedMode
		}
		config._maxMemoryP = formatMemSize(config._maxMemory)
		if config._autoGC == 0 {
			config._autoGCP = ""
		} else {
			config._autoGCP = strconv.FormatUint(config._autoGC, 10)
		}
		if config._keepAlive == defaultKeepAlive {
			config._keepAliveP = ""
		} else {
			config._keepAliveP = strconv.FormatUint(uint64(config._keepAlive), 10)
		}
		if config._logConfig != "" {
			config._logConfigP = config._logConfig
		}
	}

	m := make(map[string]interface{})
	if config._followHost != "" {
		m[FollowHost] = config._followHost
	}
	if config._followPort != 0 {
		m[FollowPort] = config._followPort
	}
	if config._followID != "" {
		m[FollowID] = config._followID
	}
	if config._followPos != 0 {
		m[FollowPos] = config._followPos
	}
	if config._replicaPriority >= 0 {
		m[ReplicaPriority] = config._replicaPriority
	}
	if config._serverID != "" {
		m[ServerID] = config._serverID
	}
	if config._readOnly {
		m[ReadOnly] = config._readOnly
	}
	if config._requirePassP != "" {
		m[RequirePass] = config._requirePassP
	}
	if config._leaderAuthP != "" {
		m[LeaderAuth] = config._leaderAuthP
	}
	if config._protectedModeP != "" {
		m[ProtectedMode] = config._protectedModeP
	}
	if config._maxMemoryP != "" {
		m[MaxMemory] = config._maxMemoryP
	}
	if config._autoGCP != "" {
		m[AutoGC] = config._autoGCP
	}
	if config._keepAliveP != "" {
		m[KeepAlive] = config._keepAliveP
	}
	if config._logConfigP != "" {
		var lcfg map[string]interface{}
		json.Unmarshal([]byte(config._logConfig), &lcfg)
		if len(lcfg) > 0 {
			m[LogConfig] = lcfg
		}
	}
	data, err := json.MarshalIndent(m, "", "\t")
	if err != nil {
		panic(err)
	}
	data = append(data, '\n')
	err = ioutil.WriteFile(config.path, data, 0600)
	if err != nil {
		panic(err)
	}
}

func parseMemSize(s string) (bytes int64, ok bool) {
	if s == "" {
		return 0, true
	}
	s = strings.ToLower(s)
	var n uint64
	var sz int64
	var err error
	if strings.HasSuffix(s, "gb") {
		n, err = strconv.ParseUint(s[:len(s)-2], 10, 64)
		sz = int64(n * 1024 * 1024 * 1024)
	} else if strings.HasSuffix(s, "mb") {
		n, err = strconv.ParseUint(s[:len(s)-2], 10, 64)
		sz = int64(n * 1024 * 1024)
	} else if strings.HasSuffix(s, "kb") {
		n, err = strconv.ParseUint(s[:len(s)-2], 10, 64)
		sz = int64(n * 1024)
	} else {
		n, err = strconv.ParseUint(s, 10, 64)
		sz = int64(n)
	}
	if err != nil {
		return 0, false
	}
	return sz, true
}

func formatMemSize(sz int64) string {
	if sz <= 0 {
		return ""
	}
	if sz < 1024 {
		return strconv.FormatInt(sz, 10)
	}
	sz /= 1024
	if sz < 1024 {
		return strconv.FormatInt(sz, 10) + "kb"
	}
	sz /= 1024
	if sz < 1024 {
		return strconv.FormatInt(sz, 10) + "mb"
	}
	sz /= 1024
	return strconv.FormatInt(sz, 10) + "gb"
}

func (config *Config) setProperty(name, value string, fromLoad bool) error {
	config.mu.Lock()
	defer config.mu.Unlock()
	var invalid bool
	switch name {
	default:
		return clientErrorf("Unsupported CONFIG parameter: %s", name)
	case RequirePass:
		config._requirePass = value
	case LeaderAuth:
		config._leaderAuth = value
	case AutoGC:
		if value == "" {
			config._autoGC = 0
		} else {
			gc, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return err
			}
			config._autoGC = gc
		}
	case MaxMemory:
		sz, ok := parseMemSize(value)
		if !ok {
			return clientErrorf("Invalid argument '%s' for CONFIG SET '%s'", value, name)
		}
		config._maxMemory = sz
	case ProtectedMode:
		switch strings.ToLower(value) {
		case "":
			if fromLoad {
				config._protectedMode = defaultProtectedMode
			} else {
				invalid = true
			}
		case "yes", "no":
			config._protectedMode = strings.ToLower(value)
		default:
			invalid = true
		}
	case KeepAlive:
		if value == "" {
			config._keepAlive = defaultKeepAlive
		} else {
			keepalive, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				invalid = true
			} else {
				config._keepAlive = int64(keepalive)
			}
		}
	case LogConfig:
		if value == "" {
			config._logConfig = ""
		} else {
			config._logConfig = value
		}
	case ReplicaPriority:
		replicaPriority, err := strconv.ParseUint(value, 10, 64)
		if err != nil || replicaPriority < 0 {
			invalid = true
		} else {
			config._replicaPriority = int64(replicaPriority)
		}
	}

	if invalid {
		return clientErrorf("Invalid argument '%s' for CONFIG SET '%s'", value, name)
	}
	return nil
}

func (config *Config) getProperties(pattern string) map[string]interface{} {
	m := make(map[string]interface{})
	for _, name := range validProperties {
		matched, _ := glob.Match(pattern, name)
		if matched {
			m[name] = config.getProperty(name)
		}
	}
	return m
}

func (config *Config) getProperty(name string) string {
	config.mu.RLock()
	defer config.mu.RUnlock()
	switch name {
	default:
		return ""
	case AutoGC:
		return strconv.FormatUint(config._autoGC, 10)
	case RequirePass:
		return config._requirePass
	case LeaderAuth:
		return config._leaderAuth
	case ProtectedMode:
		return config._protectedMode
	case MaxMemory:
		return formatMemSize(config._maxMemory)
	case KeepAlive:
		return strconv.FormatUint(uint64(config._keepAlive), 10)
	case LogConfig:
		return config._logConfig
	case ReplicaPriority:
		if config._replicaPriority < 0 {
			return ""
		} else {
			return strconv.FormatUint(uint64(config._replicaPriority), 10)
		}
	}
}

func (s *Server) cmdConfigGet(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	var name string

	if vs, name, ok = tokenval(vs); !ok {
		return NOMessage, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return NOMessage, errInvalidNumberOfArguments
	}
	m := s.config.getProperties(name)
	switch msg.OutputType {
	case JSON:
		data, err := json.Marshal(m)
		if err != nil {
			return NOMessage, err
		}
		res = resp.StringValue(`{"ok":true,"properties":` + string(data) + `,"elapsed":"` + time.Since(start).String() + "\"}")
	case RESP:
		vals := respValuesSimpleMap(m)
		res = resp.ArrayValue(vals)
	}
	return
}
func (s *Server) cmdConfigSet(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	var name string

	if vs, name, ok = tokenval(vs); !ok {
		return NOMessage, errInvalidNumberOfArguments
	}
	var value string
	if vs, value, ok = tokenval(vs); !ok {
		if strings.ToLower(name) != RequirePass {
			return NOMessage, errInvalidNumberOfArguments
		}
	}
	if len(vs) != 0 {
		return NOMessage, errInvalidNumberOfArguments
	}
	if err := s.config.setProperty(name, value, false); err != nil {
		return NOMessage, err
	}
	return OKMessage(msg, start), nil
}
func (s *Server) cmdConfigRewrite(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]

	if len(vs) != 0 {
		return NOMessage, errInvalidNumberOfArguments
	}
	s.config.write(true)
	return OKMessage(msg, start), nil
}

func (config *Config) followHost() string {
	config.mu.RLock()
	v := config._followHost
	config.mu.RUnlock()
	return v
}
func (config *Config) followPort() int {
	config.mu.RLock()
	v := config._followPort
	config.mu.RUnlock()
	return int(v)
}
func (config *Config) replicaPriority() int {
	config.mu.RLock()
	v := config._replicaPriority
	config.mu.RUnlock()
	return int(v)
}
func (config *Config) serverID() string {
	config.mu.RLock()
	v := config._serverID
	config.mu.RUnlock()
	return v
}
func (config *Config) readOnly() bool {
	config.mu.RLock()
	v := config._readOnly
	config.mu.RUnlock()
	return v
}
func (config *Config) requirePass() string {
	config.mu.RLock()
	v := config._requirePass
	config.mu.RUnlock()
	return v
}
func (config *Config) leaderAuth() string {
	config.mu.RLock()
	v := config._leaderAuth
	config.mu.RUnlock()
	return v
}
func (config *Config) protectedMode() string {
	config.mu.RLock()
	v := config._protectedMode
	config.mu.RUnlock()
	return v
}
func (config *Config) maxMemory() int {
	config.mu.RLock()
	v := config._maxMemory
	config.mu.RUnlock()
	return int(v)
}
func (config *Config) autoGC() uint64 {
	config.mu.RLock()
	v := config._autoGC
	config.mu.RUnlock()
	return v
}
func (config *Config) keepAlive() int64 {
	config.mu.RLock()
	v := config._keepAlive
	config.mu.RUnlock()
	return v
}
func (config *Config) setFollowHost(v string) {
	config.mu.Lock()
	config._followHost = v
	config.mu.Unlock()
}
func (config *Config) setFollowPort(v int) {
	config.mu.Lock()
	config._followPort = int64(v)
	config.mu.Unlock()
}
func (config *Config) setReadOnly(v bool) {
	config.mu.Lock()
	config._readOnly = v
	config.mu.Unlock()
}
func (config *Config) logConfig() string {
	config.mu.RLock()
	v := config._logConfig
	config.mu.RUnlock()
	return v
}
