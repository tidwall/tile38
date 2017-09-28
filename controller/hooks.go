package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/endpoint"
	"github.com/tidwall/tile38/controller/glob"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
)

const hookLogTTL = time.Second * 30

func hookLogSetDefaults() *buntdb.SetOptions {
	if hookLogTTL > 0 {
		return &buntdb.SetOptions{
			Expires: true, // automatically delete after 30 seconds
			TTL:     hookLogTTL,
		}
	}
	return nil
}

type hooksByName []*Hook

func (a hooksByName) Len() int {
	return len(a)
}

func (a hooksByName) Less(i, j int) bool {
	return a[i].Name < a[j].Name
}

func (a hooksByName) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (c *Controller) cmdSetHook(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	start := time.Now()

	vs := msg.Values[1:]
	var name, urls, cmd string
	var ok bool
	if vs, name, ok = tokenval(vs); !ok || name == "" {
		return server.NOMessage, d, errInvalidNumberOfArguments
	}
	if vs, urls, ok = tokenval(vs); !ok || urls == "" {
		return server.NOMessage, d, errInvalidNumberOfArguments
	}
	var endpoints []string
	for _, url := range strings.Split(urls, ",") {
		url = strings.TrimSpace(url)
		err := c.epc.Validate(url)
		if err != nil {
			log.Errorf("sethook: %v", err)
			return resp.SimpleStringValue(""), d, errInvalidArgument(url)
		}
		endpoints = append(endpoints, url)
	}
	var commandvs []resp.Value
	var cmdlc string
	var types []string
	metaMap := make(map[string]string)
	for {
		commandvs = vs
		if vs, cmd, ok = tokenval(vs); !ok || cmd == "" {
			return server.NOMessage, d, errInvalidNumberOfArguments
		}
		cmdlc = strings.ToLower(cmd)
		switch cmdlc {
		default:
			return server.NOMessage, d, errInvalidArgument(cmd)
		case "meta":
			var metakey string
			var metaval string
			if vs, metakey, ok = tokenval(vs); !ok || metakey == "" {
				return server.NOMessage, d, errInvalidNumberOfArguments
			}
			if vs, metaval, ok = tokenval(vs); !ok || metaval == "" {
				return server.NOMessage, d, errInvalidNumberOfArguments
			}
			metaMap[metakey] = metaval
			continue
		case "nearby":
			types = nearbyTypes
		case "within", "intersects":
			types = withinOrIntersectsTypes
		}
		break
	}
	s, err := c.cmdSearchArgs(cmdlc, vs, types)
	if err != nil {
		return server.NOMessage, d, err
	}
	if !s.fence {
		return server.NOMessage, d, errors.New("missing FENCE argument")
	}
	s.cmd = cmdlc

	cmsg := &server.Message{}
	*cmsg = *msg
	cmsg.Values = make([]resp.Value, len(commandvs))
	for i := 0; i < len(commandvs); i++ {
		cmsg.Values[i] = commandvs[i]
	}
	cmsg.Command = strings.ToLower(cmsg.Values[0].String())

	metas := make([]FenceMeta, 0, len(metaMap))
	for key, val := range metaMap {
		metas = append(metas, FenceMeta{key, val})
	}
	sort.Sort(hookMetaByName(metas))

	hook := &Hook{
		Key:       s.key,
		Name:      name,
		Endpoints: endpoints,
		Fence:     &s,
		Message:   cmsg,
		db:        c.qdb,
		epm:       c.epc,
		Metas:     metas,
	}
	hook.cond = sync.NewCond(&hook.mu)

	var wr bytes.Buffer
	hook.ScanWriter, err = c.newScanWriter(&wr, cmsg, s.key, s.output, s.precision, s.glob, false, s.cursor, s.limit, s.wheres, s.whereins, s.nofields)
	if err != nil {
		return server.NOMessage, d, err
	}

	if h, ok := c.hooks[name]; ok {
		if h.Equals(hook) {
			// it was a match so we do nothing. But let's signal just
			// for good measure.
			h.Signal()
			switch msg.OutputType {
			case server.JSON:
				return server.OKMessage(msg, start), d, nil
			case server.RESP:
				return resp.IntegerValue(0), d, nil
			}
		}
		h.Close()
		// delete the previous hook
		if hm, ok := c.hookcols[h.Key]; ok {
			delete(hm, h.Name)
		}
		delete(c.hooks, h.Name)
	}
	d.updated = true
	d.timestamp = time.Now()
	c.hooks[name] = hook
	hm, ok := c.hookcols[hook.Key]
	if !ok {
		hm = make(map[string]*Hook)
		c.hookcols[hook.Key] = hm
	}
	hm[name] = hook
	hook.Open()
	switch msg.OutputType {
	case server.JSON:
		return server.OKMessage(msg, start), d, nil
	case server.RESP:
		return resp.IntegerValue(1), d, nil
	}
	return server.NOMessage, d, nil
}

func (c *Controller) cmdDelHook(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var name string
	var ok bool
	if vs, name, ok = tokenval(vs); !ok || name == "" {
		return server.NOMessage, d, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return server.NOMessage, d, errInvalidNumberOfArguments
	}
	if h, ok := c.hooks[name]; ok {
		h.Close()
		if hm, ok := c.hookcols[h.Key]; ok {
			delete(hm, h.Name)
		}
		delete(c.hooks, h.Name)
		d.updated = true
	}
	d.timestamp = time.Now()

	switch msg.OutputType {
	case server.JSON:
		return server.OKMessage(msg, start), d, nil
	case server.RESP:
		if d.updated {
			return resp.IntegerValue(1), d, nil
		}
		return resp.IntegerValue(0), d, nil
	}
	return
}

func (c *Controller) cmdPDelHook(msg *server.Message) (res resp.Value, d commandDetailsT, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var pattern string
	var ok bool
	if vs, pattern, ok = tokenval(vs); !ok || pattern == "" {
		return server.NOMessage, d, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return server.NOMessage, d, errInvalidNumberOfArguments
	}

	count := 0
	for name := range c.hooks {
		match, _ := glob.Match(pattern, name)
		if match {
			if h, ok := c.hooks[name]; ok {
				h.Close()
				if hm, ok := c.hookcols[h.Key]; ok {
					delete(hm, h.Name)
				}
				delete(c.hooks, h.Name)
				count++
			}
		}
	}
	d.timestamp = time.Now()

	switch msg.OutputType {
	case server.JSON:
		return server.OKMessage(msg, start), d, nil
	case server.RESP:
		return resp.IntegerValue(count), d, nil
	}
	return
}

func (c *Controller) cmdHooks(msg *server.Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Values[1:]

	var pattern string
	var ok bool

	if vs, pattern, ok = tokenval(vs); !ok || pattern == "" {
		return server.NOMessage, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return server.NOMessage, errInvalidNumberOfArguments
	}

	var hooks []*Hook
	for name, hook := range c.hooks {
		match, _ := glob.Match(pattern, name)
		if match {
			hooks = append(hooks, hook)
		}
	}
	sort.Sort(hooksByName(hooks))

	switch msg.OutputType {
	case server.JSON:
		buf := &bytes.Buffer{}
		buf.WriteString(`{"ok":true,"hooks":[`)
		for i, hook := range hooks {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(`{`)
			buf.WriteString(`"name":` + jsonString(hook.Name))
			buf.WriteString(`,"key":` + jsonString(hook.Key))
			buf.WriteString(`,"endpoints":[`)
			for i, endpoint := range hook.Endpoints {
				if i > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(jsonString(endpoint))
			}
			buf.WriteString(`],"command":[`)
			for i, v := range hook.Message.Values {
				if i > 0 {
					buf.WriteString(`,`)
				}
				buf.WriteString(jsonString(v.String()))
			}

			buf.WriteString(`]}`)
		}
		buf.WriteString(`],"elapsed":"` + time.Now().Sub(start).String() + "\"}")
		return resp.StringValue(buf.String()), nil
	case server.RESP:
		var vals []resp.Value
		for _, hook := range hooks {
			var hvals []resp.Value
			hvals = append(hvals, resp.StringValue(hook.Name))
			hvals = append(hvals, resp.StringValue(hook.Key))
			var evals []resp.Value
			for _, endpoint := range hook.Endpoints {
				evals = append(evals, resp.StringValue(endpoint))
			}
			hvals = append(hvals, resp.ArrayValue(evals))
			hvals = append(hvals, resp.ArrayValue(hook.Message.Values))
			vals = append(vals, resp.ArrayValue(hvals))
		}
		return resp.ArrayValue(vals), nil
	}
	return resp.SimpleStringValue(""), nil
}

// Hook represents a hook.
type Hook struct {
	mu         sync.Mutex
	cond       *sync.Cond
	Key        string
	Name       string
	Endpoints  []string
	Message    *server.Message
	Fence      *liveFenceSwitches
	ScanWriter *scanWriter
	Metas      []FenceMeta
	db         *buntdb.DB
	closed     bool
	opened     bool
	query      string
	epm        *endpoint.EndpointManager
}

func (h *Hook) Equals(hook *Hook) bool {
	if h.Key != hook.Key ||
		h.Name != hook.Name ||
		len(h.Endpoints) != len(hook.Endpoints) ||
		len(h.Metas) != len(hook.Metas) {
		return false
	}
	for i, endpoint := range h.Endpoints {
		if endpoint != hook.Endpoints[i] {
			return false
		}
	}
	for i, meta := range h.Metas {
		if meta.Name != hook.Metas[i].Name ||
			meta.Value != hook.Metas[i].Value {
			return false
		}
	}
	return resp.ArrayValue(h.Message.Values).Equals(
		resp.ArrayValue(hook.Message.Values))
}

type FenceMeta struct {
	Name, Value string
}

type hookMetaByName []FenceMeta

func (arr hookMetaByName) Len() int {
	return len(arr)
}

func (arr hookMetaByName) Less(a, b int) bool {
	return arr[a].Name < arr[b].Name
}

func (arr hookMetaByName) Swap(a, b int) {
	arr[a], arr[b] = arr[b], arr[a]
}

// Open is called when a hook is first created. It calls the manager
// function in a goroutine
func (h *Hook) Open() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.opened {
		return
	}
	h.opened = true
	b, _ := json.Marshal(h.Name)
	h.query = `{"hook":` + string(b) + `}`
	go h.manager()
}

// Close closed the hook and stop the manager function
func (h *Hook) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	h.cond.Broadcast()
}

// Signal can be called at any point to wake up the hook and
// notify the manager that there may be something new in the queue.
func (h *Hook) Signal() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cond.Broadcast()
}

// the manager is a forever loop that calls proc whenever there's a signal.
// it ends when the "closed" flag is set.
func (h *Hook) manager() {
	for {
		h.mu.Lock()
		for {
			if h.closed {
				h.mu.Unlock()
				return
			}
			if h.proc() {
				break
			}
			h.mu.Unlock()
			time.Sleep(time.Second / 4)
			h.mu.Lock()
		}
		h.cond.Wait()
		h.mu.Unlock()
	}
}

// proc processes queued hook logs.
// returning true will indicate that all log entries have been
// successfully handled.
func (h *Hook) proc() (ok bool) {
	var keys, vals []string
	var ttls []time.Duration
	err := h.db.Update(func(tx *buntdb.Tx) error {

		// get keys and vals
		err := tx.AscendGreaterOrEqual("hooks", h.query, func(key, val string) bool {
			if strings.HasPrefix(key, hookLogPrefix) {
				keys = append(keys, key)
				vals = append(vals, val)
			}
			return true
		})
		if err != nil {
			return err
		}

		// delete the keys
		for _, key := range keys {
			if hookLogTTL > 0 {
				ttl, err := tx.TTL(key)
				if err != nil {
					if err != buntdb.ErrNotFound {
						return err
					}
				}
				ttls = append(ttls, ttl)
			}
			_, err = tx.Delete(key)
			if err != nil {
				if err != buntdb.ErrNotFound {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Error(err)
		return false
	}

	// send each val. on failure reinsert that one and all of the following
	for i, key := range keys {
		val := vals[i]
		idx := stringToUint64(key[len(hookLogPrefix):])
		var sent bool
		for _, endpoint := range h.Endpoints {
			err := h.epm.Send(endpoint, val)
			if err != nil {
				log.Debugf("Endpoint connect/send error: %v: %v: %v", idx, endpoint, err)
				continue
			}
			log.Debugf("Endpoint send ok: %v: %v: %v", idx, endpoint, err)
			sent = true
			break
		}
		if !sent {
			// failed to send. try to reinsert the remaining. if this fails we lose log entries.
			keys = keys[i:]
			vals = vals[i:]
			if hookLogTTL > 0 {
				ttls = ttls[i:]
			}
			h.db.Update(func(tx *buntdb.Tx) error {
				for i, key := range keys {
					val := vals[i]
					var opts *buntdb.SetOptions
					if hookLogTTL > 0 {
						opts = &buntdb.SetOptions{
							Expires: true,
							TTL:     ttls[i],
						}
					}
					_, _, err := tx.Set(key, val, opts)
					if err != nil {
						return err
					}
				}
				return nil
			})
			return false
		}
	}
	return true
}

/*
// Do performs a hook.
func (hook *Hook) Do(details *commandDetailsT) error {
	var lerrs []error
	msgs := FenceMatch(hook.Name, hook.ScanWriter, hook.Fence, details)
nextMessage:
	for _, msg := range msgs {
	nextEndpoint:
		for _, endpoint := range hook.Endpoints {
			switch endpoint.Protocol {
			case HTTP:
				if err := sendHTTPMessage(endpoint, []byte(msg)); err != nil {
					lerrs = append(lerrs, err)
					continue nextEndpoint
				}
				continue nextMessage // sent
			case Disque:
				if err := sendDisqueMessage(endpoint, []byte(msg)); err != nil {
					lerrs = append(lerrs, err)
					continue nextEndpoint
				}
				continue nextMessage // sent
			}
		}
	}
	if len(lerrs) == 0 {
		//	log.Notice("YAY")
		return nil
	}
	var errmsgs []string
	for _, err := range lerrs {
		errmsgs = append(errmsgs, err.Error())
	}
	err := errors.New("not sent: " + strings.Join(errmsgs, ","))
	log.Error(err)
	return err
}*/
