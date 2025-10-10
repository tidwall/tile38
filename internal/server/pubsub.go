package server

import (
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/log"
)

const (
	pubsubChannel = iota
	pubsubPattern
)

type pubsub struct {
	mu   sync.RWMutex
	hubs [2]map[string]*subhub
}

func newPubsub() *pubsub {
	return &pubsub{
		hubs: [2]map[string]*subhub{
			make(map[string]*subhub),
			make(map[string]*subhub),
		},
	}
}

// Publish a message to subscribers
func (s *Server) Publish(channel string, message ...string) int {
	var msgs []submsg
	s.pubsub.mu.RLock()
	if hub := s.pubsub.hubs[pubsubChannel][channel]; hub != nil {
		for target := range hub.targets {
			for _, message := range message {
				msgs = append(msgs, submsg{
					kind:    pubsubChannel,
					target:  target,
					channel: channel,
					message: message,
				})
			}
		}
	}
	for pattern, hub := range s.pubsub.hubs[pubsubPattern] {
		if match.Match(channel, pattern) {
			for target := range hub.targets {
				for _, message := range message {
					msgs = append(msgs, submsg{
						kind:    pubsubPattern,
						target:  target,
						channel: channel,
						pattern: pattern,
						message: message,
					})
				}
			}
		}
	}
	s.pubsub.mu.RUnlock()

	// broadcast to clients
	for _, msg := range msgs {
		msg.target.cond.L.Lock()
		msg.target.msgs = append(msg.target.msgs, msg)
		msg.target.cond.Broadcast()
		msg.target.cond.L.Unlock()
	}

	// Broadcast to followers
	s.sendPublishQueue(channel, message...)

	return len(msgs)
}

func (ps *pubsub) register(kind int, channel string, target *subtarget) {
	ps.mu.Lock()
	hub, ok := ps.hubs[kind][channel]
	if !ok {
		hub = newSubhub()
		ps.hubs[kind][channel] = hub
	}
	hub.targets[target] = true
	ps.mu.Unlock()
}

func (ps *pubsub) unregister(kind int, channel string, target *subtarget) {
	ps.mu.Lock()
	hub, ok := ps.hubs[kind][channel]
	if ok {
		delete(hub.targets, target)
		if len(hub.targets) == 0 {
			delete(ps.hubs[kind], channel)
		}
	}
	ps.mu.Unlock()
}

type submsg struct {
	kind    byte
	target  *subtarget
	pattern string
	channel string
	message string
}

type subtarget struct {
	cond   *sync.Cond
	msgs   []submsg
	closed bool
}

func newSubtarget() *subtarget {
	target := new(subtarget)
	target.cond = sync.NewCond(&sync.Mutex{})
	return target
}

type subhub struct {
	targets map[*subtarget]bool
}

func newSubhub() *subhub {
	hub := new(subhub)
	hub.targets = make(map[*subtarget]bool)
	return hub
}

type liveSubscriptionSwitches struct {
	// no fields. everything is managed through the Message
}

func (sub liveSubscriptionSwitches) Error() string {
	return goingLive
}

func (s *Server) cmdSubscribe(msg *Message) (resp.Value, error) {
	if len(msg.Args) < 2 {
		return resp.Value{}, errInvalidNumberOfArguments
	}
	return NOMessage, liveSubscriptionSwitches{}
}

func (s *Server) cmdPsubscribe(msg *Message) (resp.Value, error) {
	if len(msg.Args) < 2 {
		return resp.Value{}, errInvalidNumberOfArguments
	}
	return NOMessage, liveSubscriptionSwitches{}
}

func (s *Server) cmdPublish(msg *Message) (resp.Value, error) {
	start := time.Now()
	if len(msg.Args) != 3 {
		return resp.Value{}, errInvalidNumberOfArguments
	}

	channel := msg.Args[1]
	message := msg.Args[2]
	//geofence := gjson.Valid(message) && gjson.Get(message, "fence").Bool()
	n := s.Publish(channel, message) //, geofence)
	var res resp.Value
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(`{"ok":true` +
			`,"published":` + strconv.FormatInt(int64(n), 10) +
			`,"elapsed":"` + time.Since(start).String() + `"}`)
	case RESP:
		res = resp.IntegerValue(n)
	}
	return res, nil
}

func (s *Server) liveSubscription(
	conn net.Conn,
	rd *PipelineReader,
	msg *Message,
	websocket bool,
) error {
	defer conn.Close() // close connection when we are done

	outputType := msg.OutputType
	connType := msg.ConnType
	if websocket {
		outputType = JSON
	} else if msg.StrictRESP {
		outputType = RESP
	}

	var start time.Time

	// write helpers
	var writeLock sync.Mutex
	write := func(data []byte) {
		writeLock.Lock()
		defer writeLock.Unlock()
		writeLiveMessage(conn, data, outputType == JSON, connType, websocket)
	}
	writeOK := func() {
		switch outputType {
		case JSON:
			write([]byte(`{"ok":true` +
				`,"elapsed":"` + time.Since(start).String() + `"}`))
		case RESP:
			write([]byte("+OK\r\n"))
		}
	}
	writePing := func(m *Message) {
		switch outputType {
		case JSON:
			if len(m.Args) > 1 {
				write([]byte(`{"ok":true,"ping":` + jsonString(m.Args[1]) + `,"elapsed":"` + time.Since(start).String() + `"}`))
			} else {
				write([]byte(`{"ok":true,"ping":"pong","elapsed":"` + time.Since(start).String() + `"}`))
			}
		case RESP:
			data := redcon.AppendArray(nil, 2)
			data = redcon.AppendBulkString(data, "PONG")
			if len(m.Args) > 1 {
				data = redcon.AppendBulkString(data, m.Args[1])
			} else {
				data = redcon.AppendBulkString(data, "")
			}
			write(data)
		}
	}
	writeWrongNumberOfArgsErr := func(command string) {
		switch outputType {
		case JSON:
			write([]byte(`{"ok":false,"err":"invalid number of arguments"` +
				`,"elapsed":"` + time.Since(start).String() + `"}`))
		case RESP:
			write([]byte("-ERR wrong number of arguments " +
				"for '" + command + "' command\r\n"))
		}
	}
	writeOnlyPubsubErr := func() {
		switch outputType {
		case JSON:
			write([]byte(`{"ok":false` +
				`,"err":"only (P)SUBSCRIBE / (P)UNSUBSCRIBE / ` +
				`PING / QUIT allowed in this context"` +
				`,"elapsed":"` + time.Since(start).String() + `"}`))
		case RESP:
			write([]byte("-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / " +
				"PING / QUIT allowed in this context\r\n"))
		}
	}
	writeSubscribe := func(command, channel string, num int) {
		switch outputType {
		case JSON:
			write([]byte(`{"ok":true` +
				`,"command":` + jsonString(command) +
				`,"channel":` + jsonString(channel) +
				`,"num":` + strconv.FormatInt(int64(num), 10) +
				`,"elapsed":"` + time.Since(start).String() + `"}`))
		case RESP:
			b := redcon.AppendArray(nil, 3)
			b = redcon.AppendBulkString(b, command)
			b = redcon.AppendBulkString(b, channel)
			b = redcon.AppendInt(b, int64(num))
			write(b)
		}
	}
	writeMessage := func(msg submsg) {
		if msg.kind == pubsubChannel {
			switch outputType {
			case JSON:
				var data []byte
				if !gjson.Valid(msg.message) {
					data = appendJSONString(nil, msg.message)
				} else {
					data = []byte(msg.message)
				}
				write(data)
			case RESP:
				b := redcon.AppendArray(nil, 3)
				b = redcon.AppendBulkString(b, "message")
				b = redcon.AppendBulkString(b, msg.channel)
				b = redcon.AppendBulkString(b, msg.message)
				write(b)
			}
		} else {
			switch outputType {
			case JSON:
				var data []byte
				if !gjson.Valid(msg.message) {
					data = appendJSONString(nil, msg.message)
				} else {
					data = []byte(msg.message)
				}
				write(data)
			case RESP:
				b := redcon.AppendArray(nil, 4)
				b = redcon.AppendBulkString(b, "pmessage")
				b = redcon.AppendBulkString(b, msg.pattern)
				b = redcon.AppendBulkString(b, msg.channel)
				b = redcon.AppendBulkString(b, msg.message)
				write(b)
			}
		}
		s.statsTotalMsgsSent.Add(1)
	}

	m := [2]map[string]bool{
		make(map[string]bool), // pubsubChannel
		make(map[string]bool), // pubsubPattern
	}

	target := newSubtarget()

	defer func() {
		for i := 0; i < 2; i++ {
			for channel := range m[i] {
				s.pubsub.unregister(i, channel, target)
			}
		}
		target.cond.L.Lock()
		target.closed = true
		target.cond.Broadcast()
		target.cond.L.Unlock()
	}()
	go func() {
		log.Debugf("pubsub open")
		defer log.Debugf("pubsub closed")
		target.cond.L.Lock()
		defer target.cond.L.Unlock()
		for {
			for len(target.msgs) > 0 {
				msgs := target.msgs
				target.msgs = nil
				target.cond.L.Unlock()
				for _, msg := range msgs {
					writeMessage(msg)
				}
				target.cond.L.Lock()
			}
			if target.closed {
				break
			}
			target.cond.Wait()
		}
	}()

	msgs := []*Message{msg}
	for {
		for _, msg := range msgs {
			start = time.Now()
			var kind int
			var un bool
			switch msg.Command() {
			case "quit":
				writeOK()
				return nil
			case "ping":
				writePing(msg)
				continue
			case "psubscribe":
				kind, un = pubsubPattern, false
			case "punsubscribe":
				kind, un = pubsubPattern, true
			case "subscribe":
				kind, un = pubsubChannel, false
			case "unsubscribe":
				kind, un = pubsubChannel, true
			default:
				writeOnlyPubsubErr()
				continue
			}
			if len(msg.Args) < 2 {
				writeWrongNumberOfArgsErr(msg.Command())
			}
			for i := 1; i < len(msg.Args); i++ {
				channel := msg.Args[i]
				if un {
					delete(m[kind], channel)
					s.pubsub.unregister(kind, channel, target)
				} else {
					m[kind][channel] = true
					s.pubsub.register(kind, channel, target)
				}
				writeSubscribe(msg.Command(), channel, len(m[0])+len(m[1]))
			}
		}
		var err error
		msgs, err = rd.ReadMessages()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}
