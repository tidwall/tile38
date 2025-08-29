package server

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/resp"
)

// Client is an remote connection into to Tile38
type Client struct {
	id         int64          // unique id
	replPort   int            // known replication port for follower connections
	replAddr   string         // known replication addr for follower connections
	authd      bool           // client has been authenticated
	outputType Type           // Null, JSON, or RESP
	remoteAddr string         // original remote address
	in         InputStream    // input stream
	pr         PipelineReader // command reader
	out        []byte         // output write buffer

	goLiveErr error    // error type used for going line
	goLiveMsg *Message // last message for go live

	mu     sync.Mutex         // guard
	conn   io.ReadWriteCloser // out-of-loop connection.
	name   string             // optional defined name
	opened time.Time          // when the client was created/opened, unix nano
	last   time.Time          // last client request/response, unix nano

	closer io.Closer // used to close the connection
}

// Write ...
func (client *Client) Write(b []byte) (n int, err error) {
	client.out = append(client.out, b...)
	return len(b), nil
}

// CLIENT (LIST | KILL | GETNAME | SETNAME)
func (s *Server) cmdCLIENT(msg *Message, client *Client) (resp.Value, error) {
	start := time.Now()

	args := msg.Args
	if len(args) == 1 {
		return retrerr(errInvalidNumberOfArguments)
	}

	switch strings.ToLower(args[1]) {
	case "list":
		if len(args) != 2 {
			return retrerr(errInvalidNumberOfArguments)
		}
		var list []*Client
		s.connsmu.RLock()
		for _, cc := range s.conns {
			list = append(list, cc)
		}
		s.connsmu.RUnlock()
		sort.Slice(list, func(i, j int) bool {
			return list[i].id < list[j].id
		})
		now := time.Now()
		var buf []byte
		for _, client := range list {
			client.mu.Lock()
			buf = append(buf,
				fmt.Sprintf("id=%d addr=%s name=%s age=%d idle=%d\n",
					client.id,
					client.remoteAddr,
					client.name,
					now.Sub(client.opened)/time.Second,
					now.Sub(client.last)/time.Second,
				)...,
			)
			client.mu.Unlock()
		}
		if msg.OutputType == JSON {
			// Create a map of all key/value info fields
			var cmap []map[string]interface{}
			clients := strings.Split(string(buf), "\n")
			for _, client := range clients {
				client = strings.TrimSpace(client)
				m := make(map[string]interface{})
				var hasFields bool
				for _, kv := range strings.Split(client, " ") {
					kv = strings.TrimSpace(kv)
					if split := strings.SplitN(kv, "=", 2); len(split) == 2 {
						hasFields = true
						m[split[0]] = tryParseType(split[1])
					}
				}
				if hasFields {
					cmap = append(cmap, m)
				}
			}

			data, _ := json.Marshal(cmap)
			return resp.StringValue(`{"ok":true,"list":` + string(data) +
				`,"elapsed":"` + time.Since(start).String() + "\"}"), nil
		}
		return resp.BytesValue(buf), nil
	case "getname":
		if len(args) != 2 {
			return retrerr(errInvalidNumberOfArguments)
		}
		client.mu.Lock()
		name := client.name
		client.mu.Unlock()
		if msg.OutputType == JSON {
			return resp.StringValue(`{"ok":true,"name":` + jsonString(name) +
				`,"elapsed":"` + time.Since(start).String() + "\"}"), nil
		}
		return resp.StringValue(name), nil
	case "setname":
		if len(args) != 3 {
			return retrerr(errInvalidNumberOfArguments)
		}
		name := msg.Args[2]
		for i := 0; i < len(name); i++ {
			if name[i] < '!' || name[i] > '~' {
				return retrerr(clientErrorf("Client names cannot contain " +
					"spaces, newlines or special characters."))
			}
		}
		client.mu.Lock()
		client.name = name
		client.mu.Unlock()
		if msg.OutputType == JSON {
			return resp.StringValue(`{"ok":true,"elapsed":"` +
				time.Since(start).String() + "\"}"), nil
		}
		return resp.SimpleStringValue("OK"), nil
	case "kill":
		if len(args) < 3 {
			return retrerr(errInvalidNumberOfArguments)
		}
		var useAddr bool
		var addr string
		var useID bool
		var id string
		for i := 2; i < len(args); i++ {
			if useAddr || useID {
				return retrerr(errInvalidNumberOfArguments)
			}
			arg := args[i]
			if strings.Contains(arg, ":") {
				addr = arg
				useAddr = true
			} else {
				switch strings.ToLower(arg) {
				case "addr":
					i++
					if i == len(args) {
						return retrerr(errInvalidNumberOfArguments)
					}
					addr = args[i]
					useAddr = true
				case "id":
					i++
					if i == len(args) {
						return retrerr(errInvalidNumberOfArguments)
					}
					id = args[i]
					useID = true
				default:
					return retrerr(clientErrorf("No such client"))
				}
			}
		}
		var closing []io.Closer
		s.connsmu.RLock()
		for _, cc := range s.conns {
			if useID && fmt.Sprintf("%d", cc.id) == id {
				if cc.closer != nil {
					closing = append(closing, cc.closer)
				}
			} else if useAddr {
				if cc.remoteAddr == addr {
					if cc.closer != nil {
						closing = append(closing, cc.closer)
					}
				}
			}
		}
		s.connsmu.RUnlock()
		if len(closing) == 0 {
			return retrerr(clientErrorf("No such client"))
		}
		// go func() {
		// close the connections behind the scene
		for _, closer := range closing {
			closer.Close()
		}
		// }()
		if msg.OutputType == JSON {
			return resp.StringValue(`{"ok":true,"elapsed":"` +
				time.Since(start).String() + "\"}"), nil
		}
		return resp.SimpleStringValue("OK"), nil
	default:
		return retrerr(clientErrorf(
			"Syntax error, try CLIENT (LIST | KILL | GETNAME | SETNAME)",
		))
	}
}
