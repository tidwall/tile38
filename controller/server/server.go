package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/resp"
)

// This phrase is copied nearly verbatim from Redis.
// https://github.com/antirez/redis/blob/cf42c48adcea05c1bd4b939fcd36a01f23ec6303/src/networking.c
var deniedMessage = []byte(strings.TrimSpace(`
ACCESS DENIED
Tile38 is running in protected mode because protected mode is enabled, no host
address was specified, no authentication password is requested to clients. In
this mode connections are only accepted from the loopback interface. If you
want to connect from external computers to Tile38 you may adopt one of the
following solutions:

1) Disable protected mode by sending the command 'CONFIG SET protected-mode no'
   from the loopback interface by connecting to Tile38 from the same host
   the server is running, however MAKE SURE Tile38 is not publicly accessible
   from internet if you do so. Use CONFIG REWRITE to make this change
   permanent.
2) Alternatively you can just disable the protected mode by editing the Tile38
   configuration file, and setting the 'protected-mode' option to 'no', and
   then restarting the server.
3) If you started the server manually just for testing, restart it with the
   '--protected-mode no' option.
4) Use a host address or an authentication password.

NOTE: You only need to do one of the above things in order for the server
to start accepting connections from the outside.
`) + "\r\n")

// Conn represents a server connection.
type Conn struct {
	net.Conn
	Authenticated bool
}

func (conn Conn) SetKeepAlive(period time.Duration) error {
	if tcp, ok := conn.Conn.(*net.TCPConn); ok {
		if err := tcp.SetKeepAlive(true); err != nil {
			return err
		}
		return tcp.SetKeepAlivePeriod(period)
	}
	return nil
}

var errCloseHTTP = errors.New("close http")

// ListenAndServe starts a tile38 server at the specified address.
func ListenAndServe(
	host string, port int,
	protected func() bool,
	handler func(conn *Conn, msg *Message, rd *AnyReaderWriter, w io.Writer, websocket bool) error,
	opened func(conn *Conn),
	closed func(conn *Conn),
	lnp *net.Listener,
	http bool,
) error {
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}
	if lnp != nil {
		*lnp = ln
	}
	log.Infof("The server is now ready to accept connections on port %d", port)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Error(err)
			return err
		}
		go handleConn(&Conn{Conn: conn}, protected, handler, opened, closed, http)
	}
}

// func writeCommandErr(proto client.Proto, conn *Conn, err error) error {
// 	if proto == client.HTTP || proto == client.WebSocket {
// 		conn.Write([]byte(`HTTP/1.1 500 ` + err.Error() + "\r\nConnection: close\r\n\r\n"))
// 	}
// 	return err
// }

func handleConn(
	conn *Conn,
	protected func() bool,
	handler func(conn *Conn, msg *Message, rd *AnyReaderWriter, w io.Writer, websocket bool) error,
	opened func(conn *Conn),
	closed func(conn *Conn),
	http bool,
) {
	opened(conn)
	defer closed(conn)
	addr := conn.RemoteAddr().String()
	if core.ShowDebugMessages {
		log.Debugf("opened connection: %s", addr)
		defer func() {
			log.Debugf("closed connection: %s", addr)
		}()
	}
	if !strings.HasPrefix(addr, "127.0.0.1:") && !strings.HasPrefix(addr, "[::1]:") {
		if protected() {
			// This is a protected server. Only loopback is allowed.
			conn.Write(deniedMessage)
			conn.Close()
			return
		}
	}
	defer conn.Close()
	outputType := Null
	rd := NewAnyReaderWriter(conn)
	for {
		msg, err := rd.ReadMessage()

		// Just closing connection if we have deprecated HTTP or WS connection,
		// And --http-transport = false
		if !http && (msg.ConnType == WebSocket || msg.ConnType == HTTP) {
			conn.Close()
			return
		}

		if err != nil {
			if err == io.EOF {
				return
			}
			if err == errCloseHTTP ||
				strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			log.Error(err)
			return
		}
		if msg != nil && msg.Command != "" {
			if outputType != Null {
				msg.OutputType = outputType
			}
			if msg.Command == "quit" {
				if msg.OutputType == RESP {
					io.WriteString(conn, "+OK\r\n")
				}
				return
			}
			err := handler(conn, msg, rd, conn, msg.ConnType == WebSocket)
			if err != nil {
				log.Error(err)
				return
			}
			outputType = msg.OutputType
		} else {
			conn.Write([]byte("HTTP/1.1 500 Bad Request\r\nConnection: close\r\n\r\n"))
			return
		}
		if msg.ConnType == HTTP || msg.ConnType == WebSocket {
			return
		}

	}
}

// WriteWebSocketMessage write a websocket message to an io.Writer.
func WriteWebSocketMessage(w io.Writer, data []byte) error {
	var msg []byte
	buf := make([]byte, 10+len(data))
	buf[0] = 129 // FIN + TEXT
	if len(data) <= 125 {
		buf[1] = byte(len(data))
		copy(buf[2:], data)
		msg = buf[:2+len(data)]
	} else if len(data) <= 0xFFFF {
		buf[1] = 126
		binary.BigEndian.PutUint16(buf[2:], uint16(len(data)))
		copy(buf[4:], data)
		msg = buf[:4+len(data)]
	} else {
		buf[1] = 127
		binary.BigEndian.PutUint64(buf[2:], uint64(len(data)))
		copy(buf[10:], data)
		msg = buf[:10+len(data)]
	}
	_, err := w.Write(msg)
	return err
}

// OKMessage returns a default OK message in JSON or RESP.
func OKMessage(msg *Message, start time.Time) resp.Value {
	switch msg.OutputType {
	case JSON:
		return resp.StringValue(`{"ok":true,"elapsed":"` + time.Now().Sub(start).String() + "\"}")
	case RESP:
		return resp.SimpleStringValue("OK")
	}
	return resp.SimpleStringValue("")
}

var NOMessage = resp.SimpleStringValue("")
