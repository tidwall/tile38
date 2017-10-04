package server

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
	"github.com/tidwall/resp"
)

var errInvalidHTTP = errors.New("invalid HTTP request")

// Type is resp type
type Type int

const (
	Null Type = iota
	RESP
	Telnet
	Native
	HTTP
	WebSocket
	JSON
)

// Message is a resp message
type Message struct {
	Command    string
	Values     []resp.Value
	ConnType   Type
	OutputType Type
	Auth       string
}

// PipelineReader ...
type PipelineReader struct {
	rd     io.Reader
	wr     io.Writer
	packet [0xFFFF]byte
	buf    []byte
}

const kindHTTP redcon.Kind = 9999

// NewPipelineReader ...
func NewPipelineReader(rd io.ReadWriter) *PipelineReader {
	return &PipelineReader{rd: rd, wr: rd}
}

func readcrlfline(packet []byte) (line string, leftover []byte, ok bool) {
	for i := 1; i < len(packet); i++ {
		if packet[i] == '\n' && packet[i-1] == '\r' {
			return string(packet[:i-1]), packet[i+1:], true
		}
	}
	return "", packet, false
}

func readNextHTTPCommand(packet []byte, argsIn [][]byte, msg *Message, wr io.Writer) (
	complete bool, args [][]byte, kind redcon.Kind, leftover []byte, err error,
) {
	args = argsIn[:0]
	msg.ConnType = HTTP
	msg.OutputType = JSON
	opacket := packet

	ready, err := func() (bool, error) {
		var line string
		var ok bool

		// read header
		var headers []string
		for {
			line, packet, ok = readcrlfline(packet)
			if !ok {
				return false, nil
			}
			if line == "" {
				break
			}
			headers = append(headers, line)
		}
		parts := strings.Split(headers[0], " ")
		if len(parts) != 3 {
			return false, errInvalidHTTP
		}
		method := parts[0]
		path := parts[1]
		if len(path) == 0 || path[0] != '/' {
			return false, errInvalidHTTP
		}
		path, err = url.QueryUnescape(path[1:])
		if err != nil {
			return false, errInvalidHTTP
		}
		if method != "GET" && method != "POST" {
			return false, errInvalidHTTP
		}
		contentLength := 0
		websocket := false
		websocketVersion := 0
		websocketKey := ""
		for _, header := range headers[1:] {
			if header[0] == 'a' || header[0] == 'A' {
				if strings.HasPrefix(strings.ToLower(header), "authorization:") {
					msg.Auth = strings.TrimSpace(header[len("authorization:"):])
				}
			} else if header[0] == 'u' || header[0] == 'U' {
				if strings.HasPrefix(strings.ToLower(header), "upgrade:") && strings.ToLower(strings.TrimSpace(header[len("upgrade:"):])) == "websocket" {
					websocket = true
				}
			} else if header[0] == 's' || header[0] == 'S' {
				if strings.HasPrefix(strings.ToLower(header), "sec-websocket-version:") {
					var n uint64
					n, err = strconv.ParseUint(strings.TrimSpace(header[len("sec-websocket-version:"):]), 10, 64)
					if err != nil {
						return false, err
					}
					websocketVersion = int(n)
				} else if strings.HasPrefix(strings.ToLower(header), "sec-websocket-key:") {
					websocketKey = strings.TrimSpace(header[len("sec-websocket-key:"):])
				}
			} else if header[0] == 'c' || header[0] == 'C' {
				if strings.HasPrefix(strings.ToLower(header), "content-length:") {
					var n uint64
					n, err = strconv.ParseUint(strings.TrimSpace(header[len("content-length:"):]), 10, 64)
					if err != nil {
						return false, err
					}
					contentLength = int(n)
				}
			}
		}
		if websocket && websocketVersion >= 13 && websocketKey != "" {
			msg.ConnType = WebSocket
			if wr == nil {
				return false, errors.New("connection is nil")
			}
			sum := sha1.Sum([]byte(websocketKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			accept := base64.StdEncoding.EncodeToString(sum[:])
			wshead := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n"
			if _, err = wr.Write([]byte(wshead)); err != nil {
				println(4)
				return false, err
			}
		} else if contentLength > 0 {
			msg.ConnType = HTTP
			if len(packet) < contentLength {
				return false, nil
			}
			path += string(packet[:contentLength])
			packet = packet[contentLength:]
		}
		if path == "" {
			return true, nil
		}
		nmsg, err := readNativeMessageLine([]byte(path))
		if err != nil {
			return false, err
		}

		msg.OutputType = JSON
		msg.Values = nmsg.Values
		msg.Command = commandValues(nmsg.Values)
		return true, nil
	}()
	if err != nil || !ready {
		return false, args[:0], kindHTTP, opacket, err
	}
	return true, args[:0], kindHTTP, packet, nil
}
func readNextCommand(packet []byte, argsIn [][]byte, msg *Message, wr io.Writer) (
	complete bool, args [][]byte, kind redcon.Kind, leftover []byte, err error,
) {
	if packet[0] == 'G' || packet[0] == 'P' {
		// could be an HTTP request
		var line []byte
		for i := 1; i < len(packet); i++ {
			if packet[i] == '\n' {
				if packet[i-1] == '\r' {
					line = packet[:i+1]
					break
				}
			}
		}
		if len(line) == 0 {
			return false, argsIn[:0], redcon.Redis, packet, nil
		}
		if len(line) > 11 && string(line[len(line)-11:len(line)-5]) == " HTTP/" {
			return readNextHTTPCommand(packet, argsIn, msg, wr)
		}
	}
	return redcon.ReadNextCommand(packet, args)
}

// ReadMessages ...
func (rd *PipelineReader) ReadMessages() ([]*Message, error) {
	var msgs []*Message
moreData:
	n, err := rd.rd.Read(rd.packet[:])
	if err != nil {
		return nil, err
	}
	if n == 0 {
		// need more data
		goto moreData
	}
	data := rd.packet[:n]
	if len(rd.buf) > 0 {
		data = append(rd.buf, data...)
	}
	for len(data) > 0 {
		msg := &Message{}
		complete, args, kind, leftover, err := readNextCommand(data, nil, msg, rd.wr)
		if err != nil {
			break
		}
		if !complete {
			break
		}
		if kind != kindHTTP {
			msg.Command = strings.ToLower(string(args[0]))
			for i := 0; i < len(args); i++ {
				msg.Values = append(msg.Values, resp.BytesValue(args[i]))
			}
			switch kind {
			case redcon.Redis:
				msg.ConnType = RESP
				msg.OutputType = RESP
			case redcon.Tile38:
				msg.ConnType = Native
				msg.OutputType = JSON
			case redcon.Telnet:
				msg.ConnType = RESP
				msg.OutputType = RESP
			}
		} else if len(msg.Values) == 0 {
			return nil, errInvalidHTTP
		}
		msgs = append(msgs, msg)
		data = leftover
	}
	if len(data) > 0 {
		rd.buf = append(rd.buf[:0], data...)
	} else if len(rd.buf) > 0 {
		rd.buf = rd.buf[:0]
	}
	if err != nil && len(msgs) == 0 {
		return nil, err
	}
	return msgs, nil
}

func readNativeMessageLine(line []byte) (*Message, error) {
	values := make([]resp.Value, 0, 16)
reading:
	for len(line) != 0 {
		if line[0] == '{' {
			// The native protocol cannot understand json boundaries so it assumes that
			// a json element must be at the end of the line.
			values = append(values, resp.StringValue(string(line)))
			break
		}
		if line[0] == '"' && line[len(line)-1] == '"' {
			if len(values) > 0 &&
				strings.ToLower(values[0].String()) == "set" &&
				strings.ToLower(values[len(values)-1].String()) == "string" {
				// Setting a string value that is contained inside double quotes.
				// This is only because of the boundary issues of the native protocol.
				values = append(values, resp.StringValue(string(line[1:len(line)-1])))
				break
			}
		}
		i := 0
		for ; i < len(line); i++ {
			if line[i] == ' ' {
				value := string(line[:i])
				if value != "" {
					values = append(values, resp.StringValue(value))
				}
				line = line[i+1:]
				continue reading
			}
		}
		values = append(values, resp.StringValue(string(line)))
		break
	}
	return &Message{Command: commandValues(values), Values: values, ConnType: Native, OutputType: JSON}, nil
}

func commandValues(values []resp.Value) string {
	if len(values) == 0 {
		return ""
	}
	return strings.ToLower(values[0].String())
}
