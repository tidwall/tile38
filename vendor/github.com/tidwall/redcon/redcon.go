// Package redcon implements a Redis compatible server framework
package redcon

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
)

var (
	errUnbalancedQuotes       = &errProtocol{"unbalanced quotes in request"}
	errInvalidBulkLength      = &errProtocol{"invalid bulk length"}
	errInvalidMultiBulkLength = &errProtocol{"invalid multibulk length"}
	errDetached               = errors.New("detached")
	errIncompleteCommand      = errors.New("incomplete command")
	errTooMuchData            = errors.New("too much data")
)

type errProtocol struct {
	msg string
}

func (err *errProtocol) Error() string {
	return "Protocol error: " + err.msg
}

// Conn represents a client connection
type Conn interface {
	// RemoteAddr returns the remote address of the client connection.
	RemoteAddr() string
	// Close closes the connection.
	Close() error
	// WriteError writes an error to the client.
	WriteError(msg string)
	// WriteString writes a string to the client.
	WriteString(str string)
	// WriteBulk writes bulk bytes to the client.
	WriteBulk(bulk []byte)
	// WriteBulkString writes a bulk string to the client.
	WriteBulkString(bulk string)
	// WriteInt writes an integer to the client.
	WriteInt(num int)
	// WriteInt64 writes a 64-but signed integer to the client.
	WriteInt64(num int64)
	// WriteArray writes an array header. You must then write additional
	// sub-responses to the client to complete the response.
	// For example to write two strings:
	//
	//   c.WriteArray(2)
	//   c.WriteBulk("item 1")
	//   c.WriteBulk("item 2")
	WriteArray(count int)
	// WriteNull writes a null to the client
	WriteNull()
	// WriteRaw writes raw data to the client.
	WriteRaw(data []byte)
	// Context returns a user-defined context
	Context() interface{}
	// SetContext sets a user-defined context
	SetContext(v interface{})
	// SetReadBuffer updates the buffer read size for the connection
	SetReadBuffer(bytes int)
	// Detach return a connection that is detached from the server.
	// Useful for operations like PubSub.
	//
	//   dconn := conn.Detach()
	//   go func(){
	//       defer dconn.Close()
	//       cmd, err := dconn.ReadCommand()
	//       if err != nil{
	//           fmt.Printf("read failed: %v\n", err)
	//	         return
	//       }
	//       fmt.Printf("received command: %v", cmd)
	//       hconn.WriteString("OK")
	//       if err := dconn.Flush(); err != nil{
	//           fmt.Printf("write failed: %v\n", err)
	//	         return
	//       }
	//   }()
	Detach() DetachedConn
	// ReadPipeline returns all commands in current pipeline, if any
	// The commands are removed from the pipeline.
	ReadPipeline() []Command
	// PeekPipeline returns all commands in current pipeline, if any.
	// The commands remain in the pipeline.
	PeekPipeline() []Command
	// NetConn returns the base net.Conn connection
	NetConn() net.Conn
}

// NewServer returns a new Redcon server configured on "tcp" network net.
func NewServer(addr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
) *Server {
	return NewServerNetwork("tcp", addr, handler, accept, closed)
}

// NewServerNetwork returns a new Redcon server. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
func NewServerNetwork(
	net, laddr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
) *Server {
	if handler == nil {
		panic("handler is nil")
	}
	s := &Server{
		net:     net,
		laddr:   laddr,
		handler: handler,
		accept:  accept,
		closed:  closed,
		conns:   make(map[*conn]bool),
	}
	return s
}

// NewServerNetworkTLS returns a new TLS Redcon server. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
func NewServerNetworkTLS(
	net, laddr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
	config *tls.Config,
) *TLSServer {
	if handler == nil {
		panic("handler is nil")
	}
	s := Server{
		net:     net,
		laddr:   laddr,
		handler: handler,
		accept:  accept,
		closed:  closed,
		conns:   make(map[*conn]bool),
	}

	tls := &TLSServer{
		config: config,
		Server: &s,
	}
	return tls
}

// Close stops listening on the TCP address.
// Already Accepted connections will be closed.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return errors.New("not serving")
	}
	s.done = true
	return s.ln.Close()
}

// ListenAndServe serves incoming connections.
func (s *Server) ListenAndServe() error {
	return s.ListenServeAndSignal(nil)
}

// Close stops listening on the TCP address.
// Already Accepted connections will be closed.
func (s *TLSServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return errors.New("not serving")
	}
	s.done = true
	return s.ln.Close()
}

// ListenAndServe serves incoming connections.
func (s *TLSServer) ListenAndServe() error {
	return s.ListenServeAndSignal(nil)
}

// ListenAndServe creates a new server and binds to addr configured on "tcp" network net.
func ListenAndServe(addr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
) error {
	return ListenAndServeNetwork("tcp", addr, handler, accept, closed)
}

// ListenAndServeTLS creates a new TLS server and binds to addr configured on "tcp" network net.
func ListenAndServeTLS(addr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
	config *tls.Config,
) error {
	return ListenAndServeNetworkTLS("tcp", addr, handler, accept, closed, config)
}

// ListenAndServeNetwork creates a new server and binds to addr. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
func ListenAndServeNetwork(
	net, laddr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
) error {
	return NewServerNetwork(net, laddr, handler, accept, closed).ListenAndServe()
}

// ListenAndServeNetworkTLS creates a new TLS server and binds to addr. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
func ListenAndServeNetworkTLS(
	net, laddr string,
	handler func(conn Conn, cmd Command),
	accept func(conn Conn) bool,
	closed func(conn Conn, err error),
	config *tls.Config,
) error {
	return NewServerNetworkTLS(net, laddr, handler, accept, closed, config).ListenAndServe()
}

// ListenServeAndSignal serves incoming connections and passes nil or error
// when listening. signal can be nil.
func (s *Server) ListenServeAndSignal(signal chan error) error {
	ln, err := net.Listen(s.net, s.laddr)
	if err != nil {
		if signal != nil {
			signal <- err
		}
		return err
	}
	if signal != nil {
		signal <- nil
	}
	return serve(s, ln)
}

// ListenServeAndSignal serves incoming connections and passes nil or error
// when listening. signal can be nil.
func (s *TLSServer) ListenServeAndSignal(signal chan error) error {
	ln, err := tls.Listen(s.net, s.laddr, s.config)
	if err != nil {
		if signal != nil {
			signal <- err
		}
		return err
	}
	if signal != nil {
		signal <- nil
	}
	return serve(s.Server, ln)
}

func serve(s *Server, ln net.Listener) error {
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	defer func() {
		ln.Close()
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			for c := range s.conns {
				c.Close()
			}
			s.conns = nil
		}()
	}()
	for {
		lnconn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			done := s.done
			s.mu.Unlock()
			if done {
				return nil
			}
			return err
		}
		c := &conn{
			conn: lnconn,
			addr: lnconn.RemoteAddr().String(),
			wr:   NewWriter(lnconn),
			rd:   NewReader(lnconn),
		}
		s.mu.Lock()
		s.conns[c] = true
		s.mu.Unlock()
		if s.accept != nil && !s.accept(c) {
			s.mu.Lock()
			delete(s.conns, c)
			s.mu.Unlock()
			c.Close()
			continue
		}
		go handle(s, c)
	}
}

// handle manages the server connection.
func handle(s *Server, c *conn) {
	var err error
	defer func() {
		if err != errDetached {
			// do not close the connection when a detach is detected.
			c.conn.Close()
		}
		func() {
			// remove the conn from the server
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.conns, c)
			if s.closed != nil {
				if err == io.EOF {
					err = nil
				}
				s.closed(c, err)
			}
		}()
	}()

	err = func() error {
		// read commands and feed back to the client
		for {
			// read pipeline commands
			cmds, err := c.rd.readCommands(nil)
			if err != nil {
				if err, ok := err.(*errProtocol); ok {
					// All protocol errors should attempt a response to
					// the client. Ignore write errors.
					c.wr.WriteError("ERR " + err.Error())
					c.wr.Flush()
				}
				return err
			}
			c.cmds = cmds
			for len(c.cmds) > 0 {
				cmd := c.cmds[0]
				if len(c.cmds) == 1 {
					c.cmds = nil
				} else {
					c.cmds = c.cmds[1:]
				}
				s.handler(c, cmd)
			}
			if c.detached {
				// client has been detached
				return errDetached
			}
			if c.closed {
				return nil
			}
			if err := c.wr.Flush(); err != nil {
				return err
			}
		}
	}()
}

// conn represents a client connection
type conn struct {
	conn     net.Conn
	wr       *Writer
	rd       *Reader
	addr     string
	ctx      interface{}
	detached bool
	closed   bool
	cmds     []Command
}

func (c *conn) Close() error {
	c.wr.Flush()
	c.closed = true
	return c.conn.Close()
}
func (c *conn) Context() interface{}        { return c.ctx }
func (c *conn) SetContext(v interface{})    { c.ctx = v }
func (c *conn) SetReadBuffer(n int)         {}
func (c *conn) WriteString(str string)      { c.wr.WriteString(str) }
func (c *conn) WriteBulk(bulk []byte)       { c.wr.WriteBulk(bulk) }
func (c *conn) WriteBulkString(bulk string) { c.wr.WriteBulkString(bulk) }
func (c *conn) WriteInt(num int)            { c.wr.WriteInt(num) }
func (c *conn) WriteInt64(num int64)        { c.wr.WriteInt64(num) }
func (c *conn) WriteError(msg string)       { c.wr.WriteError(msg) }
func (c *conn) WriteArray(count int)        { c.wr.WriteArray(count) }
func (c *conn) WriteNull()                  { c.wr.WriteNull() }
func (c *conn) WriteRaw(data []byte)        { c.wr.WriteRaw(data) }
func (c *conn) RemoteAddr() string          { return c.addr }
func (c *conn) ReadPipeline() []Command {
	cmds := c.cmds
	c.cmds = nil
	return cmds
}
func (c *conn) PeekPipeline() []Command {
	return c.cmds
}
func (c *conn) NetConn() net.Conn {
	return c.conn
}

// BaseWriter returns the underlying connection writer, if any
func BaseWriter(c Conn) *Writer {
	if c, ok := c.(*conn); ok {
		return c.wr
	}
	return nil
}

// DetachedConn represents a connection that is detached from the server
type DetachedConn interface {
	// Conn is the original connection
	Conn
	// ReadCommand reads the next client command.
	ReadCommand() (Command, error)
	// Flush flushes any writes to the network.
	Flush() error
}

// Detach removes the current connection from the server loop and returns
// a detached connection. This is useful for operations such as PubSub.
// The detached connection must be closed by calling Close() when done.
// All writes such as WriteString() will not be written to the client
// until Flush() is called.
func (c *conn) Detach() DetachedConn {
	c.detached = true
	cmds := c.cmds
	c.cmds = nil
	return &detachedConn{conn: c, cmds: cmds}
}

type detachedConn struct {
	*conn
	cmds []Command
}

// Flush writes and Write* calls to the client.
func (dc *detachedConn) Flush() error {
	return dc.conn.wr.Flush()
}

// ReadCommand read the next command from the client.
func (dc *detachedConn) ReadCommand() (Command, error) {
	if dc.closed {
		return Command{}, errors.New("closed")
	}
	if len(dc.cmds) > 0 {
		cmd := dc.cmds[0]
		if len(dc.cmds) == 1 {
			dc.cmds = nil
		} else {
			dc.cmds = dc.cmds[1:]
		}
		return cmd, nil
	}
	cmd, err := dc.rd.ReadCommand()
	if err != nil {
		return Command{}, err
	}
	return cmd, nil
}

// Command represent a command
type Command struct {
	// Raw is a encoded RESP message.
	Raw []byte
	// Args is a series of arguments that make up the command.
	Args [][]byte
}

// Server defines a server for clients for managing client connections.
type Server struct {
	mu      sync.Mutex
	net     string
	laddr   string
	handler func(conn Conn, cmd Command)
	accept  func(conn Conn) bool
	closed  func(conn Conn, err error)
	conns   map[*conn]bool
	ln      net.Listener
	done    bool
}

// TLSServer defines a server for clients for managing client connections.
type TLSServer struct {
	*Server
	config *tls.Config
}

// Writer allows for writing RESP messages.
type Writer struct {
	w io.Writer
	b []byte
}

// NewWriter creates a new RESP writer.
func NewWriter(wr io.Writer) *Writer {
	return &Writer{
		w: wr,
	}
}

// WriteNull writes a null to the client
func (w *Writer) WriteNull() {
	w.b = AppendNull(w.b)
}

// WriteArray writes an array header. You must then write additional
// sub-responses to the client to complete the response.
// For example to write two strings:
//
//   c.WriteArray(2)
//   c.WriteBulk("item 1")
//   c.WriteBulk("item 2")
func (w *Writer) WriteArray(count int) {
	w.b = AppendArray(w.b, count)
}

// WriteBulk writes bulk bytes to the client.
func (w *Writer) WriteBulk(bulk []byte) {
	w.b = AppendBulk(w.b, bulk)
}

// WriteBulkString writes a bulk string to the client.
func (w *Writer) WriteBulkString(bulk string) {
	w.b = AppendBulkString(w.b, bulk)
}

// Buffer returns the unflushed buffer. This is a copy so changes
// to the resulting []byte will not affect the writer.
func (w *Writer) Buffer() []byte {
	return append([]byte(nil), w.b...)
}

// SetBuffer replaces the unflushed buffer with new bytes.
func (w *Writer) SetBuffer(raw []byte) {
	w.b = w.b[:0]
	w.b = append(w.b, raw...)
}

// Flush writes all unflushed Write* calls to the underlying writer.
func (w *Writer) Flush() error {
	if _, err := w.w.Write(w.b); err != nil {
		return err
	}
	w.b = w.b[:0]
	return nil
}

// WriteError writes an error to the client.
func (w *Writer) WriteError(msg string) {
	w.b = AppendError(w.b, msg)
}

// WriteString writes a string to the client.
func (w *Writer) WriteString(msg string) {
	w.b = AppendString(w.b, msg)
}

// WriteInt writes an integer to the client.
func (w *Writer) WriteInt(num int) {
	w.WriteInt64(int64(num))
}

// WriteInt64 writes a 64-bit signed integer to the client.
func (w *Writer) WriteInt64(num int64) {
	w.b = AppendInt(w.b, num)
}

// WriteRaw writes raw data to the client.
func (w *Writer) WriteRaw(data []byte) {
	w.b = append(w.b, data...)
}

// Reader represent a reader for RESP or telnet commands.
type Reader struct {
	rd    *bufio.Reader
	buf   []byte
	start int
	end   int
	cmds  []Command
}

// NewReader returns a command reader which will read RESP or telnet commands.
func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd:  bufio.NewReader(rd),
		buf: make([]byte, 4096),
	}
}

func parseInt(b []byte) (int, bool) {
	if len(b) == 1 && b[0] >= '0' && b[0] <= '9' {
		return int(b[0] - '0'), true
	}
	var n int
	var sign bool
	var i int
	if len(b) > 0 && b[0] == '-' {
		sign = true
		i++
	}
	for ; i < len(b); i++ {
		if b[i] < '0' || b[i] > '9' {
			return 0, false
		}
		n = n*10 + int(b[i]-'0')
	}
	if sign {
		n *= -1
	}
	return n, true
}

func (rd *Reader) readCommands(leftover *int) ([]Command, error) {
	var cmds []Command
	b := rd.buf[rd.start:rd.end]
	if rd.end-rd.start == 0 && len(rd.buf) > 4096 {
		rd.buf = rd.buf[:4096]
		rd.start = 0
		rd.end = 0
	}
	if len(b) > 0 {
		// we have data, yay!
		// but is this enough data for a complete command? or multiple?
	next:
		switch b[0] {
		default:
			// just a plain text command
			for i := 0; i < len(b); i++ {
				if b[i] == '\n' {
					var line []byte
					if i > 0 && b[i-1] == '\r' {
						line = b[:i-1]
					} else {
						line = b[:i]
					}
					var cmd Command
					var quote bool
					var quotech byte
					var escape bool
				outer:
					for {
						nline := make([]byte, 0, len(line))
						for i := 0; i < len(line); i++ {
							c := line[i]
							if !quote {
								if c == ' ' {
									if len(nline) > 0 {
										cmd.Args = append(cmd.Args, nline)
									}
									line = line[i+1:]
									continue outer
								}
								if c == '"' || c == '\'' {
									if i != 0 {
										return nil, errUnbalancedQuotes
									}
									quotech = c
									quote = true
									line = line[i+1:]
									continue outer
								}
							} else {
								if escape {
									escape = false
									switch c {
									case 'n':
										c = '\n'
									case 'r':
										c = '\r'
									case 't':
										c = '\t'
									}
								} else if c == quotech {
									quote = false
									quotech = 0
									cmd.Args = append(cmd.Args, nline)
									line = line[i+1:]
									if len(line) > 0 && line[0] != ' ' {
										return nil, errUnbalancedQuotes
									}
									continue outer
								} else if c == '\\' {
									escape = true
									continue
								}
							}
							nline = append(nline, c)
						}
						if quote {
							return nil, errUnbalancedQuotes
						}
						if len(line) > 0 {
							cmd.Args = append(cmd.Args, line)
						}
						break
					}
					if len(cmd.Args) > 0 {
						// convert this to resp command syntax
						var wr Writer
						wr.WriteArray(len(cmd.Args))
						for i := range cmd.Args {
							wr.WriteBulk(cmd.Args[i])
							cmd.Args[i] = append([]byte(nil), cmd.Args[i]...)
						}
						cmd.Raw = wr.b
						cmds = append(cmds, cmd)
					}
					b = b[i+1:]
					if len(b) > 0 {
						goto next
					} else {
						goto done
					}
				}
			}
		case '*':
			// resp formatted command
			marks := make([]int, 0, 16)
		outer2:
			for i := 1; i < len(b); i++ {
				if b[i] == '\n' {
					if b[i-1] != '\r' {
						return nil, errInvalidMultiBulkLength
					}
					count, ok := parseInt(b[1 : i-1])
					if !ok || count <= 0 {
						return nil, errInvalidMultiBulkLength
					}
					marks = marks[:0]
					for j := 0; j < count; j++ {
						// read bulk length
						i++
						if i < len(b) {
							if b[i] != '$' {
								return nil, &errProtocol{"expected '$', got '" +
									string(b[i]) + "'"}
							}
							si := i
							for ; i < len(b); i++ {
								if b[i] == '\n' {
									if b[i-1] != '\r' {
										return nil, errInvalidBulkLength
									}
									size, ok := parseInt(b[si+1 : i-1])
									if !ok || size < 0 {
										return nil, errInvalidBulkLength
									}
									if i+size+2 >= len(b) {
										// not ready
										break outer2
									}
									if b[i+size+2] != '\n' ||
										b[i+size+1] != '\r' {
										return nil, errInvalidBulkLength
									}
									i++
									marks = append(marks, i, i+size)
									i += size + 1
									break
								}
							}
						}
					}
					if len(marks) == count*2 {
						var cmd Command
						if rd.rd != nil {
							// make a raw copy of the entire command when
							// there's a underlying reader.
							cmd.Raw = append([]byte(nil), b[:i+1]...)
						} else {
							// just assign the slice
							cmd.Raw = b[:i+1]
						}
						cmd.Args = make([][]byte, len(marks)/2)
						// slice up the raw command into the args based on
						// the recorded marks.
						for h := 0; h < len(marks); h += 2 {
							cmd.Args[h/2] = cmd.Raw[marks[h]:marks[h+1]]
						}
						cmds = append(cmds, cmd)
						b = b[i+1:]
						if len(b) > 0 {
							goto next
						} else {
							goto done
						}
					}
				}
			}
		}
	done:
		rd.start = rd.end - len(b)
	}
	if leftover != nil {
		*leftover = rd.end - rd.start
	}
	if len(cmds) > 0 {
		return cmds, nil
	}
	if rd.rd == nil {
		return nil, errIncompleteCommand
	}
	if rd.end == len(rd.buf) {
		// at the end of the buffer.
		if rd.start == rd.end {
			// rewind the to the beginning
			rd.start, rd.end = 0, 0
		} else {
			// must grow the buffer
			newbuf := make([]byte, len(rd.buf)*2)
			copy(newbuf, rd.buf)
			rd.buf = newbuf
		}
	}
	n, err := rd.rd.Read(rd.buf[rd.end:])
	if err != nil {
		return nil, err
	}
	rd.end += n
	return rd.readCommands(leftover)
}

// ReadCommand reads the next command.
func (rd *Reader) ReadCommand() (Command, error) {
	if len(rd.cmds) > 0 {
		cmd := rd.cmds[0]
		rd.cmds = rd.cmds[1:]
		return cmd, nil
	}
	cmds, err := rd.readCommands(nil)
	if err != nil {
		return Command{}, err
	}
	rd.cmds = cmds
	return rd.ReadCommand()
}

// Parse parses a raw RESP message and returns a command.
func Parse(raw []byte) (Command, error) {
	rd := Reader{buf: raw, end: len(raw)}
	var leftover int
	cmds, err := rd.readCommands(&leftover)
	if err != nil {
		return Command{}, err
	}
	if leftover > 0 {
		return Command{}, errTooMuchData
	}
	return cmds[0], nil

}
