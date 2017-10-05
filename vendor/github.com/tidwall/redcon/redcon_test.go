package redcon

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestRandomCommands fills a bunch of random commands and test various
// ways that the reader may receive data.
func TestRandomCommands(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// build random commands.
	gcmds := make([][]string, 10000)
	for i := 0; i < len(gcmds); i++ {
		args := make([]string, (rand.Int()%50)+1) // 1-50 args
		for j := 0; j < len(args); j++ {
			n := rand.Int() % 10
			if j == 0 {
				n++
			}
			arg := make([]byte, n)
			for k := 0; k < len(arg); k++ {
				arg[k] = byte(rand.Int() % 0xFF)
			}
			args[j] = string(arg)
		}
		gcmds[i] = args
	}
	// create a list of a buffers
	var bufs []string

	// pipe valid RESP commands
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		msg := fmt.Sprintf("*%d\r\n", len(args))
		for j := 0; j < len(args); j++ {
			msg += fmt.Sprintf("$%d\r\n%s\r\n", len(args[j]), args[j])
		}
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// pipe valid plain commands
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		var msg string
		for j := 0; j < len(args); j++ {
			quotes := false
			var narg []byte
			arg := args[j]
			if len(arg) == 0 {
				quotes = true
			}
			for k := 0; k < len(arg); k++ {
				switch arg[k] {
				default:
					narg = append(narg, arg[k])
				case ' ':
					quotes = true
					narg = append(narg, arg[k])
				case '\\', '"', '*':
					quotes = true
					narg = append(narg, '\\', arg[k])
				case '\r':
					quotes = true
					narg = append(narg, '\\', 'r')
				case '\n':
					quotes = true
					narg = append(narg, '\\', 'n')
				}
			}
			msg += " "
			if quotes {
				msg += "\""
			}
			msg += string(narg)
			if quotes {
				msg += "\""
			}
		}
		if msg != "" {
			msg = msg[1:]
		}
		msg += "\r\n"
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// pipe valid RESP commands in broken chunks
	lmsg := ""
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		msg := fmt.Sprintf("*%d\r\n", len(args))
		for j := 0; j < len(args); j++ {
			msg += fmt.Sprintf("$%d\r\n%s\r\n", len(args[j]), args[j])
		}
		msg = lmsg + msg
		if len(msg) > 0 {
			lmsg = msg[len(msg)/2:]
			msg = msg[:len(msg)/2]
		}
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, lmsg)
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// pipe valid RESP commands in large broken chunks
	lmsg = ""
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		msg := fmt.Sprintf("*%d\r\n", len(args))
		for j := 0; j < len(args); j++ {
			msg += fmt.Sprintf("$%d\r\n%s\r\n", len(args[j]), args[j])
		}
		if len(lmsg) < 1500 {
			lmsg += msg
			continue
		}
		msg = lmsg + msg
		if len(msg) > 0 {
			lmsg = msg[len(msg)/2:]
			msg = msg[:len(msg)/2]
		}
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, lmsg)
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// Pipe the buffers in a background routine
	rd, wr := io.Pipe()
	go func() {
		defer wr.Close()
		for _, msg := range bufs {
			io.WriteString(wr, msg)
		}
	}()
	defer rd.Close()
	cnt := 0
	idx := 0
	start := time.Now()
	r := NewReader(rd)
	for {
		cmd, err := r.ReadCommand()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		if len(cmd.Args) == 3 && string(cmd.Args[0]) == "RESET" &&
			string(cmd.Args[1]) == "THE" && string(cmd.Args[2]) == "INDEX" {
			if idx != len(gcmds) {
				t.Fatalf("did not process all commands")
			}
			idx = 0
			break
		}
		if len(cmd.Args) != len(gcmds[idx]) {
			t.Fatalf("len not equal for index %d -- %d != %d", idx, len(cmd.Args), len(gcmds[idx]))
		}
		for i := 0; i < len(cmd.Args); i++ {
			if i == 0 {
				if len(cmd.Args[i]) == len(gcmds[idx][i]) {
					ok := true
					for j := 0; j < len(cmd.Args[i]); j++ {
						c1, c2 := cmd.Args[i][j], gcmds[idx][i][j]
						if c1 >= 'A' && c1 <= 'Z' {
							c1 += 32
						}
						if c2 >= 'A' && c2 <= 'Z' {
							c2 += 32
						}
						if c1 != c2 {
							ok = false
							break
						}
					}
					if ok {
						continue
					}
				}
			} else if string(cmd.Args[i]) == string(gcmds[idx][i]) {
				continue
			}
			t.Fatalf("not equal for index %d/%d", idx, i)
		}
		idx++
		cnt++
	}
	if false {
		dur := time.Now().Sub(start)
		fmt.Printf("%d commands in %s - %.0f ops/sec\n", cnt, dur, float64(cnt)/(float64(dur)/float64(time.Second)))
	}
}
func testDetached(t *testing.T, conn DetachedConn) {
	conn.WriteString("DETACHED")
	if err := conn.Flush(); err != nil {
		t.Fatal(err)
	}
}
func TestServerTCP(t *testing.T) {
	testServerNetwork(t, "tcp", ":12345")
}
func TestServerUnix(t *testing.T) {
	os.RemoveAll("/tmp/redcon-unix.sock")
	defer os.RemoveAll("/tmp/redcon-unix.sock")
	testServerNetwork(t, "unix", "/tmp/redcon-unix.sock")
}

func testServerNetwork(t *testing.T, network, laddr string) {
	s := NewServerNetwork(network, laddr,
		func(conn Conn, cmd Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "detach":
				go testDetached(t, conn.Detach())
			case "int":
				conn.WriteInt(100)
			case "bulk":
				conn.WriteBulkString("bulk")
			case "bulkbytes":
				conn.WriteBulk([]byte("bulkbytes"))
			case "null":
				conn.WriteNull()
			case "err":
				conn.WriteError("ERR error")
			case "array":
				conn.WriteArray(2)
				conn.WriteInt(99)
				conn.WriteString("Hi!")
			}
		},
		func(conn Conn) bool {
			//log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn Conn, err error) {
			//log.Printf("closed: %s [%v]", conn.RemoteAddr(), err)
		},
	)
	if err := s.Close(); err == nil {
		t.Fatalf("expected an error, should not be able to close before serving")
	}
	go func() {
		time.Sleep(time.Second / 4)
		if err := ListenAndServeNetwork(network, laddr, func(conn Conn, cmd Command) {}, nil, nil); err == nil {
			t.Fatalf("expected an error, should not be able to listen on the same port")
		}
		time.Sleep(time.Second / 4)

		err := s.Close()
		if err != nil {
			t.Fatal(err)
		}
		err = s.Close()
		if err == nil {
			t.Fatalf("expected an error")
		}
	}()
	done := make(chan bool)
	signal := make(chan error)
	go func() {
		defer func() {
			done <- true
		}()
		err := <-signal
		if err != nil {
			t.Fatal(err)
		}
		c, err := net.Dial(network, laddr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		do := func(cmd string) (string, error) {
			io.WriteString(c, cmd)
			buf := make([]byte, 1024)
			n, err := c.Read(buf)
			if err != nil {
				return "", err
			}
			return string(buf[:n]), nil
		}
		res, err := do("PING\r\n")
		if err != nil {
			t.Fatal(err)
		}
		if res != "+PONG\r\n" {
			t.Fatalf("expecting '+PONG\r\n', got '%v'", res)
		}
		res, err = do("BULK\r\n")
		if err != nil {
			t.Fatal(err)
		}
		if res != "$4\r\nbulk\r\n" {
			t.Fatalf("expecting bulk, got '%v'", res)
		}
		res, err = do("BULKBYTES\r\n")
		if err != nil {
			t.Fatal(err)
		}
		if res != "$9\r\nbulkbytes\r\n" {
			t.Fatalf("expecting bulkbytes, got '%v'", res)
		}
		res, err = do("INT\r\n")
		if err != nil {
			t.Fatal(err)
		}
		if res != ":100\r\n" {
			t.Fatalf("expecting int, got '%v'", res)
		}
		res, err = do("NULL\r\n")
		if err != nil {
			t.Fatal(err)
		}
		if res != "$-1\r\n" {
			t.Fatalf("expecting nul, got '%v'", res)
		}
		res, err = do("ARRAY\r\n")
		if err != nil {
			t.Fatal(err)
		}
		if res != "*2\r\n:99\r\n+Hi!\r\n" {
			t.Fatalf("expecting array, got '%v'", res)
		}
		res, err = do("ERR\r\n")
		if err != nil {
			t.Fatal(err)
		}
		if res != "-ERR error\r\n" {
			t.Fatalf("expecting array, got '%v'", res)
		}
		res, err = do("DETACH\r\n")
		if err != nil {
			t.Fatal(err)
		}
		if res != "+DETACHED\r\n" {
			t.Fatalf("expecting string, got '%v'", res)
		}
	}()
	go func() {
		err := s.ListenServeAndSignal(signal)
		if err != nil {
			t.Fatal(err)
		}
	}()
	<-done
}

func TestWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	wr := NewWriter(buf)
	wr.WriteError("ERR bad stuff")
	wr.Flush()
	if buf.String() != "-ERR bad stuff\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
	wr.WriteString("HELLO")
	wr.Flush()
	if buf.String() != "+HELLO\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
	wr.WriteInt(-1234)
	wr.Flush()
	if buf.String() != ":-1234\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
	wr.WriteNull()
	wr.Flush()
	if buf.String() != "$-1\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
	wr.WriteBulk([]byte("HELLO\r\nPLANET"))
	wr.Flush()
	if buf.String() != "$13\r\nHELLO\r\nPLANET\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
	wr.WriteBulkString("HELLO\r\nPLANET")
	wr.Flush()
	if buf.String() != "$13\r\nHELLO\r\nPLANET\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
	wr.WriteArray(3)
	wr.WriteBulkString("THIS")
	wr.WriteBulkString("THAT")
	wr.WriteString("THE OTHER THING")
	wr.Flush()
	if buf.String() != "*3\r\n$4\r\nTHIS\r\n$4\r\nTHAT\r\n+THE OTHER THING\r\n" {
		t.Fatal("failed")
	}
	buf.Reset()
}
func testMakeRawCommands(rawargs [][]string) []string {
	var rawcmds []string
	for i := 0; i < len(rawargs); i++ {
		rawcmd := "*" + strconv.FormatUint(uint64(len(rawargs[i])), 10) + "\r\n"
		for j := 0; j < len(rawargs[i]); j++ {
			rawcmd += "$" + strconv.FormatUint(uint64(len(rawargs[i][j])), 10) + "\r\n"
			rawcmd += rawargs[i][j] + "\r\n"
		}
		rawcmds = append(rawcmds, rawcmd)
	}
	return rawcmds
}

func TestReaderRespRandom(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for h := 0; h < 10000; h++ {
		var rawargs [][]string
		for i := 0; i < 100; i++ {
			var args []string
			n := int(rand.Int() % 16)
			for j := 0; j < n; j++ {
				arg := make([]byte, rand.Int()%512)
				rand.Read(arg)
				args = append(args, string(arg))
			}
		}
		rawcmds := testMakeRawCommands(rawargs)
		data := strings.Join(rawcmds, "")
		rd := NewReader(bytes.NewBufferString(data))
		for i := 0; i < len(rawcmds); i++ {
			if len(rawargs[i]) == 0 {
				continue
			}
			cmd, err := rd.ReadCommand()
			if err != nil {
				t.Fatal(err)
			}
			if string(cmd.Raw) != rawcmds[i] {
				t.Fatalf("expected '%v', got '%v'", rawcmds[i], string(cmd.Raw))
			}
			if len(cmd.Args) != len(rawargs[i]) {
				t.Fatalf("expected '%v', got '%v'", len(rawargs[i]), len(cmd.Args))
			}
			for j := 0; j < len(rawargs[i]); j++ {
				if string(cmd.Args[j]) != rawargs[i][j] {
					t.Fatalf("expected '%v', got '%v'", rawargs[i][j], string(cmd.Args[j]))
				}
			}
		}
	}
}

func TestPlainReader(t *testing.T) {
	rawargs := [][]string{
		{"HELLO", "WORLD"},
		{"HELLO", "WORLD"},
		{"HELLO", "PLANET"},
		{"HELLO", "JELLO"},
		{"HELLO ", "JELLO"},
	}
	rawcmds := []string{
		"HELLO WORLD\n",
		"HELLO WORLD\r\n",
		"  HELLO  PLANET \r\n",
		" \"HELLO\" \"JELLO\" \r\n",
		" \"HELLO \" JELLO \n",
	}
	rawres := []string{
		"*2\r\n$5\r\nHELLO\r\n$5\r\nWORLD\r\n",
		"*2\r\n$5\r\nHELLO\r\n$5\r\nWORLD\r\n",
		"*2\r\n$5\r\nHELLO\r\n$6\r\nPLANET\r\n",
		"*2\r\n$5\r\nHELLO\r\n$5\r\nJELLO\r\n",
		"*2\r\n$6\r\nHELLO \r\n$5\r\nJELLO\r\n",
	}
	data := strings.Join(rawcmds, "")
	rd := NewReader(bytes.NewBufferString(data))
	for i := 0; i < len(rawcmds); i++ {
		if len(rawargs[i]) == 0 {
			continue
		}
		cmd, err := rd.ReadCommand()
		if err != nil {
			t.Fatal(err)
		}
		if string(cmd.Raw) != rawres[i] {
			t.Fatalf("expected '%v', got '%v'", rawres[i], string(cmd.Raw))
		}
		if len(cmd.Args) != len(rawargs[i]) {
			t.Fatalf("expected '%v', got '%v'", len(rawargs[i]), len(cmd.Args))
		}
		for j := 0; j < len(rawargs[i]); j++ {
			if string(cmd.Args[j]) != rawargs[i][j] {
				t.Fatalf("expected '%v', got '%v'", rawargs[i][j], string(cmd.Args[j]))
			}
		}
	}
}

func TestParse(t *testing.T) {
	_, err := Parse(nil)
	if err != errIncompleteCommand {
		t.Fatalf("expected '%v', got '%v'", errIncompleteCommand, err)
	}
	_, err = Parse([]byte("*1\r\n"))
	if err != errIncompleteCommand {
		t.Fatalf("expected '%v', got '%v'", errIncompleteCommand, err)
	}
	_, err = Parse([]byte("*-1\r\n"))
	if err != errInvalidMultiBulkLength {
		t.Fatalf("expected '%v', got '%v'", errInvalidMultiBulkLength, err)
	}
	_, err = Parse([]byte("*0\r\n"))
	if err != errInvalidMultiBulkLength {
		t.Fatalf("expected '%v', got '%v'", errInvalidMultiBulkLength, err)
	}
	cmd, err := Parse([]byte("*1\r\n$1\r\nA\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if string(cmd.Raw) != "*1\r\n$1\r\nA\r\n" {
		t.Fatalf("expected '%v', got '%v'", "*1\r\n$1\r\nA\r\n", string(cmd.Raw))
	}
	if len(cmd.Args) != 1 {
		t.Fatalf("expected '%v', got '%v'", 1, len(cmd.Args))
	}
	if string(cmd.Args[0]) != "A" {
		t.Fatalf("expected '%v', got '%v'", "A", string(cmd.Args[0]))
	}
	cmd, err = Parse([]byte("A\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if string(cmd.Raw) != "*1\r\n$1\r\nA\r\n" {
		t.Fatalf("expected '%v', got '%v'", "*1\r\n$1\r\nA\r\n", string(cmd.Raw))
	}
	if len(cmd.Args) != 1 {
		t.Fatalf("expected '%v', got '%v'", 1, len(cmd.Args))
	}
	if string(cmd.Args[0]) != "A" {
		t.Fatalf("expected '%v', got '%v'", "A", string(cmd.Args[0]))
	}
}
