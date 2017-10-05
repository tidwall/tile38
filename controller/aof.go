package controller

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/redcon"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/controller/server"
)

// AsyncHooks indicates that the hooks should happen in the background.

type errAOFHook struct {
	err error
}

func (err errAOFHook) Error() string {
	return fmt.Sprintf("hook: %v", err.err)
}

var errInvalidAOF = errors.New("invalid aof file")

func (c *Controller) loadAOF() error {
	fi, err := c.aof.Stat()
	if err != nil {
		return err
	}
	start := time.Now()
	var count int
	defer func() {
		d := time.Now().Sub(start)
		ps := float64(count) / (float64(d) / float64(time.Second))
		suf := []string{"bytes/s", "KB/s", "MB/s", "GB/s", "TB/s"}
		bps := float64(fi.Size()) / (float64(d) / float64(time.Second))
		for i := 0; bps > 1024; i++ {
			if len(suf) == 1 {
				break
			}
			bps /= 1024
			suf = suf[1:]
		}
		byteSpeed := fmt.Sprintf("%.0f %s", bps, suf[0])
		log.Infof("AOF loaded %d commands: %.2fs, %.0f/s, %s",
			count, float64(d)/float64(time.Second), ps, byteSpeed)
	}()
	var buf []byte
	var args [][]byte
	var packet [0xFFFF]byte
	var msg server.Message
	for {
		n, err := c.aof.Read(packet[:])
		if err != nil {
			if err == io.EOF {
				if len(buf) > 0 {
					return io.ErrUnexpectedEOF
				}
				return nil
			}
			return err
		}
		c.aofsz += n
		data := packet[:n]
		if len(buf) > 0 {
			data = append(buf, data...)
		}
		var complete bool
		for {
			complete, args, _, data, err = redcon.ReadNextCommand(data, args[:0])
			if err != nil {
				return err
			}
			if !complete {
				break
			}
			if len(args) > 0 {
				msg.Values = msg.Values[:0]
				for _, arg := range args {
					msg.Values = append(msg.Values, resp.BytesValue(arg))
				}
				msg.Command = qlower(args[0])
				if _, _, err := c.command(&msg, nil, nil); err != nil {
					if commandErrIsFatal(err) {
						return err
					}
				}
				count++
			}
		}
		if len(data) > 0 {
			buf = append(buf[:0], data...)
		} else if len(buf) > 0 {
			buf = buf[:0]
		}
	}
}

func qlower(s []byte) string {
	if len(s) == 3 {
		if s[0] == 'S' && s[1] == 'E' && s[2] == 'T' {
			return "set"
		}
		if s[0] == 'D' && s[1] == 'E' && s[2] == 'L' {
			return "del"
		}
	}
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' || s[i] <= 'Z' {
			return strings.ToLower(string(s))
		}
	}
	return string(s)
}

func commandErrIsFatal(err error) bool {
	// FSET (and other writable commands) may return errors that we need
	// to ignore during the loading process. These errors may occur (though unlikely)
	// due to the aof rewrite operation.
	switch err {
	case errKeyNotFound, errIDNotFound:
		return false
	}
	return true
}

func (c *Controller) writeAOF(value resp.Value, d *commandDetailsT) error {
	if d != nil {
		if !d.updated {
			return nil // just ignore writes if the command did not update
		}
		if c.config.followHost() == "" {
			// process hooks, for leader only
			if d.parent {
				// process children only
				for _, d := range d.children {
					if err := c.queueHooks(d); err != nil {
						return err
					}
				}
			} else {
				// process parent
				if err := c.queueHooks(d); err != nil {
					return err
				}
			}
		}
	}
	if c.shrinking {
		var values []string
		for _, value := range value.Array() {
			values = append(values, value.String())
		}
		c.shrinklog = append(c.shrinklog, values)
	}
	data, err := value.MarshalRESP()
	if err != nil {
		return err
	}
	n, err := c.aof.Write(data)
	if err != nil {
		return err
	}
	c.aofsz += n

	// notify aof live connections that we have new data
	c.fcond.L.Lock()
	c.fcond.Broadcast()
	c.fcond.L.Unlock()

	if d != nil {
		// write to live connection streams
		c.lcond.L.Lock()
		if d.parent {
			for _, d := range d.children {
				c.lstack = append(c.lstack, d)
			}
		} else {
			c.lstack = append(c.lstack, d)
		}
		c.lcond.Broadcast()
		c.lcond.L.Unlock()
	}
	return nil
}

func (c *Controller) queueHooks(d *commandDetailsT) error {
	// big list of all of the messages
	var hmsgs [][]byte
	var hooks []*Hook
	// find the hook by the key
	if hm, ok := c.hookcols[d.key]; ok {
		for _, hook := range hm {
			// match the fence
			msgs := FenceMatch(hook.Name, hook.ScanWriter, hook.Fence, hook.Metas, d)
			if len(msgs) > 0 {
				// append each msg to the big list
				hmsgs = append(hmsgs, msgs...)
				hooks = append(hooks, hook)
			}
		}
	}
	if len(hmsgs) == 0 {
		return nil
	}

	// queue the message in the buntdb database
	err := c.qdb.Update(func(tx *buntdb.Tx) error {
		for _, msg := range hmsgs {
			c.qidx++ // increment the log id
			key := hookLogPrefix + uint64ToString(c.qidx)
			_, _, err := tx.Set(key, string(msg), hookLogSetDefaults())
			if err != nil {
				return err
			}
			log.Debugf("queued hook: %d", c.qidx)
		}
		_, _, err := tx.Set("hook:idx", uint64ToString(c.qidx), nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	// all the messages have been queued.
	// notify the hooks
	for _, hook := range hooks {
		hook.Signal()
	}
	return nil
}

// Converts string to an integer
func stringToUint64(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

// Converts a uint to a string
func uint64ToString(u uint64) string {
	s := strings.Repeat("0", 20) + strconv.FormatUint(u, 10)
	return s[len(s)-20:]
}

type liveAOFSwitches struct {
	pos int64
}

func (s liveAOFSwitches) Error() string {
	return "going live"
}

func (c *Controller) cmdAOFMD5(msg *server.Message) (res string, err error) {
	start := time.Now()
	vs := msg.Values[1:]
	var ok bool
	var spos, ssize string
	if vs, spos, ok = tokenval(vs); !ok || spos == "" {
		return "", errInvalidNumberOfArguments
	}
	if vs, ssize, ok = tokenval(vs); !ok || ssize == "" {
		return "", errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return "", errInvalidNumberOfArguments
	}
	pos, err := strconv.ParseInt(spos, 10, 64)
	if err != nil || pos < 0 {
		return "", errInvalidArgument(spos)
	}
	size, err := strconv.ParseInt(ssize, 10, 64)
	if err != nil || size < 0 {
		return "", errInvalidArgument(ssize)
	}
	sum, err := c.checksum(pos, size)
	if err != nil {
		return "", err
	}
	switch msg.OutputType {
	case server.JSON:
		res = fmt.Sprintf(`{"ok":true,"md5":"%s","elapsed":"%s"}`, sum, time.Now().Sub(start))
	case server.RESP:
		data, err := resp.SimpleStringValue(sum).MarshalRESP()
		if err != nil {
			return "", err
		}
		res = string(data)
	}
	return res, nil
}

func (c *Controller) cmdAOF(msg *server.Message) (res string, err error) {
	vs := msg.Values[1:]
	var ok bool
	var spos string
	if vs, spos, ok = tokenval(vs); !ok || spos == "" {
		return "", errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return "", errInvalidNumberOfArguments
	}
	pos, err := strconv.ParseInt(spos, 10, 64)
	if err != nil || pos < 0 {
		return "", errInvalidArgument(spos)
	}
	f, err := os.Open(c.aof.Name())
	if err != nil {
		return "", err
	}
	defer f.Close()
	n, err := f.Seek(0, 2)
	if err != nil {
		return "", err
	}
	if n < pos {
		return "", errors.New("pos is too big, must be less that the aof_size of leader")
	}
	var s liveAOFSwitches
	s.pos = pos
	return "", s
}

func (c *Controller) liveAOF(pos int64, conn net.Conn, rd *server.PipelineReader, msg *server.Message) error {
	c.mu.Lock()
	c.aofconnM[conn] = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.aofconnM, conn)
		c.mu.Unlock()
		conn.Close()
	}()

	if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
		return err
	}

	c.mu.RLock()
	f, err := os.Open(c.aof.Name())
	c.mu.RUnlock()
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(pos, 0); err != nil {
		return err
	}
	cond := sync.NewCond(&sync.Mutex{})
	var mustQuit bool
	go func() {
		defer func() {
			cond.L.Lock()
			mustQuit = true
			cond.Broadcast()
			cond.L.Unlock()
		}()
		for {
			vs, err := rd.ReadMessages()
			if err != nil {
				if err != io.EOF {
					log.Error(err)
				}
				return
			}
			for _, v := range vs {
				switch v.Command {
				default:
					log.Error("received a live command that was not QUIT")
					return
				case "quit", "":
					return
				}
			}
		}
	}()
	go func() {
		defer func() {
			cond.L.Lock()
			mustQuit = true
			cond.Broadcast()
			cond.L.Unlock()
		}()
		err := func() error {
			_, err := io.Copy(conn, f)
			if err != nil {
				return err
			}

			b := make([]byte, 4096)
			// The reader needs to be OK with the eof not
			for {
				n, err := f.Read(b)
				if err != io.EOF && n > 0 {
					if err != nil {
						return err
					}
					if _, err := conn.Write(b[:n]); err != nil {
						return err
					}
					continue
				}
				c.fcond.L.Lock()
				c.fcond.Wait()
				c.fcond.L.Unlock()
			}
		}()
		if err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") &&
				!strings.Contains(err.Error(), "bad file descriptor") {
				log.Error(err)
			}
			return
		}
	}()
	for {
		cond.L.Lock()
		if mustQuit {
			cond.L.Unlock()
			return nil
		}
		cond.Wait()
		cond.L.Unlock()
	}
}
