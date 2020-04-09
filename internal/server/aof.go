package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/geojson"
	"github.com/tidwall/gjson"
	"github.com/tidwall/redcon"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/log"
)

type errAOFHook struct {
	err error
}

func (err errAOFHook) Error() string {
	return fmt.Sprintf("hook: %v", err.err)
}

var errInvalidAOF = errors.New("invalid aof file")

func (s *Server) loadAOF() error {
	fi, err := s.aof.Stat()
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
	for {
		n, err := s.aof.Read(packet[:])
		if err != nil {
			if err == io.EOF {
				if len(buf) > 0 {
					return io.ErrUnexpectedEOF
				}
				return nil
			}
			return err
		}
		s.aofsz += n
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
				var msg Message
				msg.Args = msg.Args[:0]
				for _, arg := range args {
					msg.Args = append(msg.Args, string(arg))
				}
				if _, _, err := s.command(&msg, nil); err != nil {
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

// flushAOF flushes all aof buffer data to disk. Set sync to true to sync the
// fsync the file.
func (s *Server) flushAOF(sync bool) {
	if len(s.aofbuf) > 0 {
		_, err := s.aof.Write(s.aofbuf)
		if err != nil {
			panic(err)
		}
		if sync {
			if err := s.aof.Sync(); err != nil {
				panic(err)
			}
		}
		if cap(s.aofbuf) > 1024*1024*32 {
			s.aofbuf = make([]byte, 0, 1024*1024*32)
		} else {
			s.aofbuf = s.aofbuf[:0]
		}
	}
}

func (s *Server) writeAOF(args []string, d *commandDetails) error {
	if d != nil && !d.updated {
		// just ignore writes if the command did not update
		return nil
	}

	if s.shrinking {
		nargs := make([]string, len(args))
		copy(nargs, args)
		s.shrinklog = append(s.shrinklog, nargs)
	}

	if s.aof != nil {
		atomic.StoreInt32(&s.aofdirty, 1) // prewrite optimization flag
		n := len(s.aofbuf)
		s.aofbuf = redcon.AppendArray(s.aofbuf, len(args))
		for _, arg := range args {
			s.aofbuf = redcon.AppendBulkString(s.aofbuf, arg)
		}
		s.aofsz += len(s.aofbuf) - n
	}

	// notify aof live connections that we have new data
	s.fcond.L.Lock()
	s.fcond.Broadcast()
	s.fcond.L.Unlock()

	// process geofences
	if d != nil {
		// webhook geofences
		if s.config.followHost() == "" {
			// for leader only
			if d.parent {
				// queue children
				for _, d := range d.children {
					if err := s.queueHooks(d); err != nil {
						return err
					}
				}
			} else {
				// queue parent
				if err := s.queueHooks(d); err != nil {
					return err
				}
			}
		}

		// live geofences
		s.lcond.L.Lock()
		if len(s.lives) > 0 {
			if d.parent {
				// queue children
				for _, d := range d.children {
					s.lstack = append(s.lstack, d)
				}
			} else {
				// queue parent
				s.lstack = append(s.lstack, d)
			}
			s.lcond.Broadcast()
		}
		s.lcond.L.Unlock()
	}
	return nil
}

func (s *Server) getQueueCandidates(d *commandDetails) []*Hook {
	var candidates []*Hook
	// add the hooks with "outside" detection
	if len(s.hooksOut) > 0 {
		for _, hook := range s.hooksOut {
			if hook.Key == d.key {
				candidates = append(candidates, hook)
			}
		}
	}
	// search the hook spatial tree
	for _, obj := range []geojson.Object{d.obj, d.oldObj} {
		if obj == nil {
			continue
		}
		rect := obj.Rect()
		s.hookTree.Search(
			[2]float64{rect.Min.X, rect.Min.Y},
			[2]float64{rect.Max.X, rect.Max.Y},
			func(_, _ [2]float64, value interface{}) bool {
				hook := value.(*Hook)
				if hook.Key != d.key {
					return true
				}
				var found bool
				for _, candidate := range candidates {
					if candidate == hook {
						found = true
						break
					}
				}
				if !found {
					candidates = append(candidates, hook)
				}
				return true
			},
		)
	}
	return candidates
}

func (s *Server) queueHooks(d *commandDetails) error {
	// Create the slices that will store all messages and hooks
	var cmsgs, wmsgs []string
	var whooks []*Hook

	// Compile a slice of potential hook recipients
	candidates := s.getQueueCandidates(d)
	for _, hook := range candidates {
		// Calculate all matching fence messages for all candidates and append
		// them to the appropriate message slice
		msgs := FenceMatch(hook.Name, hook.Scanner, hook.Fence, hook.Metas, d)
		if len(msgs) > 0 {
			if hook.channel {
				cmsgs = append(cmsgs, msgs...)
			} else {
				wmsgs = append(wmsgs, msgs...)
				whooks = append(whooks, hook)
			}
		}
	}

	// Return nil if there are no messages to be sent
	if len(cmsgs)+len(wmsgs) == 0 {
		return nil
	}

	// Sort both message channel and webhook message slices
	if len(cmsgs) > 1 {
		sortMsgs(cmsgs)
	}
	if len(wmsgs) > 1 {
		sortMsgs(wmsgs)
	}

	// Publish all channel messages if any exist
	if len(cmsgs) > 0 {
		for _, m := range cmsgs {
			s.Publish(gjson.Get(m, "hook").String(), m)
		}
	}

	// Queue the webhook messages in the buntdb database
	err := s.qdb.Update(func(tx *buntdb.Tx) error {
		for _, msg := range wmsgs {
			s.qidx++ // increment the log id
			key := hookLogPrefix + uint64ToString(s.qidx)
			_, _, err := tx.Set(key, msg, hookLogSetDefaults)
			if err != nil {
				return err
			}
			log.Debugf("queued hook: %d", s.qidx)
		}
		_, _, err := tx.Set("hook:idx", uint64ToString(s.qidx), nil)
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
	for _, hook := range whooks {
		hook.Signal()
	}
	return nil
}

// sortMsgs sorts passed notification messages by their detect and hook fields
func sortMsgs(msgs []string) {
	sort.SliceStable(msgs, func(i, j int) bool {
		detectI := msgDetectCode(gjson.Get(msgs[i], "detect").String())
		detectJ := msgDetectCode(gjson.Get(msgs[j], "detect").String())
		if detectI < detectJ {
			return true
		}
		if detectI > detectJ {
			return false
		}
		hookI := gjson.Get(msgs[i], "hook").String()
		hookJ := gjson.Get(msgs[j], "hook").String()
		return hookI < hookJ
	})
}

// msgDetectCode returns a weight value for the passed detect value
func msgDetectCode(detect string) int {
	switch detect {
	case "exit":
		return 1
	case "outside":
		return 2
	case "enter":
		return 3
	case "inside":
		return 4
	default:
		return 0
	}
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
	return goingLive
}

func (s *Server) cmdAOFMD5(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	var spos, ssize string

	if vs, spos, ok = tokenval(vs); !ok || spos == "" {
		return NOMessage, errInvalidNumberOfArguments
	}
	if vs, ssize, ok = tokenval(vs); !ok || ssize == "" {
		return NOMessage, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return NOMessage, errInvalidNumberOfArguments
	}
	pos, err := strconv.ParseInt(spos, 10, 64)
	if err != nil || pos < 0 {
		return NOMessage, errInvalidArgument(spos)
	}
	size, err := strconv.ParseInt(ssize, 10, 64)
	if err != nil || size < 0 {
		return NOMessage, errInvalidArgument(ssize)
	}
	sum, err := s.checksum(pos, size)
	if err != nil {
		return NOMessage, err
	}
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(
			fmt.Sprintf(`{"ok":true,"md5":"%s","elapsed":"%s"}`, sum, time.Now().Sub(start)))
	case RESP:
		res = resp.SimpleStringValue(sum)
	}
	return res, nil
}

func (s *Server) cmdAOF(msg *Message) (res resp.Value, err error) {
	if s.aof == nil {
		return NOMessage, errors.New("aof disabled")
	}
	vs := msg.Args[1:]

	var ok bool
	var spos string
	if vs, spos, ok = tokenval(vs); !ok || spos == "" {
		return NOMessage, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return NOMessage, errInvalidNumberOfArguments
	}
	pos, err := strconv.ParseInt(spos, 10, 64)
	if err != nil || pos < 0 {
		return NOMessage, errInvalidArgument(spos)
	}
	f, err := os.Open(s.aof.Name())
	if err != nil {
		return NOMessage, err
	}
	defer f.Close()
	n, err := f.Seek(0, 2)
	if err != nil {
		return NOMessage, err
	}
	if n < pos {
		return NOMessage, errors.New("pos is too big, must be less that the aof_size of leader")
	}
	var ls liveAOFSwitches
	ls.pos = pos
	return NOMessage, ls
}

func (s *Server) liveAOF(pos int64, conn net.Conn, rd *PipelineReader, msg *Message) error {
	s.mu.Lock()
	s.aofconnM[conn] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.aofconnM, conn)
		s.mu.Unlock()
		conn.Close()
	}()

	if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
		return err
	}

	s.mu.RLock()
	f, err := os.Open(s.aof.Name())
	s.mu.RUnlock()
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
				switch v.Command() {
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
				s.fcond.L.Lock()
				s.fcond.Wait()
				s.fcond.L.Unlock()
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
