package server

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/buntdb"
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

func (s *Server) loadAOF() (err error) {
	fi, err := s.aof.Stat()
	if err != nil {
		return err
	}
	start := time.Now()
	var count int
	defer func() {
		d := time.Since(start)
		ps := float64(count) / (float64(d) / float64(time.Second))
		suf := []string{"bytes/s", "KB/s", "MB/s", "GB/s", "TB/s"}
		bps := float64(fi.Size()) / (float64(d) / float64(time.Second))
		for i := 0; bps > 1024 && len(suf) > 1; i++ {
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
			if err != io.EOF {
				return err
			}
			if len(buf) > 0 {
				// There was an incomplete command or other data at the end of
				// the AOF file. Attempt to recover the file by truncating the
				// file at the end position of the last complete command.
				log.Warnf("Truncating %d bytes due to an incomplete command\n",
					len(buf))
				s.aofsz -= len(buf)
				if err := s.aof.Truncate(int64(s.aofsz)); err != nil {
					return err
				}
				if _, err := s.aof.Seek(int64(s.aofsz), 0); err != nil {
					return err
				}
			}
			return nil
		}
		s.aofsz += n
		data := packet[:n]
		if len(buf) > 0 {
			data = append(buf, data...)
		}
		var complete bool
		for {
			if len(data) > 0 && data[0] == 0 {
				// Zeros found in AOF file (issue #230).
				// Just ignore it and move the next byte.
				data = data[1:]
				continue
			}
			complete, args, _, data, err = redcon.ReadNextCommand(data,
				args[:0])
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
	// to ignore during the loading process. These errors may occur (though
	// unlikely) due to the aof rewrite operation.
	return !(err == errKeyNotFound || err == errIDNotFound)
}

// flushAOF flushes all aof buffer data to disk. Set sync to true to sync the
// fsync the file.
func (s *Server) flushAOF(sync bool) {
	if len(s.aofbuf) == 0 {
		return
	}
	_, err := s.aof.Write(s.aofbuf)
	if err != nil {
		panic(err)
	}
	// send a broadcast to all sleeping followers
	s.fcond.Broadcast()
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

func (s *Server) writeAOF(args []string, d *commandDetails) error {
	if d != nil && !d.updated {
		// just ignore writes if the command did not update
		return nil
	}

	if s.shrinking {
		// A copy is required because the args array may be reused by the
		// caller.
		nargs := make([]string, len(args))
		copy(nargs, args)
		s.shrinklog = append(s.shrinklog, args)
	}

	if s.aof != nil {
		s.aofdirty.Store(true) // prewrite optimization flag
		n := len(s.aofbuf)
		s.aofbuf = redcon.AppendArray(s.aofbuf, len(args))
		for _, arg := range args {
			s.aofbuf = redcon.AppendBulkString(s.aofbuf, arg)
		}
		s.aofsz += len(s.aofbuf) - n
	}

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
				s.lstack = append(s.lstack, d.children...)
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
	candidates := make(map[*Hook]bool)
	// add the hooks with "outside" detection
	s.hooksOut.Ascend(nil, func(v interface{}) bool {
		hook := v.(*Hook)
		if hook.Key == d.key {
			candidates[hook] = true
		}
		return true
	})
	// look for candidates that might "cross" geofences
	if d.old != nil && d.obj != nil && s.hookCross.Len() > 0 {
		r1, r2 := d.old.Rect(), d.obj.Rect()
		s.hookCross.Search(
			[2]float64{
				math.Min(r1.Min.X, r2.Min.X),
				math.Min(r1.Min.Y, r2.Min.Y),
			},
			[2]float64{
				math.Max(r1.Max.X, r2.Max.X),
				math.Max(r1.Max.Y, r2.Max.Y),
			},
			func(min, max [2]float64, value interface{}) bool {
				hook := value.(*Hook)
				if hook.Key == d.key {
					candidates[hook] = true
				}
				return true
			})
	}
	// look for candidates that overlap the old object
	if d.old != nil {
		r1 := d.old.Rect()
		s.hookTree.Search(
			[2]float64{r1.Min.X, r1.Min.Y},
			[2]float64{r1.Max.X, r1.Max.Y},
			func(min, max [2]float64, value interface{}) bool {
				hook := value.(*Hook)
				if hook.Key == d.key {
					candidates[hook] = true
				}
				return true
			})
	}
	// look for candidates that overlap the new object
	if d.obj != nil {
		r1 := d.obj.Rect()
		s.hookTree.Search(
			[2]float64{r1.Min.X, r1.Min.Y},
			[2]float64{r1.Max.X, r1.Max.Y},
			func(min, max [2]float64, value interface{}) bool {
				hook := value.(*Hook)
				if hook.Key == d.key {
					candidates[hook] = true
				}
				return true
			})
	}
	if len(candidates) == 0 {
		return nil
	}
	// return the candidates as a slice
	ret := make([]*Hook, 0, len(candidates))
	for hook := range candidates {
		ret = append(ret, hook)
	}
	return ret
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
		msgs := FenceMatch(hook.Name, hook.ScanWriter, hook.Fence, hook.Metas,
			d)
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

// AOFMD5 pos size
func (s *Server) cmdAOFMD5(msg *Message) (resp.Value, error) {
	start := time.Now()

	// >> Args

	args := msg.Args
	if len(args) != 3 {
		return retrerr(errInvalidNumberOfArguments)
	}
	pos, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || pos < 0 {
		return retrerr(errInvalidArgument(args[1]))
	}
	size, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || size < 0 {
		return retrerr(errInvalidArgument(args[2]))
	}

	// >> Operation

	sum, err := s.checksum(pos, size)
	if err != nil {
		return retrerr(err)
	}

	// >> Response

	if msg.OutputType == JSON {
		return resp.StringValue(fmt.Sprintf(
			`{"ok":true,"md5":"%s","elapsed":"%s"}`,
			sum, time.Since(start))), nil
	}
	return resp.SimpleStringValue(sum), nil
}

// AOF pos
func (s *Server) cmdAOF(msg *Message) (resp.Value, error) {
	if s.aof == nil {
		return retrerr(errors.New("aof disabled"))
	}

	// >> Args

	args := msg.Args
	if len(args) != 2 {
		return retrerr(errInvalidNumberOfArguments)
	}

	pos, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || pos < 0 {
		return retrerr(errInvalidArgument(args[1]))
	}

	// >> Operation

	f, err := os.Open(s.aof.Name())
	if err != nil {
		return retrerr(err)
	}
	defer f.Close()

	n, err := f.Seek(0, 2)
	if err != nil {
		return retrerr(err)
	}

	if n < pos {
		return retrerr(errors.New(
			"pos is too big, must be less that the aof_size of leader"))
	}

	// >> Response

	var ls liveAOFSwitches
	ls.pos = pos
	return NOMessage, ls
}

func (s *Server) liveAOF(pos int64, conn net.Conn, rd *PipelineReader) error {
	s.mu.RLock()
	f, err := os.Open(s.aof.Name())
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.aofconnM[conn] = f
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.aofconnM, conn)
		s.mu.Unlock()
		conn.Close()
		f.Close()
	}()

	if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
		return err
	}
	if _, err := f.Seek(pos, 0); err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			f.Close()
			conn.Close()
			wg.Done()
		}()
		// Any incoming message should end the connection
		rd.ReadMessages()
	}()
	_, err = io.Copy(conn, f)
	if err != nil {
		return err
	}

	b := make([]byte, 4096*2)
	for {
		n, err := f.Read(b)
		if n > 0 {
			if _, err := conn.Write(b[:n]); err != nil {
				return err
			}
		}
		if err == io.EOF {
			s.fcond.L.Lock()
			s.fcond.Wait()
			s.fcond.L.Unlock()
		} else if err != nil {
			if errors.Is(err, os.ErrClosed) {
				// The live aof file can be closed when a client (follower) has
				// closed their connection or following an AOFSHRINK operation.
				err = nil
			}
			return err
		}
	}
}
