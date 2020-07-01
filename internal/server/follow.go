package server

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/internal/log"
)

var errNoLongerFollowing = errors.New("no longer following")

const checksumsz = 512 * 1024

func (s *Server) cmdFollow(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	var host, sport string

	if vs, host, ok = tokenval(vs); !ok || host == "" {
		return NOMessage, errInvalidNumberOfArguments
	}
	if vs, sport, ok = tokenval(vs); !ok || sport == "" {
		return NOMessage, errInvalidNumberOfArguments
	}
	if len(vs) != 0 {
		return NOMessage, errInvalidNumberOfArguments
	}
	host = strings.ToLower(host)
	sport = strings.ToLower(sport)
	var update bool
	if host == "no" && sport == "one" {
		update = s.config.followHost() != "" || s.config.followPort() != 0
		s.config.setFollowHost("")
		s.config.setFollowPort(0)
	} else {
		n, err := strconv.ParseUint(sport, 10, 64)
		if err != nil {
			return NOMessage, errInvalidArgument(sport)
		}
		port := int(n)
		update = s.config.followHost() != host || s.config.followPort() != port
		if update {
			if err = s.validateLeader(host, port); err != nil {
				return NOMessage, err
			}
		}
		s.config.setFollowHost(host)
		s.config.setFollowPort(port)
	}
	s.config.write(false)
	if update {
		s.followc.add(1)
		if s.config.followHost() != "" {
			log.Infof("following new host '%s' '%s'.", host, sport)
			go s.follow(s.config.followHost(), s.config.followPort(), s.followc.get())
		} else {
			log.Infof("following no one")
		}
	}
	return OKMessage(msg, start), nil
}

// cmdReplConf is a command handler that sets replication configuration info
func (s *Server) cmdReplConf(msg *Message, client *Client) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	var cmd, val string

	// Parse the message
	if vs, cmd, ok = tokenval(vs); !ok || cmd == "" {
		return NOMessage, errInvalidNumberOfArguments
	}
	if vs, val, ok = tokenval(vs); !ok || val == "" {
		return NOMessage, errInvalidNumberOfArguments
	}

	// Switch on the command received
	switch cmd {
	case "listening-port":
		// Parse the port as an integer
		port, err := strconv.Atoi(val)
		if err != nil {
			return NOMessage, errInvalidArgument(val)
		}

		// Apply the replication port to the client and return
		s.connsmu.RLock()
		defer s.connsmu.RUnlock()
		for _, c := range s.conns {
			if c.remoteAddr == client.remoteAddr {
				c.mu.Lock()
				c.replPort = port
				c.mu.Unlock()
				return OKMessage(msg, start), nil
			}
		}
	}
	return NOMessage, fmt.Errorf("cannot find follower")
}

func doServer(conn *RESPConn) (map[string]string, error) {
	v, err := conn.Do("server")
	if err != nil {
		return nil, err
	}
	if v.Error() != nil {
		return nil, v.Error()
	}
	arr := v.Array()
	m := make(map[string]string)
	for i := 0; i < len(arr)/2; i++ {
		m[arr[i*2+0].String()] = arr[i*2+1].String()
	}
	return m, err
}

func (s *Server) followHandleCommand(args []string, followc int, w io.Writer) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.followc.get() != followc {
		return s.aofsz, errNoLongerFollowing
	}
	msg := &Message{Args: args}
	var details *commandDetails
	switch msg.Command() {
	case "loadsnapshot": // if leader loaded it, we're screwed.
		return s.aofsz, fmt.Errorf("leader loaded snapshot")
	case "savesnapshot": // if leader saved it, we will download for the future
		vs := msg.Args[1:]
		var ok bool
		var snapshotIdStr string
		if vs, snapshotIdStr, ok = tokenval(vs); !ok || snapshotIdStr == "" {
			return s.aofsz, fmt.Errorf("failed to find snapshot ID string: %v", msg.Args)
		}
		go func() {
			log.Infof("Leader saved snapshot %s, fetching...", snapshotIdStr)
			_, _ = s.fetchSnapshot(snapshotIdStr)
		}()
	default:  // other commands are replayed verbatim
		_, _d, err := s.command(msg, nil)
		if err != nil {
			if commandErrIsFatal(err) {
				return s.aofsz, err
			}
		}
		details = &_d
	}
	if err := s.writeAOF(args, details); err != nil {
		return s.aofsz, err
	}
	if len(s.aofbuf) > 10240 {
		s.flushAOF(false)
	}
	return s.aofsz, nil
}

func (s *Server) followDoLeaderAuth(conn *RESPConn, auth string) error {
	v, err := conn.Do("auth", auth)
	if err != nil {
		return err
	}
	if v.Error() != nil {
		return v.Error()
	}
	if v.String() != "OK" {
		return errors.New("cannot follow: auth no ok")
	}
	return nil
}

// Check that we can follow a given host:port, return error if we cannot.
func (s *Server) validateLeader(host string, port int) error {
	auth := s.config.leaderAuth()
	conn, err := DialTimeout(fmt.Sprintf("%s:%d", host, port), time.Second*2)
	if err != nil {
		return fmt.Errorf("cannot follow: %v", err)
	}
	defer conn.Close()
	if auth != "" {
		if err := s.followDoLeaderAuth(conn, auth); err != nil {
			return fmt.Errorf("cannot follow: %v", err)
		}
	}
	m, err := doServer(conn)
	if err != nil {
		return fmt.Errorf("cannot follow: %v", err)
	}
	if m["id"] == "" {
		return fmt.Errorf("cannot follow: invalid id")
	}
	if m["id"] == s.config.serverID() {
		return fmt.Errorf("cannot follow self")
	}
	if m["following"] != "" {
		return fmt.Errorf("cannot follow a follower")
	}
	return nil
}

func (s *Server) followStep(host string, port int, followc int, lTop, fTop int64) error {
	if s.followc.get() != followc {
		return errNoLongerFollowing
	}
	s.mu.Lock()
	s.fcup = false
	s.mu.Unlock()
	if err := s.validateLeader(host, port); err != nil {
		return err
	}
	addr := fmt.Sprintf("%s:%d", host, port)

	// verify checksum
	relPos, err := s.followCheckSome(addr, followc, lTop, fTop)
	if err != nil {
		return err
	}

	conn, err := DialTimeout(fmt.Sprintf("%s:%d", host, port), time.Second*2)
	m, err := doServer(conn)
	if err != nil {
		return err
	}
	lSize, err := strconv.ParseInt(m["aof_size"], 10, 64)
	if err != nil {
		return err
	}

	// Send the replication port to the leader
	v, err := conn.Do("replconf", "listening-port", s.port)
	if err != nil {
		return err
	}
	if v.Error() != nil {
		return v.Error()
	}
	if v.String() != "OK" {
		return errors.New("invalid response to replconf request")
	}
	if core.ShowDebugMessages {
		log.Debug("follow:", addr, ":replconf")
	}

	v, err = conn.Do("aof", lTop+relPos)
	if err != nil {
		return err
	}
	if v.Error() != nil {
		return v.Error()
	}
	if v.String() != "OK" {
		return errors.New("invalid response to aof live request")
	}
	if core.ShowDebugMessages {
		log.Debug("follow:", addr, ":read aof")
	}

	caughtUp := relPos >= lSize-lTop
	if caughtUp {
		s.mu.Lock()
		s.fcup = true
		s.fcuponce = true
		s.mu.Unlock()
		log.Info("caught up")
	}
	nullw := ioutil.Discard
	for {
		v, telnet, _, err := conn.rd.ReadMultiBulk()
		if err != nil {
			return err
		}
		vals := v.Array()
		if telnet || v.Type() != resp.Array {
			return errors.New("invalid multibulk")
		}
		svals := make([]string, len(vals))
		for i := 0; i < len(vals); i++ {
			svals[i] = vals[i].String()
		}

		fSize, err := s.followHandleCommand(svals, followc, nullw)
		if err != nil {
			return err
		}
		if !caughtUp {
			if fSize-fTop >= lSize-lTop {
				caughtUp = true
				s.mu.Lock()
				s.flushAOF(false)
				s.fcup = true
				s.fcuponce = true
				s.mu.Unlock()
				log.Info("caught up")
			}
		}
	}
}


func (s * Server) syncToLatestSnapshot(host string, port int, followc int) (lTop, fTop int64, err error) {
	if s.followc.get() != followc {
		err = errNoLongerFollowing
		return
	}
	if err = s.validateLeader(host, port); err != nil {
		return
	}
	var conn *RESPConn
	if conn, err = DialTimeout(fmt.Sprintf("%s:%d", host, port), time.Second*2); err != nil {
		return
	}
	defer conn.Close()
	if s.snapshotMeta, err = connLastSnapshotMeta(conn); err != nil {
		return
	}
	// No snapshot on the server: return 0 offsets
	if s.snapshotMeta._idstr == "" {
		return
	}
	lTop = s.snapshotMeta._offset
	if err = s.doLoadSnapshot(s.snapshotMeta._idstr); err != nil {
		return
	}
	s.aof.Close()
	s.aofsz = 0
	if s.aof, err = os.Create(s.aof.Name()); err != nil {
		log.Fatalf("could not recreate aof, possible data loss. %s", err.Error())
		return
	}
	if err = s.writeAOF([]string{"LOADSNAPSHOT", s.snapshotMeta._idstr}, nil); err != nil {
		log.Errorf("Failed to write AOF for synced snapshot: %v", err)
		return
	}
	fTop = s.aofsz
	s.snapshotMeta._offset = s.aofsz
	s.snapshotMeta.path = filepath.Join(s.dir, "snapshot_meta")
	if err = s.snapshotMeta.save(); err != nil {
		log.Errorf("Failed to save synced snapshot meta: %v", err)
		return
	}
	return
}

func (s *Server) follow(host string, port int, followc int) {
	var lTop, fTop int64
	var err error
	if lTop, fTop, err = s.syncToLatestSnapshot(host, port, followc); err != nil {
		log.Errorf("Failed to sync to the latest snapshot: %v", err)
		return
	}
	for {
		if err = s.followStep(host, port, followc, lTop, fTop); err == errNoLongerFollowing {
			return
		} else if err != nil && err != io.EOF {
			log.Error("follow: " + err.Error())
		}
		time.Sleep(time.Second)
	}
}
