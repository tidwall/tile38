package server

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tidwall/tile38/internal/log"
)

// checksum performs a simple md5 checksum on the aof file
func (s *Server) checksum(pos, size int64) (sum string, err error) {
	if pos+size > s.aofsz {
		return "", io.EOF
	}
	var f *os.File
	f, err = os.Open(s.aof.Name())
	if err != nil {
		return
	}
	defer f.Close()
	sumr := md5.New()
	err = func() error {
		if size == 0 {
			n, err := f.Seek(s.aofsz, 0)
			if err != nil {
				return err
			}
			if pos >= n {
				return io.EOF
			}
			return nil
		}
		_, err = f.Seek(pos, 0)
		if err != nil {
			return err
		}
		_, err = io.CopyN(sumr, f, size)
		if err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			err = io.EOF
		}
		return "", err
	}
	return fmt.Sprintf("%x", sumr.Sum(nil)), nil
}

func connAOFMD5(conn *RESPConn, pos, size int64) (sum string, err error) {
	v, err := conn.Do("aofmd5", pos, size)
	if err != nil {
		return "", err
	}
	if v.Error() != nil {
		errmsg := v.Error().Error()
		if errmsg == "ERR EOF" || errmsg == "EOF" {
			return "", io.EOF
		}
		return "", v.Error()
	}
	sum = v.String()
	if len(sum) != 32 {
		return "", errors.New("checksum not ok")
	}
	return sum, nil
}

func (s *Server) matchChecksums(conn *RESPConn, lPos, fPos, size int64) (match bool, err error) {
	sum, err := s.checksum(fPos, size)
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	csum, err := connAOFMD5(conn, lPos, size)
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	return csum == sum, nil
}

// Given leader offset lTop, and follower offset fTop, return the offset
// for where the follower should replicate from. It can only be the whole
// size of the follower's AOF, otherwise the two AOFs are incompatible.
func (s *Server) findFollowPos(addr string, followc int, lTop, fTop int64) (relPos int64, err error) {
	defer s.WriterLock()()
	if s.followc.get() != followc {
		err = errNoLongerFollowing
		return
	}

	conn, e := DialTimeout(addr, time.Second*2)
	if e != nil {
		err = e
		return
	}
	defer conn.Close()

	relPos = s.aofsz - fTop // we'll be returning this, or setting an error
	if relPos == 0 {
		return
	}

	var match bool
	// if AOF is small, check all of it.
	if relPos <= checksumsz {
		match, err = s.matchChecksums(conn, lTop, fTop, relPos)
		if err != nil {
			return
		}
		if !match {
			log.Infof("AOF does not match")
			err = errInvalidAOF
		}
		return
	}

	// whether the beginning matches
	match, err = s.matchChecksums(conn, lTop, fTop, checksumsz)
	if err != nil {
		return
	}
	if !match {
		log.Infof("beginning of AOF does not match")
		err = errInvalidAOF
		return
	}
	// whether the end matches
	fPos := s.aofsz - checksumsz
	lPos := fPos - fTop + lTop
	match, err = s.matchChecksums(conn, lPos, fPos, checksumsz)
	if err != nil {
		return
	}
	if !match {
		log.Infof("end of AOF does not match")
		err = errInvalidAOF
	}
	return
}
