package server

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/tidwall/tile38/core"
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


// Given leader offset lTop, and follower offset fTop, find a position within the leader AOF
// that the follower should replicate from. The position is relative to the given offsets.
// It corresponds to the size of the common AOF between the two sides, following the
// respective offsets.
func (s *Server) findFollowPos(addr string, followc int, lTop, fTop int64) (relPos int64, err error) {
	if core.ShowDebugMessages {
		log.Debug("follow:", addr, ":check some")
	}
	defer s.WriterLock()()
	if s.followc.get() != followc {
		return 0, errNoLongerFollowing
	}
	if s.aofsz < checksumsz {
		return 0, nil
	}

	conn, err := DialTimeout(addr, time.Second*2)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	m, err := doServer(conn)
	if err != nil {
		return 0, err
	}
	lSize, err := strconv.ParseInt(m["aof_size"], 10, 64)
	if err != nil {
		return 0, err
	}

	lMin := lTop
	lMax := lSize - checksumsz
	lLimit := lSize
	fMin := fTop
	fMax := s.aofsz - checksumsz
	fLimit := s.aofsz
	match, err := s.matchChecksums(conn, lMin, fMin, checksumsz)
	if err != nil {
		return 0, err
	}

	if match {
		// bump up the mins
		lMin += checksumsz
		fMin += checksumsz

		for {
			if fMax < fMin || fMax+checksumsz > fLimit {
				break
			} else {
				match, err = s.matchChecksums(conn, lMax, fMax, checksumsz)
				if err != nil {
					return 0, err
				}
				if match {
					fMin = fMax + checksumsz
					lMin = lMax + checksumsz
				} else {
					fLimit = fMax
					lLimit = lMax
				}
				fMax = (fLimit-fMin)/2 - checksumsz/2 + fMin // multiply
				lMax = (lLimit-lMin)/2 - checksumsz/2 + lMin
			}
		}
	}

	// If we're not at the end of our AOF, we have diverged somehow.
	if fMin < s.aofsz {
		log.Warnf("extra AOF data. fMin %d, aof size %d", fMin, s.aofsz)
		return 0, errInvalidAOF
	}
	return fMin - fTop, nil
}
