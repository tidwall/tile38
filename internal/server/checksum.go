package server

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/tidwall/resp"
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

// getEndOfLastValuePositionInFile is a very slow operation because it reads the file
// backwards on byte at a time. Eek. It seek+read, seek+read, etc.
func getEndOfLastValuePositionInFile(fname string, startPos int64) (int64, error) {
	pos := startPos
	f, err := os.Open(fname)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	readByte := func() (byte, error) {
		if pos <= 0 {
			return 0, io.EOF
		}
		pos--
		if _, err := f.Seek(pos, 0); err != nil {
			return 0, err
		}
		b := make([]byte, 1)
		if n, err := f.Read(b); err != nil {
			return 0, err
		} else if n != 1 {
			return 0, errors.New("invalid read")
		}
		return b[0], nil
	}
	for {
		c, err := readByte()
		if err != nil {
			return 0, err
		}
		if c == '*' {
			if _, err := f.Seek(pos, 0); err != nil {
				return 0, err
			}
			rd := resp.NewReader(f)
			_, telnet, n, err := rd.ReadMultiBulk()
			if err != nil || telnet {
				continue // keep reading backwards
			}
			return pos + int64(n), nil
		}
	}
}

// followCheckSome is not a full checksum. It just "checks some" data.
// We will do some various checksums on the leader until we find the correct position to start at.
func (s *Server) followCheckSome(addr string, followc int, lTop, fTop int64) (relPos int64, err error) {
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
	fPos := fMin
	if fPos == fTop {
		return 0, err
	}

	// we want to truncate at a command location
	// search for nearest command
	fPos, err = getEndOfLastValuePositionInFile(s.aof.Name(), fPos)
	if err != nil {
		return 0, err
	}
	if fPos == fMin {
		if core.ShowDebugMessages {
			log.Debug("follow: aof fully intact")
		}
		return fPos - fTop, nil
	}
	log.Warnf("truncating aof to %d", fPos)
	// any errror below are fatal.
	s.aof.Close()
	fname := s.aof.Name()
	if err := os.Truncate(fname, fPos); err != nil {
		log.Fatalf("could not truncate aof, possible data loss. %s", err.Error())
		return 0, err
	}
	s.aof, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Fatalf("could not create aof, possible data loss. %s", err.Error())
		return 0, err
	}
	// reset the entire system.
	log.Infof("reloading aof commands")
	s.reset()
	if err := s.loadAOF(nilOffset); err != nil {
		log.Fatalf("could not reload aof, possible data loss. %s", err.Error())
		return 0, err
	}
	if s.aofsz != fPos {
		log.Fatalf("aof size mismatch during reload, possible data loss.")
		return 0, errors.New("?")
	}
	return fPos - fTop, nil
}
