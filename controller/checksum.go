package controller

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/core"
)

// checksum performs a simple md5 checksum on the aof file
func (c *Controller) checksum(pos, size int64) (sum string, err error) {
	if pos+size > int64(c.aofsz) {
		return "", io.EOF
	}
	var f *os.File
	f, err = os.Open(c.aof.Name())
	if err != nil {
		return
	}
	defer f.Close()
	sumr := md5.New()
	err = func() error {
		if size == 0 {
			n, err := f.Seek(int64(c.aofsz), 0)
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

func connAOFMD5(conn *Conn, pos, size int64) (sum string, err error) {
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

func (c *Controller) matchChecksums(conn *Conn, pos, size int64) (match bool, err error) {
	sum, err := c.checksum(pos, size)
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	csum, err := connAOFMD5(conn, pos, size)
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
func (c *Controller) followCheckSome(addr string, followc int) (pos int64, err error) {
	if core.ShowDebugMessages {
		log.Debug("follow:", addr, ":check some")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.followc.get() != followc {
		return 0, errNoLongerFollowing
	}
	if c.aofsz < checksumsz {
		return 0, nil
	}

	conn, err := DialTimeout(addr, time.Second*2)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	min := int64(0)
	max := int64(c.aofsz) - checksumsz
	limit := int64(c.aofsz)
	match, err := c.matchChecksums(conn, min, checksumsz)
	if err != nil {
		return 0, err
	}

	if match {
		min += checksumsz // bump up the min
		for {
			if max < min || max+checksumsz > limit {
				pos = min
				break
			} else {
				match, err = c.matchChecksums(conn, max, checksumsz)
				if err != nil {
					return 0, err
				}
				if match {
					min = max + checksumsz
				} else {
					limit = max
				}
				max = (limit-min)/2 - checksumsz/2 + min // multiply
			}
		}
	}
	fullpos := pos
	fname := c.aof.Name()
	if pos == 0 {
		c.aof.Close()
		c.aof, err = os.Create(fname)
		if err != nil {
			log.Fatalf("could not recreate aof, possible data loss. %s", err.Error())
			return 0, err
		}
		return 0, nil
	}

	// we want to truncate at a command location
	// search for nearest command
	pos, err = getEndOfLastValuePositionInFile(c.aof.Name(), fullpos)
	if err != nil {
		return 0, err
	}
	if pos == fullpos {
		if core.ShowDebugMessages {
			log.Debug("follow: aof fully intact")
		}
		return pos, nil
	}
	log.Warnf("truncating aof to %d", pos)
	// any errror below are fatal.
	c.aof.Close()
	if err := os.Truncate(fname, pos); err != nil {
		log.Fatalf("could not truncate aof, possible data loss. %s", err.Error())
		return 0, err
	}
	c.aof, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Fatalf("could not create aof, possible data loss. %s", err.Error())
		return 0, err
	}
	// reset the entire system.
	log.Infof("reloading aof commands")
	c.aofsz = 0
	c.reset(false)
	if err := c.loadAOF(); err != nil {
		log.Fatalf("could not reload aof, possible data loss. %s", err.Error())
		return 0, err
	}
	if int64(c.aofsz) != pos {
		log.Fatalf("aof size mismatch during reload, possible data loss.")
		return 0, errors.New("?")
	}
	return pos, nil
}
