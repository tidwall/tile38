package server

import (
	"errors"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/tidwall/tile38/core"
	"github.com/tidwall/tile38/internal/log"
)

var errAOFAlreadyShrinking = errors.New("aof already shrinking")
var errAOFDisabled = errors.New("aof disabled")
var errAOFFailed = errors.New("aof failed")

func (server *Server) cmdAOFShrink() error {
	if server.aof == nil {
		return errAOFDisabled
	}
	start := time.Now()
	ul := server.WriterLock()
	if server.shrinking {
		ul()
		return errAOFAlreadyShrinking
	}
	server.shrinking = true
	server.shrinklog = nil
	ul()

	defer func() {
		ul := server.WriterLock()
		server.shrinking = false
		server.shrinklog = nil
		ul()
		log.Infof("aof shrink ended %v", time.Now().Sub(start))
	}()

	// TODO: worry about ongoing replication that might be going over
	// the part we're about to truncate

	dst, err := os.Create(core.AppendFileName + "-shrink")
	if err != nil {
		log.Errorf("Failed creating -shrink file: %v", err)
		return errAOFFailed
	}

	// Write "savesnapshot ID" at the top of the new file
	var aofbuf []byte
	values := []string{"SAVESNAPSHOT", server.snapshotMeta._idstr}
	aofbuf = append(aofbuf, '*')
	aofbuf = append(aofbuf, strconv.FormatInt(int64(len(values)), 10)...)
	aofbuf = append(aofbuf, '\r', '\n')
	for _, value := range values {
		aofbuf = append(aofbuf, '$')
		aofbuf = append(aofbuf, strconv.FormatInt(int64(len(value)), 10)...)
		aofbuf = append(aofbuf, '\r', '\n')
		aofbuf = append(aofbuf, value...)
		aofbuf = append(aofbuf, '\r', '\n')
	}
	if _, err := dst.Write(aofbuf); err != nil {
		log.Errorf("Failed writing header for -shrink file: %v", err)
		return errAOFFailed
	}
	newOffset, err := dst.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("Failed seeking in -shrink file: %v", err)
		return errAOFFailed
	}

	src, err := os.Open(server.aof.Name())
	if err != nil {
		log.Errorf("Failed opening existing AOF file: %v", err)
		return errAOFFailed
	}
	if _, err := src.Seek(server.snapshotMeta._offset, io.SeekStart); err != nil {
		log.Errorf("Failed seeking in existing AOF file: %v", err)
		return errAOFFailed
	}

	if _, err := io.Copy(dst, src); err != nil {
		log.Errorf("Failed copying data: %v", err)
		return errAOFFailed
	}

	if err := dst.Sync(); err != nil {
		log.Errorf("Failed syncing -shrink file: %v", err)
		return errAOFFailed
	}

	defer server.WriterLock()()
	// flush the aof buffer
	server.flushAOF(false)
	aofbuf = aofbuf[:0]
	for _, values := range server.shrinklog {
		// append the values to the aof buffer
		aofbuf = append(aofbuf, '*')
		aofbuf = append(aofbuf, strconv.FormatInt(int64(len(values)), 10)...)
		aofbuf = append(aofbuf, '\r', '\n')
		for _, value := range values {
			aofbuf = append(aofbuf, '$')
			aofbuf = append(aofbuf, strconv.FormatInt(int64(len(value)), 10)...)
			aofbuf = append(aofbuf, '\r', '\n')
			aofbuf = append(aofbuf, value...)
			aofbuf = append(aofbuf, '\r', '\n')
		}
	}
	if _, err := dst.Write(aofbuf); err != nil {
		log.Errorf("Failed writing shrink log file: %v", err)
		return errAOFFailed
	}
	if err := dst.Sync(); err != nil {
		log.Errorf("Failed syncing -shrink file: %v", err)
		return errAOFFailed
	}

	// we now have a shrunken aof file that is fully in-sync with
	// the current dataset. let's swap out the on disk files and
	// point to the new file.

	// anything below this point is unrecoverable. just log and exit process
	// back up the live aof, just in case of fatal error
	if err := server.aof.Close(); err != nil {
		log.Fatalf("shrink live aof close fatal operation: %v", err)
		return errAOFFailed
	}
	if err := dst.Close(); err != nil {
		log.Fatalf("shrink new aof close fatal operation: %v", err)
		return errAOFFailed
	}
	if err := os.Rename(core.AppendFileName, core.AppendFileName+"-bak"); err != nil {
		log.Fatalf("shrink backup fatal operation: %v", err)
		return errAOFFailed
	}
	if err := os.Rename(core.AppendFileName+"-shrink", core.AppendFileName); err != nil {
		log.Fatalf("shrink rename fatal operation: %v", err)
		return errAOFFailed
	}
	server.aof, err = os.OpenFile(core.AppendFileName, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.Fatalf("shrink openfile fatal operation: %v", err)
		return errAOFFailed
	}
	server.aofsz, err = server.aof.Seek(0, 2)
	if err != nil {
		log.Fatalf("shrink seek end fatal operation: %v", err)
		return errAOFFailed
	}
	server.snapshotMeta._offset = newOffset
	if err := server.snapshotMeta.save(); err != nil {
		log.Errorf("Failed saving snapshot meta: %v", err)
		return errAOFFailed
	}

	os.Remove(core.AppendFileName + "-bak") // ignore error
	// kill all followers connections
	for conn := range server.aofconnM {
		conn.Close()
	}
	return nil
}
