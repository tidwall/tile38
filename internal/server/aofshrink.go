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
var errAOFShrinkFailed = errors.New("aof shrink failed")

func writeResp(dst io.Writer, messages [][]string) error {
	var buf []byte
	for _, words := range messages {
		buf = append(buf, '*')
		buf = append(buf, strconv.FormatInt(int64(len(words)), 10)...)
		buf = append(buf, '\r', '\n')
		for _, word := range words {
			buf = append(buf, '$')
			buf = append(buf, strconv.FormatInt(int64(len(word)), 10)...)
			buf = append(buf, '\r', '\n')
			buf = append(buf, word...)
			buf = append(buf, '\r', '\n')
		}
	}
	_, err := dst.Write(buf)
	return err
}

func (server *Server) cmdAOFShrink() error {
	if server.aof == nil {
		return errAOFDisabled
	}

	alreadyShrinking := func() bool {
		defer server.WriterLock()()
		if server.shrinking {
			return true
		} else {
			server.shrinking = true
			return false
		}
	}()
	if alreadyShrinking {
		return errAOFAlreadyShrinking
	}

	start := time.Now()
	defer func() {
		defer server.WriterLock()()
		server.shrinking = false
		server.shrinklog = nil
		log.Infof("aof shrink finished %v", time.Now().Sub(start))
	}()

	shrunkName := core.AppendFileName + "-shrink"
	dst, err := os.Create(shrunkName)
	if err != nil {
		log.Errorf("Failed creating shrunk file: %v", err)
		return errAOFShrinkFailed
	}

	// Write "savesnapshot ID" at the top of the new file
	header := [][]string{{"SAVESNAPSHOT", server.snapshotMeta._idstr}}
	if err := writeResp(dst, header); err != nil {
		log.Errorf("Failed writing header for shrunk file: %v", err)
		return errAOFShrinkFailed
	}
	newOffset, err := dst.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("Failed seeking in shrunk file: %v", err)
		return errAOFShrinkFailed
	}
	src, err := os.Open(server.aof.Name())
	if err != nil {
		log.Errorf("Failed opening existing AOF file: %v", err)
		return errAOFShrinkFailed
	}
	if _, err := src.Seek(server.snapshotMeta._offset, io.SeekStart); err != nil {
		log.Errorf("Failed seeking in existing AOF file: %v", err)
		return errAOFShrinkFailed
	}
	if _, err := io.Copy(dst, src); err != nil {
		log.Errorf("Failed copying data: %v", err)
		return errAOFShrinkFailed
	}
	if err := dst.Sync(); err != nil {
		log.Errorf("Failed syncing shrunk file: %v", err)
		return errAOFShrinkFailed
	}

	defer server.WriterLock()()
	// flush the aof buffer
	server.flushAOF(false)
	// dump all records from shrinklog
	if err := writeResp(dst, server.shrinklog); err != nil {
		log.Errorf("Failed dumping shrink log records: %v", err)
		return errAOFShrinkFailed
	}
	if err := dst.Sync(); err != nil {
		log.Errorf("Failed syncing shrunk file: %v", err)
		return errAOFShrinkFailed
	}

	// we now have a shrunken aof file that is fully in-sync with the current state.
	// let's swap out the on disk files and point to the new file.
	if err := server.aof.Close(); err != nil {
		log.Errorf("Failed closing AOF file: %v", err)
		return errAOFShrinkFailed
	}
	if err := dst.Close(); err != nil {
		log.Errorf("Failed closing shrunk file: %v", err)
		return errAOFShrinkFailed
	}
	if err := os.Rename(core.AppendFileName, core.AppendFileName+"-bak"); err != nil {
		log.Errorf("Failed renaming AOF file: %v", err)
		return errAOFShrinkFailed
	}
	if err := os.Rename(shrunkName, core.AppendFileName); err != nil {
		log.Errorf("Failed renaming shrunk file: %v", err)
		return errAOFShrinkFailed
	}
	if server.aof, err = os.OpenFile(core.AppendFileName, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		log.Errorf("Failed opening AOF file: %v", err)
		return errAOFShrinkFailed
	}
	if server.aofsz, err = server.aof.Seek(0, 2); err != nil {
		log.Errorf("Failed seeking in AOF file: %v", err)
		return errAOFShrinkFailed
	}
	server.snapshotMeta._offset = newOffset
	if err := server.snapshotMeta.save(); err != nil {
		log.Errorf("Failed saving snapshot meta: %v", err)
		return errAOFShrinkFailed
	}

	os.Remove(core.AppendFileName + "-bak")  // ignore error
	// kill all followers connections
	for conn := range server.aofconnM {
		conn.Close()
	}
	return nil
}
