package server

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/log"
)

func (s *Server) saveSnapshot() {
	snapshotId := rand.Uint64()
	snapshotIdStr := strconv.FormatUint(snapshotId, 16)
	log.Infof("Saving snapshot %s (%v)", snapshotIdStr, snapshotId)

	snapshotDir := filepath.Join(
		s.dir, fmt.Sprintf("snapshot.%s", snapshotIdStr))
	if err := os.Mkdir(snapshotDir, 0700); err != nil {
		log.Errorf("Failed to create snapshot dir: %v", err)
		return
	}
	colByKey := make(map[string]*collection.Collection)
	s.scanGreaterOrEqual(
		"",
		func(key string, col *collection.Collection) bool {
			colByKey[key] = col
			return true
		})

	for key, col := range colByKey {
		colDir := filepath.Join(snapshotDir, key)
		if err := os.Mkdir(colDir, 0700); err != nil {
			log.Errorf("Failed to create collection dir: %v", err)
			return
		}
		log.Infof("Saving collection %s ...", key)
		if err := col.Save(colDir, snapshotId); err != nil {
			log.Errorf("Snapshot failed: %v", err)
			return
		}
		log.Infof("Collection %s saved", key)
	}
	log.Infof("Saved snapshot %s", snapshotIdStr)
}

func (s *Server) loadSnapshot(msg *Message) {
	vs := msg.Args[1:]
	var ok bool
	var snapshotIdStr string
	if vs, snapshotIdStr, ok = tokenval(vs); !ok || snapshotIdStr == "" {
		log.Errorf("Failed to find snapshot ID string: %v", msg.Args)
		return
	}

	snapshotId, err := strconv.ParseUint(snapshotIdStr, 16, 64)
	if err != nil {
		log.Errorf("Failed to parse snapshot id: %v", err)
		return
	}
	log.Infof("Loading snapshot %s (%v)", snapshotIdStr, snapshotId)

	snapshotDir := filepath.Join(
		s.dir, fmt.Sprintf("snapshot.%s", snapshotIdStr))

	dirs, err := ioutil.ReadDir(snapshotDir)
	if err != nil {
		log.Errorf("Failed to read snapshots dir: %v", err)
		return
	}

	var keys []string
	for _, dir := range dirs {
		if dir.IsDir() {
			keys = append(keys, dir.Name())
		}
	}

	for _, key := range keys {
		log.Infof("Loading collection %s ...", key)
		colDir := filepath.Join(snapshotDir, key)
		col := collection.New()
		if err := col.Load(colDir, snapshotId, &s.geomParseOpts); err != nil {
			log.Errorf("Failed to load collection: %v", err)
			return
		}
		s.setCol(key, col)
	}
	log.Infof("Loaded snapshot %s", snapshotIdStr)
}
