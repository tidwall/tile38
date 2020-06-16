package server

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/tidwall/gjson"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/log"
)

const (
	Id     = "id"
	Offset = "offset"
)

// Record of the last snapshot for this dataset
type SnapshotMeta struct {
	path string

	mu sync.RWMutex

	_idstr	string
	_offset	uint64
}

func loadSnapshotMeta(path string) (sm *SnapshotMeta, err error) {
	sm = &SnapshotMeta{path: path}
	var jsonStr string
	var data []byte
	data, err = ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return sm, nil
		} else {
			return nil, err
		}
	}

	jsonStr = string(data)
	sm._idstr = gjson.Get(jsonStr, Id).String()
	sm._offset = gjson.Get(jsonStr, Offset).Uint()

	return sm, nil
}

func (sm *SnapshotMeta) save() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	m := make(map[string]interface{})
	if sm._idstr != "" {
		m[Id] = sm._idstr
	}
	if sm._offset != 0 {
		m[Offset] = sm._offset
	}
	data, err := json.MarshalIndent(m, "","\t")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	err = ioutil.WriteFile(sm.path, data, 0600)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) getSnapshotDir(snapshotIdStr string) string {
	return filepath.Join(s.dir, "snapshots", snapshotIdStr)
}

func (s *Server) saveSnapshot() {
	snapshotId := rand.Uint64()
	snapshotIdStr := strconv.FormatUint(snapshotId, 16)
	log.Infof("Saving snapshot %s (%v)", snapshotIdStr, snapshotId)

	snapshotDir := s.getSnapshotDir(snapshotIdStr)
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

	var wg sync.WaitGroup
	for key, col := range colByKey {
		colDir := filepath.Join(snapshotDir, key)
		if err := os.Mkdir(colDir, 0700); err != nil {
			log.Errorf("Failed to create collection dir: %v", err)
			return
		}
		wg.Add(1)
		go func(c *collection.Collection, k string) {
			log.Infof("Saving collection %s ...", k)
			if err := c.Save(colDir, snapshotId); err != nil {
				log.Errorf("Collection %s failed: %v", k, err)
				return
			}
			log.Infof("Collection %s saved", k)
			wg.Done()
		}(col, key)
	}
	wg.Wait()
	log.Infof("Saved snapshot %s", snapshotIdStr)

	if err:= s.writeAOF([]string{"SAVESNAPSHOT", snapshotIdStr}, nil); err != nil {
		log.Errorf("Failed to write AOF for snapshot: %v", err)
		return
	}

	s.snapshotMeta._idstr = snapshotIdStr
	s.snapshotMeta._offset = uint64(s.aofsz)
	if err := s.snapshotMeta.save(); err != nil {
		log.Errorf("Failed to save snapshot meta: %v", err)
		return
	}

	// Deployment must make push_snapshot script available on the system.
	// The script must take two argument: ID string and the source dir.
	log.Infof("Pushing snapshot %s...", snapshotIdStr)
	cmd := exec.Command("push_snapshot", snapshotIdStr, snapshotDir)
	if err := cmd.Run(); err != nil {
		log.Errorf("Failed to push snapshot: %v", err)
		return
	}
	log.Infof("Pushed snapshot %s", snapshotIdStr)
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

	snapshotDir := s.getSnapshotDir(snapshotIdStr)
	if _, err := os.Stat(snapshotDir); os.IsNotExist(err) {
		log.Infof("Pulling snapshot %s...", snapshotIdStr)
		// Deployment must make pull_snapshot script available on the system.
		// The script must take two argument: ID string and the destination dir.
		cmd := exec.Command("pull_snapshot", snapshotIdStr, snapshotDir)
		if err := cmd.Run(); err != nil {
			log.Errorf("Failed to pull snapshot: %v", err)
			return
		}
	}

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

	var wg sync.WaitGroup
	for _, key := range keys {
		log.Infof("Loading collection %s ...", key)
		colDir := filepath.Join(snapshotDir, key)
		col := collection.New()
		wg.Add(1)
		go func(c *collection.Collection, k string) {
			defer wg.Done()
			if err := c.Load(colDir, snapshotId, &s.geomParseOpts); err != nil {
				log.Errorf("Collection %s failed: %v", k, err)
				return
			}
			s.setCol(k, c)
			log.Infof("Collection %s loaded", k)
		}(col, key)
	}
	wg.Wait()
	log.Infof("Loaded snapshot %s", snapshotIdStr)
}
