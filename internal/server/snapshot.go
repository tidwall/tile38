package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/resp"
	"github.com/tidwall/tile38/internal/collection"
	"github.com/tidwall/tile38/internal/log"
)

var errSnapshotLoadFailed = errors.New("snapshot load failed")

const (
	Id     = "id"
	Offset = "offset"
)

// Record of the last snapshot for this dataset
type SnapshotMeta struct {
	path string

	mu sync.RWMutex

	_idstr	string
	_offset	int64
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
	sm._offset = gjson.Get(jsonStr, Offset).Int()

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

func connLastSnapshotMeta(conn *RESPConn) (snapshotMeta *SnapshotMeta, err error) {
	v, e := conn.Do("snapshot latest meta")
	if e != nil {
		err = e
		return
	}
	if v.Error() != nil {
		err = v.Error()
		return
	}
	vals := v.Array()
	snapshotMeta = &SnapshotMeta{
		_idstr: vals[0].String(),
		_offset: int64(vals[1].Integer()),
	}
	return
}

func (s *Server) cmdSnapshotLastMeta(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(
			fmt.Sprintf(
				`{"ok":true,"id":"%s","offset":%d,elapsed":"%s"}`,
				s.snapshotMeta._idstr,
				s.snapshotMeta._offset,
				time.Now().Sub(start)))
	case RESP:
		res = resp.ArrayValue([]resp.Value{
			resp.SimpleStringValue(s.snapshotMeta._idstr),
			resp.IntegerValue(int(s.snapshotMeta._offset)),
		})
	}
	return res, nil
}

func (s *Server) getSnapshotDir(snapshotIdStr string) string {
	return filepath.Join(s.dir, "snapshots", snapshotIdStr)
}

func (s *Server) cmdSaveSnapshot(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	snapshotId := rand.Uint64()
	snapshotIdStr := strconv.FormatUint(snapshotId, 16)
	if err := s.writeAOF([]string{"SAVESNAPSHOT", snapshotIdStr}, nil); err != nil {
		log.Errorf("Failed to write AOF for snapshot: %v", err)
		return NOMessage, errInvalidAOF
	}
	go s.doSaveSnapshot(snapshotId, snapshotIdStr, s.aofsz)
	switch msg.OutputType {
	case JSON:
		res = resp.StringValue(
			fmt.Sprintf(
				`{"ok":true,"id":"%s",elapsed":"%s"}`,
				snapshotIdStr,
				time.Now().Sub(start)))
	case RESP:
		res = resp.SimpleStringValue(snapshotIdStr)
	}
	return res, nil
}

func (s *Server) doSaveSnapshot(snapshotId uint64, snapshotIdStr string, offset int64) {
	log.Infof("Saving snapshot %s...", snapshotIdStr)

	snapshotDir := s.getSnapshotDir(snapshotIdStr)
	if err := os.MkdirAll(snapshotDir, 0700); err != nil {
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

	// Deployment must make push_snapshot script available on the system.
	// The script must take two argument: ID string and the source dir.
	// The script must be able to indicate when snapshot is fully ready in s3.
	log.Infof("Pushing snapshot %s...", snapshotIdStr)
	cmd := exec.Command("push_snapshot", snapshotIdStr, snapshotDir)
	if err := cmd.Run(); err != nil {
		log.Errorf("Failed to push snapshot: %v", err)
		return
	}
	log.Infof("Pushed snapshot %s", snapshotIdStr)

	s.snapshotMeta._idstr = snapshotIdStr
	s.snapshotMeta._offset = offset
	if err := s.snapshotMeta.save(); err != nil {
		log.Errorf("Failed to save snapshot meta: %v", err)
		return
	}
}

func (s *Server) cmdLoadSnapshot(msg *Message) (res resp.Value, err error) {
	start := time.Now()
	vs := msg.Args[1:]
	var ok bool
	var snapshotIdStr string
	if vs, snapshotIdStr, ok = tokenval(vs); !ok || snapshotIdStr == "" {
		log.Errorf("Failed to find snapshot ID string: %v", msg.Args)
		return NOMessage, errInvalidNumberOfArguments
	}
	if err := s.doLoadSnapshot(snapshotIdStr); err != nil {
		log.Errorf("Failed to load snapshot: %v", err)
		return NOMessage, errSnapshotLoadFailed
	}
	return OKMessage(msg, start), nil
}

func (s *Server) fetchSnapshot(snapshotIdStr string) (snapshotDir string, err error){
	snapshotDir = s.getSnapshotDir(snapshotIdStr)
	if _, err = os.Stat(snapshotDir); os.IsNotExist(err) {
		if err = os.MkdirAll(snapshotDir, 0700); err != nil {
			log.Errorf("Failed to create snapshot dir: %v", err)
			return
		}
		log.Infof("Pulling snapshot %s... (not found locally)", snapshotIdStr)
		// Deployment must make pull_snapshot script available on the system.
		// The script must take two argument: ID string and the destination dir.
		// The script must be able to wait for snapshot to become fully ready in s3.
		cmd := exec.Command("pull_snapshot", snapshotIdStr, snapshotDir)
		if err = cmd.Run(); err != nil {
			log.Errorf("Failed to pull snapshot: %v", err)
			return
		}
		log.Infof("Pulled snapshot %s", snapshotIdStr)
	} else {
		log.Infof("Found %s locally, not pulling.", snapshotIdStr)
	}
	return
}

func (s *Server) doLoadSnapshot(snapshotIdStr string) error {
	snapshotId, err := strconv.ParseUint(snapshotIdStr, 16, 64)
	if err != nil {
		log.Errorf("Failed to parse snapshot id: %v", err)
		return err
	}
	log.Infof("Loading snapshot %s...", snapshotIdStr)
	snapshotDir, err := s.fetchSnapshot(snapshotIdStr)
	if err != nil {
		log.Errorf("Failed to create snapshot dir: %v", err)
		return err
	}

	dirs, err := ioutil.ReadDir(snapshotDir)
	if err != nil {
		log.Errorf("Failed to read snapshots dir: %v", err)
		return err
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
	return nil
}
