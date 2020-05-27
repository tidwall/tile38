package collection

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"unsafe"

	"github.com/tidwall/tile38/internal/log"
)

func (c *Collection) Save(dir string, snapshotId uint64) (err error) {
	if err = c.saveFields(filepath.Join(dir, "fields"), snapshotId); err != nil {
		log.Errorf("Failed to save fields")
		return
	}
	log.Infof("Saved fields")

	if err = c.saveStats(filepath.Join(dir, "stats"), snapshotId); err != nil {
		log.Errorf("Failed to save stats")
		return
	}
	log.Infof("Saved stats")
	return
}

func (c *Collection) Load(dir string, snapshotId uint64) (err error) {
	if err = c.loadFields(filepath.Join(dir, "fields"), snapshotId); err != nil {
		log.Errorf("Failed to load fields")
		return
	}
	log.Infof("Loaded fields")

	if err = c.loadStats(filepath.Join(dir, "stats"), snapshotId); err != nil {
		log.Errorf("Failed to load stats")
		return
	}
	log.Infof("Loaded stats")

	return
}

func (c *Collection) saveStats(statsFile string, snapshotId uint64) (err error) {
	var f *os.File
	f, err = os.Create(statsFile)
	log.Infof("Created stats file: %s", statsFile)
	if err != nil {
		return
	}
	defer func() {
		if f.Close() != nil {
			log.Errorf("Failed to close %s", statsFile)
		}
	}()

	if err = binary.Write(f, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into fields file")
		return
	}
	log.Infof("Wrote snapshotId into stats file")

	if err = binary.Write(f, binary.BigEndian, uint64(c.weight)); err != nil {
		log.Errorf("Failed to write weight into fields file")
		return
	}
	log.Infof("Wrote weight into stats file")

	if err = binary.Write(f, binary.BigEndian, uint64(c.points)); err != nil {
		log.Errorf("Failed to write points into fields file")
		return
	}
	log.Infof("Wrote points into stats file")

	if err = binary.Write(f, binary.BigEndian, uint64(c.objects)); err != nil {
		log.Errorf("Failed to write objects into fields file")
		return
	}
	log.Infof("Wrote objects into stats file")

	if err = binary.Write(f, binary.BigEndian, uint64(c.nobjects)); err != nil {
		log.Errorf("Failed to write nobjects into fields file")
		return
	}
	log.Infof("Wrote nobjects into stats file")

	if err = binary.Write(f, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into stats file")
		return
	}
	log.Infof("Wrote snapshotId into stats file")
	return
}

func (c * Collection) loadStats(statsFile string, snapshotId uint64) (err error) {
	var f *os.File
	f, err = os.Open(statsFile)
	log.Infof("Opened stats file: %s", statsFile)
	if err != nil {
		return
	}
	defer func() {
		if f.Close() != nil {
			log.Errorf("Failed to close %s", statsFile)
		}
	}()

	var word uint64
	if _, err = f.Seek(-8, io.SeekEnd); err != nil {
		log.Errorf("Failed to seek the end of stats file")
		return
	}
	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read snapshotId from the end of stats file")
		return
	}
	if word != snapshotId {
		err = errors.New("SnapshotId at the end does not match")
		log.Errorf("expected %v found %v", snapshotId, word)
		return
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		log.Errorf("Failed to seek the beginning of stats file")
		return
	}
	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read snapshotId from the beginning of stats file")
		return
	}
	if word != snapshotId {
		err = errors.New("SnapshotId at the beginning does not match")
		return
	}

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read weight from stats file")
		return
	}
	log.Infof("Read weight from stats file")
	c.weight = int(word)

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read points from stats file")
		return
	}
	log.Infof("Read points from stats file")
	c.points = int(word)

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read objects from stats file")
		return
	}
	log.Infof("Read objects from stats file")
	c.objects = int(word)

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read nobjects from stats file")
		return
	}
	log.Infof("Read nobjects from stats file")
	c.nobjects = int(word)

	log.Infof("weight: %v", c.weight)
	log.Infof("points: %v", c.points)
	log.Infof("objects: %v", c.objects)
	log.Infof("nobjects: %v", c.nobjects)

	return
}

func (c *Collection) saveFields(fieldsFile string, snapshotId uint64) (err error) {
	var f *os.File
	f, err = os.Create(fieldsFile)
	log.Infof("Created fields file: %s", fieldsFile)
	if err != nil {
		return
	}
	defer func() {
		if f.Close() != nil {
			log.Errorf("Failed to close %s", fieldsFile)
		}
	}()

	if err = binary.Write(f, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into fields file")
		return
	}
	log.Infof("Wrote snapshotId into fields file")

	nFields := len(c.fieldMap)
	if err = binary.Write(f, binary.BigEndian, uint64(nFields)); err != nil {
		log.Errorf("Failed to write nFields into fields file")
		return
	}
	log.Infof("Wrote nFields into fields file")

	for name, idx := range c.fieldMap {
		nameBytes := []byte(name)
		nBytes := len(nameBytes)
		if err = binary.Write(f, binary.BigEndian, uint64(nBytes)); err != nil {
			log.Errorf("Failed to write nBytes into fields file")
			return
		}
		log.Infof("Wrote nBytes into lenName file")

		if _, err = f.Write(nameBytes); err != nil {
			log.Errorf("Failed to write nameBytes into fields file")
			return
		}
		log.Infof("Wrote nameBytes into fields file")

		if err = binary.Write(f, binary.BigEndian, uint64(idx)); err != nil {
			log.Errorf("Failed to write idx into fields file")
			return
		}
		log.Infof("Wrote idx into fields file")
	}

	if err = saveFieldValues(f, c.fieldValues, len(c.fieldArr)); err != nil {
		log.Errorf("Failed to save field values")
		return
	}

	if err = binary.Write(f, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into fields file")
		return
	}
	log.Infof("Wrote snapshotId into fields file")

	return
}

func (c * Collection) loadFields(fieldsFile string, snapshotId uint64) (err error) {
	var f *os.File
	f, err = os.Open(fieldsFile)
	log.Infof("Opened fields file: %s", fieldsFile)
	if err != nil {
		return
	}
	defer func() {
		if f.Close() != nil {
			log.Errorf("Failed to close %s", fieldsFile)
		}
	}()

	var word uint64
	if _, err  = f.Seek(-8, io.SeekEnd); err != nil {
		log.Errorf("Failed to seek the end of fields file")
		return
	}
	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read snapshotId from the end of fields file")
		return
	}
	if word != snapshotId {
		err = errors.New("SnapshotId at the end does not match")
		log.Errorf("expected %v found %v", snapshotId, word)
		return
	}
	if _, err  = f.Seek(0, io.SeekStart); err != nil {
		log.Errorf("Failed to seek the beginning of fields file")
		return
	}
	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read snapshotId from the beginning of fields file")
		return
	}
	if word != snapshotId {
		err = errors.New("SnapshotId at the beginning does not match")
		return
	}

	var nFields, nBytes, idx uint64
	if err = binary.Read(f, binary.BigEndian, &nFields); err != nil {
		log.Errorf("Failed to nFields from fields file")
		return
	}
	log.Infof("Read nFields from fields file")

	c.fieldMap = make(map[string]int)
	for i := uint64(0); i < nFields; i++ {
		if err = binary.Read(f, binary.BigEndian, &nBytes); err != nil {
			log.Errorf("Failed to read nBytes from fields file")
			return
		}
		log.Infof("Read nBytes from fields file")
		nameBytes := make([]byte, nBytes)
		if _, err = f.Read(nameBytes); err != nil {
			log.Errorf("Failed to read nameBytes from fields file")
			return
		}
		log.Infof("Read nameBytes from fields file")
		if err = binary.Read(f, binary.BigEndian, &idx); err != nil {
			log.Errorf("Failed to read idx from fields file")
			return
		}
		log.Infof("Read idx from fields file")

		field := string(nameBytes)
		c.fieldMap[field] = int(idx)
		c.addToFieldArr(field)
	}

	if c.fieldValues, err = loadFieldValues(f); err != nil {
		log.Errorf("Failed to load field values")
		return
	}
	log.Infof("Loaded field values")

	log.Infof("map: %v", c.fieldMap)
	log.Infof("arr: %v", c.fieldArr)
	//log.Infof("values: %v", c.fieldValues)

	return
}


func freeListAsBytes(row []fieldValuesSlot) []byte {
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&row))
	header.Len *= 4
	header.Cap *= 4
	return *(*[]byte)(unsafe.Pointer(&header))
}

func bytesAsFreeList(row []byte) []fieldValuesSlot {
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&row))
	header.Len /= 4
	header.Cap /= 4
	return *(*[]fieldValuesSlot)(unsafe.Pointer(&header))
}

func floatsAsBytes(row []float64) []byte {
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&row))
	header.Len *= 8
	header.Cap *= 8
	return *(*[]byte)(unsafe.Pointer(&header))
}

func bytesAsFloats(row []byte) []float64 {
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&row))
	header.Len /= 8
	header.Cap /= 8
	return *(*[]float64)(unsafe.Pointer(&header))

}

func saveFieldValues(f *os.File, fv *fieldValues, nCols int) (err error) {
	nFreeRows := len(fv.freelist)
	if err = binary.Write(f, binary.BigEndian, uint64(nFreeRows)); err != nil {
		log.Errorf("Failed to write nFreeRows into fields file")
		return
	}
	log.Infof("Wrote nFreeRows into fields file")

	if _, err = f.Write(freeListAsBytes(fv.freelist)); err != nil {
		log.Errorf("Failed to write freeList into fields file")
		return
	}
	log.Infof("Wrote freeList into fields file")

	nRows := len(fv.data)
	if err = binary.Write(f, binary.BigEndian, uint64(nRows)); err != nil {
		log.Errorf("Failed to write nRows into fields file")
		return
	}
	log.Infof("Wrote nRows into fields file")

	if err = binary.Write(f, binary.BigEndian, uint64(nCols)); err != nil {
		log.Errorf("Failed to write nCols into fields file")
		return
	}
	log.Infof("Wrote nCols into fields file")

	zeros := make([]float64, nCols)
	for i, row := range fv.data {
		// log.Infof("ROW %d: %v length %d", i, row, len(row))
		// log.Infof("bytes: %v", floatsAsBytes(row))
		if _, err = f.Write(floatsAsBytes(row)); err != nil {
			log.Errorf("Failed to write row %d into fields file", i)
			return
		}
		// shorter rows need to be zero-padded at the end
		if len(row) < nCols {
			pad := zeros[:nCols-len(row)]
			// log.Infof("pad bytes: %v", floatsAsBytes(pad))
			if _, err = f.Write(floatsAsBytes(pad)); err != nil {
				log.Errorf("Failed to zero-pad row %d into file", i)
				return
			}
		}
	}
	log.Infof("Wrote data into fields file")
	return
}

func loadFieldValues(f *os.File) (fv *fieldValues, err error) {
	var nRows, nCols, nFreeRows uint64
	if err = binary.Read(f, binary.BigEndian, &nFreeRows); err != nil {
		log.Errorf("Failed to nFreeRows from fields file")
		return
	}
	log.Infof("Read nFreeRows from fields file: %v", nFreeRows)
	byteFreeList := make([]byte, 4*nFreeRows)
	if _, err = f.Read(byteFreeList); err != nil {
		log.Errorf("Failed to read freeList from fields file")
		return
	}
	log.Infof("Read freeList from fields file")

	if err = binary.Read(f, binary.BigEndian, &nRows); err != nil {
		log.Errorf("Failed to nRows from fields file")
		return
	}
	log.Infof("Read nRows from fields file: %v", nRows)
	if err = binary.Read(f, binary.BigEndian, &nCols); err != nil {
		log.Errorf("Failed to nCols from fields file")
		return
	}
	log.Infof("Read nCols from fields file: %v", nCols)
	byteData := make([]byte, 8*nRows*nCols)
	if _, err = f.Read(byteData); err != nil {
		log.Errorf("Failed to read fields data from fields file")
		return
	}
	log.Infof("Read fields data from fields file")

	fv = &fieldValues{
		freelist: bytesAsFreeList(byteFreeList),
		data: make([][]float64, nRows),
	}
	for i := uint64(0); i < nRows; i++ {
		fv.data[i] = bytesAsFloats(byteData[i*8*nCols:(i+1)*8*nCols])
	}
	return
}
