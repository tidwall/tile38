package collection

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"unsafe"

	"github.com/tidwall/btree"
	"github.com/tidwall/geoindex"
	"github.com/tidwall/geojson"
	"github.com/tidwall/rbang"
	"github.com/tidwall/tile38/internal/log"
	"github.com/tidwall/tinybtree"
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

	var itemMap map[*itemT]uint32
	if itemMap, err = c.saveItems(filepath.Join(dir, "itemsData"), filepath.Join(dir, "itemsTree"), snapshotId); err != nil {
		log.Errorf("Failed to save items")
	}
	log.Infof("Saved items")

	if err = c.saveValuesTree(filepath.Join(dir, "valuesTree"), itemMap, snapshotId); err != nil {
		log.Errorf("Failed to save valuesTree")
	}
	log.Infof("Saved valuesTree")

	if err = c.saveIndexTree(filepath.Join(dir, "indexTree"), itemMap, snapshotId); err != nil {
		log.Errorf("Failed to save indexTree")
	}
	log.Infof("Saved indexTree")
	return
}

func (c *Collection) Load(dir string, snapshotId uint64, parseOpts *geojson.ParseOptions) (err error) {
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

	var itemList []*itemT
	if itemList, err = c.loadItemsData(filepath.Join(dir, "itemsData"), snapshotId, parseOpts); err != nil {
		log.Errorf("Failed to load itemsData")
		return
	}
	log.Infof("Loaded itemsData")

	if err = c.loadItemsTree(filepath.Join(dir, "itemsTree"), itemList, snapshotId); err != nil {
		log.Errorf("Failed to load itemsTree")
		return
	}
	log.Infof("Loaded itemsTree")

	if err = c.loadValuesTree(filepath.Join(dir, "valuesTree"), itemList, snapshotId); err != nil {
		log.Errorf("Failed to load valuesTree")
		return
	}
	log.Infof("Loaded valuesTree")

	if err = c.loadIndexTree(filepath.Join(dir, "indexTree"), itemList, snapshotId); err != nil {
		log.Errorf("Failed to load indexTree")
		return
	}
	log.Infof("Loaded indexTree")

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

	if err = binary.Write(f, binary.BigEndian, uint64(c.weight)); err != nil {
		log.Errorf("Failed to write weight into fields file")
		return
	}

	if err = binary.Write(f, binary.BigEndian, uint64(c.points)); err != nil {
		log.Errorf("Failed to write points into fields file")
		return
	}

	if err = binary.Write(f, binary.BigEndian, uint64(c.objects)); err != nil {
		log.Errorf("Failed to write objects into fields file")
		return
	}

	if err = binary.Write(f, binary.BigEndian, uint64(c.nobjects)); err != nil {
		log.Errorf("Failed to write nobjects into fields file")
		return
	}

	if err = binary.Write(f, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into stats file")
		return
	}
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

	if err = verifySnapshotId(f, snapshotId); err != nil {
		return
	}

	var word uint64

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read weight from stats file")
		return
	}
	c.weight = int(word)

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read points from stats file")
		return
	}
	c.points = int(word)

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read objects from stats file")
		return
	}
	c.objects = int(word)

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read nobjects from stats file")
		return
	}
	c.nobjects = int(word)

	if err = verifySnapshotId(f, snapshotId); err != nil {
		return
	}

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

	nFields := len(c.fieldMap)
	if err = binary.Write(f, binary.BigEndian, uint64(nFields)); err != nil {
		log.Errorf("Failed to write nFields into fields file")
		return
	}

	for name, idx := range c.fieldMap {
		nameBytes := []byte(name)
		nBytes := len(nameBytes)
		if err = binary.Write(f, binary.BigEndian, uint64(nBytes)); err != nil {
			log.Errorf("Failed to write nBytes into fields file")
			return
		}

		if _, err = f.Write(nameBytes); err != nil {
			log.Errorf("Failed to write nameBytes into fields file")
			return
		}

		if err = binary.Write(f, binary.BigEndian, uint64(idx)); err != nil {
			log.Errorf("Failed to write idx into fields file")
			return
		}
	}

	if err = saveFieldValues(f, c.fieldValues, len(c.fieldArr)); err != nil {
		log.Errorf("Failed to save field values")
		return
	}

	if err = binary.Write(f, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into fields file")
		return
	}

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

	if err = verifySnapshotId(f, snapshotId); err != nil {
		return
	}

	var nFields, nBytes, idx uint64
	if err = binary.Read(f, binary.BigEndian, &nFields); err != nil {
		log.Errorf("Failed to nFields from fields file")
		return
	}

	c.fieldMap = make(map[string]int)
	for i := uint64(0); i < nFields; i++ {
		if err = binary.Read(f, binary.BigEndian, &nBytes); err != nil {
			log.Errorf("Failed to read nBytes from fields file")
			return
		}
		nameBytes := make([]byte, nBytes)
		if _, err = io.ReadFull(f, nameBytes); err != nil {
			log.Errorf("Failed to read nameBytes from fields file")
			return
		}
		if err = binary.Read(f, binary.BigEndian, &idx); err != nil {
			log.Errorf("Failed to read idx from fields file")
			return
		}

		field := string(nameBytes)
		c.fieldMap[field] = int(idx)
		c.addToFieldArr(field)
	}

	if c.fieldValues, err = loadFieldValues(f); err != nil {
		log.Errorf("Failed to load field values")
		return
	}

	if err = verifySnapshotId(f, snapshotId); err != nil {
		return
	}

	return
}

func saveFieldValues(f *os.File, fv *fieldValues, nCols int) (err error) {
	freeListBytes := freeListAsBytes(fv.freelist)
	nFreelistBytes := len(freeListBytes)
	if err = binary.Write(f, binary.BigEndian, uint64(nFreelistBytes)); err != nil {
		log.Errorf("Failed to write nFreelistBytes into fields file")
		return
	}

	if _, err = f.Write(freeListBytes); err != nil {
		log.Errorf("Failed to write freeList into fields file")
		return
	}

	nRows := len(fv.data)
	if err = binary.Write(f, binary.BigEndian, uint64(nRows)); err != nil {
		log.Errorf("Failed to write nRows into fields file")
		return
	}

	if err = binary.Write(f, binary.BigEndian, uint64(nCols)); err != nil {
		log.Errorf("Failed to write nCols into fields file")
		return
	}

	zeros := make([]float64, nCols)
	for i, row := range fv.data {
		if _, err = f.Write(floatsAsBytes(row)); err != nil {
			log.Errorf("Failed to write row %d into fields file", i)
			return
		}
		// shorter rows need to be zero-padded at the end
		if len(row) < nCols {
			pad := zeros[:nCols-len(row)]
			if _, err = f.Write(floatsAsBytes(pad)); err != nil {
				log.Errorf("Failed to zero-pad row %d into file", i)
				return
			}
		}
	}
	return
}

func loadFieldValues(f *os.File) (fv *fieldValues, err error) {
	var nRows, nCols, nFreelistBytes uint64
	if err = binary.Read(f, binary.BigEndian, &nFreelistBytes); err != nil {
		log.Errorf("Failed to read nFreelistBytes from fields file")
		return
	}
	byteFreeList := make([]byte, nFreelistBytes)
	if _, err = io.ReadFull(f, byteFreeList); err != nil {
		log.Errorf("Failed to read freeList from fields file")
		return
	}

	if err = binary.Read(f, binary.BigEndian, &nRows); err != nil {
		log.Errorf("Failed to nRows from fields file")
		return
	}
	if err = binary.Read(f, binary.BigEndian, &nCols); err != nil {
		log.Errorf("Failed to nCols from fields file")
		return
	}
	byteData := make([]byte, 8*nRows*nCols)
	if _, err = io.ReadFull(f, byteData); err != nil {
		log.Errorf("Failed to read fields data from fields file")
		return
	}

	fv = &fieldValues{
		freelist: bytesAsFreeList(byteFreeList),
		data: make([][]float64, nRows),
	}
	for i := uint64(0); i < nRows; i++ {
		fv.data[i] = bytesAsFloats(byteData[i*8*nCols:(i+1)*8*nCols])
	}
	return
}

func (c *Collection) saveItems(dataFile string, treeFile string, snapshotId uint64) (itemMap map[*itemT]uint32, err error) {
	var df, tf *os.File
	df, err = os.Create(dataFile)
	log.Infof("Created items data file: %s", dataFile)
	if err != nil {
		return
	}
	defer func() {
		if df.Close() != nil {
			log.Errorf("Failed to close %s", dataFile)
		}
	}()
	tf, err = os.Create(treeFile)
	log.Infof("Created items tree file: %s", treeFile)
	if err != nil {
		return
	}
	defer func() {
		if tf.Close() != nil {
			log.Errorf("Failed to close %s", treeFile)
		}
	}()

	bdw := bufio.NewWriter(df)
	defer func() {
		if bdw.Flush() != nil {
			log.Errorf("Failed to flush %s", dataFile)
		}
	}()

	btw := bufio.NewWriter(tf)
	defer func() {
		if btw.Flush() != nil {
			log.Errorf("Failed to flush %s", treeFile)
		}
	}()

	if err = binary.Write(bdw, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into items data file")
		return
	}

	if err = binary.Write(btw, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into tree data file")
		return
	}

	if err = binary.Write(bdw, binary.BigEndian, uint32(c.items.Len())); err != nil {
		log.Errorf("Failed to write tree length into items data file")
		return
	}

	itemMap = make(map[*itemT]uint32, c.items.Len())
	var itemNum uint32
	itemSaver := func (w io.Writer, value interface{}) (err error) {
		item := value.(*itemT)
		itemMap[item] = itemNum
		if err = binary.Write(w, binary.BigEndian, itemNum); err != nil {
			return
		}

		// using closure to access data file, into which we dump the actual data
		if err = saveString(bdw, item.id); err != nil {
			return
		}
		if err = binary.Write(bdw, binary.BigEndian, int32(item.fieldValuesSlot)); err != nil {
			return
		}
		if err = binary.Write(bdw, binary.BigEndian, objIsSpatial(item.obj)); err != nil {
			return
		}
		if err = saveString(bdw, item.obj.String()); err != nil {
			return
		}

		itemNum++
		return
	}

	if err = c.items.Save(btw, itemSaver); err != nil {
		log.Errorf("Failed to save items tree and data")
	}

	if err = binary.Write(bdw, binary.BigEndian, snapshotId); err != nil {
		log.Errorf( "Failed to write snapshotId into items data file")
		return
	}

	if err = binary.Write(btw, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into items tree file")
		return
	}

	return
}

func (c * Collection) loadItemsData(dataFile string, snapshotId uint64, parseOpts *geojson.ParseOptions) (itemList []*itemT, err error) {
	var f *os.File
	f, err = os.Open(dataFile)
	log.Infof("Opened itemsData file: %s", dataFile)
	if err != nil {
		return
	}
	defer func() {
		if f.Close() != nil {
			log.Errorf("Failed to close %s", dataFile)
		}
	}()
	br := bufio.NewReader(f)

	if err = verifySnapshotId(br, snapshotId); err != nil {
		return
	}

	var word uint32
	if err = binary.Read(br, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read tree length from items data file")
	}
	nItems := int(word)
	itemList = make([]*itemT, nItems)
	buf := make([]byte, 0)
	var idStr, objStr string
	var spatial bool
	var fvs int32

	type todoItem struct {
		index int
		spatial bool
		id string
		json string
		slot fieldValuesSlot
	}

	todoChannel := make(chan todoItem, 1024)

	var wg sync.WaitGroup
	var nWorkers int
	if runtime.NumCPU() > 10 {
		nWorkers = 10
	} else {
		nWorkers = 2
	}
	for i := 0; i < nWorkers; i++ {
		go func() {
			defer wg.Done()
			var obj geojson.Object
			for ti := range todoChannel {
				if ti.spatial {
					if obj, err = geojson.Parse(ti.json, parseOpts); err != nil {
						log.Errorf("Failed to parse object from data file, json %s", ti.json)
						return
					}
				} else {
					obj = String(objStr)
				}
				itemList[ti.index] = &itemT{id: ti.id, obj: obj, fieldValuesSlot: ti.slot}
			}
		}()
		wg.Add(1)
	}

	for i := 0; i < nItems; i++ {
		if idStr, buf, err = loadString(br, buf); err != nil {
			log.Errorf("Failed to read ID from data file, item %d", i)
			return
		}
		if err = binary.Read(br, binary.BigEndian, &fvs); err != nil {
			log.Errorf("Failed to read fieldValuesSlot from data file, item %d", i)
			return
		}
		if err = binary.Read(br, binary.BigEndian, &spatial); err != nil {
			log.Errorf("Failed to read spatial bool from data file, item %d", i)
			return
		}
		if objStr, buf, err = loadString(br, buf); err != nil {
			log.Errorf("Failed to read object from data file, item %d", i)
			return
		}
		todoChannel <- todoItem{
			index: i,
			spatial: spatial,
			id: idStr,
			json: objStr,
			slot: fieldValuesSlot(fvs),
		}
	}
	close(todoChannel)

	wg.Wait()

	if err = verifySnapshotId(br, snapshotId); err != nil {
		return
	}
	return
}

func (c * Collection) loadItemsTree(treeFile string, itemList []*itemT, snapshotId uint64) (err error) {
	var f *os.File
	f, err = os.Open(treeFile)
	log.Infof("Opened itemsTree file: %s", treeFile)
	if err != nil {
		return
	}
	defer func() {
		if f.Close() != nil {
			log.Errorf("Failed to close %s", treeFile)
		}
	}()
	br := bufio.NewReader(f)

	if err = verifySnapshotId(br, snapshotId); err != nil {
		return
	}

	itemLoader := func(r io.Reader, obuf []byte) (value interface{}, buf []byte, err error) {
		var itemNum uint32
		if err = binary.Read(r, binary.BigEndian, &itemNum); err != nil {
			return
		}
		return itemList[itemNum], obuf,nil
	}

	if c.items, err = tinybtree.Load(br, itemLoader); err != nil {
		log.Errorf("Failed to load itemsTree")
	}

	if err = verifySnapshotId(br, snapshotId); err != nil {
		return
	}
	return
}

func (c * Collection) saveValuesTree(treeFile string, itemMap map[*itemT]uint32, snapshotId uint64) (err error) {
	var f *os.File
	f, err = os.Create(treeFile)
	log.Infof("Created valuesTree file: %s", treeFile)
	if err != nil {
		return
	}
	defer func() {
		if f.Close() != nil {
			log.Errorf("Failed to close %s", treeFile)
		}
	}()
	bw := bufio.NewWriter(f)
	defer func() {
		if bw.Flush() != nil {
			log.Errorf("Failed to flush %s", treeFile)
		}
	}()

	if err = binary.Write(bw, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into valuesTree file")
		return
	}

	itemSaver := func (w io.Writer, itm btree.Item) (err error) {
		item := itm.(*itemT)
		if err = binary.Write(w, binary.BigEndian, itemMap[item]); err != nil {
			return
		}
		return
	}

	if err = c.values.Save(bw, itemSaver); err != nil {
		log.Errorf("Failed to save values tree")
	}

	if err = binary.Write(bw, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into valuesTree file")
		return
	}
	return
}

func (c * Collection) loadValuesTree(treeFile string, itemList []*itemT, snapshotId uint64) (err error) {
	var f *os.File
	f, err = os.Open(treeFile)
	log.Infof("Opened valuesTree file: %s", treeFile)
	if err != nil {
		return
	}
	defer func() {
		if f.Close() != nil {
			log.Errorf("Failed to close %s", treeFile)
		}
	}()
	br := bufio.NewReader(f)

	if err = verifySnapshotId(br, snapshotId); err != nil {
		return
	}

	itemLoader := func(r io.Reader, obuf []byte) (item btree.Item, buf []byte, err error) {
		var itemNum uint32
		if err = binary.Read(r, binary.BigEndian, &itemNum); err != nil {
			return
		}
		return itemList[itemNum], obuf,nil
	}

	if c.values, err = btree.Load(br, itemLoader); err != nil {
		log.Errorf("Failed to load valuesTree")
	}

	if err = verifySnapshotId(br, snapshotId); err != nil {
		return
	}
	return
}

func (c *Collection) saveIndexTree(indexFile string, itemMap map[*itemT]uint32, snapshotId uint64) (err error) {
	var f *os.File
	f, err = os.Create(indexFile)
	log.Infof("Created indexTree file: %s", indexFile)
	if err != nil {
		return
	}
	defer func() {
		if f.Close() != nil {
			log.Errorf("Failed to close %s", indexFile)
		}
	}()
	bw := bufio.NewWriter(f)
	defer func() {
		if bw.Flush() != nil {
			log.Errorf("Failed to flush %s", indexFile)
		}
	}()

	if err = binary.Write(bw, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into indexTree file")
		return
	}

	itemSaver := func (w io.Writer, data interface{}) (err error) {
		item := data.(*itemT)
		if err = binary.Write(w, binary.BigEndian, itemMap[item]); err != nil {
			return
		}
		return
	}

	if err = c.index.Save(bw, itemSaver); err != nil {
		log.Errorf("Failed to save indexTree")
	}

	if err = binary.Write(bw, binary.BigEndian, snapshotId); err != nil {
		log.Errorf("Failed to write snapshotId into IndexTree file")
		return
	}
	return
}

func (c * Collection) loadIndexTree(treeFile string, itemList []*itemT, snapshotId uint64) (err error) {
	var f *os.File
	f, err = os.Open(treeFile)
	log.Infof("Opened indexTree file: %s", treeFile)
	if err != nil {
		return
	}
	defer func() {
		if f.Close() != nil {
			log.Errorf("Failed to close %s", treeFile)
		}
	}()
	br := bufio.NewReader(f)

	if err = verifySnapshotId(br, snapshotId); err != nil {
		return
	}

	itemLoader := func(r io.Reader, obuf []byte) (value interface{}, buf []byte, err error) {
		var itemNum uint32
		if err = binary.Read(r, binary.BigEndian, &itemNum); err != nil {
			return
		}
		return itemList[itemNum], obuf,nil
	}

	c.index = geoindex.Wrap(&rbang.RTree{})
	if err = c.index.Load(br, itemLoader); err != nil {
		log.Errorf("Failed to load valuesTree")
	}
	if err = verifySnapshotId(br, snapshotId); err != nil {
		return
	}

	return
}

// Helper functions
func stringAsBytes(s string) []byte {
	var b []byte
	bHdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	bHdr.Data = uintptr(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&s)).Data))
	bHdr.Len = len(s)
	bHdr.Cap = len(s)
	return b
}

func saveString(w io.Writer, s string) (err error) {
	keyBytes := stringAsBytes(s)
	numBytes := len(keyBytes)
	if err = binary.Write(w, binary.BigEndian, uint64(numBytes)); err != nil {
		return
	}
	if _, err = w.Write(keyBytes); err != nil {
		return
	}
	return
}

func ensureLen(slc []byte, sz int) []byte {
	if cap(slc) < sz {
		return make([]byte, sz)
	}
	return slc[:sz]
}

func loadString(r io.Reader, buf []byte) (s string, newBuf []byte, err error) {
	var numBytes uint64
	if err = binary.Read(r, binary.BigEndian, &numBytes); err != nil {
		return
	}
	newBuf = ensureLen(buf, int(numBytes))
	if _, err = io.ReadFull(r, newBuf); err != nil {
		return
	}
	return string(newBuf), newBuf,nil
}


func freeListAsBytes(row []fieldValuesSlot) []byte {
	fvsSize := int(unsafe.Sizeof(fieldValuesSlot(0)))
	header := (*reflect.SliceHeader)(unsafe.Pointer(&row))
	header.Len *= fvsSize
	header.Cap *= fvsSize
	return *(*[]byte)(unsafe.Pointer(&row))
}

func bytesAsFreeList(row []byte) []fieldValuesSlot {
	fvsSize := int(unsafe.Sizeof(fieldValuesSlot(0)))
	header := (*reflect.SliceHeader)(unsafe.Pointer(&row))
	header.Len /= fvsSize
	header.Cap /= fvsSize
	return *(*[]fieldValuesSlot)(unsafe.Pointer(&row))
}

func floatsAsBytes(row []float64) []byte {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&row))
	header.Len *= 8
	header.Cap *= 8
	return *(*[]byte)(unsafe.Pointer(&row))
}

func bytesAsFloats(row []byte) []float64 {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&row))
	header.Len /= 8
	header.Cap /= 8
	return *(*[]float64)(unsafe.Pointer(&row))
}

func verifySnapshotId(w io.Reader, snapshotId uint64) (err error){
	var word uint64
	if err = binary.Read(w, binary.BigEndian, &word); err != nil {
		log.Errorf("Failed to read snapshotId")
		return
	}
	if word != snapshotId {
		err = errors.New("SnapshotId does not match")
		log.Errorf("expected %v found %v", snapshotId, word)
		return
	}
	return
}
