package collection

import (
	"encoding/binary"
	"os"
	"reflect"
	"unsafe"

	"github.com/tidwall/tile38/internal/log"
)

type fieldValues struct {
	freelist []fieldValuesSlot
	data     [][]float64
}

type fieldValuesSlot int

const nilValuesSlot fieldValuesSlot = -1

func (f *fieldValues) get(k fieldValuesSlot) []float64 {
	if k == nilValuesSlot {
		return nil
	}
	return f.data[int(k)]
}

func (f *fieldValues) set(k fieldValuesSlot, itemData []float64) fieldValuesSlot {
	// if we're asked to store into the nil values slot, it means one of two things:
	//   - we are doing a replace on an item that previously had nil fields
	//   - we are inserting a new item
	// in either case, check if the new values are not nil, and if so allocate a
	// new slot
	if k == nilValuesSlot {
		if itemData == nil {
			return nilValuesSlot
		}

		// first check if there is a slot on the freelist to reuse
		if len(f.freelist) > 0 {
			var slot fieldValuesSlot
			slot, f.freelist = f.freelist[len(f.freelist)-1], f.freelist[:len(f.freelist)-1]
			f.data[slot] = itemData
			return slot
		}

		// no reusable slot, append
		f.data = append(f.data, itemData)
		return fieldValuesSlot(len(f.data) - 1)

	}
	f.data[int(k)] = itemData
	return k
}

func (f *fieldValues) remove(k fieldValuesSlot) {
	if k == nilValuesSlot {
		return
	}
	f.data[int(k)] = nil
	f.freelist = append(f.freelist, k)
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
