package rbang

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"unsafe"
)

func (tr *RTree) Save(f io.Writer, saveValue func (w io.Writer, value interface{}) error) (err error) {
	if err = binary.Write(f, binary.BigEndian, uint64(tr.height)); err != nil {
		return
	}
	fmt.Printf("Wrote height\n")

	if err = binary.Write(f, binary.BigEndian, uint64(tr.count)); err != nil {
		return
	}
	fmt.Printf("Wrote count\n")

	gotTree := tr.root.data != nil
	if err = binary.Write(f, binary.BigEndian, gotTree); err != nil {
		return
	}
	fmt.Printf("Wrote gotTree\n")

	if gotTree {
		if err = tr.root.save(f, saveValue, tr.height); err != nil {
			return
		}
	}

	return
}

func (r *rect) save(f io.Writer,
	saveValue func (w io.Writer, data interface{}) error,
	height int,
) (err error) {
	if _, err = f.Write(floatsAsBytes(r.min[:])); err != nil {
		return
	}
	if _, err = f.Write(floatsAsBytes(r.max[:])); err != nil {
		return
	}
	fmt.Printf("Wrote node min/max\n")

	n := r.data.(*node)
	nItems := uint8(n.count)
	if err = binary.Write(f, binary.BigEndian, nItems); err != nil {
		return
	}
	fmt.Printf("Wrote nItems: %v\n", nItems)

	gotChildren := height > 0
	if err = binary.Write(f, binary.BigEndian, gotChildren); err != nil {
		return
	}
	fmt.Printf("Wrote gotChildren: %v\n", gotChildren)

	if gotChildren {
		for i := 0; i < n.count; i++ {
			if err = n.rects[i].save(f, saveValue, height-1); err != nil {
				return
			}
		}
	} else {
		for i := 0; i < n.count; i++ {
			if _, err = f.Write(floatsAsBytes(n.rects[i].min[:])); err != nil {
				return
			}
			if _, err = f.Write(floatsAsBytes(n.rects[i].max[:])); err != nil {
				return
			}
			if err = saveValue(f, n.rects[i].data); err != nil {
				return
			}
		}
	}
	return
}

func (tr *RTree) Load(
	f io.Reader,
	loadValue func (r io.Reader, obuf []byte) (interface{}, []byte, error),
) (err error) {
	var word uint64

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		return
	}
	fmt.Printf("Read height\n")
	tr.height = int(word)

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		return
	}
	fmt.Printf("Read count\n")
	tr.count = int(word)

	var gotTree bool
	if err = binary.Read(f, binary.BigEndian, &gotTree); err != nil {
		return
	}
	fmt.Printf("Read gotTree: %v\n", gotTree)

	if gotTree {
		// this buffer will be re-used or replaced for a larger one, as needed
		buf := make([]byte, 32)
		if tr.root, buf, err = load(f, buf, loadValue); err != nil {
			return
		}
	}

	return
}

func load(
	f io.Reader,
	oldBuf []byte,
	loadValue func (r io.Reader, obuf []byte) (interface{}, []byte, error),
) (r rect, buf []byte, err error) {
	buf = oldBuf[:]

	if err = r.setMinMaxFromFile(f, buf); err != nil {
		return
	}

	n := &node{}
	r.data = n

	var short uint8
	if err = binary.Read(f, binary.BigEndian, &short); err != nil {
		return
	}
	fmt.Printf("Read numItems: %d\n", short)
	n.count = int(short)

	var gotChildren bool
	if err = binary.Read(f, binary.BigEndian, &gotChildren); err != nil {
		return
	}
	fmt.Printf("Read gotChildren: %v\n", gotChildren)

	if gotChildren {
		for i := 0; i < n.count; i++ {
			if n.rects[i], buf, err = load(f, buf, loadValue); err != nil {
				return
			}
		}
	} else {
		for i := 0; i < n.count; i++ {
			if err = n.rects[i].setMinMaxFromFile(f, buf); err != nil {
				return
			}
			if n.rects[i].data, buf, err = loadValue(f, buf); err != nil {
				return
			}
		}
	}

	return
}

func (r *rect) setMinMaxFromFile(f io.Reader, buf []byte) (err error) {
	buf = buf[:32]
	if _, err = f.Read(buf); err != nil {
		return
	}
	floatsMinMax := bytesAsFloats(buf)
	r.min[0] = floatsMinMax[0]
	r.min[1] = floatsMinMax[1]
	r.max[0] = floatsMinMax[2]
	r.max[1] = floatsMinMax[3]

	return
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
