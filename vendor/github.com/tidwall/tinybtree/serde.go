package tinybtree

import (
	"encoding/binary"
	"io"
	"reflect"
	"unsafe"
)

func (tr *BTree) Save(f io.Writer, saveValue func (w io.Writer, value interface{}) error) (err error) {
	if err = binary.Write(f, binary.BigEndian, uint64(tr.height)); err != nil {
		return
	}

	if err = binary.Write(f, binary.BigEndian, uint64(tr.length)); err != nil {
		return
	}

	gotTree := tr.root != nil
	if err = binary.Write(f, binary.BigEndian, gotTree); err != nil {
		return
	}

	if gotTree {
		if err = tr.root.save(f, saveValue, tr.height); err != nil {
			return
		}
	}

	return
}

func Load(
	f io.Reader,
	loadValue func (r io.Reader, obuf []byte) (interface{}, []byte, error),
) (tr BTree, err error) {
	var word uint64

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		return
	}
	tr.height = int(word)

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		return
	}
	tr.length = int(word)

	var gotTree bool
	if err = binary.Read(f, binary.BigEndian, &gotTree); err != nil {
		return
	}

	if gotTree {
		// this buffer will be re-used or replaced for a larger one, as needed
		buf := make([]byte, 0)
		if tr.root, buf, err = load(f, buf, loadValue, tr.height); err != nil {
			return
		}
	}

	return
}

func (n *node) save(
	f io.Writer,
	saveValue func (w io.Writer, value interface{}) error,
	height int,
) (err error) {
	if err = binary.Write(f, binary.BigEndian, uint8(n.numItems)); err != nil {
		return
	}
	// values on this node
	for i := 0; i < n.numItems; i++ {
		item := n.items[i]
		if err = saveString(f, item.key); err != nil {
			return
		}
		if err = saveValue(f, item.value); err != nil {
			return
		}
	}
	// children
	if height > 0 {
		for i := 0; i <= n.numItems; i++ {
			if err = n.children[i].save(f, saveValue, height-1); err != nil {
				return
			}
		}
	}

	return
}

func load(
	f io.Reader,
	oldBuf []byte,
	loadValue func (r io.Reader, obuf []byte) (interface{}, []byte, error),
	height int,
) (n *node, buf []byte, err error) {
	buf = oldBuf[:]
	n = &node{}
	var short uint8
	if err = binary.Read(f, binary.BigEndian, &short); err != nil {
		return
	}
	n.numItems = int(short)
	var key string
	var value interface{}
	// values on this node
	for i := 0; i < n.numItems; i++ {
		if key, buf, err = loadString(f, buf); err != nil {
			return
		}
		if value, buf, err = loadValue(f, buf); err != nil {
			return
		}
		n.items[i] = item{key, value}
	}
	// children
	if height > 0 {
		for i := 0; i <= n.numItems; i++ {
			if n.children[i], buf, err = load(f, buf, loadValue, height-1); err != nil {
				return
			}
		}
	}

	return
}

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
