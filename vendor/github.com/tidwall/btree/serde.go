package btree

import (
	"encoding/binary"
	"io"
)

func (t *BTree) Save(
	f io.Writer,
	saveItem func (w io.Writer, value Item) error,
) (err error) {
	if err = binary.Write(f, binary.BigEndian, uint64(t.degree)); err != nil {
		return
	}

	if err = binary.Write(f, binary.BigEndian, uint64(t.length)); err != nil {
		return
	}

	gotTree := t.root != nil
	if err = binary.Write(f, binary.BigEndian, gotTree); err != nil {
		return
	}

	if gotTree {
		if err = t.root.save(f, saveItem); err != nil {
			return
		}
	}

	return
}

func (n *node) save(
	f io.Writer,
	saveItem func (w io.Writer, item Item) error,
) (err error) {
	nItems := len(n.items)
	if err = binary.Write(f, binary.BigEndian, uint8(nItems)); err != nil {
		return
	}

	gotChildren := len(n.children) > 0
	if err = binary.Write(f, binary.BigEndian, gotChildren); err != nil {
		return
	}
	// values on this node
	for i := 0; i < nItems; i++ {
		item := n.items[i]
		if err = saveItem(f, item); err != nil {
			return
		}
	}
	// children
	if gotChildren {
		for i := 0; i <= nItems; i++ {
			if err = n.children[i].save(f, saveItem); err != nil {
				return
			}
		}
	}

	return
}

func Load(
	f io.Reader,
	loadItem func (r io.Reader, obuf []byte) (Item, []byte, error),
) (t *BTree, err error) {
	t = &BTree{}
	var word uint64

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		return
	}
	t.degree = int(word)

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		return
	}
	t.length = int(word)

	var gotTree bool
	if err = binary.Read(f, binary.BigEndian, &gotTree); err != nil {
		return
	}

	if gotTree {
		// this buffer will be re-used or replaced for a larger one, as needed
		buf := make([]byte, 0)
		if t.root, buf, err = load(f, buf, loadItem); err != nil {
			return
		}
	}
	return
}

func load(
	f io.Reader,
	oldBuf []byte,
	loadItem func (r io.Reader, obuf []byte) (Item, []byte, error),
) (n *node, buf []byte, err error) {
	buf = oldBuf[:]
	n = &node{}

	var short uint8
	if err = binary.Read(f, binary.BigEndian, &short); err != nil {
		return
	}
	nItems := int(short)

	var gotChildren bool
	if err = binary.Read(f, binary.BigEndian, &gotChildren); err != nil {
		return
	}

	// values on this node
	var item Item
	n.items = make([]Item, nItems)
	for i := 0; i < nItems; i++ {
		if item, buf, err = loadItem(f, buf); err != nil {
			return
		}
		n.items[i] = item
	}
	// children
	if gotChildren {
		n.children = make([]*node, nItems+1)
		for i := 0; i <= nItems; i++ {
			if n.children[i], buf, err = load(f, buf, loadItem); err != nil {
				return
			}
		}
	}

	return
}
