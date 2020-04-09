package collection

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
