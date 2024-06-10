package object

import (
	"encoding/binary"
	"unsafe"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/tile38/internal/field"
)

type pointObject struct {
	base Object
	pt   geojson.SimplePoint
}

type geoObject struct {
	base Object
	geo  geojson.Object
}

const opoint = 1
const ogeo = 2

type Object struct {
	head   string // tuple (kind,expires,id)
	fields field.List
}

func (o *Object) geo() geojson.Object {
	if o != nil {
		switch o.head[0] {
		case opoint:
			return &(*pointObject)(unsafe.Pointer(o)).pt
		case ogeo:
			return (*geoObject)(unsafe.Pointer(o)).geo
		}
	}
	return nil
}

// uvarint is a slightly modified version of binary.Uvarint, and it's a little
// faster. But it lacks overflow checks which are not needed for our use.
func uvarint(s string) (uint64, int) {
	var x uint64
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b < 0x80 {
			return x | uint64(b)<<(i*7), i + 1
		}
		x |= uint64(b&0x7f) << (i * 7)
	}
	return 0, 0
}

func varint(s string) (int64, int) {
	ux, n := uvarint(s)
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, n
}

func (o *Object) ID() string {
	if o.head[1] == 0 {
		return o.head[2:]
	}
	_, n := varint(o.head[1:])
	return o.head[1+n:]
}

func (o *Object) Fields() field.List {
	return o.fields
}

func (o *Object) Expires() int64 {
	ex, _ := varint(o.head[1:])
	return ex
}

func (o *Object) Rect() geometry.Rect {
	ogeo := o.geo()
	if ogeo == nil {
		return geometry.Rect{}
	}
	return ogeo.Rect()
}

func (o *Object) Geo() geojson.Object {
	return o.geo()
}

func (o *Object) String() string {
	ogeo := o.geo()
	if ogeo == nil {
		return ""
	}
	return ogeo.String()
}

func (o *Object) IsSpatial() bool {
	_, ok := o.geo().(String)
	return !ok
}

func (o *Object) Weight() int {
	var weight int
	weight += len(o.ID())
	ogeo := o.geo()
	if ogeo != nil {
		if o.IsSpatial() {
			weight += ogeo.NumPoints() * 16
		} else {
			weight += len(ogeo.String())
		}
	}
	weight += o.Fields().Weight()
	return weight
}

func makeHead(kind byte, id string, expires int64) string {
	var exb [20]byte
	exn := 1
	if expires != 0 {
		exn = binary.PutVarint(exb[:], expires)
	}
	n := 1 + exn + len(id)
	head := make([]byte, n)
	head[0] = kind
	copy(head[1:], exb[:exn])
	copy(head[1+exn:], id)
	return *(*string)(unsafe.Pointer(&head))
}

func newPoint(id string, pt geometry.Point, expires int64, fields field.List,
) *Object {
	return (*Object)(unsafe.Pointer(&pointObject{
		Object{
			head:   makeHead(opoint, id, expires),
			fields: fields,
		},
		geojson.SimplePoint{Point: pt},
	}))
}
func newGeo(id string, geo geojson.Object, expires int64, fields field.List,
) *Object {
	return (*Object)(unsafe.Pointer(&geoObject{
		Object{
			head:   makeHead(ogeo, id, expires),
			fields: fields,
		},
		geo,
	}))
}

func New(id string, geo geojson.Object, expires int64, fields field.List,
) *Object {
	switch p := geo.(type) {
	case *geojson.SimplePoint:
		return newPoint(id, p.Base(), expires, fields)
	case *geojson.Point:
		if p.IsSimple() {
			return newPoint(id, p.Base(), expires, fields)
		}
	}
	return newGeo(id, geo, expires, fields)
}
