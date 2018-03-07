package geojson

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tidwall/gjson"
	"github.com/tidwall/tile38/geojson/poly"
)

const (
	point              = 0
	multiPoint         = 1
	lineString         = 2
	multiLineString    = 3
	polygon            = 4
	multiPolygon       = 5
	geometryCollection = 6
	feature            = 7
	featureCollection  = 8
)

var (
	errNotEnoughData = errors.New("not enough data")
	errTooMuchData   = errors.New("too much data")
	errInvalidData   = errors.New("invalid data")
)

var ( // json errors
	fmtErrTypeIsUnknown              = "the type '%s' is unknown"
	errInvalidTypeMember             = errors.New("type member is invalid. Expecting a string")
	errInvalidCoordinates            = errors.New("coordinates member is invalid. Expecting an array")
	errCoordinatesRequired           = errors.New("coordinates member is required")
	errInvalidGeometries             = errors.New("geometries member is invalid. Expecting an array")
	errGeometriesRequired            = errors.New("geometries member is required")
	errInvalidGeometryMember         = errors.New("geometry member is invalid. Expecting an object")
	errGeometryMemberRequired        = errors.New("geometry member is required")
	errInvalidFeaturesMember         = errors.New("features member is invalid. Expecting an array")
	errFeaturesMemberRequired        = errors.New("features member is required")
	errInvalidFeature                = errors.New("invalid feature in collection")
	errInvalidPropertiesMember       = errors.New("properties member in invalid. Expecting an array")
	errInvalidCoordinatesValue       = errors.New("coordinates member has an invalid value")
	errLineStringInvalidCoordinates  = errors.New("coordinates must be an array of two or more positions")
	errInvalidNumberOfPositionValues = errors.New("position must have two or more numbers")
	errInvalidPositionValue          = errors.New("position has an invalid value")
	errCoordinatesMustBeArray        = errors.New("coordinates member must be an array of positions")
	errMustBeALinearRing             = errors.New("polygon must have at least 4 positions and the first and last position must be the same")
	errBBoxInvalidType               = errors.New("bBox member is an invalid. Expecting an array")
	errBBoxInvalidNumberOfValues     = errors.New("bBox member requires exactly 4 or 6 values")
	errBBoxInvalidValue              = errors.New("bBox has an invalid value")
	errInvalidGeometry               = errors.New("invalid geometry in collection")
)

const nilz = 0

// Object is a geojson object
type Object interface {
	bboxPtr() *BBox
	hasPositions() bool
	appendJSON(dst []byte) []byte

	// WithinBBox detects if the object is fully contained inside a bbox.
	WithinBBox(bbox BBox) bool
	// IntersectsBBox detects if the object intersects a bbox.
	IntersectsBBox(bbox BBox) bool
	// Within detects if the object is fully contained inside another object.
	Within(o Object) bool
	// Intersects detects if the object intersects another object.
	Intersects(o Object) bool
	// Nearby detects if the object is nearby a position.
	Nearby(center Position, meters float64) bool
	// CalculatedBBox is exterior bbox containing the object.
	CalculatedBBox() BBox
	// CalculatedPoint is a point representation of the object.
	CalculatedPoint() Position
	// JSON is the json representation of the object. This might not be exactly the same as the original.
	JSON() string
	// String returns a string representation of the object. This may be JSON or something else.
	String() string
	// PositionCount return the number of coordinates.
	PositionCount() int
	// Weight returns the in-memory size of the object.
	Weight() int
	// MarshalJSON allows the object to be encoded in json.Marshal calls.
	MarshalJSON() ([]byte, error)
	// Geohash converts the object to a geohash value.
	Geohash(precision int) (string, error)
	// IsBBoxDefined returns true if the object has a defined bbox.
	IsBBoxDefined() bool
	// IsGeometry return true if the object is a geojson geometry object. false if it something else.
	IsGeometry() bool
}

func positionBBox(i int, bbox BBox, ps []Position) (int, BBox) {
	for _, p := range ps {
		if i == 0 {
			bbox.Min = p
			bbox.Max = p
		} else {
			if p.X < bbox.Min.X {
				bbox.Min.X = p.X
			}
			if p.Y < bbox.Min.Y {
				bbox.Min.Y = p.Y
			}
			if p.X > bbox.Max.X {
				bbox.Max.X = p.X
			}
			if p.Y > bbox.Max.Y {
				bbox.Max.Y = p.Y
			}
		}
		i++
	}
	return i, bbox
}

func isLinearRing(ps []Position) bool {
	return len(ps) >= 4 && ps[0] == ps[len(ps)-1]
}

// ObjectJSON parses geojson and returns an Object
func ObjectJSON(json string) (Object, error) {
	return objectMap(json, root)
}

var (
	root  = 0 // accept all types
	gcoll = 1 // accept only geometries
	feat  = 2 // accept only geometries
	fcoll = 3 // accept only features
)

func objectMap(json string, from int) (Object, error) {
	var err error
	res := gjson.Get(json, "type")
	if res.Type != gjson.String {
		return nil, errInvalidTypeMember
	}
	typ := res.String()
	if from != root {
		switch from {
		case gcoll, feat:
			switch typ {
			default:
				return nil, fmt.Errorf(fmtErrTypeIsUnknown, typ)
			case "Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon", "GeometryCollection":
			}
		case fcoll:
			switch typ {
			default:
				return nil, fmt.Errorf(fmtErrTypeIsUnknown, typ)
			case "Feature":
			}
		}
	}

	var o Object
	switch typ {
	default:
		return nil, fmt.Errorf(fmtErrTypeIsUnknown, typ)
	case "Point":
		o, err = fillSimplePointOrPoint(fillLevel1Map(json))
	case "MultiPoint":
		o, err = fillMultiPoint(fillLevel2Map(json))
	case "LineString":
		o, err = fillLineString(fillLevel2Map(json))
	case "MultiLineString":
		o, err = fillMultiLineString(fillLevel3Map(json))
	case "Polygon":
		o, err = fillPolygon(fillLevel3Map(json))
	case "MultiPolygon":
		o, err = fillMultiPolygon(fillLevel4Map(json))
	case "GeometryCollection":
		o, err = fillGeometryCollectionMap(json)
	case "Feature":
		o, err = fillFeatureMap(json)
	case "FeatureCollection":
		o, err = fillFeatureCollectionMap(json)
	}
	return o, err
}

func withinObjectShared(g Object, o Object, pin func(v Polygon) bool) bool {
	bbp := o.bboxPtr()
	if bbp != nil {
		if !g.WithinBBox(*bbp) {
			return false
		}
		if o.IsBBoxDefined() {
			return true
		}
	}
	switch v := o.(type) {
	default:
		return false
	case Point:
		return g.WithinBBox(v.CalculatedBBox())
	case SimplePoint:
		return g.WithinBBox(v.CalculatedBBox())
	case Polygon:
		if len(v.Coordinates) == 0 {
			return false
		}
		return pin(v)
	case MultiPolygon:
		for _, coords := range v.Coordinates {
			if pin(Polygon{Coordinates: coords}) {
				return true
			}
		}
		return false
	case Feature:
		return g.Within(v.Geometry)
	case FeatureCollection:
		if len(v.Features) == 0 {
			return false
		}
		for _, f := range v.Features {
			if !g.Within(f) {
				return false
			}
		}
		return true
	case GeometryCollection:
		if len(v.Geometries) == 0 {
			return false
		}
		for _, f := range v.Geometries {
			if !g.Within(f) {
				return false
			}
		}
		return true
	}
}

func intersectsObjectShared(g Object, o Object, pin func(v Polygon) bool) bool {
	bbp := o.bboxPtr()
	if bbp != nil {
		if !g.IntersectsBBox(*bbp) {
			return false
		}
		if o.IsBBoxDefined() {
			return true
		}
	}
	switch v := o.(type) {
	default:
		return false
	case Point:
		return g.IntersectsBBox(v.CalculatedBBox())
	case SimplePoint:
		return g.IntersectsBBox(v.CalculatedBBox())
	case Polygon:
		if len(v.Coordinates) == 0 {
			return false
		}
		return pin(v)
	case MultiPolygon:
		for _, coords := range v.Coordinates {
			if pin(Polygon{Coordinates: coords}) {
				return true
			}
		}
		return false
	case Feature:
		return g.Intersects(v.Geometry)
	case FeatureCollection:
		if len(v.Features) == 0 {
			return false
		}
		for _, f := range v.Features {
			if g.Intersects(f) {
				return true
			}
		}
		return false
	case GeometryCollection:
		if len(v.Geometries) == 0 {
			return false
		}
		for _, f := range v.Geometries {
			if g.Intersects(f) {
				return true
			}
		}
		return false
	}
}

// CirclePolygon returns a Polygon around the radius.
func CirclePolygon(x, y, meters float64, steps int) Polygon {
	if steps < 3 {
		steps = 3
	}
	p := Polygon{
		Coordinates: [][]Position{make([]Position, steps+1)},
	}
	center := Position{X: x, Y: y, Z: 0}
	step := 360.0 / float64(steps)
	i := 0
	for deg := 360.0; deg > 0; deg -= step {
		c := Position(poly.Point(center.Destination(meters, deg)))
		p.Coordinates[0][i] = c
		i++
	}
	p.Coordinates[0][i] = p.Coordinates[0][0]
	return p
}

// The object's calculated bounding box must intersect the radius of the circle to pass.
func nearbyObjectShared(g Object, x, y float64, meters float64) bool {
	if !g.hasPositions() {
		return false
	}
	center := Position{X: x, Y: y, Z: 0}
	bbox := g.CalculatedBBox()
	if bbox.Min.X == bbox.Max.X && bbox.Min.Y == bbox.Max.Y {
		// just a point, return is point is inside of the circle
		return center.DistanceTo(bbox.Min) <= meters
	}
	circlePoly := CirclePolygon(x, y, meters, 12)
	return g.Intersects(circlePoly)
}

func jsonMarshalString(s string) []byte {
	for i := 0; i < len(s); i++ {
		if s[i] < ' ' || s[i] == '\\' || s[i] == '"' || s[i] > 126 {
			b, _ := json.Marshal(s)
			return b
		}
	}
	b := make([]byte, len(s)+2)
	b[0] = '"'
	copy(b[1:], s)
	b[len(b)-1] = '"'
	return b
}

func stripWhitespace(s string) string {
	var p []byte
	var str bool
	var escs int
	for i := 0; i < len(s); i++ {
		c := s[i]
		if str {
			// We're inside of a string. Look out for '"' and '\' characters.
			if c == '\\' {
				// Increment the escape character counter.
				escs++
			} else {
				if c == '"' && escs%2 == 0 {
					// We reached the end of string
					str = false
				}
				// Reset the escape counter
				escs = 0
			}
		} else if c == '"' {
			// We encountared a double quote character.
			str = true
		} else if c <= ' ' {
			// Ignore the whitespace
			if p == nil {
				p = []byte(s[:i])
			}
			continue
		}
		// Append the character
		if p != nil {
			p = append(p, c)
		}
	}
	if p == nil {
		return s
	}
	return string(p)
}
