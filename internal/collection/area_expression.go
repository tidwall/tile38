package collection

import (
	"math"
	"strings"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

// BinaryOp represents various operators for expressions
type BinaryOp byte

// expression operator enum
const (
	NOOP BinaryOp = iota
	AND
	OR
	tokenAND    = "and"
	tokenOR     = "or"
	tokenNOT    = "not"
	tokenLParen = "("
	tokenRParen = ")"
)
const defaultCircleSteps = 64

// AreaExpression is (maybe negated) either an spatial object or operator +
// children (other expressions).
type AreaExpression struct {
	negate   bool
	obj      geojson.Object
	op       BinaryOp
	children children
}

type children []*AreaExpression

func ExpressionFromObject(object geojson.Object) *AreaExpression {
	return &AreaExpression{obj: object}
}

// String representation, helpful in logging.
func (e *AreaExpression) String() (res string) {
	if e.obj != nil {
		res = e.obj.String()
	} else {
		var chStrings []string
		for _, c := range e.children {
			chStrings = append(chStrings, c.String())
		}
		switch e.op {
		case NOOP:
			res = "empty operator"
		case AND:
			res = "(" + strings.Join(chStrings, " "+tokenAND+" ") + ")"
		case OR:
			res = "(" + strings.Join(chStrings, " "+tokenOR+" ") + ")"
		default:
			res = "unknown operator"
		}
	}
	if e.negate {
		res = tokenNOT + " " + res
	}
	return
}

// Whether this is an actual expression vs just an object
func (e *AreaExpression) IsCompound() bool {
	return e.obj == nil
}

// Return an actual object
func (e *AreaExpression) Obj() geojson.Object {
	return e.obj
}

// These could probably move to geojson.geometry.rect
func newRect(minX, minY, maxX, maxY float64) geometry.Rect {
	return geometry.Rect{
		Min: geometry.Point{X: minX, Y: minY},
		Max: geometry.Point{X: maxX, Y: maxY},
	}
}

func andRect(r1, r2 geometry.Rect) geometry.Rect {
	return newRect(
		math.Max(r1.Min.X, r2.Min.X),
		math.Max(r1.Min.Y, r2.Min.Y),
		math.Min(r1.Max.X, r2.Max.X),
		math.Min(r1.Max.Y, r2.Max.Y),
	)
}

func orRect(r1, r2 geometry.Rect) geometry.Rect {
	return newRect(
		math.Min(r1.Min.X, r2.Min.X),
		math.Min(r1.Min.Y, r2.Min.Y),
		math.Max(r1.Max.X, r2.Max.X),
		math.Max(r1.Max.Y, r2.Max.Y),
	)
}

// Return tightest rectangle for this expression
func (e *AreaExpression) Rect(minX, minY, maxX, maxY float64) (rect geometry.Rect) {
	if e.obj != nil {
		if e.negate {
			rect = newRect(minX, minY, maxX, maxY)
		} else {
			rect = e.obj.Rect()
		}
		return
	}
	var found bool
	var joinRect func(r1, r2 geometry.Rect) geometry.Rect
	if e.op == AND {
		joinRect = andRect
	} else {
		joinRect = orRect
	}
	for _, c := range e.children {
		childRect := c.Rect(minX, minY, maxX, maxY)
		if !found {
			rect = childRect
			found = true
		} else {
			rect = joinRect(rect, childRect)
		}
	}
	return
}


// Return boolean value modulo negate field of the expression.
func (e *AreaExpression) maybeNegate(val bool) bool {
	if e.negate {
		return !val
	}
	return val
}

// Methods for testing an AreaExpression against the spatial object.
func (e *AreaExpression) testObject(
	o geojson.Object,
	objObjTest func(o1, o2 geojson.Object) bool,
	exprObjTest func(ae *AreaExpression, ob geojson.Object) bool,
) bool {
	if e.obj != nil {
		return objObjTest(e.obj, o)
	}
	switch e.op {
	case AND:
		for _, c := range e.children {
			if !exprObjTest(c, o) {
				return false
			}
		}
		return true
	case OR:
		for _, c := range e.children {
			if exprObjTest(c, o) {
				return true
			}
		}
		return false
	}
	return false
}

func (e *AreaExpression) rawIntersects(o geojson.Object) bool {
	return e.testObject(o, geojson.Object.Intersects, (*AreaExpression).Intersects)
}

func (e *AreaExpression) rawContains(o geojson.Object) bool {
	return e.testObject(o, geojson.Object.Contains, (*AreaExpression).Contains)
}

func (e *AreaExpression) rawWithin(o geojson.Object) bool {
	return e.testObject(o, geojson.Object.Within, (*AreaExpression).Within)
}

func (e *AreaExpression) Intersects(o geojson.Object) bool {
	return e.maybeNegate(e.rawIntersects(o))
}

func (e *AreaExpression) Contains(o geojson.Object) bool {
	return e.maybeNegate(e.rawContains(o))
}

func (e *AreaExpression) Within(o geojson.Object) bool {
	return e.maybeNegate(e.rawWithin(o))
}

// Methods for testing an AreaExpression against another AreaExpression.
func (e *AreaExpression) testExpression(
	other *AreaExpression,
	exprObjTest func(ae *AreaExpression, ob geojson.Object) bool,
	rawExprExprTest func(ae1, ae2 *AreaExpression) bool,
	exprExprTest func(ae1, ae2 *AreaExpression) bool,
) bool {
	if other.negate {
		oppositeExp := &AreaExpression{negate: !e.negate, obj: e.obj, op: e.op, children: e.children}
		nonNegateOther := &AreaExpression{obj: other.obj, op: other.op, children: other.children}
		return exprExprTest(oppositeExp, nonNegateOther)
	}
	if other.obj != nil {
		return exprObjTest(e, other.obj)
	}
	switch other.op {
	case AND:
		for _, c := range other.children {
			if !rawExprExprTest(e, c) {
				return false
			}
		}
		return true
	case OR:
		for _, c := range other.children {
			if rawExprExprTest(e, c) {
				return true
			}
		}
		return false
	}
	return false
}

func (e *AreaExpression) rawIntersectsExpr(other *AreaExpression) bool {
	return e.testExpression(
		other,
		(*AreaExpression).rawIntersects,
		(*AreaExpression).rawIntersectsExpr,
		(*AreaExpression).IntersectsExpr)
}

func (e *AreaExpression) rawWithinExpr(other *AreaExpression) bool {
	return e.testExpression(
		other,
		(*AreaExpression).rawWithin,
		(*AreaExpression).rawWithinExpr,
		(*AreaExpression).WithinExpr)
}

func (e *AreaExpression) rawContainsExpr(other *AreaExpression) bool {
	return e.testExpression(
		other,
		(*AreaExpression).rawContains,
		(*AreaExpression).rawContainsExpr,
		(*AreaExpression).ContainsExpr)
}

func (e *AreaExpression) IntersectsExpr(other *AreaExpression) bool {
	return e.maybeNegate(e.rawIntersectsExpr(other))
}

func (e *AreaExpression) WithinExpr(other *AreaExpression) bool {
	return e.maybeNegate(e.rawWithinExpr(other))
}

func (e *AreaExpression) ContainsExpr(other *AreaExpression) bool {
	return e.maybeNegate(e.rawContainsExpr(other))
}
