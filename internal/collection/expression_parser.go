package collection

import (
	"strconv"
	"strings"

	"github.com/tidwall/geojson"
)

// parentStack is a helper object for parsing complex expressions
type parentStack []*AreaExpression

func (ps *parentStack) isEmpty() bool {
	return len(*ps) == 0
}

func (ps *parentStack) push(e *AreaExpression) {
	*ps = append(*ps, e)
}

func (ps *parentStack) pop() (e *AreaExpression, empty bool) {
	n := len(*ps)
	if n == 0 {
		return nil, true
	}
	x := (*ps)[n-1]
	*ps = (*ps)[:n-1]
	return x, false
}

func ParseAreaExpression(
	vsin []string, doClip bool,
	getCol func(string) *Collection, geomParseOpts geojson.ParseOptions, fixMissingBounds bool,
) (vsout []string, ae *AreaExpression, fixedBounds bool, err error) {
	ps := &parentStack{}
	vsout = vsin[:]
	var negate, needObj bool
loop:
	for {
		nvs, wtok, ok := nextToken(vsout)
		if !ok || len(wtok) == 0 {
			break
		}
		switch strings.ToLower(wtok) {
		case tokenLParen:
			newExpr := &AreaExpression{negate: negate, op: NOOP}
			negate = false
			needObj = false
			if ae != nil {
				ae.children = append(ae.children, newExpr)
			}
			ae = newExpr
			ps.push(ae)
			vsout = nvs
		case tokenRParen:
			if needObj {
				err = errInvalidArgument(tokenRParen)
				return
			}
			parent, empty := ps.pop()
			if empty {
				err = errInvalidArgument(tokenRParen)
				return
			}
			ae = parent
			vsout = nvs
		case tokenNOT:
			negate = !negate
			needObj = true
			vsout = nvs
		case tokenAND:
			if needObj {
				err = errInvalidArgument(tokenAND)
				return
			}
			needObj = true
			if ae == nil {
				err = errInvalidArgument(tokenAND)
				return
			} else if ae.obj == nil {
				switch ae.op {
				case OR:
					numChildren := len(ae.children)
					if numChildren < 2 {
						err = errInvalidNumberOfArguments
						return
					}
					ae.children = append(
						ae.children[:numChildren-1],
						&AreaExpression{
							op:       AND,
							children: []*AreaExpression{ae.children[numChildren-1]}})
				case NOOP:
					ae.op = AND
				}
			} else {
				ae = &AreaExpression{op: AND, children: []*AreaExpression{ae}}
			}
			vsout = nvs
		case tokenOR:
			if needObj {
				err = errInvalidArgument(tokenOR)
				return
			}
			needObj = true
			if ae == nil {
				err = errInvalidArgument(tokenOR)
				return
			} else if ae.obj == nil {
				switch ae.op {
				case AND:
					if len(ae.children) < 2 {
						err = errInvalidNumberOfArguments
						return
					}
					ae = &AreaExpression{op: OR, children: []*AreaExpression{ae}}
				case NOOP:
					ae.op = OR
				}
			} else {
				ae = &AreaExpression{op: OR, children: []*AreaExpression{ae}}
			}
			vsout = nvs
		case "point", "circle", "object", "bounds", "hash", "quadkey", "tile", "get":
			parsedVs, parsedObj, areaErr := parseArea(vsout, doClip, getCol, geomParseOpts)
			if areaErr != nil {
				err = areaErr
				return
			}
			newExpr := &AreaExpression{negate: negate, obj: parsedObj, op: NOOP}
			negate = false
			needObj = false
			if ae == nil {
				ae = newExpr
			} else {
				ae.children = append(ae.children, newExpr)
			}
			vsout = parsedVs
		default:
			if fixMissingBounds {
				if _, err := strconv.ParseFloat(wtok, 64); err == nil {
					// It's likely that the output was not specified, but rather the search bounds.
					vsout = append([]string{"BOUNDS"}, vsout...)
					fixedBounds = true
					continue loop
				}
			}
			break loop
		}
	}
	if !ps.isEmpty() || needObj || ae == nil || (ae.obj == nil && len(ae.children) == 0) {
		err = errInvalidNumberOfArguments
	}
	return
}


func ParseNearbyAreaExpression(vsin []string, fence bool) (vs []string, ae *AreaExpression, rs RoamSwitches, err error) {
	var obj geojson.Object
	vs, obj, rs, err = parseNearbyArea(vsin, fence)
	ae = &AreaExpression{obj: obj}
	return
}
