package server

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/tidwall/tile38/internal/field"
	"github.com/tidwall/tile38/internal/log"
	lua "github.com/yuin/gopher-lua"
	luajson "layeh.com/gopher-json"
)

const defaultSearchOutput = outputObjects

var errInvalidNumberOfArguments = errors.New("invalid number of arguments")
var errKeyNotFound = errors.New("key not found")
var errIDNotFound = errors.New("id not found")
var errIDAlreadyExists = errors.New("id already exists")
var errPathNotFound = errors.New("path not found")
var errKeyHasHooksSet = errors.New("key has hooks set")
var errNotRectangle = errors.New("not a rectangle")

func errInvalidArgument(arg string) error {
	return fmt.Errorf("invalid argument '%s'", arg)
}
func errDuplicateArgument(arg string) error {
	return fmt.Errorf("duplicate argument '%s'", arg)
}
func token(line string) (newLine, token string) {
	for i := 0; i < len(line); i++ {
		if line[i] == ' ' {
			return line[i+1:], line[:i]
		}
	}
	return "", line
}

func tokenval(vs []string) (nvs []string, token string, ok bool) {
	if len(vs) > 0 {
		token = vs[0]
		nvs = vs[1:]
		ok = true
	}
	return
}

func lc(s1, s2 string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := 0; i < len(s1); i++ {
		ch := s1[i]
		if ch >= 'A' && ch <= 'Z' {
			if ch+32 != s2[i] {
				return false
			}
		} else if ch != s2[i] {
			return false
		}
	}
	return true
}

type whereT struct {
	expr bool
	name string
	minx bool
	min  field.Value
	maxx bool
	max  field.Value
}

func mLT(a, b field.Value) bool  { return a.Less(b) }
func mLTE(a, b field.Value) bool { return !mLT(b, a) }
func mGT(a, b field.Value) bool  { return mLT(b, a) }
func mGTE(a, b field.Value) bool { return !mLT(a, b) }
func mEQ(a, b field.Value) bool  { return a.Equals(b) }

func (where whereT) matchField(value field.Value) bool {
	switch where.min.Data() {
	case "<":
		return mLT(value, where.max)
	case "<=":
		return mLTE(value, where.max)
	case ">":
		return mGT(value, where.max)
	case ">=":
		return mGTE(value, where.max)
	case "==":
		return mEQ(value, where.max)
	case "!=":
		return !mEQ(value, where.max)
	}
	if !where.minx {
		if mLT(value, where.min) { // if value < where.min {
			return false
		}
	} else {
		if mLTE(value, where.min) { // if value <= where.min {
			return false
		}
	}
	if !where.maxx {
		if mGT(value, where.max) { // if value > where.max {
			return false
		}
	} else {
		if mGTE(value, where.max) { // if value >= where.max {
			return false
		}
	}
	return true
}

type whereinT struct {
	name   string
	valArr []field.Value
}

func (wherein whereinT) match(value field.Value) bool {
	for _, val := range wherein.valArr {
		if mEQ(val, value) {
			return true
		}
	}
	return false
}

type whereevalT struct {
	c        *Server
	luaState *lua.LState
	fn       *lua.LFunction
}

func (whereeval whereevalT) Close() {
	luaSetRawGlobals(
		whereeval.luaState, map[string]lua.LValue{
			"ARGV": lua.LNil,
		})
	whereeval.c.luapool.Put(whereeval.luaState)
}

func luaSetField(tbl *lua.LTable, name string, val field.Value) {
	var lval lua.LValue
	switch val.Kind() {
	case field.Null:
		lval = lua.LNil
	case field.False:
		lval = lua.LFalse
	case field.True:
		lval = lua.LTrue
	case field.Number:
		lval = lua.LNumber(val.Num())
	default:
		lval = lua.LString(val.Data())
	}
	tbl.RawSetString(name, lval)
}

func (whereeval whereevalT) match(fieldsWithNames map[string]field.Value,
	id string, props string) (ok bool, err error,
) {
	fieldsTbl := whereeval.luaState.CreateTable(0, len(fieldsWithNames))
	for name, val := range fieldsWithNames {
		luaSetField(fieldsTbl, name, val)
	}
	propsTbl := lua.LValue(whereeval.luaState.CreateTable(0, 0))
	if props != "" {
		var err error
		tbl, err := luajson.Decode(whereeval.luaState, []byte(props))
		if err == nil {
			propsTbl = tbl
		}
	}
	luaSetRawGlobals(
		whereeval.luaState, map[string]lua.LValue{
			"ID":         lua.LString(id),
			"FIELDS":     fieldsTbl,
			"PROPERTIES": propsTbl,
		})
	defer func() {
		luaSetRawGlobals(
			whereeval.luaState, map[string]lua.LValue{
				"ID":         lua.LNil,
				"FIELDS":     lua.LNil,
				"PROPERTIES": lua.LNil,
			})
	}()

	whereeval.luaState.Push(whereeval.fn)
	if err := whereeval.luaState.PCall(0, 1, nil); err != nil {
		emsg := err.Error()
		if strings.Contains(emsg, "attempt to index a non-table") {
			log.Debugf("Lua error: %v", emsg)
			return false, nil
		}
		return false, err
	}
	ret := whereeval.luaState.Get(-1)
	whereeval.luaState.Pop(1)

	if ret == nil {
		return false, nil
	}
	// Make bool out of returned lua value
	switch ret.Type() {
	case lua.LTNil:
		return false, nil
	case lua.LTBool:
		return ret == lua.LTrue, nil
	case lua.LTNumber:
		return float64(ret.(lua.LNumber)) != 0, nil
	case lua.LTString:
		return ret.String() != "", nil
	case lua.LTTable:
		tbl := ret.(*lua.LTable)
		if tbl.Len() != 0 {
			return true, nil
		}
		var match bool
		tbl.ForEach(func(lk lua.LValue, lv lua.LValue) { match = true })
		return match, nil
	}
	return false, fmt.Errorf("script returned value of type %s", ret.Type())
}

type searchScanBaseTokens struct {
	key        string
	cursor     uint64
	output     outputT
	precision  uint64
	fence      bool
	distance   bool
	nodwell    bool
	detect     map[string]bool
	accept     map[string]bool
	globs      []string
	wheres     []whereT
	whereins   []whereinT
	whereevals []whereevalT
	nofields   bool
	ulimit     bool
	limit      uint64
	usparse    bool
	sparse     uint8
	desc       bool
	clip       bool
	buffer     float64
	hasbuffer  bool
	mvt        bool
	tileX      int
	tileY      int
	tileZ      int
}

func (s *Server) parseSearchScanBaseTokens(
	cmd string, t searchScanBaseTokens, vs []string,
) (
	vsout []string, tout searchScanBaseTokens, err error,
) {
	var ok bool
	if vs, t.key, ok = tokenval(vs); !ok || t.key == "" {
		err = errInvalidNumberOfArguments
		return
	}

	fromFence := t.fence

	var slimit string
	var ssparse string
	var scursor string
	var asc bool
	for {
		nvs, wtok, ok := tokenval(vs)
		if ok && len(wtok) > 0 {
			switch strings.ToLower(wtok) {
			case "buffer":
				vs = nvs
				var sbuf string
				if vs, sbuf, ok = tokenval(vs); !ok || sbuf == "" {
					err = errInvalidNumberOfArguments
					return
				}
				var buf float64
				buf, err = strconv.ParseFloat(sbuf, 64)
				if err != nil || buf < 0 || math.IsInf(buf, 0) || math.IsNaN(buf) {
					err = errInvalidArgument(sbuf)
					return
				}
				t.buffer = buf
				t.hasbuffer = true
				continue
			case "cursor":
				vs = nvs
				if scursor != "" {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				if vs, scursor, ok = tokenval(vs); !ok || scursor == "" {
					err = errInvalidNumberOfArguments
					return
				}
				continue
			case "where":
				vs = nvs
				if detectExprToken(vs) {
					// using expressions
					// WHERE expr
					var expr string
					if vs, expr, ok = tokenval(vs); !ok {
						err = errInvalidNumberOfArguments
						return
					}
					t.wheres = append(t.wheres, whereT{name: expr, expr: true})
					continue
				} else {
					// using field filter
					// WHERE min max
					var name, smin, smax string
					if vs, name, ok = tokenval(vs); !ok {
						err = errInvalidNumberOfArguments
						return
					}
					if vs, smin, ok = tokenval(vs); !ok {
						err = errInvalidNumberOfArguments
						return
					}
					if vs, smax, ok = tokenval(vs); !ok {
						err = errInvalidNumberOfArguments
						return
					}
					var minx, maxx bool
					smin = strings.ToLower(smin)
					smax = strings.ToLower(smax)
					if smax == "+inf" || smax == "inf" {
						smax = "inf"
					}
					switch smin {
					case "<", "<=", ">", ">=", "==", "!=":
					default:
						if strings.HasPrefix(smin, "(") {
							minx = true
							smin = smin[1:]
						}
						if strings.HasPrefix(smax, "(") {
							maxx = true
							smax = smax[1:]
						}
					}
					t.wheres = append(t.wheres, whereT{
						name: name,
						minx: minx,
						min:  field.ValueOf(smin),
						maxx: maxx,
						max:  field.ValueOf(smax),
					})
					continue
				}
			case "wherein":
				vs = nvs
				var name, nvalsStr, valStr string
				if vs, name, ok = tokenval(vs); !ok {
					err = errInvalidNumberOfArguments
					return
				}
				if vs, nvalsStr, ok = tokenval(vs); !ok {
					err = errInvalidNumberOfArguments
					return
				}
				var i, nvals uint64
				if nvals, err = strconv.ParseUint(nvalsStr, 10, 64); err != nil {
					err = errInvalidArgument(nvalsStr)
					return
				}
				valArr := make([]field.Value, nvals)
				for i = 0; i < nvals; i++ {
					if vs, valStr, ok = tokenval(vs); !ok {
						err = errInvalidNumberOfArguments
						return
					}
					valArr[i] = field.ValueOf(valStr)
				}
				t.whereins = append(t.whereins, whereinT{
					name:   name,
					valArr: valArr,
				})
				continue
			case "whereevalsha":
				fallthrough
			case "whereeval":
				scriptIsSha := strings.ToLower(wtok) == "whereevalsha"
				vs = nvs
				var script, nargsStr, arg string
				if vs, script, ok = tokenval(vs); !ok || script == "" {
					err = errInvalidNumberOfArguments
					return
				}
				if vs, nargsStr, ok = tokenval(vs); !ok || nargsStr == "" {
					err = errInvalidNumberOfArguments
					return
				}

				var i, nargs uint64
				if nargs, err = strconv.ParseUint(nargsStr, 10, 64); err != nil {
					err = errInvalidArgument(nargsStr)
					return
				}

				var luaState *lua.LState
				luaState, err = s.luapool.Get()
				if err != nil {
					return
				}

				argsTbl := luaState.CreateTable(len(vs), 0)
				for i = 0; i < nargs; i++ {
					if vs, arg, ok = tokenval(vs); !ok || arg == "" {
						err = errInvalidNumberOfArguments
						return
					}
					argsTbl.Append(lua.LString(arg))
				}

				var shaSum string
				if scriptIsSha {
					shaSum = script
				} else {
					shaSum = Sha1Sum(script)
				}

				luaSetRawGlobals(
					luaState, map[string]lua.LValue{
						"ARGV": argsTbl,
					})

				compiled, ok := s.luascripts.Get(shaSum)
				var fn *lua.LFunction
				if ok {
					fn = &lua.LFunction{
						IsG: false,
						Env: luaState.Env,

						Proto:     compiled,
						GFunction: nil,
						Upvalues:  make([]*lua.Upvalue, 0),
					}
				} else if scriptIsSha {
					err = errShaNotFound
					return
				} else {
					fn, err = luaState.Load(strings.NewReader(script), "f_"+shaSum)
					if err != nil {
						err = makeSafeErr(err)
						return
					}
					s.luascripts.PutLRU(shaSum, fn.Proto)
				}
				t.whereevals = append(t.whereevals, whereevalT{
					c: s, luaState: luaState, fn: fn,
				})
				continue
			case "nofields":
				vs = nvs
				if t.nofields {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				t.nofields = true
				continue
			case "limit":
				vs = nvs
				if slimit != "" {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				if vs, slimit, ok = tokenval(vs); !ok || slimit == "" {
					err = errInvalidNumberOfArguments
					return
				}
				continue
			case "sparse":
				vs = nvs
				if ssparse != "" {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				if vs, ssparse, ok = tokenval(vs); !ok || ssparse == "" {
					err = errInvalidNumberOfArguments
					return
				}
				continue
			case "fence":
				vs = nvs
				if t.fence && !fromFence {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				t.fence = true
				continue
			case "commands":
				vs = nvs
				if t.accept != nil {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				t.accept = make(map[string]bool)
				var peek string
				if vs, peek, ok = tokenval(vs); !ok || peek == "" {
					err = errInvalidNumberOfArguments
					return
				}
				for _, s := range strings.Split(peek, ",") {
					part := strings.TrimSpace(strings.ToLower(s))
					if t.accept[part] {
						err = errDuplicateArgument(s)
						return
					}
					t.accept[part] = true
				}
				if len(t.accept) == 0 {
					t.accept = nil
				}
				continue
			case "distance":
				vs = nvs
				if t.distance {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				t.distance = true
				continue
			case "detect":
				vs = nvs
				if t.detect != nil {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				t.detect = make(map[string]bool)
				var peek string
				if vs, peek, ok = tokenval(vs); !ok || peek == "" {
					err = errInvalidNumberOfArguments
					return
				}
				for _, s := range strings.Split(peek, ",") {
					part := strings.TrimSpace(strings.ToLower(s))
					switch part {
					default:
						err = errInvalidArgument(peek)
						return
					case "inside", "outside", "enter", "exit", "cross":
					}
					if t.detect[part] {
						err = errDuplicateArgument(s)
						return
					}
					t.detect[part] = true
				}
				if len(t.detect) == 0 {
					t.detect = map[string]bool{
						"inside":  true,
						"outside": true,
						"enter":   true,
						"exit":    true,
						"cross":   true,
					}
				}
				continue
			case "nodwell":
				vs = nvs
				if t.desc || asc {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				t.nodwell = true
				continue
			case "desc":
				vs = nvs
				if t.desc || asc {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				t.desc = true
				continue
			case "asc":
				vs = nvs
				if t.desc || asc {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				asc = true
				continue
			case "match":
				vs = nvs
				var glob string
				if vs, glob, ok = tokenval(vs); !ok || glob == "" {
					err = errInvalidNumberOfArguments
					return
				}
				t.globs = append(t.globs, glob)
				continue
			case "clip":
				vs = nvs
				if t.clip {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				t.clip = true
				continue
			}
		}
		break
	}

	// check to make sure that there aren't any conflicts
	if cmd == "scan" || cmd == "search" {
		if ssparse != "" {
			err = errors.New("SPARSE is not allowed for " + strings.ToUpper(cmd))
			return
		}
		if t.fence {
			err = errors.New("FENCE is not allowed for " + strings.ToUpper(cmd))
			return
		}
	} else {
		if t.desc {
			err = errors.New("DESC is not allowed for " + strings.ToUpper(cmd))
			return
		}
		if asc {
			err = errors.New("ASC is not allowed for " + strings.ToUpper(cmd))
			return
		}
	}
	if ssparse != "" && slimit != "" {
		err = errors.New("LIMIT is not allowed when SPARSE is specified")
		return
	}
	if scursor != "" && ssparse != "" {
		err = errors.New("CURSOR is not allowed when SPARSE is specified")
		return
	}
	if scursor != "" && t.fence {
		err = errors.New("CURSOR is not allowed when FENCE is specified")
		return
	}
	if t.detect != nil && !t.fence {
		err = errors.New("DETECT is not allowed when FENCE is not specified")
		return
	}

	t.output = defaultSearchOutput
	var nvs []string
	var sprecision string
	var which string
	if nvs, which, ok = tokenval(vs); ok && which != "" {
		updline := true
		switch strings.ToLower(which) {
		default:
			if cmd == "scan" {
				err = errInvalidArgument(which)
				return
			}
			updline = false
		case "count":
			t.output = outputCount
		case "objects":
			t.output = outputObjects
		case "points":
			t.output = outputPoints
		case "hashes":
			t.output = outputHashes
			if nvs, sprecision, ok = tokenval(nvs); !ok || sprecision == "" {
				err = errInvalidNumberOfArguments
				return
			}
		case "bounds":
			t.output = outputBounds
		case "ids":
			t.output = outputIDs
		}
		if updline {
			vs = nvs
		}
	}
	if scursor != "" {
		if t.cursor, err = strconv.ParseUint(scursor, 10, 64); err != nil {
			err = errInvalidArgument(scursor)
			return
		}
	}
	if sprecision != "" {
		t.precision, err = strconv.ParseUint(sprecision, 10, 64)
		if err != nil || t.precision == 0 || t.precision > 12 {
			err = errInvalidArgument(sprecision)
			return
		}
	}
	if slimit != "" {
		t.ulimit = true
		if t.limit, err = strconv.ParseUint(slimit, 10, 64); err != nil || t.limit == 0 {
			err = errInvalidArgument(slimit)
			return
		}
	}
	if ssparse != "" {
		t.usparse = true
		var sparse uint64
		if sparse, err = strconv.ParseUint(ssparse, 10, 8); err != nil ||
			sparse == 0 || sparse > 16 {
			err = errInvalidArgument(ssparse)
			return
		}
		t.sparse = uint8(sparse)
		t.limit = math.MaxUint64
	}
	vsout = vs
	tout = t
	return
}

func detectExprToken(vs []string) bool {
	// Detect the kind of where, either:
	// - expr
	// - name min max
	if len(vs) == 0 {
		return false
	} else if len(vs) == 1 || (len(vs) == 2 && len(vs[1]) == 0) {
		return true
	}
	v := vs[1]
	if (v[0] >= 'a' && v[0] <= 'z') || (v[0] >= 'A' && v[0] <= 'Z') {
		if (v[0] == 'i' || v[0] == 'I') && strings.ToLower(v) == "inf" {
			return false
		}
		return true
	}
	return false
}

type parentStack []*areaExpression

func (ps *parentStack) isEmpty() bool {
	return len(*ps) == 0
}

func (ps *parentStack) push(e *areaExpression) {
	*ps = append(*ps, e)
}

func (ps *parentStack) pop() (e *areaExpression, empty bool) {
	n := len(*ps)
	if n == 0 {
		return nil, true
	}
	x := (*ps)[n-1]
	*ps = (*ps)[:n-1]
	return x, false
}

func (s *Server) parseAreaExpression(vsin []string, doClip bool) (vsout []string, ae *areaExpression, err error) {
	ps := &parentStack{}
	vsout = vsin[:]
	var negate, needObj bool
loop:
	for {
		nvs, wtok, ok := tokenval(vsout)
		if !ok || len(wtok) == 0 {
			break
		}
		switch strings.ToLower(wtok) {
		case tokenLParen:
			newExpr := &areaExpression{negate: negate, op: NOOP}
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
						&areaExpression{
							op:       AND,
							children: []*areaExpression{ae.children[numChildren-1]}})
				case NOOP:
					ae.op = AND
				}
			} else {
				ae = &areaExpression{op: AND, children: []*areaExpression{ae}}
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
					ae = &areaExpression{op: OR, children: []*areaExpression{ae}}
				case NOOP:
					ae.op = OR
				}
			} else {
				ae = &areaExpression{op: OR, children: []*areaExpression{ae}}
			}
			vsout = nvs
		case "point", "circle", "object", "bounds", "hash", "quadkey", "tile", "get", "sector":
			parsedVs, parsedObj, areaErr := s.parseArea(vsout, doClip)
			if areaErr != nil {
				err = areaErr
				return
			}
			newExpr := &areaExpression{negate: negate, obj: parsedObj, op: NOOP}
			negate = false
			needObj = false
			if ae == nil {
				ae = newExpr
			} else {
				ae.children = append(ae.children, newExpr)
			}
			vsout = parsedVs
		default:
			break loop
		}
	}
	if !ps.isEmpty() || needObj || ae == nil || (ae.obj == nil && len(ae.children) == 0) {
		err = errInvalidNumberOfArguments
	}
	return
}
