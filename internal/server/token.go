package server

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

const defaultSearchOutput = outputObjects

var errInvalidNumberOfArguments = errors.New("invalid number of arguments")
var errKeyNotFound = errors.New("key not found")
var errIDNotFound = errors.New("id not found")
var errIDAlreadyExists = errors.New("id already exists")
var errPathNotFound = errors.New("path not found")
var errKeyHasHooksSet = errors.New("key has hooks set")

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

func tokenvalbytes(vs []string) (nvs []string, token []byte, ok bool) {
	if len(vs) > 0 {
		token = []byte(vs[0])
		nvs = vs[1:]
		ok = true
	}
	return
}

func tokenlc(line string) (newLine, token string) {
	for i := 0; i < len(line); i++ {
		ch := line[i]
		if ch == ' ' {
			return line[i+1:], line[:i]
		}
		if ch >= 'A' && ch <= 'Z' {
			lc := make([]byte, 0, 16)
			if i > 0 {
				lc = append(lc, []byte(line[:i])...)
			}
			lc = append(lc, ch+32)
			i++
			for ; i < len(line); i++ {
				ch = line[i]
				if ch == ' ' {
					return line[i+1:], string(lc)
				}
				if ch >= 'A' && ch <= 'Z' {
					lc = append(lc, ch+32)
				} else {
					lc = append(lc, ch)
				}
			}
			return "", string(lc)
		}
	}
	return "", line
}
func lcb(s1 []byte, s2 string) bool {
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
	field string
	minx  bool
	min   float64
	maxx  bool
	max   float64
}

func (where whereT) match(value float64) bool {
	if !where.minx {
		if value < where.min {
			return false
		}
	} else {
		if value <= where.min {
			return false
		}
	}
	if !where.maxx {
		if value > where.max {
			return false
		}
	} else {
		if value >= where.max {
			return false
		}
	}
	return true
}

func zMinMaxFromWheres(wheres []whereT) (minZ, maxZ float64) {
	for _, w := range wheres {
		if w.field == "z" {
			minZ = w.min
			maxZ = w.max
			return
		}
	}
	minZ = math.Inf(-1)
	maxZ = math.Inf(+1)
	return
}

type whereinT struct {
	field  string
	valMap map[float64]struct{}
}

func (wherein whereinT) match(value float64) bool {
	_, ok := wherein.valMap[value]
	return ok
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

func (whereeval whereevalT) match(fieldsWithNames map[string]float64) bool {
	fieldsTbl := whereeval.luaState.CreateTable(0, len(fieldsWithNames))
	for field, val := range fieldsWithNames {
		fieldsTbl.RawSetString(field, lua.LNumber(val))
	}

	luaSetRawGlobals(
		whereeval.luaState, map[string]lua.LValue{
			"FIELDS": fieldsTbl,
		})
	defer luaSetRawGlobals(
		whereeval.luaState, map[string]lua.LValue{
			"FIELDS": lua.LNil,
		})

	whereeval.luaState.Push(whereeval.fn)
	if err := whereeval.luaState.PCall(0, 1, nil); err != nil {
		panic(err.Error())
	}
	ret := whereeval.luaState.Get(-1)
	whereeval.luaState.Pop(1)

	// Make bool out of returned lua value
	switch ret.Type() {
	case lua.LTNil:
		return false
	case lua.LTBool:
		return ret == lua.LTrue
	case lua.LTNumber:
		return float64(ret.(lua.LNumber)) != 0
	case lua.LTString:
		return ret.String() != ""
	case lua.LTTable:
		tbl := ret.(*lua.LTable)
		if tbl.Len() != 0 {
			return true
		}
		var match bool
		tbl.ForEach(func(lk lua.LValue, lv lua.LValue) { match = true })
		return match
	}
	panic(fmt.Sprintf("Script returned value of type %s", ret.Type()))
}

type searchScanBaseTokens struct {
	key        string
	cursor     uint64
	output     outputT
	precision  uint64
	lineout    string
	fence      bool
	distance   bool
	nodwell    bool
	detect     map[string]bool
	accept     map[string]bool
	glob       string
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
				var field, smin, smax string
				if vs, field, ok = tokenval(vs); !ok || field == "" {
					err = errInvalidNumberOfArguments
					return
				}
				if vs, smin, ok = tokenval(vs); !ok || smin == "" {
					err = errInvalidNumberOfArguments
					return
				}
				if vs, smax, ok = tokenval(vs); !ok || smax == "" {
					err = errInvalidNumberOfArguments
					return
				}
				var minx, maxx bool
				var min, max float64
				if strings.ToLower(smin) == "-inf" {
					min = math.Inf(-1)
				} else {
					if strings.HasPrefix(smin, "(") {
						minx = true
						smin = smin[1:]
					}
					min, err = strconv.ParseFloat(smin, 64)
					if err != nil {
						err = errInvalidArgument(smin)
						return
					}
				}
				if strings.ToLower(smax) == "+inf" {
					max = math.Inf(+1)
				} else {
					if strings.HasPrefix(smax, "(") {
						maxx = true
						smax = smax[1:]
					}
					max, err = strconv.ParseFloat(smax, 64)
					if err != nil {
						err = errInvalidArgument(smax)
						return
					}
				}
				t.wheres = append(t.wheres, whereT{field, minx, min, maxx, max})
				continue
			case "wherein":
				vs = nvs
				var field, nvalsStr, valStr string
				if vs, field, ok = tokenval(vs); !ok || field == "" {
					err = errInvalidNumberOfArguments
					return
				}
				if vs, nvalsStr, ok = tokenval(vs); !ok || nvalsStr == "" {
					err = errInvalidNumberOfArguments
					return
				}
				var i, nvals uint64
				if nvals, err = strconv.ParseUint(nvalsStr, 10, 64); err != nil {
					err = errInvalidArgument(nvalsStr)
					return
				}
				valMap := make(map[float64]struct{})
				var val float64
				var empty struct{}
				for i = 0; i < nvals; i++ {
					if vs, valStr, ok = tokenval(vs); !ok || valStr == "" {
						err = errInvalidNumberOfArguments
						return
					}
					if val, err = strconv.ParseFloat(valStr, 64); err != nil {
						err = errInvalidArgument(valStr)
						return
					}
					valMap[val] = empty
				}
				t.whereins = append(t.whereins, whereinT{field, valMap})
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
					s.luascripts.Put(shaSum, fn.Proto)
				}
				t.whereevals = append(t.whereevals, whereevalT{s, luaState, fn})
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
				if t.glob != "" {
					err = errDuplicateArgument(strings.ToUpper(wtok))
					return
				}
				if vs, t.glob, ok = tokenval(vs); !ok || t.glob == "" {
					err = errInvalidNumberOfArguments
					return
				}
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
		if t.precision, err = strconv.ParseUint(sprecision, 10, 64); err != nil || t.precision == 0 || t.precision > 64 {
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
		if sparse, err = strconv.ParseUint(ssparse, 10, 8); err != nil || sparse == 0 || sparse > 8 {
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
