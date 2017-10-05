package tests

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/tidwall/tile38/controller"
	tlog "github.com/tidwall/tile38/controller/log"
	"github.com/tidwall/tile38/core"
)

var errTimeout = errors.New("timeout")

func mockCleanup() {
	fmt.Printf("Cleanup: may take some time... ")
	files, _ := ioutil.ReadDir(".")
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "data-mock-") {
			os.RemoveAll(file.Name())
		}
	}
	fmt.Printf("OK\n")
}

type mockServer struct {
	port int
	//join string
	//n    *finn.Node
	//m    *Machine
	conn redis.Conn
}

func mockOpenServer() (*mockServer, error) {
	rand.Seed(time.Now().UnixNano())
	port := rand.Int()%20000 + 20000
	dir := fmt.Sprintf("data-mock-%d", port)
	fmt.Printf("Starting test server at port %d\n", port)
	logOutput := ioutil.Discard
	if os.Getenv("PRINTLOG") == "1" {
		logOutput = os.Stderr
	}
	core.DevMode = true
	s := &mockServer{port: port}
	tlog.SetOutput(logOutput)
	go func() {
		if err := controller.ListenAndServe("localhost", port, dir, true); err != nil {
			log.Fatal(err)
		}
	}()
	if err := s.waitForStartup(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *mockServer) waitForStartup() error {
	var lerr error
	start := time.Now()
	for {
		if time.Now().Sub(start) > time.Second*5 {
			if lerr != nil {
				return lerr
			}
			return errTimeout
		}
		resp, err := redis.String(s.Do("SET", "please", "allow", "POINT", "33", "-115"))
		if err != nil {
			lerr = err
		} else if resp != "OK" {
			lerr = errors.New("not OK")
		} else {
			resp, err := redis.Int(s.Do("DEL", "please", "allow"))
			if err != nil {
				lerr = err
			} else if resp != 1 {
				lerr = errors.New("not 1")
			} else {
				return nil
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (mc *mockServer) Close() {
	if mc.conn != nil {
		mc.conn.Close()
	}
}

func (mc *mockServer) ResetConn() {
	if mc.conn != nil {
		mc.conn.Close()
		mc.conn = nil
	}
}

func (s *mockServer) DoPipeline(cmds [][]interface{}) ([]interface{}, error) {
	if s.conn == nil {
		var err error
		s.conn, err = redis.Dial("tcp", fmt.Sprintf(":%d", s.port))
		if err != nil {
			return nil, err
		}
	}
	//defer conn.Close()
	for _, cmd := range cmds {
		if err := s.conn.Send(cmd[0].(string), cmd[1:]...); err != nil {
			return nil, err
		}
	}
	if err := s.conn.Flush(); err != nil {
		return nil, err
	}
	var resps []interface{}
	for i := 0; i < len(cmds); i++ {
		resp, err := s.conn.Receive()
		if err != nil {
			resps = append(resps, err)
		} else {
			resps = append(resps, resp)
		}
	}
	return resps, nil
}
func (s *mockServer) Do(commandName string, args ...interface{}) (interface{}, error) {
	resps, err := s.DoPipeline([][]interface{}{
		append([]interface{}{commandName}, args...),
	})
	if err != nil {
		return nil, err
	}
	if len(resps) != 1 {
		return nil, errors.New("invalid number or responses")
	}
	return resps[0], nil
}

func (mc *mockServer) DoBatch(commands ...interface{}) error { //[][]interface{}) error {
	var tag string
	for _, commands := range commands {
		switch commands := commands.(type) {
		case string:
			tag = commands
		case [][]interface{}:
			for i := 0; i < len(commands); i += 2 {
				cmds := commands[i]
				if dur, ok := cmds[0].(time.Duration); ok {
					time.Sleep(dur)
				} else {
					if err := mc.DoExpect(commands[i+1][0], cmds[0].(string), cmds[1:]...); err != nil {
						if tag == "" {
							return fmt.Errorf("batch[%d]: %v", i/2, err)
						} else {
							return fmt.Errorf("batch[%d][%v]: %v", i/2, tag, err)
						}
					}
				}
			}
			tag = ""
		}
	}
	return nil
}

func normalize(v interface{}) interface{} {
	switch v := v.(type) {
	default:
		return v
	case []interface{}:
		for i := 0; i < len(v); i++ {
			v[i] = normalize(v[i])
		}
	case []uint8:
		return string(v)
	}
	return v
}
func (mc *mockServer) DoExpect(expect interface{}, commandName string, args ...interface{}) error {
	resp, err := mc.Do(commandName, args...)
	if err != nil {
		if exs, ok := expect.(string); ok {
			if err.Error() == exs {
				return nil
			}
		}
		return err
	}
	oresp := resp
	resp = normalize(resp)
	if expect == nil && resp != nil {
		return fmt.Errorf("expected '%v', got '%v'", expect, resp)
	}
	if vv, ok := resp.([]interface{}); ok {
		var ss []string
		for _, v := range vv {
			if v == nil {
				ss = append(ss, "nil")
			} else if s, ok := v.(string); ok {
				ss = append(ss, s)
			} else if b, ok := v.([]uint8); ok {
				if b == nil {
					ss = append(ss, "nil")
				} else {
					ss = append(ss, string(b))
				}
			} else {
				ss = append(ss, fmt.Sprintf("%v", v))
			}
		}
		resp = ss
	}
	if b, ok := resp.([]uint8); ok {
		if b == nil {
			resp = nil
		} else {
			resp = string([]byte(b))
		}
	}
	if fn, ok := expect.(func(v, org interface{}) (resp, expect interface{})); ok {
		resp, expect = fn(resp, oresp)
	}
	if fn, ok := expect.(func(v interface{}) (resp, expect interface{})); ok {
		resp, expect = fn(resp)
	}
	if fmt.Sprintf("%v", resp) != fmt.Sprintf("%v", expect) {
		return fmt.Errorf("expected '%v', got '%v'", expect, resp)
	}
	return nil
}
func round(v float64, decimals int) float64 {
	var pow float64 = 1
	for i := 0; i < decimals; i++ {
		pow *= 10
	}
	return float64(int((v*pow)+0.5)) / pow
}

func exfloat(v float64, decimals int) func(v interface{}) (resp, expect interface{}) {
	ex := round(v, decimals)
	return func(v interface{}) (resp, expect interface{}) {
		var s string
		if b, ok := v.([]uint8); ok {
			s = string(b)
		} else {
			s = fmt.Sprintf("%v", v)
		}
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return v, ex
		}
		return round(n, decimals), ex
	}
}
