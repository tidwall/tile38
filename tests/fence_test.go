package tests

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/gjson"
)

func subTestFence(g *testGroup) {

	// Standard
	g.regSubTest("basic", fence_basic_test)
	g.regSubTest("channel message order", fence_channel_message_order_test)
	g.regSubTest("detect inside,outside", fence_detect_inside_test)

	// Roaming
	g.regSubTest("roaming live", fence_roaming_live_test)
	g.regSubTest("roaming channel", fence_roaming_channel_test)
	g.regSubTest("roaming webhook", fence_roaming_webhook_test)

	// channel meta
	g.regSubTest("channel meta", fence_channel_meta_test)

	// various
	g.regSubTest("detect eecio", fence_eecio_test)
	g.regSubTest("a5 channel", fence_a5_channel_test)
	g.regSubTest("a5 webhook", fence_a5_webhook_test)
}

type fenceReader struct {
	conn net.Conn
	rd   *bufio.Reader
}

func (fr *fenceReader) receive() (string, error) {
	if err := fr.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		return "", err
	}
	line, err := fr.rd.ReadBytes('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 4 || line[0] != '$' || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
		return "", errors.New("invalid message")
	}
	n, err := strconv.ParseUint(string(line[1:len(line)-2]), 10, 64)
	if err != nil {
		return "", err
	}
	buf := make([]byte, int(n)+2)
	_, err = io.ReadFull(fr.rd, buf)
	if err != nil {
		return "", err
	}
	if buf[len(buf)-2] != '\r' || buf[len(buf)-1] != '\n' {
		return "", errors.New("invalid message")
	}
	js := buf[:len(buf)-2]
	var m interface{}
	if err := json.Unmarshal(js, &m); err != nil {
		return "", err
	}
	return string(js), nil
}

func (fr *fenceReader) receiveExpect(valex ...string) error {
	s, err := fr.receive()
	if err != nil {
		return err
	}
	for i := 0; i < len(valex); i += 2 {
		if gjson.Get(s, valex[i]).String() != valex[i+1] {
			return fmt.Errorf("expected '%s'='%s', got '%s'", valex[i], valex[i+1], gjson.Get(s, valex[i]).String())
		}
	}
	return nil
}

func fence_basic_test(mc *mockServer) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "NEARBY mykey FENCE POINT 33 -115 5000\r\n")
	if err != nil {
		return err
	}

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	res := string(buf[:n])
	if res != "+OK\r\n" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}
	rd := &fenceReader{conn, bufio.NewReader(conn)}

	// send a point
	c, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer c.Close()

	res, err = redis.String(c.Do("SET", "mykey", "myid1", "POINT", 33, -115))
	if err != nil {
		return err
	}
	if res != "OK" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}

	// receive the message
	if err := rd.receiveExpect("command", "set",
		"detect", "enter",
		"key", "mykey",
		"id", "myid1",
		"object.type", "Point",
		"object.coordinates", "[-115,33]"); err != nil {
		return err
	}

	if err := rd.receiveExpect("command", "set",
		"detect", "inside",
		"key", "mykey",
		"id", "myid1",
		"object.type", "Point",
		"object.coordinates", "[-115,33]"); err != nil {
		return err
	}

	res, err = redis.String(c.Do("SET", "mykey", "myid1", "POINT", 34, -115))
	if err != nil {
		return err
	}
	if res != "OK" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}

	// receive the message
	if err := rd.receiveExpect("command", "set",
		"detect", "exit",
		"key", "mykey",
		"id", "myid1",
		"object.type", "Point",
		"object.coordinates", "[-115,34]"); err != nil {
		return err
	}

	if err := rd.receiveExpect("command", "set",
		"detect", "outside",
		"key", "mykey",
		"id", "myid1",
		"object.type", "Point",
		"object.coordinates", "[-115,34]"); err != nil {
		return err
	}
	return nil
}

func fence_channel_message_order_test(mc *mockServer) error {
	// Create a channel to store the goroutines error
	finalErr := make(chan error)

	var ready atomic.Bool
	// Concurrently subscribe for notifications
	go func() {
		// Create the subscription connection to Tile38 to subscribe for updates
		sc, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
		if err != nil {
			log.Println(err)
			return
		}
		defer sc.Close()

		// Subscribe the subscription client to the * pattern
		psc := redis.PubSubConn{Conn: sc}
		if err := psc.PSubscribe("*"); err != nil {
			log.Println(err)
			return
		}
		var msgs []string
		// While not a permanent error on the connection.
	loop:
		for sc.Err() == nil {
			switch v := psc.Receive().(type) {
			case redis.Message:
				if v.Channel == "status" && string(v.Data) == "ready" {
					ready.Store(true)
				} else {
					msgs = append(msgs, string(v.Data))
					if len(msgs) == 8 {
						break loop
					}
				}
			case error:
				fmt.Printf("%s\n", err.Error())
			}
		}
		// Verify all messages
		correctOrder := []string{"exit:A", "exit:B", "outside:A", "outside:B", "enter:C", "enter:D", "inside:C", "inside:D"}
		for i := range msgs {
			if gjson.Get(msgs[i], "detect").String()+":"+
				gjson.Get(msgs[i], "hook").String() != correctOrder[i] {
				finalErr <- errors.New("INVALID MESSAGE ORDER")
			}
		}
		finalErr <- nil
	}()
	// Create the base connection for setting up points and geofences
	bc, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer bc.Close()

	for !ready.Load() {
		if _, err := do(bc, "PUBLISH status ready"); err != nil {
			return err
		}
	}

	// Fire all setup commands on the base client
	for _, cmd := range []string{
		"SET points point POINT 33.412529053733444 -111.93368911743164",
		`SETCHAN A WITHIN points FENCE OBJECT {"type":"Polygon","coordinates":[[[-111.95205688476562,33.400491820565236],[-111.92630767822266,33.400491820565236],[-111.92630767822266,33.422272258866045],[-111.95205688476562,33.422272258866045],[-111.95205688476562,33.400491820565236]]]}`,
		`SETCHAN B WITHIN points FENCE OBJECT {"type":"Polygon","coordinates":[[[-111.93952560424803,33.403501285221594],[-111.92630767822266,33.403501285221594],[-111.92630767822266,33.41997983836345],[-111.93952560424803,33.41997983836345],[-111.93952560424803,33.403501285221594]]]}`,
		`SETCHAN C WITHIN points FENCE OBJECT {"type":"Polygon","coordinates":[[[-111.9255781173706,33.40342963251261],[-111.91201686859131,33.40342963251261],[-111.91201686859131,33.41994401881284],[-111.9255781173706,33.41994401881284],[-111.9255781173706,33.40342963251261]]]}`,
		`SETCHAN D WITHIN points FENCE OBJECT {"type":"Polygon","coordinates":[[[-111.92562103271484,33.40063513076968],[-111.90021514892578,33.40063513076968],[-111.90021514892578,33.42212898435788],[-111.92562103271484,33.42212898435788],[-111.92562103271484,33.40063513076968]]]}`,
		"SET points point POINT 33.412529053733444 -111.91909790039062",
	} {
		if _, err := do(bc, cmd); err != nil {
			return err
		}
	}
	return <-finalErr
}

func fence_detect_inside_test(mc *mockServer) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "WITHIN users FENCE DETECT inside,outside POINTS BOUNDS 33.618824 -84.457973 33.654359 -84.399859\r\n")
	if err != nil {
		return err
	}

	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	res := string(buf[:n])
	if res != "+OK\r\n" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}
	rd := &fenceReader{conn, bufio.NewReader(conn)}

	// send a point
	c, err := redis.Dial("tcp", fmt.Sprintf(":%d", mc.port))
	if err != nil {
		return err
	}
	defer c.Close()

	res, err = redis.String(c.Do("SET", "users", "200", "POINT", "33.642301", "-84.43118"))
	if err != nil {
		return err
	}
	if res != "OK" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}

	if err := rd.receiveExpect("command", "set",
		"detect", "inside",
		"key", "users",
		"id", "200",
		"point", `{"lat":33.642301,"lon":-84.43118}`); err != nil {
		return err
	}

	res, err = redis.String(c.Do("SET", "users", "200", "POINT", "34.642301", "-84.43118"))
	if err != nil {
		return err
	}
	if res != "OK" {
		return fmt.Errorf("expected OK, got '%v'", res)
	}

	// receive the message
	if err := rd.receiveExpect("command", "set",
		"detect", "outside",
		"key", "users",
		"id", "200",
		"point", `{"lat":34.642301,"lon":-84.43118}`); err != nil {
		return err
	}
	return nil
}

// do performs the passed command on the passed redis client
func do(c redis.Conn, cmd string) (interface{}, error) {
	// Split out all parameters
	params := strings.Split(cmd, " ")

	// Produce a slice of interfaces for use in the arguments
	var args []interface{}
	for _, p := range params[1:] {
		args = append(args, p)
	}

	// Perform the request and return the response
	return c.Do(params[0], args...)
}

func fence_channel_meta_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"SETCHAN", "carbon", "NEARBY", "x", "MATCH", "carbon*", "FENCE", "NODWELL", "points", "ROAM", "x", "*", "200000"}, {"1"},
		{"OUTPUT", "json"}, {`{"ok":true}`},
		// check for valid json on the chans command
		{"CHANS", "*"}, {
			func(v interface{}) (resp, expect interface{}) {
				// v is the value as strings or slices of strings
				// test will pass as long as `resp` and `expect` are the same.
				if !json.Valid([]byte(v.(string))) {
					return v, "Valid JSON"
				}
				return true, true
			},
		},
	})
}

func dialTile38(port int) (redis.Conn, error) {
	conn, err := redis.Dial("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	if _, err := conn.Do("OUTPUT", "json"); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func doTile38(c redis.Conn, cmd string, args ...interface{}) (string, error) {
	js, err := redis.String(c.Do(cmd, args...))
	if !gjson.Get(js, "ok").Bool() {
		return "", errors.New(gjson.Get(js, "err").String())
	}
	return js, err
}

// fence_a5_channel_test runs a channel fence over an A5 cell, both with and
// without an A5S output, then checks the A5 output on the fence messages.
// A5 cell 51575d8000000000 is the resolution 10 cell holding POINT 52 13.
func fence_a5_channel_test(mc *mockServer) error {
	conn, err := dialTile38(mc.port)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := doTile38(conn, "SETCHAN", "test-a5", "WITHIN", "a5fleet",
		"FENCE", "A5", "51575d8000000000"); err != nil {
		return err
	}
	// same fence, but asking for the A5 cell of each match as the output
	if _, err := doTile38(conn, "SETCHAN", "test-a5-out", "WITHIN", "a5fleet",
		"FENCE", "A5S", "10", "A5", "51575d8000000000"); err != nil {
		return err
	}
	if _, err := doTile38(conn, "SUBSCRIBE", "test-a5"); err != nil {
		return err
	}
	if _, err := doTile38(conn, "SUBSCRIBE", "test-a5-out"); err != nil {
		return err
	}

	sc, err := dialTile38(mc.port)
	if err != nil {
		return err
	}
	defer sc.Close()
	// inside the cell, then well outside of it
	if _, err := doTile38(sc, "SET", "a5fleet", "truck", "POINT", 52, 13); err != nil {
		return err
	}
	if _, err := doTile38(sc, "SET", "a5fleet", "truck", "POINT", 0, 0); err != nil {
		return err
	}
	if _, err := doTile38(sc, "PUBLISH", "test-a5", "DONE"); err != nil {
		return err
	}
	if _, err := doTile38(sc, "PUBLISH", "test-a5-out", "DONE"); err != nil {
		return err
	}

	var detects []string
	var a5s []string
	for done := 0; done < 2; {
		js, err := redis.String(conn.Receive())
		if err != nil {
			return err
		}
		if js == `"DONE"` {
			done++
			continue
		}
		detects = append(detects, gjson.Get(js, "detect").String())
		if a5 := gjson.Get(js, "a5"); a5.Exists() {
			a5s = append(a5s, a5.String())
		}
	}
	// both channels report the same transitions
	if got := strings.Join(detects, ","); got != "enter,enter,inside,inside,exit,exit,outside,outside" {
		return fmt.Errorf("expected 'enter,enter,inside,inside,exit,exit,outside,outside', got '%s'", got)
	}
	// only the channel with the A5 output carries a5 cells
	if got := strings.Join(a5s, ","); got != "51575d8000000000,51575d8000000000,4f05dc8000000000,4f05dc8000000000" {
		return fmt.Errorf("expected 4 a5 cells, got '%s'", got)
	}
	return nil
}

// fence_a5_webhook_test is fence_a5_channel_test over a SETHOOK endpoint,
// so the hook side of the A5 fence gets exercised too.
func fence_a5_webhook_test(mc *mockServer) error {
	msgs := make(chan string, 32)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err == nil {
			select {
			case msgs <- string(body):
			default:
			}
		}
		fmt.Fprintln(w, "OK")
	}))
	defer ts.Close()

	conn, err := dialTile38(mc.port)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := doTile38(conn, "SETHOOK", "test-a5-hook", ts.URL,
		"WITHIN", "a5hookfleet", "FENCE", "DETECT", "enter,exit",
		"A5", "51575d8000000000"); err != nil {
		return err
	}
	defer doTile38(conn, "DELHOOK", "test-a5-hook")

	// inside the cell, then well outside of it
	if _, err := doTile38(conn, "SET", "a5hookfleet", "truck", "POINT", 52, 13); err != nil {
		return err
	}
	if _, err := doTile38(conn, "SET", "a5hookfleet", "truck", "POINT", 0, 0); err != nil {
		return err
	}

	var detects []string
	for i := 0; i < 2; i++ {
		select {
		case msg := <-msgs:
			detects = append(detects, gjson.Get(msg, "detect").String())
		case <-time.After(time.Second * 10):
			return fmt.Errorf("timeout waiting for webhook message %d, got '%s'",
				i+1, strings.Join(detects, ","))
		}
	}
	if got := strings.Join(detects, ","); got != "enter,exit" {
		return fmt.Errorf("expected 'enter,exit', got '%s'", got)
	}
	return nil
}

func fence_eecio_test(mc *mockServer) error {
	// simulates issue #578
	var wg sync.WaitGroup
	wg.Add(3)
	ch := make(chan bool)
	var err1, err2, err3 error
	var msgs1, msgs2 []string
	// terminal 1
	go func() {
		defer wg.Done()
		err1 = func() error {
			conn, err := dialTile38(mc.port)
			if err != nil {
				return err
			}
			defer conn.Close()
			_, err = doTile38(conn,
				"SETCHAN", "test-eec", "NEARBY", "fleet",
				"FENCE", "DETECT", "enter,exit,cross",
				"POINT", "10.000", "10.000", "10000")
			if err != nil {
				return err
			}
			_, err = doTile38(conn, "SUBSCRIBE", "test-eec")
			if err != nil {
				return err
			}
			ch <- true
			for {
				js, err := redis.String(conn.Receive())
				if err != nil {
					return err
				}
				if js == `"DONE"` {
					break
				}
				msgs1 = append(msgs1, js)
			}
			return nil
		}()
	}()
	// terminal 2
	go func() {
		defer wg.Done()
		err2 = func() error {
			conn, err := dialTile38(mc.port)
			if err != nil {
				return err
			}
			defer conn.Close()
			_, err = doTile38(conn,
				"SETCHAN", "test-eecio", "NEARBY", "fleet",
				"FENCE", "DETECT", "enter,exit,cross,inside,outside",
				"POINT", "10.000", "10.000", "10000")
			if err != nil {
				return err
			}
			_, err = doTile38(conn, "SUBSCRIBE", "test-eecio")
			if err != nil {
				return err
			}
			ch <- true
			for {
				js, err := redis.String(conn.Receive())
				if err != nil {
					return err
				}
				if js == `"DONE"` {
					break
				}
				msgs2 = append(msgs2, js)
			}
			return nil
		}()
	}()
	// terminal 3
	var ok bool
	go func() {
		defer wg.Done()
		err3 = func() error {
			<-ch // terminal 1
			<-ch // terminal 2
			conn, err := dialTile38(mc.port)
			if err != nil {
				return err
			}
			defer conn.Close()
			if _, err = doTile38(conn,
				"SET", "fleet", "vehicle_1",
				"POINT", "10.0", "10.0"); err != nil {
				return err
			}
			if _, err = doTile38(conn,
				"SET", "fleet", "vehicle_1",
				"POINT", "0.0", "0.0"); err != nil {
				return err
			}
			if _, err = doTile38(conn,
				"SET", "fleet", "vehicle_1",
				"POINT", "20.0", "20.0"); err != nil {
				return err
			}
			if _, err = doTile38(conn, "PUBLISH", "test-eecio",
				"DONE"); err != nil {
				return err
			}
			if _, err = doTile38(conn, "PUBLISH", "test-eec",
				"DONE"); err != nil {
				return err
			}
			ok = true
			return nil
		}()
	}()
	var timeok atomic.Bool
	go func() {
		time.Sleep(time.Second * 30)
		if !timeok.Load() {
			panic("timeout")
		}
	}()
	wg.Wait()
	timeok.Store(true)
	if err3 != nil {
		return err3
	}
	if !ok {
		if err2 != nil {
			return err2
		}
		if err1 != nil {
			return err1
		}
	}
	var detects []string
	for i := 0; i < len(msgs1); i++ {
		detects = append(detects, gjson.Get(msgs1[i], "detect").String())
	}
	if strings.Join(detects, ",") != "enter,exit,cross" {
		errmsg := fmt.Sprintf("expected 'enter,exit,cross', got '%s'\n",
			strings.Join(detects, ","))
		return errors.New(errmsg)
	}
	detects = nil
	for i := 0; i < len(msgs2); i++ {
		detects = append(detects, gjson.Get(msgs2[i], "detect").String())
	}

	if strings.Join(detects, ",") != "enter,inside,exit,outside,cross,outside" {
		errmsg := fmt.Sprintf(
			"expected 'enter,inside,exit,outside,cross,outside', got '%s'\n",
			strings.Join(detects, ","))
		return errors.New(errmsg)
	}

	return nil
}
