package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/gjson"
	"github.com/tidwall/tile38/internal/server"
)

const tile38Port = 9191
const httpPort = 9292
const dir = "data"

var tile38Addr string
var httpAddr string

var wd string

var minX float64
var minY float64
var maxX float64
var maxY float64
var pool = &redis.Pool{
	MaxIdle:     3,
	IdleTimeout: 240 * time.Second,
	Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", tile38Addr)
	},
}
var providedTile38 bool
var providedHTTP bool

const blank = false
const hookServer = true

var logf *os.File

func main() {
	flag.StringVar(&tile38Addr, "tile38", "",
		"Tile38 address, leave blank to start a new server")
	flag.StringVar(&httpAddr, "hook", "",
		"Hook HTTP url, leave blank to start a new server")
	flag.Parse()
	log.Println("mockfill-107 (Github #107: Memory leak)")

	if tile38Addr == "" {
		tile38Addr = "127.0.0.1:" + strconv.FormatInt(int64(tile38Port), 10)
	} else {
		providedTile38 = true
	}
	if httpAddr == "" {
		httpAddr = "http://127.0.0.1:" + strconv.FormatInt(int64(httpPort), 10) + "/hook"
	} else {
		providedHTTP = true
	}
	var err error
	wd, err = os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	logf, err = os.Create("log")
	if err != nil {
		log.Fatal(err)
	}
	defer logf.Close()
	if !providedTile38 {
		copyAOF()
		go startTile38Server()
	}
	if !providedHTTP {
		if hookServer {
			go startHookServer()
		}
	}
	go waitForServers(func() {
		log.Printf("servers ready")
		logServer("START")
		setPoints()
		logServer("DONE")
	})
	select {}
}

func startTile38Server() {
	log.Println("start tile38 server")
	err := server.Serve("localhost", tile38Port, "data", false, 200*time.Millisecond, 50*time.Millisecond)
	if err != nil {
		log.Fatal(err)
	}
}

func startHookServer() {
	log.Println("start hook server")
	http.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "pong")
	})
	http.HandleFunc("/hook", func(w http.ResponseWriter, req *http.Request) {
		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(string(data))
	})
	err := http.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", httpPort), nil)
	if err != nil {
		log.Fatal(err)
	}
}

func waitForServers(cb func()) {
	log.Println("wait for servers")
	var err error
	start := time.Now()
	for {
		if time.Since(start) > time.Second*5 {
			log.Fatal("connection failed:", err)
		}
		func() {
			conn := pool.Get()
			defer conn.Close()
			var s string
			s, err = redis.String(conn.Do("PING"))
			if err != nil {
				return
			}
			if s != "PONG" {
				log.Fatalf("expected '%v', got '%v'", "PONG", s)
			}
		}()
		if err == nil {
			break
		}
		time.Sleep(time.Second / 5)
	}
	if hookServer {
		start = time.Now()
		for {
			if time.Since(start) > time.Second*5 {
				log.Fatal("connection failed:", err)
			}
			func() {
				var resp *http.Response
				resp, err = http.Get(httpAddr + "/notreal")
				if err != nil {
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 && resp.StatusCode != 404 {
					log.Fatalf("expected '%v', got '%v'", "200 or 404",
						resp.StatusCode)
				}
			}()
			if err == nil {
				break
			}
			time.Sleep(time.Second / 5)
		}
	}
	cb()
}

func downloadAOF() {
	log.Println("downloading aof")
	resp, err := http.Get("https://github.com/tidwall/tile38/files/675225/appendonly.aof.zip")
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	rd, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range rd.File {
		if path.Ext(f.Name) == ".aof" {
			rc, err := f.Open()
			if err != nil {
				log.Fatal(err)
			}
			defer rc.Close()

			data, err := ioutil.ReadAll(rc)
			if err != nil {
				log.Fatal(err)
			}
			err = ioutil.WriteFile(path.Join(wd, "appendonly.aof"), data, 0666)
			if err != nil {
				log.Fatal(err)
			}
			return
		}
	}
	log.Fatal("invalid appendonly.aof.zip")
}

func copyAOF() {
	if err := os.RemoveAll(path.Join(wd, "data")); err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(path.Join(wd, "data"), 0777); err != nil {
		log.Fatal(err)
	}
	fin, err := os.Open(path.Join(wd, "appendonly.aof"))
	if err != nil {
		if os.IsNotExist(err) {
			downloadAOF()
			fin, err = os.Open(path.Join(wd, "appendonly.aof"))
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}
	}
	defer fin.Close()

	log.Println("load aof")
	fout, err := os.Create(path.Join(wd, "data", "appendonly.aof"))
	if err != nil {
		log.Fatal(err)
	}
	defer fout.Close()
	data, err := ioutil.ReadAll(fin)
	if err != nil {
		log.Fatal(err)
	}
	rep := httpAddr
	rep = "$" + strconv.FormatInt(int64(len(rep)), 10) + "\r\n" + rep + "\r\n"
	data = bytes.Replace(data,
		[]byte("$23\r\nhttp://172.17.0.1:9999/\r\n"), []byte(rep), -1)
	if blank {
		data = nil
	}
	if _, err := fout.Write(data); err != nil {
		log.Fatal(err)
	}
}

func respGet(resp interface{}, idx ...int) interface{} {
	for i := 0; i < len(idx); i++ {
		arr, _ := redis.Values(resp, nil)
		resp = arr[idx[i]]
	}
	return resp
}

type PSAUX struct {
	User    string
	PID     int
	CPU     float64
	Mem     float64
	VSZ     int
	RSS     int
	TTY     string
	Stat    string
	Start   string
	Time    string
	Command string
}

func atoi(s string) int {
	n, _ := strconv.ParseInt(s, 10, 64)
	return int(n)
}
func atof(s string) float64 {
	n, _ := strconv.ParseFloat(s, 64)
	return float64(n)
}
func psaux(pid int) PSAUX {
	var res []byte
	res, err := exec.Command("ps", "ux", "-p", strconv.FormatInt(int64(pid), 10)).CombinedOutput()
	if err != nil {
		return PSAUX{}
	}
	pids := strconv.FormatInt(int64(pid), 10)
	for _, line := range strings.Split(string(res), "\n") {
		var words []string
		for _, word := range strings.Split(line, " ") {
			if word != "" {
				words = append(words, word)
			}
		}
		if len(words) >= 11 {
			if words[1] == pids {
				return PSAUX{
					User:    words[0],
					PID:     atoi(words[1]),
					CPU:     atof(words[2]),
					Mem:     atof(words[3]),
					VSZ:     atoi(words[4]),
					RSS:     atoi(words[5]),
					TTY:     words[6],
					Stat:    words[7],
					Start:   words[8],
					Time:    words[9],
					Command: words[10],
				}
			}
		}
	}
	return PSAUX{}
}
func respGetFloat(resp interface{}, idx ...int) float64 {
	resp = respGet(resp, idx...)
	f, _ := redis.Float64(resp, nil)
	return f
}
func logServer(tag string) {
	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("OUTPUT", "json")
	if err != nil {
		log.Fatal(err)
	}
	_, err = redis.String(conn.Do("GC"))
	if err != nil {
		log.Fatal(err)
	}
	json, err := redis.String(conn.Do("SERVER"))
	if err != nil {
		log.Fatal(err)
	}
	_, err = conn.Do("OUTPUT", "resp")
	if err != nil {
		log.Fatal(err)
	}
	rss := float64(psaux(int(gjson.Get(json, "stats.pid").Int())).RSS) / 1024
	heapSize := gjson.Get(json, "stats.heap_size").Float() / 1024 / 1024
	heapReleased := gjson.Get(json, "stats.heap_released").Float() / 1024 / 1024
	fmt.Fprintf(logf, "%s %10.2f MB (heap) %10.2f MB (released) %10.2f MB (system)\n",
		time.Now().Format("2006-01-02T15:04:05Z07:00"),
		heapSize, heapReleased, rss)
}
func setPoints() {
	go func() {
		var i int
		for range time.NewTicker(time.Second * 1).C {
			logServer(fmt.Sprintf("SECOND-%d", i*1))
			i++
		}
	}()

	rand.Seed(time.Now().UnixNano())
	n := 1000000
	ex := time.Second * 10
	log.Printf("time to pump data (%d points, expires %s)", n, ex)
	conn := pool.Get()
	defer conn.Close()
	if blank {
		minX = -124.40959167480469
		minY = 32.53415298461914
		maxX = -114.13121032714844
		maxY = 42.009521484375
	} else {
		resp, err := conn.Do("bounds", "boundies")
		if err != nil {
			log.Fatal(err)
		}
		minX = respGetFloat(resp, 0, 0)
		minY = respGetFloat(resp, 0, 1)
		maxX = respGetFloat(resp, 1, 0)
		maxY = respGetFloat(resp, 1, 1)
	}
	log.Printf("bbox: [[%.4f,%.4f],[%.4f,%.4f]]\n", minX, minY, maxX, maxY)
	var idx uint64
	for i := 0; i < 4; i++ {
		go func() {
			conn := pool.Get()
			defer conn.Close()
			for i := 0; i < n; i++ {
				atomic.AddUint64(&idx, 1)
				id := fmt.Sprintf("person:%d", idx)
				x := rand.Float64()*(maxX-minX) + minX
				y := rand.Float64()*(maxY-minY) + minY
				ok, err := redis.String(conn.Do("SET", "people", id,
					"EX", float64(ex/time.Second),
					"POINT", y, x))
				if err != nil {
					log.Fatal(err)
				}
				if ok != "OK" {
					log.Fatalf("expected 'OK', got '%v", ok)
				}
				log.Printf("SET people %v EX %v POINT %v %v",
					id, float64(ex/time.Second), y, x)
			}
		}()
	}
	select {}
}
