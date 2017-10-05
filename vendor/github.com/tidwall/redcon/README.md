<p align="center">
<img 
    src="logo.png" 
    width="336" height="75" border="0" alt="REDCON">
<br>
<a href="https://travis-ci.org/tidwall/redcon"><img src="https://img.shields.io/travis/tidwall/redcon.svg?style=flat-square" alt="Build Status"></a>
<a href="https://godoc.org/github.com/tidwall/redcon"><img src="https://img.shields.io/badge/api-reference-blue.svg?style=flat-square" alt="GoDoc"></a>
</p>

<p align="center">Fast Redis compatible server framework for Go</p>

Redcon is a custom Redis server framework for Go that is fast and simple to use. The reason for this library it to give an efficient server front-end for the [BuntDB](https://github.com/tidwall/buntdb) and [Tile38](https://github.com/tidwall/tile38) projects.

Features
--------
- Create a [Fast](#benchmarks) custom Redis compatible server in Go
- Simple interface. One function `ListenAndServe` and two types `Conn` & `Command`
- Support for pipelining and telnet commands
- Works with Redis clients such as [redigo](https://github.com/garyburd/redigo), [redis-py](https://github.com/andymccurdy/redis-py), [node_redis](https://github.com/NodeRedis/node_redis), and [jedis](https://github.com/xetorthio/jedis)
- [TLS Support](#tls-example)

Installing
----------

```
go get -u github.com/tidwall/redcon
```

Example
-------

Here's a full example of a Redis clone that accepts:

- SET key value
- GET key
- DEL key
- PING
- QUIT

You can run this example from a terminal:

```sh
go run example/clone.go
```

```go
package main

import (
	"log"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

var addr = ":6380"

func main() {
	var mu sync.RWMutex
	var items = make(map[string][]byte)
	go log.Printf("started server at %s", addr)
	err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				items[string(cmd.Args[1])] = cmd.Args[2]
				mu.Unlock()
				conn.WriteString("OK")
			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.RLock()
				val, ok := items[string(cmd.Args[1])]
				mu.RUnlock()
				if !ok {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				_, ok := items[string(cmd.Args[1])]
				delete(items, string(cmd.Args[1]))
				mu.Unlock()
				if !ok {
					conn.WriteInt(0)
				} else {
					conn.WriteInt(1)
				}
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

TLS Example
-----------

Redcon has full TLS support through the `ListenAndServeTLS` function.

The [same example](example/tls/clone.go) is also provided for serving Redcon over TLS. 

```sh
go run example/tls/clone.go
```

Benchmarks
----------

**Redis**: Single-threaded, no disk persistence.

```
$ redis-server --port 6379 --appendonly no
```
```
redis-benchmark -p 6379 -t set,get -n 10000000 -q -P 512 -c 512
SET: 941265.12 requests per second
GET: 1189909.50 requests per second
```

**Redcon**: Single-threaded, no disk persistence.

```
$ GOMAXPROCS=1 go run example/clone.go
```
```
redis-benchmark -p 6380 -t set,get -n 10000000 -q -P 512 -c 512
SET: 2018570.88 requests per second
GET: 2403846.25 requests per second
```

**Redcon**: Multi-threaded, no disk persistence.

```
$ GOMAXPROCS=0 go run example/clone.go
```
```
$ redis-benchmark -p 6380 -t set,get -n 10000000 -q -P 512 -c 512
SET: 1944390.38 requests per second
GET: 3993610.25 requests per second
```

*Running on a MacBook Pro 15" 2.8 GHz Intel Core i7 using Go 1.7*

Contact
-------
Josh Baker [@tidwall](http://twitter.com/tidwall)

License
-------
Redcon source code is available under the MIT [License](/LICENSE).
