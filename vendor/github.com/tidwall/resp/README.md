RESP
====

[![Build Status](https://travis-ci.org/tidwall/resp.svg?branch=master)](https://travis-ci.org/tidwall/resp)
[![GoDoc](https://godoc.org/github.com/tidwall/resp?status.svg)](https://godoc.org/github.com/tidwall/resp)

RESP is a [Go](http://golang.org/) library that provides a reader, writer, and server implementation for the [Redis RESP Protocol](http://redis.io/topics/protocol).

RESP is short for **REdis Serialization Protocol**.
While the protocol was designed specifically for Redis, it can be used for other client-server software projects.

The RESP protocol has the advantages of being human readable and with performance of a binary protocol.

\*\* **Note: If you are looking for a high-performance Redis server for Go, please checkout [Redcon](https://github.com/tidwall/redcon). It's much faster than this implementation and can handle pipelining.** \*\*

Features
--------

- [Reader](#reader) and [Writer](#writer) types for streaming RESP values from files, networks, or byte streams.
- [Server Implementation](#server) for creating your own RESP server. [Clients](#clients) use the same tools and libraries as Redis.
- [Append-only File](#append-only-file) type for persisting RESP values to disk. 

Installation
------------

Install resp using the "go get" command:

    go get github.com/tidwall/resp

The Go distribution is Resp's only dependency.

Documentation
-------------

- [API Reference](http://godoc.org/github.com/tidwall/resp)

Server
------

A Redis clone that implements the SET and GET commands.

- You can interact using the Redis CLI (redis-cli). http://redis.io/download
- Or, use the telnet by typing in "telnet localhost 6380" and type in "set key value" and "get key".
- Or, use a client library such as http://github.com/garyburd/redigo
- The "QUIT" command will close the connection.

```go
package main

import (
    "errors"
    "log"
    "sync"
    "github.com/tidwall/resp"
)

func main() {
    var mu sync.RWMutex
    kvs := make(map[string]string)
    s := resp.NewServer()
    s.HandleFunc("set", func(conn *resp.Conn, args []resp.Value) bool {
        if len(args) != 3 {
            conn.WriteError(errors.New("ERR wrong number of arguments for 'set' command"))
        } else {
            mu.Lock()
            kvs[args[1].String()] = args[2].String()
            mu.Unlock()
            conn.WriteSimpleString("OK")
        }
        return true
    })
    s.HandleFunc("get", func(conn *resp.Conn, args []resp.Value) bool {
        if len(args) != 2 {
            conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
        } else {
            mu.RLock()
            s, ok := kvs[args[1].String()]
            mu.RUnlock()
            if !ok {
                conn.WriteNull()
            } else {
                conn.WriteString(s)
            }
        }
        return true
    })
    if err := s.ListenAndServe(":6379"); err != nil {
        log.Fatal(err)
    }
}
```

Reader
------

The resp Reader type allows for an application to read raw RESP values from a file, network, or byte stream.

```go
raw := "*3\r\n$3\r\nset\r\n$6\r\nleader\r\n$7\r\nCharlie\r\n"
raw += "*3\r\n$3\r\nset\r\n$8\r\nfollower\r\n$6\r\nSkyler\r\n"
rd := resp.NewReader(bytes.NewBufferString(raw))
for {
    v, _, err := rd.ReadValue()
    if err == io.EOF {
        break
    }
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Read %s\n", v.Type())
    if v.Type() == Array {
        for i, v := range v.Array() {
            fmt.Printf("  #%d %s, value: '%s'\n", i, v.Type(), v)
        }
    }
}
// Output:
// Read Array
//   #0 BulkString, value: 'set'
//   #1 BulkString, value: 'leader'
//   #2 BulkString, value: 'Charlie'
// Read Array
//   #0 BulkString, value: 'set'
//   #1 BulkString, value: 'follower'
//   #2 BulkString, value: 'Skyler'
```

Writer
------

The resp Writer type allows for an application to write raw RESP values to a file, network, or byte stream.

```go
var buf bytes.Buffer
wr := resp.NewWriter(&buf)
wr.WriteArray([]resp.Value{resp.StringValue("set"), resp.StringValue("leader"), resp.StringValue("Charlie")})
wr.WriteArray([]resp.Value{resp.StringValue("set"), resp.StringValue("follower"), resp.StringValue("Skyler")})
fmt.Printf("%s", buf.String())
// Output:
// *3\r\n$3\r\nset\r\n$6\r\nleader\r\n$7\r\nCharlie\r\n
// *3\r\n$3\r\nset\r\n$8\r\nfollower\r\n$6\r\nSkyler\r\n
```

Append-Only File
----------------

An append only file (AOF) allows your application to  persist values to disk. It's very easy to use, and includes the same level of durablilty and binary format as [Redis AOF Persistence](http://redis.io/topics/persistence).

Check out the [AOF documentation](https://godoc.org/github.com/tidwall/resp#AOF) for more information

```go
// create and fill an appendonly file
aof, err := resp.OpenAOF("appendonly.aof")
if err != nil {
    log.Fatal(err)
}
// append a couple values and close the file
aof.Append(resp.MultiBulkValue("set", "leader", "Charlie"))
aof.Append(resp.MultiBulkValue("set", "follower", "Skyler"))
aof.Close()

// reopen and scan all values
aof, err = resp.OpenAOF("appendonly.aof")
if err != nil {
    log.Fatal(err)
}
defer aof.Close()
aof.Scan(func(v Value) {
    fmt.Printf("%s\n", v.String())
})

// Output:
// [set leader Charlie]
// [set follower Skyler]
}

```

Clients
-------

There are bunches of [RESP Clients](http://redis.io/clients). Most any client that supports Redis will support this implementation.

Contact
-------

Josh Baker [@tidwall](http://twitter.com/tidwall)

License
-------

Tile38 source code is available under the MIT [License](/LICENSE).

