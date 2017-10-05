# Redbench
[![GoDoc](https://img.shields.io/badge/api-reference-blue.svg?style=flat-square)](https://godoc.org/github.com/tidwall/redbench) 

Redbench is a Go package that allows for bootstrapping benchmarks
for servers using a custom implementation of the Redis protocol. It provides
the same inputs and outputs as the
[redis-benchmark](https://redis.io/topics/benchmarks) tool. 

The purpose of this library is to provide benchmarking for 
[Redcon](https://github.com/tidwall/redcon) compatible servers such as
[Tile38](https://github.com/tidwall/tile38), but also works well for Redis
operations that are not covered by the `redis-benchmark` tool such as the 
`GEO*` commands, custom lua scripts, or [Redis Modules](http://antirez.com/news/106).

## Getting Started

### Installing

To start using Redbench, install Go and run `go get`:

```sh
$ go get -u github.com/tidwall/redbench
```

This will retrieve the library.

### Example

The following example will run a benchmark for the `PING,SET,GET,GEOADD,GEORADIUS`
commands on a server at 127.0.0.1:6379.

```go
package main

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/tidwall/redbench"
)

func main() {
	redbench.Bench("PING", "127.0.0.1:6379", nil, nil, func(buf []byte) []byte {
		return redbench.AppendCommand(buf, "PING")
	})
	redbench.Bench("SET", "127.0.0.1:6379", nil, nil, func(buf []byte) []byte {
		return redbench.AppendCommand(buf, "SET", "key:string", "val")
	})
	redbench.Bench("GET", "127.0.0.1:6379", nil, nil, func(buf []byte) []byte {
		return redbench.AppendCommand(buf, "GET", "key:string")
	})
	rand.Seed(time.Now().UnixNano())
	redbench.Bench("GEOADD", "127.0.0.1:6379", nil, nil, func(buf []byte) []byte {
		return redbench.AppendCommand(buf, "GEOADD", "key:geo",
			strconv.FormatFloat(rand.Float64()*360-180, 'f', 7, 64),
			strconv.FormatFloat(rand.Float64()*170-85, 'f', 7, 64),
			strconv.Itoa(rand.Int()))
	})
	redbench.Bench("GEORADIUS", "127.0.0.1:6379", nil, nil, func(buf []byte) []byte {
		return redbench.AppendCommand(buf, "GEORADIUS", "key:geo",
			strconv.FormatFloat(rand.Float64()*360-180, 'f', 7, 64),
			strconv.FormatFloat(rand.Float64()*170-85, 'f', 7, 64),
			"10", "km")
	})
}
```

Which is similar to executing:

```
$ redis-benchmark -t PING,SET,GET
```

For a more complete example, check out [tile38-benchmark](https://github.com/tidwall/tile38/blob/master/cmd/tile38-benchmark/main.go) from the [Tile38](https://github.com/tidwall/tile38) project.

### Custom Options

```go
type Options struct {
	Requests int
	Clients  int
	Pipeline int
	Quiet    bool
	CSV      bool
	Stdout   io.Writer
	Stderr   io.Writer
}
```

## Contact
Josh Baker [@tidwall](http://twitter.com/tidwall)

## License
Redbench source code is available under the MIT [License](/LICENSE).

