# lotsa

Lotsa is a simple Go library for executing lots of operations spread over any number of threads.

## Install

```
go get -u github.com/tidwall/lotsa
```

## Example

Here we load 1,000,000 operations spread over 4 threads.

```go
var total int64
lotsa.Ops(1000000, 4,
    func(i, thread int) {
        atomic.AddInt64(&total, 1)
    },
)
println(total)
```

Prints `1000000`

To output some benchmarking results, set the `lotsa.Output` prior to calling `lotsa.Ops`

```go
var total int64
lotsa.Output = os.Stdout
lotsa.Ops(1000000, 4,
    func(i, thread int) {
        atomic.AddInt64(&total, 1)
    },
)
```

Prints: 

```
1,000,000 ops over 4 threads in 23ms, 43,580,037/sec, 22 ns/op
```

## Contact

Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

Source code is available under the MIT [License](/LICENSE).
