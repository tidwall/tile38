# lotsa

Lotsa is a simple Go library for executing lots of operations spread over any number of threads.

## Install

```
go get -u github.com/tidwall/lotsa
```

## Example

Here we load 1,000,000 operations spread over 4 threads.

```
var total int64
lotsa.Ops(1000000, 4,
    func(i, thread int) {
        atomic.AddInt64(&total, 1)
    },
)
println(total)
```

Prints `1000000`

## Contact

Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

Source code is available under the MIT [License](/LICENSE).
