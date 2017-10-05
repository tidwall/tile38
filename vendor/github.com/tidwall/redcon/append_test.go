package redcon

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
)

func TestNextCommand(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	start := time.Now()
	for time.Since(start) < time.Second {
		// keep copy of pipeline args for final compare
		var plargs [][][]byte

		// create a pipeline of random number of commands with random data.
		N := rand.Int() % 10000
		var data []byte
		for i := 0; i < N; i++ {
			nargs := rand.Int() % 10
			data = AppendArray(data, nargs)
			var args [][]byte
			for j := 0; j < nargs; j++ {
				arg := make([]byte, rand.Int()%100)
				if _, err := rand.Read(arg); err != nil {
					t.Fatal(err)
				}
				data = AppendBulk(data, arg)
				args = append(args, arg)
			}
			plargs = append(plargs, args)
		}

		// break data into random number of chunks
		chunkn := rand.Int() % 100
		if chunkn == 0 {
			chunkn = 1
		}
		if len(data) < chunkn {
			continue
		}
		var chunks [][]byte
		var chunksz int
		for i := 0; i < len(data); i += chunksz {
			chunksz = rand.Int() % (len(data) / chunkn)
			var chunk []byte
			if i+chunksz < len(data) {
				chunk = data[i : i+chunksz]
			} else {
				chunk = data[i:]
			}
			chunks = append(chunks, chunk)
		}

		// process chunks
		var rbuf []byte
		var fargs [][][]byte
		for _, chunk := range chunks {
			var data []byte
			if len(rbuf) > 0 {
				data = append(rbuf, chunk...)
			} else {
				data = chunk
			}
			for {
				complete, args, _, leftover, err := ReadNextCommand(data, nil)
				data = leftover
				if err != nil {
					t.Fatal(err)
				}
				if !complete {
					break
				}
				fargs = append(fargs, args)
			}
			rbuf = append(rbuf[:0], data...)
		}
		// compare final args to original
		if len(plargs) != len(fargs) {
			t.Fatalf("not equal size: %v != %v", len(plargs), len(fargs))
		}
		for i := 0; i < len(plargs); i++ {
			if len(plargs[i]) != len(fargs[i]) {
				t.Fatalf("not equal size for item %v: %v != %v", i, len(plargs[i]), len(fargs[i]))
			}
			for j := 0; j < len(plargs[i]); j++ {
				if !bytes.Equal(plargs[i][j], plargs[i][j]) {
					t.Fatalf("not equal for item %v:%v: %v != %v", i, j, len(plargs[i][j]), len(fargs[i][j]))
				}
			}
		}
	}
}
