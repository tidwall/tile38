package log

import (
	"bytes"
	"io/ioutil"
	"strings"
	"testing"
)

func TestLog(t *testing.T) {
	f := &bytes.Buffer{}
	SetOutput(f)
	Printf("hello %v", "everyone")
	if !strings.HasSuffix(f.String(), "hello everyone\n") {
		t.Fatal("fail")
	}
}

func BenchmarkLogPrintf(t *testing.B) {
	SetOutput(ioutil.Discard)
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		Printf("X %s", "Y")
	}

}
