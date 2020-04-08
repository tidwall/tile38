package tests

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	clear   = "\x1b[0m"
	bright  = "\x1b[1m"
	dim     = "\x1b[2m"
	black   = "\x1b[30m"
	red     = "\x1b[31m"
	green   = "\x1b[32m"
	yellow  = "\x1b[33m"
	blue    = "\x1b[34m"
	magenta = "\x1b[35m"
	cyan    = "\x1b[36m"
	white   = "\x1b[37m"
)

func TestAll(t *testing.T) {
	mockCleanup(false)
	defer mockCleanup(false)

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		mockCleanup(false)
		os.Exit(1)
	}()

	mc, err := mockOpenServer(false)
	if err != nil {
		t.Fatal(err)
	}
	defer mc.Close()
	runSubTest(t, "keys", mc, subTestKeys)
	runSubTest(t, "json", mc, subTestJSON)
	runSubTest(t, "search", mc, subTestSearch)
	runSubTest(t, "testcmd", mc, subTestTestCmd)
	runSubTest(t, "fence", mc, subTestFence)
	runSubTest(t, "scripts", mc, subTestScripts)
	runSubTest(t, "info", mc, subTestInfo)
	runSubTest(t, "client", mc, subTestClient)
	runSubTest(t, "timeouts", mc, subTestTimeout)
}

func runSubTest(t *testing.T, name string, mc *mockServer, test func(t *testing.T, mc *mockServer)) {
	t.Run(name, func(t *testing.T) {
		fmt.Printf(bright+"Testing %s\n"+clear, name)
		test(t, mc)
	})
}

func runStep(t *testing.T, mc *mockServer, name string, step func(mc *mockServer) error) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Helper()
		if err := func() error {
			// reset the current server
			mc.ResetConn()
			defer mc.ResetConn()
			// clear the database so the test is consistent
			if err := mc.DoBatch([][]interface{}{
				{"OUTPUT", "resp"}, {"OK"},
				{"FLUSHDB"}, {"OK"},
			}); err != nil {
				return err
			}
			if err := step(mc); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			fmt.Printf("["+red+"fail"+clear+"]: %s\n", name)
			t.Fatal(err)
		}
		fmt.Printf("["+green+"ok"+clear+"]: %s\n", name)
	})
}

func BenchmarkAll(b *testing.B) {
	mockCleanup(true)
	defer mockCleanup(true)

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		mockCleanup(true)
		os.Exit(1)
	}()

	mc, err := mockOpenServer(true)
	if err != nil {
		b.Fatal(err)
	}
	defer mc.Close()
	runSubBenchmark(b, "search", mc, subBenchSearch)
}

func loadBenchmarkPoints(b *testing.B, mc *mockServer) (err error) {
	const nPoints = 200000
	rand.Seed(time.Now().UnixNano())

	// add a bunch of points
	for i := 0; i < nPoints; i++ {
		val := fmt.Sprintf("val:%d", i)
		var resp string
		var lat, lon, fval float64
		fval = rand.Float64()
		lat = rand.Float64()*180 - 90
		lon = rand.Float64()*360 - 180
		resp, err = redis.String(mc.conn.Do("SET",
			"mykey", val,
			"FIELD", "foo", fval,
			"POINT", lat, lon))
		if err != nil {
			return
		}
		if resp != "OK" {
			err = fmt.Errorf("expected 'OK', got '%s'", resp)
			return
		}
	}
	return
}

func runSubBenchmark(b *testing.B, name string, mc *mockServer, bench func(t *testing.B, mc *mockServer)) {
	b.Run(name, func(b *testing.B) {
		bench(b, mc)
	})
}

func runBenchStep(b *testing.B, mc *mockServer, name string, step func(mc *mockServer) error) {
	b.Helper()
	b.Run(name, func(b *testing.B) {
		b.Helper()
		if err := func() error {
			// reset the current server
			mc.ResetConn()
			defer mc.ResetConn()
			// clear the database so the test is consistent
			if err := mc.DoBatch([][]interface{}{
				{"OUTPUT", "resp"}, {"OK"},
				{"FLUSHDB"}, {"OK"},
			}); err != nil {
				return err
			}
			err := loadBenchmarkPoints(b, mc)
			if err != nil {
				return err
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := step(mc); err != nil {
					return err
				}
			}
			return nil
		}(); err != nil {
			b.Fatal(err)
		}
	})
}
