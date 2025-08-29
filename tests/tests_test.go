package tests

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"sync/atomic"

	"github.com/gomodule/redigo/redis"
	"github.com/tidwall/limiter"
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

func TestIntegration(t *testing.T) {

	mockCleanup(true)
	defer mockCleanup(true)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		mockCleanup(false)
		os.Exit(1)
	}()

	regTestGroup("keys", subTestKeys)
	regTestGroup("json", subTestJSON)
	regTestGroup("search", subTestSearch)
	regTestGroup("testcmd", subTestTestCmd)
	regTestGroup("client", subTestClient)
	regTestGroup("scripts", subTestScripts)
	regTestGroup("fence", subTestFence)
	regTestGroup("info", subTestInfo)
	regTestGroup("timeouts", subTestTimeout)
	regTestGroup("metrics", subTestMetrics)
	regTestGroup("follower", subTestFollower)
	regTestGroup("aof", subTestAOF)
	regTestGroup("monitor", subTestMonitor)
	regTestGroup("proto", subTestProto)
	runTestGroups(t)
}

var allGroups []*testGroup

func runTestGroups(t *testing.T) {
	limit := runtime.NumCPU()
	if limit > 16 {
		limit = 16
	}
	l := limiter.New(limit)

	// Initialize all stores as "skipped", but they'll be unset if the test is
	// not actually skipped.
	for _, g := range allGroups {
		for _, s := range g.subs {
			s.skipped.Store(true)
		}
	}
	for _, g := range allGroups {
		func(g *testGroup) {
			t.Run(g.name, func(t *testing.T) {
				for _, s := range g.subs {
					func(s *testGroupSub) {
						t.Run(s.name, func(t *testing.T) {
							s.skipped.Store(false)
							var wg sync.WaitGroup
							wg.Add(1)
							var err error
							go func() {
								l.Begin()
								defer func() {
									l.End()
									wg.Done()
								}()
								err = s.run()
							}()
							if false {
								t.Parallel()
								t.Run("bg", func(t *testing.T) {
									wg.Wait()
									if err != nil {
										t.Fatal(err)
									}
								})
							}
						})
					}(s)
				}
			})
		}(g)
	}

	done := make(chan bool)
	go func() {
		defer func() { done <- true }()
		// count the largest sub test name
		var largest int
		for _, g := range allGroups {
			for _, s := range g.subs {
				if !s.skipped.Load() {
					if len(s.name) > largest {
						largest = len(s.name)
					}
				}
			}
		}

		for {
			finished := true
			for _, g := range allGroups {
				skipped := true
				for _, s := range g.subs {
					if !s.skipped.Load() {
						skipped = false
						break
					}
				}
				if !skipped && !g.printed.Load() {
					fmt.Printf("\n"+bright+"Testing %s"+clear+"\n", g.name)
					g.printed.Store(true)
				}
				const frtmp = "%s ... "
				for _, s := range g.subs {
					if !s.skipped.Load() && !s.printedName.Load() {
						pref := fmt.Sprintf(frtmp, s.name)
						nspaces := largest - len(pref) + 5
						if nspaces < 0 {
							nspaces = 0
						}
						spaces := strings.Repeat(" ", nspaces)
						fmt.Printf("%s%s", pref, spaces)
						s.printedName.Store(true)
					}
					if s.done.Load() && !s.printedResult.Load() {
						if s.err != nil {
							fmt.Printf("[" + red + "fail" + clear + "]\n")
						} else {
							fmt.Printf("[" + green + "ok" + clear + "]\n")
						}
						s.printedResult.Store(true)
					}
					if !s.skipped.Load() && !s.done.Load() {
						finished = false
						break
					}
				}
				if !finished {
					break
				}
			}
			if finished {
				break
			}
			time.Sleep(time.Second / 4)
		}
	}()
	<-done
	var fail bool
	for _, g := range allGroups {
		for _, s := range g.subs {
			if s.err != nil {
				t.Errorf("%s/%s/%s\n%s", t.Name(), g.name, s.name, s.err)
				fail = true
			}
		}
	}
	if fail {
		t.Fail()
	}

}

type testGroup struct {
	name    string
	subs    []*testGroupSub
	printed atomic.Bool
}

type testGroupSub struct {
	g             *testGroup
	name          string
	fn            func(mc *mockServer) error
	err           error
	skipped       atomic.Bool
	done          atomic.Bool
	printedName   atomic.Bool
	printedResult atomic.Bool
}

func regTestGroup(name string, fn func(g *testGroup)) {
	g := &testGroup{name: name}
	allGroups = append(allGroups, g)
	fn(g)
}

func (g *testGroup) regSubTest(name string, fn func(mc *mockServer) error) {
	s := &testGroupSub{g: g, name: name, fn: fn}
	g.subs = append(g.subs, s)
}

func (s *testGroupSub) run() (err error) {
	// This all happens in a background routine.
	defer func() {
		s.err = err
		s.done.Store(true)
	}()
	return func() error {
		mc, err := mockOpenServer(MockServerOptions{
			Silent:  true,
			Metrics: true,
		})
		if err != nil {
			return err
		}
		defer mc.Close()
		return s.fn(mc)
	}()
}

func BenchmarkAll(b *testing.B) {
	mockCleanup(true)
	defer mockCleanup(true)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		mockCleanup(true)
		os.Exit(1)
	}()

	mc, err := mockOpenServer(MockServerOptions{
		Silent: true, Metrics: true,
	})
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
