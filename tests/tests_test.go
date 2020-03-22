package tests

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"
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
	mockCleanup()
	defer mockCleanup()

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		mockCleanup()
		os.Exit(1)
	}()

	mc, err := mockOpenServer()
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
	runSubTest(t, "expressions", mc, subTestExpressions)
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
