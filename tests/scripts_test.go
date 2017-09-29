package tests

import (
	"testing"
)

func subTestScripts(t *testing.T, mc *mockServer) {
	runStep(t, mc, "BASIC", scripts_BASIC_test)
	runStep(t, mc, "ATOMIC", scripts_ATOMIC_test)
	runStep(t, mc, "READONLY", scripts_READONLY_test)
	runStep(t, mc, "NONATOMIC", scripts_NONATOMIC_test)
}

func scripts_BASIC_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"EVAL", "return 2 + 2", "0"}, {"4"},
		{"SCRIPT LOAD", "return 2 + 2"}, {"2dd1b44209ecb49617af05caf0491390a03c1cc4"},
		{"SCRIPT EXISTS", "2dd1b44209ecb49617af05caf0491390a03c1cc4", "no_script"}, {"[1 0]"},
		{"EVALSHA", "2dd1b44209ecb49617af05caf0491390a03c1cc4", "0"}, {"4"},
		{"SCRIPT FLUSH"}, {"OK"},
		{"SCRIPT EXISTS", "2dd1b44209ecb49617af05caf0491390a03c1cc4", "no_script"}, {"[0 0]"},
		{"EVAL", "return ARGS[1] .. ' and ' .. ARGS[2]", "0", "arg1", "arg2"}, {"arg1 and arg2"},
		{"EVAL", "return tile38.call('get', KEYS[1], ARGS[1])", "1", "mykey", "myid"}, {nil},
		{"EVAL", "return tile38.call('set', KEYS[1], ARGS[1], 'point', 33, -115)", "1", "mykey", "myid1"}, {"OK"},
		{"EVAL", "return tile38.call('get', KEYS[1], ARGS[1], ARGS[2])", "1", "mykey", "myid1", "point"}, {"[33 -115]"},
	})
}

// TODO: need some async setup for concurrent requests
func scripts_ATOMIC_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
	})
}

func scripts_READONLY_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
	})
}

func scripts_NONATOMIC_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
	})
}
