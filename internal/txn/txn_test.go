package txn

import (
	"testing"
	"time"
)

func TestDeadlineUnset(t *testing.T) {
	ts := &Status{}

	if !ts.GetDeadlineTime().IsZero() {
		t.Fatalf("expected zero deadline time")
	}
}

func TestWithDeadline(t *testing.T) {
	t0 := time.Now()
	ts := &Status{}
	ts = ts.WithDeadline(t0)

	if !ts.GetDeadlineTime().Equal(t0) {
		t.Fatalf("expected %v got %v", t0, ts.GetDeadlineTime())
	}
}
