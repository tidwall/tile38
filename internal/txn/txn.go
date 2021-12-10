package txn

import (
	"sync/atomic"
	"time"
)

const deadlineMask = ^int64(0x3)
const errCodeMask = int64(0x3)

type Status struct {
	s     *Scheduler
	state int64
}

func (ts *Status) IsAborted() bool {
	if ts == nil {
		return false
	}
	ts.updateIfNeeded()
	return ts.err() != nil
}

func (ts *Status) Error() error {
	if ts == nil {
		return nil
	}
	ts.updateIfNeeded()
	return ts.err()
}

func (ts *Status) GetDeadlineTime() time.Time {
	if ts == nil {
		return time.Time{}
	}
	deadline := ts.state & deadlineMask
	if deadline == 0 {
		return time.Time{}
	}
	return time.Unix(0, deadline)
}

func (ts *Status) WithDeadline(t time.Time) *Status {
	if t.IsZero() {
		return ts
	}
	if ts == nil {
		return &Status{
			state: t.UnixNano() & deadlineMask,
		}
	}
	existingState := ts.state
	existingDeadline := existingState & deadlineMask
	if existingDeadline != 0 {
		if t.UnixNano() > existingDeadline {
			return ts
		}
	}
	newState := (t.UnixNano() & deadlineMask) | (existingState & errCodeMask)
	return &Status{
		s:     ts.s,
		state: newState,
	}
}

func (ts *Status) ResetError() {
	ts.state = ts.state & deadlineMask
}

func (ts *Status) updateIfNeeded() {
	errCode := ts.state & 0x3
	if errCode != 0 {
		// Already set an error, no update needed
		return
	}

	now := time.Now().UnixNano()
	deadline := ts.state & deadlineMask
	if deadline != 0 {
		// Check if deadline is hit
		if now >= deadline {
			ts.state = ts.state | int64(errCodeDeadline)
			return
		}
	}

	if ts.s != nil {
		// Check if we are interrupted
		readDeadline := atomic.LoadInt64(&ts.s.readDeadline)
		if readDeadline != 0 && now >= readDeadline {
			ts.state = ts.state | int64(errCodeInterrupted)
			return
		}
	}
}

func (ts *Status) err() txnErr {
	errCode := ts.state & errCodeMask
	switch byte(errCode) {
	case errCodeClosed:
		return ClosedError{}
	case errCodeInterrupted:
		return InterruptedError{}
	case errCodeDeadline:
		return DeadlineError{}
	}
	return nil
}
