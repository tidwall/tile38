package txn

import (
	"sync/atomic"
	"time"
)

const extraTimeMask = ^int64(0x3)
const errCodeMask = int64(0x3)

type Status struct {
	s        *Scheduler
	deadline int64
	flags    int64
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
	if ts.deadline == 0 {
		return time.Time{}
	}
	return time.Unix(0, ts.deadline)
}

func (ts *Status) WithDeadline(t time.Time) *Status {
	if t.IsZero() {
		return ts
	}
	if ts == nil {
		return &Status{
			deadline: t.UnixNano(),
		}
	}
	existingDeadline := ts.deadline
	if existingDeadline != 0 {
		if t.UnixNano() > existingDeadline {
			return ts
		}
	}

	return &Status{
		s:        ts.s,
		deadline: t.UnixNano(),
	}
}

func (ts *Status) Retry() {
	// Release the current lock
	ts.s.completeRead()

	// Add in extra time for next time, and clear error
	extraTime := ts.flags & extraTimeMask
	if extraTime != 0 {
		ts.flags = (extraTime * 2) & extraTimeMask
	} else {
		ts.flags = int64(ts.s.maxWriteDelay) & extraTimeMask
	}

	// Reacquire reader lock
	ts.s.Read()
}

func (ts *Status) updateIfNeeded() {
	errCode := ts.flags & errCodeMask
	if errCode != 0 {
		// Already set an error, no update needed
		return
	}

	now := time.Now().UnixNano()
	extraTime := ts.flags & extraTimeMask
	if ts.deadline != 0 {
		// Check if deadline is hit
		if now >= ts.deadline {
			ts.flags = extraTime | int64(errCodeDeadline)
			return
		}
	}

	if ts.s != nil {
		// Check if we are interrupted
		readDeadline := atomic.LoadInt64(&ts.s.readDeadline)
		if readDeadline != 0 && now >= (readDeadline+extraTime) {
			ts.flags = extraTime | int64(errCodeInterrupted)
			return
		}
	}
}

func (ts *Status) err() txnErr {
	errCode := byte(ts.flags & errCodeMask)
	switch errCode {
	case errCodeClosed:
		return ClosedError{}
	case errCodeInterrupted:
		return InterruptedError{}
	case errCodeDeadline:
		return DeadlineError{}
	}
	return nil
}
