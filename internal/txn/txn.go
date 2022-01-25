package txn

import (
	"sync/atomic"
	"time"
)

const deadlineMask = ^int64(0x7)
const errCodeMask = int64(0x3)
const signalInterruptedMask = int64(0x4)

type Status struct {
	s      *Scheduler
	status int64
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
	deadline := ts.status & deadlineMask
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
			status: t.UnixNano() & deadlineMask,
		}
	}
	existingDeadline := ts.status & deadlineMask
	if existingDeadline != 0 {
		if t.UnixNano() > existingDeadline {
			return ts
		}
	}

	return &Status{
		s:      ts.s,
		status: (t.UnixNano() & deadlineMask) | (ts.status & ^deadlineMask),
	}
}

func (ts *Status) Retry() {
	// Release the current lock
	ts.s.completeRead()

	// clear error
	ts.status = ts.status & ^errCodeMask

	// Reacquire reader lock
	ts.s.Read()
}

func (ts *Status) updateIfNeeded() {
	errCode := ts.status & errCodeMask
	if errCode != 0 {
		// Already set an error, no update needed
		return
	}

	now := time.Now().UnixNano()
	deadline := ts.status & deadlineMask
	if deadline != 0 {
		// Check if deadline is hit
		if now >= deadline {
			ts.status |= int64(errCodeDeadline)
			return
		}
	}

	if ts.s != nil {
		// Check if we are interrupted
		readDeadline := atomic.LoadInt64(&ts.s.readDeadline)
		if readDeadline != 0 && now >= readDeadline {
			if ts.status&signalInterruptedMask != 0 {
				// Note: the first time we are interrupted we do not call
				// notify interrupted. This is because we could have been scheduled
				// slightly before the deadline and then been interrupted, hence
				// we don't want to adjust the scheduler's stats until we've been
				// granted a full timeslice.
				ts.s.notifyInterrupted()
			}
			ts.status = deadline | int64(errCodeInterrupted) | signalInterruptedMask
			return
		}
	}
}

func (ts *Status) err() txnErr {
	errCode := byte(ts.status & errCodeMask)
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
