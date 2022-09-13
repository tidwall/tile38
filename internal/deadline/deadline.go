package deadline

import "time"

// Deadline allows for commands to expire when they run too long
type Deadline struct {
	unixNano int64
	hit      bool
}

// New returns a new deadline object
func New(dl time.Time) *Deadline {
	return &Deadline{unixNano: dl.UnixNano()}
}

// Check the deadline and panic when reached
//
//go:noinline
func (dl *Deadline) Check() {
	if dl == nil || dl.unixNano == 0 {
		return
	}
	if !dl.hit && time.Now().UnixNano() > dl.unixNano {
		dl.hit = true
		panic("deadline")
	}
}

// Hit returns true if the deadline has been hit
func (dl *Deadline) Hit() bool {
	return dl.hit
}

// GetDeadlineTime returns the time object for the deadline, and an
// "empty" boolean
func (dl *Deadline) GetDeadlineTime() time.Time {
	return time.Unix(0, dl.unixNano)
}
