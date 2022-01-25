package txn

import (
	"sync"
	"sync/atomic"
	"time"
)

const maxReaders = 1 << 30

type Scheduler struct {
	readDeadline int64  // Deadline for all reads to complete prior to scheduling a write, atomic access required
	interrupted  uint32 // Whether any readers were interrupted to enter write phase, atomic access required

	readerCount       int32 // Number of currently executing readers, atomic access required
	pausedReaderCount int32 // Number of readers paused waiting for writers
	writerCount       int32 // Number of currently executing or waiting writers
	writeDeadline     int64 // Deadline for writes to complete prior to allowing reads
	writing           bool  // Write phase currently in progress

	mu         sync.Mutex // Mutex guarding readerCount, writerCount, readDeadline, writeDeadline, writing
	endWrites  *sync.Cond // Broadcasts when the write phase is complete (writing transitions from true to false)
	writerDone *sync.Cond // Signals when a write completes and other writes are pending
	noReaders  *sync.Cond // Signals when no readers are executing

	writeDelay time.Duration // Maximum time we will wait for ability enter a write phase - after this time we will interrupt scanners
	readDelay  time.Duration // Maximum time we will spend on writes after we enter a write phase
}

func NewScheduler(initialWriteDelay time.Duration, maxReadDelay time.Duration) *Scheduler {
	s := &Scheduler{
		writeDelay: initialWriteDelay,
		readDelay:  maxReadDelay,
	}
	s.endWrites = sync.NewCond(&s.mu)
	s.writerDone = sync.NewCond(&s.mu)
	s.noReaders = sync.NewCond(&s.mu)
	return s
}

func (s *Scheduler) Write() (done func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writerCount > 0 {
		// If there's already a writer running, no need to signal readers,
		// go down the slow path
		return s.execWriteSlow()
	}

	// Signal readers a write is pending and slow path should be used
	nReaders := atomic.AddInt32(&s.readerCount, -maxReaders) + maxReaders - s.pausedReaderCount
	if nReaders > 0 {
		// If we need to wait for readers to complete, use slow path
		return s.execWriteSlow()
	}

	// Uncontended fast path - no readers or writers running
	s.writing = true
	s.writerCount++
	return s.completeWrite
}

func (s *Scheduler) Read() (done func()) {
	nReaders := atomic.AddInt32(&s.readerCount, 1)
	if nReaders < 0 { // < 0 means a write is pending
		return s.execReadSlow()
	}

	// Uncontended read path
	return s.completeRead
}

func (s *Scheduler) Scan() (done func(), status *Status) {
	ts := Status{
		s: s,
	}

	nReaders := atomic.AddInt32(&s.readerCount, 1)
	if nReaders < 0 { // < 0 means a write is pending
		return s.execReadSlow(), &ts
	}

	// Set signalInterruptedMask since we're guaranteed to get the full time
	// slice here
	ts.status = signalInterruptedMask

	// Uncontended read path
	return s.completeRead, &ts
}

func (s *Scheduler) execWriteSlow() (done func()) {
	// One thread is always responsible for attempting to initiate writes and
	// then signalling subsequent writers to run
	writeInitiator := false

	if s.writerCount == 0 {
		// No other writes pending

		// set a deadline to end scans at
		atomic.StoreInt64(&s.readDeadline, time.Now().Add(s.writeDelay).UnixNano())

		// take responsibility for initiating the write phase
		writeInitiator = true
	}
	s.writerCount++

	for {
		if writeInitiator {
			// this goroutine is responsible for initiating the write phase

			// First, wait for all readers to complete. The read deadline we set above should
			// ensure that long running scanners stop in bounded time.
			readers := atomic.LoadInt32(&s.readerCount) + maxReaders - s.pausedReaderCount
			for readers > 0 {
				s.noReaders.Wait()
				readers = atomic.LoadInt32(&s.readerCount) + maxReaders - s.pausedReaderCount
			}

			// Set the write deadline and mark that we're now writing
			s.writeDeadline = time.Now().Add(s.readDelay).UnixNano()
			s.writing = true

			// Adjust the max write delay interval based on whether interruptions happened
			// If we had to interrupt, double max write delay, if not, shrink by 25%, subject
			// to reasonable limits
			if atomic.CompareAndSwapUint32(&s.interrupted, 1, 0) {
				if s.writeDelay < 1*time.Minute {
					s.writeDelay *= 2
				}
			} else {
				if s.writeDelay > 1*time.Microsecond {
					s.writeDelay = s.writeDelay / 4 * 3
				}
			}
			break
		} else {
			// this goroutine should write after the initiator completes

			// First, wait for current writer to complete
			s.writerDone.Wait()

			// check if we've been in the write phase for too long - if so, let readers run again
			// and retry the wait
			now := time.Now().UnixNano()
			if s.writeDeadline < now && s.pausedReaderCount > 0 {
				// since writes are paused here, this goroutine becomes responsible to reinitiate the write phase
				s.writing = false
				atomic.StoreInt64(&s.readDeadline, time.Now().Add(s.writeDelay).UnixNano())
				writeInitiator = true
				s.endWrites.Broadcast()
				continue
			}
			break
		}
	}

	return s.completeWrite
}

func (s *Scheduler) execReadSlow() (done func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// While write phase is in progress, wait for completion
	for s.writing {
		s.endWrites.Wait()
	}

	// Writes are not active, but perhaps we need to pause due to the write deadline
	now := time.Now().UnixNano()
	for s.writerCount > 0 && s.readDeadline <= now {
		s.pausedReaderCount++
		readers := atomic.LoadInt32(&s.readerCount) + maxReaders - s.pausedReaderCount
		if readers == 0 {
			s.noReaders.Signal()
		}
		s.endWrites.Wait()
		s.pausedReaderCount--
		now = time.Now().UnixNano()
	}

	return s.completeRead
}

func (s *Scheduler) completeRead() {
	r := atomic.AddInt32(&s.readerCount, -1)
	if r < 0 {
		s.completeReadSlow()
		return
	}
}

func (s *Scheduler) completeReadSlow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	readers := atomic.LoadInt32(&s.readerCount) + maxReaders - s.pausedReaderCount
	if readers == 0 {
		s.noReaders.Signal()
	}
}

func (s *Scheduler) completeWrite() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writerCount--
	if s.writerCount == 0 {
		s.writing = false
		atomic.AddInt32(&s.readerCount, maxReaders)
		atomic.StoreInt64(&s.readDeadline, 0)
		s.endWrites.Broadcast()
	} else {
		s.writerDone.Signal()
	}
}

func (s *Scheduler) notifyInterrupted() {
	atomic.CompareAndSwapUint32(&s.interrupted, 0, 1)
}
