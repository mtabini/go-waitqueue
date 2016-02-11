// Package waitqueue provides a mechanism to synchronize a master task with several related
// but unbounded subtasks such that:
//
// - When the queue is unlocked, an arbitrary number of subtasks can run concurrently without
// any delay
// - When the queue is locked, an arbitrary number of subtasks can be submitted without locking
// the calling thread, but will queue and executed only once the queue is later unlocked
// - When the queue is locked, a request to lock the queue again fails, returning immediately with
// a `false` Boolean value
// - When a request is made to lock the queue, all extant subtasks already being executed are
// allowed to finish before the lock is obtained.
package waitqueue

import (
	"sync"
)

// WaitQueue is a lockable work queue
type WaitQueue struct {
	lock   sync.RWMutex // Used to lock `locked`
	locked bool         // If true, the queue is locked `ExecuteOrDefer` defers the execution of closures
	work   sync.RWMutex // Used to lock extant goroutines while the queue is locked
}

// New returns a new WaitQueue ready for use.
func New() *WaitQueue {
	return &WaitQueue{}
}

// ExecuteOrDefer executes a subtask synchronously if the queue is unlocked, and returns its result. If
// the queue is locked, it instead schedules the subtask for asynchronous execution once the queue becomes
// unlocked, and returns immediately with a nil value.
func (w *WaitQueue) ExecuteOrDefer(closure func() error) error {
	// Acquire a read lock on `locked`. This ensures that:
	// (a) w.locked won't change under our feet, which means that a Lock operation
	//     won't start until we're done here.
	// (b) w.wg will be in a valid wait state after we're done. This could mean
	//     either that the queue is locked and we need to wait, or that the queue
	//     has been unlocked and we can move forward

	w.lock.RLock()
	defer w.lock.RUnlock()

	if w.locked {

		// If `locked` is true, the queue is locked. We spawn a goroutine
		// where we wait for the work lock to get unlocked.

		go func() {
			w.work.RLock()
			defer w.work.RUnlock()

			closure()
		}()

		return nil
	}

	// If execution reaches this line, the queue is unlocked and
	// we can run our closure

	return closure()
}

// Lock locks the queue, waiting first for any extant subtasks to complete. No more
// subtasks are allowed to run until a subsequent call to Unlock()
//
// If the queue is already locked, Lock() returns immediately with `false`. Otherwise
// it returns `true` once it has acquired a lock.
//
// This function must not be called from inside a subtask!
func (w *WaitQueue) Lock() bool {
	w.lock.Lock()
	defer w.lock.Unlock()

	// If the queue is already locked, we simply return false,
	// indicating to the caller that it cannot acquire a lock.

	if w.locked {
		return false
	}

	// Otherwise, we proceed to lock the queue by locking the
	// internal mutex for writing.

	w.locked = true
	w.work.Lock()

	return true
}

// Unlock unlocks the queue. All subtasks that have accumulated during the lock
// are allowed to start running again.
func (w *WaitQueue) Unlock() {
	w.lock.Lock()
	defer w.lock.Unlock()

	// If the queue is locked, we unlock it and signal to extant
	// goroutines that it's safe to start working again.

	if w.locked {
		w.locked = false
		w.work.Unlock()
	}
}
