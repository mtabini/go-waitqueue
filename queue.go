package waitqueue

import (
	"sync"
)

type WaitQueue struct {
	lock   sync.RWMutex // Used to lock `locked`
	locked bool         // If true, the queue is locked `ExecuteOrDefer` defers the execution of closures
	work   sync.RWMutex // Used to lock extant goroutines while the queue is locked
}

func New() *WaitQueue {
	return &WaitQueue{}
}

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
