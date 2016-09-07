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

// BoundedMutuallyExclusiveWaitQueue represents a work queue that allows a bounded number of tasks to execute
// concurrently, where each task can be assigned to its own sequential queue.
type BoundedMutuallyExclusiveWaitQueue struct {
	sem    chan int
	queues map[string]*WaitQueue
}

// NewBounded returns a new BoundedMutuallyExclusiveWaitQueue ready for use.
// `size` is a positive integer indicating the maximum number of queues that can
// be locked concurrently.
// `queueName` is the name of the queue to manage.
// `additionalQueueNames` are the names of any other queues to manage.
func NewBounded(size int, queueName string, additionalQueueNames ...string) *BoundedMutuallyExclusiveWaitQueue {
	queues := map[string]*WaitQueue{}
	queues[queueName] = New()

	for _, additionalQueueName := range additionalQueueNames {
		queues[additionalQueueName] = New()
	}

	return &BoundedMutuallyExclusiveWaitQueue{
		sem: make(chan int, size),
		queues: queues,
	}
}

// Lock obtains an exclusive lock on the queue identified by `name`. If the lock
// can be obtained, Lock checks that there are sufficient resources available
// (See BoundedMutuallyExclusiveWaitQueue.NewBounded for details on setting global
// resource constraints), blocking in the case that there are not.
// With the exception of this behavior, Lock functions the same as WaitQueue.Lock.
func (w *BoundedMutuallyExclusiveWaitQueue) Lock(name string) bool {
	locked := w.queues[name].Lock()

	// If we obtain the lock, we block until there are sufficient resources to
	// perform work.
	if locked {
		w.sem <- 1
	}

	return locked
}

// Unlock unlocks the queue identified by `name`. See WaitQueue.Unlock for further details.
func (w *BoundedMutuallyExclusiveWaitQueue) Unlock(name string) {
	w.queues[name].Unlock()
	<-w.sem
}

// ExecuteOrDefer executes `closure` synchronously if queue `name` is unlocked,
// and returns nil on success, or `closure`'s error information otherwise.
// If queue `name` is locked, ExecuteOrDefer instead schedules `closure` for asynchronous execution
// once queue `name` becomes unlocked, and returns immediately with ``nil``.
func (w *BoundedMutuallyExclusiveWaitQueue) ExecuteOrDefer(name string, closure func() error) error {
	return w.queues[name].ExecuteOrDefer(closure)
}
