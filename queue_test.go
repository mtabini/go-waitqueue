package waitqueue

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func returnError() error {
	return errors.New("Error")
}

func TestWaitQueue(t *testing.T) {
	q := New()

	if !q.Lock() {
		t.Fatal("Unable to lock an empty queue")
	}

	if q.Lock() {
		t.Fatal("Able to double-lock a queue")
	}

	q.Unlock()

	if q.ExecuteOrDefer(returnError) == nil {
		t.Fatal("Closures are being queued on an unlocked queue")
	}

	q.Lock()

	if q.ExecuteOrDefer(returnError) != nil {
		t.Fatal("Closures are not being queued on a locked queue")
	}
}

func TestUnlockedConcurrency(t *testing.T) {
	q := New()
	wg := sync.WaitGroup{}

	wg.Add(2)

	closure := func() {
		q.ExecuteOrDefer(func() error {
			wg.Done()
			return nil
		})
	}

	go closure()
	go closure()

	c := make(chan struct{})

	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("Closures are hanging on an unlocked queue")

	case <-c:
		// Good
	}
}

func TestLockedConcurrency(t *testing.T) {
	q := New()

	q.Lock()

	wg := sync.WaitGroup{}

	wg.Add(2)

	closure := func() {
		q.ExecuteOrDefer(func() error {
			wg.Done()
			return nil
		})
	}

	go closure()
	go closure()

	c := make(chan struct{})

	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-time.After(time.Second):

	case <-c:
		t.Fatal("Closures are running on a locked queue")
	}

	q.Unlock()

	select {
	case <-time.After(time.Second):
		t.Fatal("Closures are not running on after a queue has been unlocked")

	case <-c:
	}
}

// TestPrecedence tests that, when a queue is locked, it waits for all extant closures
// to finish running before allowing a lock to happen
func TestPrecedence(t *testing.T) {
	q := New()

	q.Lock()

	wg := sync.WaitGroup{}
	wg.Add(1)

	closureWg := sync.WaitGroup{}
	closureWg.Add(2)

	c := make(chan struct{}, 1)
	cc := make(chan struct{})
	ccc := make(chan struct{})

	// With the queue locked, we queue up two closures that wait
	// on an external waitgroup before sending data to `c`.
	//
	// We monitor whether these are running with the following closure,
	// which sends a signal on `cc` when both the closures have fired
	// on `c`.

	go func() {
		count := 0

		for {
			select {
			case <-c:
				count++
				if count >= 2 {
					cc <- struct{}{}
				}
			}
		}
	}()

	closure := func() {
		q.ExecuteOrDefer(func() error {
			closureWg.Done()

			wg.Wait()

			c <- struct{}{}

			return nil
		})
	}

	go closure()
	go closure()

	// Unlocking the queue should now cause the closures
	// to start executing. We use `closureWg` to synchronize
	// our main thread so that no more execution takes place
	// until the closures have started.

	q.Unlock()
	closureWg.Wait()

	// The closures are now running, but they are stuck waiting
	// for `wg` to clear. This should mean that we cannot acquire
	// a new lock until they are done executing.
	//
	// We fire off a new goroutine that simply tries to acquire
	// a lock and fires off a signal on `ccc`

	go func() {
		if q.Lock() == false {
			t.Fatal("Unable to lock the queue.")
		}

		defer q.Unlock()

		ccc <- struct{}{}
	}()

	// The closures are still waiting for `wg`, so we wait a second
	// to make sure that the lock doesn't take place.

	select {
	case <-time.After(time.Second):
		break // OK

	case <-cc:
		t.Fatal("Closures are ignoring a waitgroup.")

	case <-ccc:
		t.Fatal("A queue is not waiting for extant closures to complete before locking.")
	}

	// We now mark `wg` as unblocked, which should cause the closures to finish
	// executing, thus eventually allowing the queue to get locked again, and
	// our inner closure to start running.

	wg.Done()

	select {
	case <-time.After(time.Second):
		t.Fatal("A queue does not lock after extant closures have completed")

	case <-ccc:
		// OK
	}
}

func BenchmarkQueueRead(b *testing.B) {
	q := New()
	wg := sync.WaitGroup{}

	c1 := func() error {
		wg.Done()
		return nil
	}

	c := func() {
		q.ExecuteOrDefer(c1)
	}

	wg.Add(b.N)

	for i := 0; i < b.N; i++ {
		go c()
	}

	wg.Wait()
}

func BenchmarkUnqueuedRead(b *testing.B) {
	wg := sync.WaitGroup{}

	c := func() {
		wg.Done()
	}

	wg.Add(b.N)

	for i := 0; i < b.N; i++ {
		go c()
	}

	wg.Wait()
}

func BenchmarkLockedQueue(b *testing.B) {
	wg := sync.WaitGroup{}

	q := New()
	q.Lock()

	c1 := func() error {
		wg.Done()
		return nil
	}

	c := func() {
		q.ExecuteOrDefer(c1)
	}

	wg.Add(b.N)

	for i := 0; i < b.N; i++ {
		go c()
	}

	q.Unlock()

	wg.Wait()
}

func TestBoundedMutuallyExclusiveWaitQueue(t *testing.T) {
	q := NewBounded(2, "group 1", "group 2", "group 3", "group 4")

	if !q.Lock("group 1") {
		t.Fatal("Unable to lock empty queue")
	}

	if q.Lock("group 1") {
		t.Fatal("Able to double-lock queue")
	}

	if !q.Lock("group 2") {
		t.Fatal("Unable to lock queue of different name")
	}

	wg := sync.WaitGroup{}

	wg.Add(2)

	closure := func(group string) {
		q.ExecuteOrDefer(group, func() error {
			wg.Done()
			return nil
		})
	}

	go closure("group 1")
	go closure("group 2")

	c := make(chan struct{})

	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-time.After(time.Second):

	case <-c:
		t.Fatal("Closures are running on a locked queue")
	}

	q.Unlock("group 1")
	q.Unlock("group 2")

	select {
	case <-time.After(time.Second):
		t.Fatal("Closures are not running after a queue has been unlocked")

	case <-c:
	}

	quit := make(chan int)
	working := make(chan int)

	runTestFunc := func(group string) {
		q.Lock(group)
		defer q.Unlock(group)
		working <- 1
		<-quit
	}

	go runTestFunc("group 1")

	select {
	case <-time.After(time.Second):
		t.Fatal("group 1 did not start work")
	case <-working:
	}

	go runTestFunc("group 2")

	select {
	case <-time.After(time.Second):
		t.Fatal("Group 2 did not start work")
	case <-working:
	}

	go runTestFunc("group 3")

	select {
	case <-time.After(time.Second):

	case <-working:
		t.Fatal("Group 3 started work prematurely")
	}

	go runTestFunc("group 4")

	select {
	case <-time.After(time.Second):

	case <-working:
		t.Fatal("Group 4 started work prematurely")
	}

	quit <- 1

	select {
	case <-time.After(time.Second):
		t.Fatal("Group did not start work")
	case <-working:
	}

	select {
	case <-time.After(time.Second):

	case <-working:
		t.Fatal("Group started work prematurely")
	}

	quit <- 1

	select {
	case <-time.After(time.Second):
		t.Fatal("Group did not start work")
	case <-working:
	}
	close(working)
	close(quit)
}
