package utils

import (
	"sync"
	"time"
)

type TimeoutWaitGroup struct {
	wg      sync.WaitGroup
	mutex   sync.Mutex
	counter int
	done    chan struct{}
	closed  bool
}

func NewTimeoutWaitGroup() *TimeoutWaitGroup {
	return &TimeoutWaitGroup{
		done: make(chan struct{}),
	}
}

func (wg *TimeoutWaitGroup) Add(delta int) {
	wg.mutex.Lock()
	defer wg.mutex.Unlock()

	// check negative counter
	if wg.counter+delta < 0 {
		panic("sync: negative WaitGroup counter")
	}

	// recreate channel when new work is added
	if wg.counter == 0 && delta > 0 && wg.closed {
		wg.done = make(chan struct{})
		wg.closed = false
	}

	wg.counter += delta
	wg.wg.Add(delta)

	// signal done when counter reaches 0
	if wg.counter == 0 && !wg.closed {
		close(wg.done)
		wg.closed = true
	}
}

func (wg *TimeoutWaitGroup) Done() {
	wg.Add(-1) // reuse Add method
}

func (wg *TimeoutWaitGroup) Wait() {
	wg.wg.Wait()
}

func (wg *TimeoutWaitGroup) WaitTimeout(timeout time.Duration) bool {
	wg.mutex.Lock()
	if wg.counter == 0 {
		wg.mutex.Unlock()
		return true
	}
	done := wg.done
	wg.mutex.Unlock()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
