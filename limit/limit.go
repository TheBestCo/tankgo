package limit

import "sync"

// ConcurrentLimit interface.
type ConcurrentLimit interface {
	Request()
	Done()
}

// NewConcurrentLimit constructor.
func NewConcurrentLimit(num int) *ConcurrentLimitImpl {
	return &ConcurrentLimitImpl{
		ch: make(chan struct{}, num),
		wg: &sync.WaitGroup{},
	}
}

// ConcurrentLimitImpl struct.
type ConcurrentLimitImpl struct {
	ch chan struct{}
	wg *sync.WaitGroup
}

// Request function blocks until a ticket is available.
func (l *ConcurrentLimitImpl) Request() {
	l.ch <- struct{}{}
	l.wg.Add(1)
}

// TryRequest function.
func (l *ConcurrentLimitImpl) TryRequest() bool {
	select {
	case l.ch <- struct{}{}:
		l.wg.Add(1)
		return true
	default:
		return false
	}
}

// Done function resolves the requested ticket.
func (l *ConcurrentLimitImpl) Done() {
	select {
	case <-l.ch:
		l.wg.Done()
	default:
		// channel was empty
	}
}

// Wait waits for every ticket to be resolved.
func (l *ConcurrentLimitImpl) Wait() {
	l.wg.Wait()
}
