// Package asyncjob provides an in-memory asynchronous job manager built on
// goroutines, channels (bounded semaphore + cancellation), sync.Mutex,
// sync.Cond, and sync.WaitGroup. There is no send/close race because task
// storage is a mutex-backed slice; workers coordinate with sync.Cond.
package asyncjob

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrStopped is returned when Submit cannot proceed because shutdown started.
	ErrStopped = errors.New("asyncjob: manager stopped")
)

// JobFunc is executed by a worker. ctx is cancelled when Shutdown begins.
type JobFunc func(ctx context.Context) error

// Manager queues jobs in memory and executes them on a fixed-size worker pool.
type Manager struct {
	slots chan struct{} // tokens: available capacity in the queue

	mu  sync.Mutex
	cv  *sync.Cond
	q   []func()
	cap int

	stopped bool

	stopCtx    context.Context
	stopCancel context.CancelFunc
	execCtx    context.Context
	execCancel context.CancelFunc 

	wg sync.WaitGroup
}

// NewManager starts workerCount goroutines and a bounded queue of queueSize jobs.
func NewManager(workerCount, queueSize int) *Manager {
	if workerCount < 1 {
		workerCount = 1
	}
	if queueSize < 1 {
		queueSize = 1
	}
	stopCtx, stopCancel := context.WithCancel(context.Background())
	execCtx, execCancel := context.WithCancel(context.Background())

	slots := make(chan struct{}, queueSize)
	for range queueSize {
		slots <- struct{}{}
	}

	m := &Manager{
		slots:      slots,
		cap:        queueSize,
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
		execCtx:    execCtx,
		execCancel: execCancel,
	}
	m.cv = sync.NewCond(&m.mu)
	for range workerCount {
		m.wg.Add(1)
		go m.worker()
	}
	return m
}

func (m *Manager) worker() {
	defer m.wg.Done()
	for {
		job := m.dequeue()
		if job == nil {
			return
		}
		job()
	}
}

func (m *Manager) dequeue() func() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for len(m.q) == 0 && !m.stopped {
		m.cv.Wait()
	}
	if len(m.q) == 0 {
		return nil
	}
	fn := m.q[0]
	copy(m.q, m.q[1:])
	m.q = m.q[:len(m.q)-1]

	m.slots <- struct{}{}
	m.cv.Broadcast()
	return fn
}

// Submit enqueues job for asynchronous execution. waitCtx bounds waiting for
// a free queue slot (via the token channel).
func (m *Manager) Submit(waitCtx context.Context, job JobFunc) error {
	if job == nil {
		return errors.New("asyncjob: nil job")
	}
	select {
	case <-m.slots:
	case <-m.stopCtx.Done():
		return ErrStopped
	case <-waitCtx.Done():
		return waitCtx.Err()
	}

	wrapped := func() { _ = job(m.execCtx) }

	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		select {
		case m.slots <- struct{}{}:
		default:
		}
		return ErrStopped
	}
	m.q = append(m.q, wrapped)
	m.cv.Broadcast()
	m.mu.Unlock()
	return nil
}

// Shutdown cancels contexts, marks the manager stopped, wakes workers, and
// waits until the queue is drained and workers exit. ctx bounds the wait.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.execCancel()
	m.stopCancel()

	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return nil
	}
	m.stopped = true
	m.cv.Broadcast()
	m.mu.Unlock()

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
