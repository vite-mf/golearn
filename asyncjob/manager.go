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
	// bufferVacancies counts free logical queue slots (semaphore as channel).
	bufferVacancies chan struct{}

	queueMu   sync.Mutex
	queueCond *sync.Cond // wake workers on new job, slot freed, or shutdown
	jobQueue  []func()

	isShutdown bool

	// submitShutdownCtx is canceled during Shutdown so Submit unblocks waiting for a slot.
	submitShutdownCtx    context.Context
	submitShutdownCancel context.CancelFunc
	// jobExecutionCtx is passed into each JobFunc; canceled first in Shutdown.
	jobExecutionCtx    context.Context
	jobExecutionCancel context.CancelFunc

	workerWaitGroup sync.WaitGroup
}

// NewManager starts workerCount goroutines and a bounded queue of queueSize jobs.
func NewManager(workerCount, queueSize int) *Manager {
	if workerCount < 1 {
		workerCount = 1
	}
	if queueSize < 1 {
		queueSize = 1
	}
	// Initialize shutdown context
	// Initialize job execution context
	submitShutdownCtx, submitShutdownCancel := context.WithCancel(context.Background())
	jobExecutionCtx, jobExecutionCancel := context.WithCancel(context.Background())

	// Initialize buffer vacancies
	// Backpressure mechanism
	bufferVacancies := make(chan struct{}, queueSize)
	// Fill buffer vacancies
	for range queueSize {
		bufferVacancies <- struct{}{}
	}

	m := &Manager{
		bufferVacancies:      bufferVacancies,
		submitShutdownCtx:    submitShutdownCtx,
		submitShutdownCancel: submitShutdownCancel,
		jobExecutionCtx:      jobExecutionCtx,
		jobExecutionCancel:   jobExecutionCancel,
	}
	// Initialize queue condition
	m.queueCond = sync.NewCond(&m.queueMu)
	for range workerCount {
		// Add 1 to worker wait group
		m.workerWaitGroup.Add(1)
		// Goroutine to handle jobs
		go m.worker()
	}
	return m
}

func (m *Manager) worker() {
	defer m.workerWaitGroup.Done()
	for {
		job := m.dequeue()
		if job == nil {
			return
		}
		job()
	}
}

func (m *Manager) dequeue() func() {
	m.queueMu.Lock()
	defer m.queueMu.Unlock()
	for len(m.jobQueue) == 0 && !m.isShutdown {
		m.queueCond.Wait()
	}
	if len(m.jobQueue) == 0 {
		return nil
	}
	fn := m.jobQueue[0]
	// Shiftleft
	copy(m.jobQueue, m.jobQueue[1:])
	// Remove last element
	m.jobQueue = m.jobQueue[:len(m.jobQueue)-1]

	m.bufferVacancies <- struct{}{}
	m.queueCond.Broadcast()
	return fn
}

// Submit enqueues job for asynchronous execution. waitCtx bounds waiting for
// a free queue slot (via the token channel).
func (m *Manager) Submit(waitCtx context.Context, job JobFunc) error {
	if job == nil {
		return errors.New("asyncjob: nil job")
	}
	select {
	case <-m.bufferVacancies:
	case <-m.submitShutdownCtx.Done():
		return ErrStopped
	case <-waitCtx.Done():
		return waitCtx.Err()
	}

	runJob := func() { _ = job(m.jobExecutionCtx) }

	m.queueMu.Lock()
	if m.isShutdown {
		m.queueMu.Unlock()
		select {
		case m.bufferVacancies <- struct{}{}:
		default:
		}
		return ErrStopped
	}
	m.jobQueue = append(m.jobQueue, runJob)
	m.queueCond.Broadcast()
	m.queueMu.Unlock()
	return nil
}

// Shutdown cancels contexts, marks the manager stopped, wakes workers, and
// waits until the queue is drained and workers exit. ctx bounds the wait.
func (m *Manager) Shutdown(ctx context.Context) error {
	// Cancel job execution context
	m.jobExecutionCancel()
	// Cancel submit shutdown context
	m.submitShutdownCancel()

	m.queueMu.Lock()
	if m.isShutdown {
		m.queueMu.Unlock()
		return nil
	}
	m.isShutdown = true
	m.queueCond.Broadcast()
	m.queueMu.Unlock()

	done := make(chan struct{})
	go func() {
		m.workerWaitGroup.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
