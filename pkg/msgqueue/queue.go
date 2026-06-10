// Package msgqueue implements a bounded in-memory message queue using the
// producer–consumer pattern with a worker pool. Backpressure uses a token
// channel; buffering uses a mutex-backed slice and sync.Cond (no send/close
// races on the same channel).
package msgqueue

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrClosed is returned when Publish cannot proceed because Close started.
	ErrClosed = errors.New("msgqueue: queue closed")
)

// Handler processes one message. Returning an error does not stop the queue.
type Handler[T any] func(ctx context.Context, msg T) error

// Queue is a bounded MPMC queue with worker pool consumption.
type Queue[T any] struct {
	// bufferVacancies is a buffered channel counting free logical buffer slots:
	// receive one token before enqueueing; send one back after dequeue.
	bufferVacancies chan struct{}

	queueMu   sync.Mutex
	queueCond *sync.Cond // wake workers on new message, slot freed, or close

	pendingMessages []T
	handler         Handler[T]

	isClosed bool

	shutdownCtx    context.Context // passed to Handler; canceled on Close
	shutdownCancel context.CancelFunc

	workerWaitGroup sync.WaitGroup
}

// New creates a queue with workerCount consumers and a buffer of buf messages.
func New[T any](workerCount, buf int, h Handler[T]) *Queue[T] {
	// Initialize worker count
	if workerCount < 1 {
		workerCount = 1
	}
	// Initialize buffer size
	if buf < 1 {
		buf = 1
	}
	// Initialize shutdown context
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	// Initialize buffer vacancies
	// Backpressure mechanism
	bufferVacancies := make(chan struct{}, buf)
	for range buf {
		bufferVacancies <- struct{}{}
	}
	// Initialize queue
	q := &Queue[T]{
		bufferVacancies: bufferVacancies,
		handler:         h,
		shutdownCtx:     shutdownCtx,
		shutdownCancel:  shutdownCancel,
	}
	q.queueCond = sync.NewCond(&q.queueMu)
	for range workerCount {
		// Start worker
		q.workerWaitGroup.Add(1)
		// Goroutine to handle messages
		go q.worker()
	}
	return q
}

func (q *Queue[T]) worker() {
	defer q.workerWaitGroup.Done()
	for {
		// Dequeue message
		msg, ok := q.dequeue()
		if !ok {
			return
		}
		// Handle message
		_ = q.handler(q.shutdownCtx, msg)
	}
}

func (q *Queue[T]) dequeue() (T, bool) {
	q.queueMu.Lock()
	// Wait for new message or close
	defer q.queueMu.Unlock()
	var zero T
	for len(q.pendingMessages) == 0 && !q.isClosed {
		q.queueCond.Wait()
	}
	if len(q.pendingMessages) == 0 {
		return zero, false
	}
	msg := q.pendingMessages[0]
	// Shiftleft
	copy(q.pendingMessages, q.pendingMessages[1:])
	// Remove last element
	q.pendingMessages = q.pendingMessages[:len(q.pendingMessages)-1]

	// Return buffer vacancy
	q.bufferVacancies <- struct{}{}
	// Wake workers
	q.queueCond.Broadcast()
	return msg, true
}

// Publish delivers msg to the pool. waitCtx bounds waiting for buffer space.
func (q *Queue[T]) Publish(waitCtx context.Context, msg T) error {
	select {
	case <-q.bufferVacancies:
		// Wait 1 token from buffer vacancies
	case <-q.shutdownCtx.Done():
		// Return error if shutdown context is done
		return ErrClosed
	case <-waitCtx.Done():
		// Return error if wait context is done
		return waitCtx.Err()
	}

	// Lock queue
	q.queueMu.Lock()
	// Return error if queue is closed
	if q.isClosed {
		q.queueMu.Unlock()
		// Return buffer vacancy to avoid deadlock
		select {
		case q.bufferVacancies <- struct{}{}:
		default:
		}
		return ErrClosed
	}
	// Append message to pending messages
	q.pendingMessages = append(q.pendingMessages, msg)
	// Wake workers
	q.queueCond.Broadcast()
	// Unlock queue
	q.queueMu.Unlock()
	return nil
}

// Close cancels the handler context, marks the queue closed, drains pending
// messages, and waits for workers to exit. ctx bounds the wait.
func (q *Queue[T]) Close(ctx context.Context) error {
	q.shutdownCancel()

	q.queueMu.Lock()
	if q.isClosed {
		q.queueMu.Unlock()
		return nil
	}
	q.isClosed = true
	q.queueCond.Broadcast()
	q.queueMu.Unlock()

	done := make(chan struct{})
	go func() {
		q.workerWaitGroup.Wait()
		close(done)
	}()
	select {
	case <-done:
		// Return nil if workers are done
		return nil
	case <-ctx.Done():
		// Return error if wait context is done
		return ctx.Err()
	}
}
