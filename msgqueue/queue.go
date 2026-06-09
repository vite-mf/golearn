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
	slots chan struct{}

	mu sync.Mutex
	cv *sync.Cond

	msgs []T
	h    Handler[T]

	closed bool

	stopCtx    context.Context
	stopCancel context.CancelFunc

	wg sync.WaitGroup
}

// New creates a queue with workerCount consumers and a buffer of buf messages.
func New[T any](workerCount, buf int, h Handler[T]) *Queue[T] {
	if workerCount < 1 {
		workerCount = 1
	}
	if buf < 1 {
		buf = 1
	}
	stopCtx, stopCancel := context.WithCancel(context.Background())
	slots := make(chan struct{}, buf)
	for range buf {
		slots <- struct{}{}
	}
	q := &Queue[T]{
		slots:      slots,
		h:          h,
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
	}
	q.cv = sync.NewCond(&q.mu)
	for range workerCount {
		q.wg.Add(1)
		go q.worker()
	}
	return q
}

func (q *Queue[T]) worker() {
	defer q.wg.Done()
	for {
		msg, ok := q.dequeue()
		if !ok {
			return
		}
		_ = q.h(q.stopCtx, msg)
	}
}

func (q *Queue[T]) dequeue() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var zero T
	for len(q.msgs) == 0 && !q.closed {
		q.cv.Wait()
	}
	if len(q.msgs) == 0 {
		return zero, false
	}
	msg := q.msgs[0]
	copy(q.msgs, q.msgs[1:])
	q.msgs = q.msgs[:len(q.msgs)-1]

	q.slots <- struct{}{}
	q.cv.Broadcast()
	return msg, true
}

// Publish delivers msg to the pool. waitCtx bounds waiting for buffer space.
func (q *Queue[T]) Publish(waitCtx context.Context, msg T) error {
	select {
	case <-q.slots:
	case <-q.stopCtx.Done():
		return ErrClosed
	case <-waitCtx.Done():
		return waitCtx.Err()
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		select {
		case q.slots <- struct{}{}:
		default:
		}
		return ErrClosed
	}
	q.msgs = append(q.msgs, msg)
	q.cv.Broadcast()
	q.mu.Unlock()
	return nil
}

// Close cancels the handler context, marks the queue closed, drains pending
// messages, and waits for workers to exit. ctx bounds the wait.
func (q *Queue[T]) Close(ctx context.Context) error {
	q.stopCancel()

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	q.cv.Broadcast()
	q.mu.Unlock()

	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
