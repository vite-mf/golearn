package msgqueue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueue_PublishClose(t *testing.T) {
	t.Parallel()
	var sum atomic.Int64
	q := New(3, 4, func(ctx context.Context, n int) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		sum.Add(int64(n))
		return nil
	})
	for i := range 20 {
		require.NoError(t, q.Publish(context.Background(), i))
	}
	require.NoError(t, q.Close(context.Background()))
	require.Greater(t, sum.Load(), int64(0))
}

func TestQueue_PublishAfterClose(t *testing.T) {
	t.Parallel()
	q := New(1, 1, func(context.Context, int) error { return nil })
	require.NoError(t, q.Close(context.Background()))
	require.ErrorIs(t, q.Publish(context.Background(), 1), ErrClosed)
}

func TestQueue_ConcurrentPublishClose(t *testing.T) {
	t.Parallel()
	var processed atomic.Int32
	q := New(4, 32, func(ctx context.Context, n int) error {
		time.Sleep(time.Microsecond * 5)
		processed.Add(1)
		return nil
	})
	var wg sync.WaitGroup
	for range 80 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.Publish(context.Background(), 1)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		_ = q.Close(context.Background())
	}()
	wg.Wait()
	require.NoError(t, q.Close(context.Background()))
}
