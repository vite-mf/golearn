package asyncjob

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestManager_SubmitShutdown(t *testing.T) {
	t.Parallel()
	var ran atomic.Int32
	m := NewManager(2, 4)
	for range 10 {
		require.NoError(t, m.Submit(context.Background(), func(context.Context) error {
			ran.Add(1)
			return nil
		}))
	}
	require.NoError(t, m.Shutdown(context.Background()))
	require.Equal(t, int32(10), ran.Load())
}

func TestManager_ShutdownCancelsExecContext(t *testing.T) {
	t.Parallel()
	m := NewManager(1, 2)
	var sawCancel atomic.Bool
	done := make(chan struct{})
	require.NoError(t, m.Submit(context.Background(), func(ctx context.Context) error {
		defer close(done)
		<-ctx.Done()
		sawCancel.Store(true)
		return ctx.Err()
	}))
	require.NoError(t, m.Shutdown(context.Background()))
	<-done
	require.True(t, sawCancel.Load())
}

func TestManager_SubmitAfterShutdown(t *testing.T) {
	t.Parallel()
	m := NewManager(1, 1)
	require.NoError(t, m.Shutdown(context.Background()))
	require.ErrorIs(t, m.Submit(context.Background(), func(context.Context) error { return nil }), ErrStopped)
}

func TestManager_ConcurrentSubmitShutdown(t *testing.T) {
	t.Parallel()
	m := NewManager(4, 32)
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.Submit(context.Background(), func(ctx context.Context) error {
				time.Sleep(time.Microsecond * 10)
				return nil
			})
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		_ = m.Shutdown(context.Background())
	}()
	wg.Wait()
	require.NoError(t, m.Shutdown(context.Background()))
}

func TestManager_NilJob(t *testing.T) {
	t.Parallel()
	m := NewManager(1, 1)
	t.Cleanup(func() { _ = m.Shutdown(context.Background()) })
	require.Error(t, m.Submit(context.Background(), nil))
}
