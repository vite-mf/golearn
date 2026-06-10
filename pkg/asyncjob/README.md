# asyncjob

In-memory asynchronous job manager demonstrating native Go concurrency:

- **Worker pool**: goroutines block on `sync.Cond` until jobs are available.
- **Bounded queue**: buffered **`bufferVacancies`** (semaphore) plus mutex-backed **`jobQueue`** so producers wait for capacity without racing `close` on a work channel.
- **Primitives**: `queueMu`, `queueCond`, `workerWaitGroup`, `submitShutdownCtx` / `jobExecutionCtx`, and channels.
- **Shutdown**: cancels **`jobExecutionCtx`** then **`submitShutdownCtx`**, sets **`isShutdown`**, broadcasts, then waits on **`workerWaitGroup`** until the queue drains.

## Usage

```go
import "golearn/pkg/asyncjob"

m := asyncjob.NewManager(4, 64)
defer func() { _ = m.Shutdown(context.Background()) }()

_ = m.Submit(context.Background(), func(ctx context.Context) error {
	// honour ctx cancellation when Shutdown runs
	return nil
})
```
