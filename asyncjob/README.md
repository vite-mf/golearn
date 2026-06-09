# asyncjob

In-memory asynchronous job manager demonstrating native Go concurrency:

- **Worker pool**: goroutines block on `sync.Cond` until jobs are available.
- **Bounded queue**: a buffered **token channel** (`slots`) couples with a mutex-backed slice so producers wait for capacity without racing `close` on the work channel.
- **Primitives**: `sync.Mutex`, `sync.Cond`, `sync.WaitGroup`, `context.Context`, and channels.
- **Shutdown**: cancels execution and stop contexts, marks stopped, broadcasts waiters, then waits for workers to drain the queue.

## Usage

```go
m := asyncjob.NewManager(4, 64)
defer func() { _ = m.Shutdown(context.Background()) }()

_ = m.Submit(context.Background(), func(ctx context.Context) error {
	// honour ctx cancellation when Shutdown runs
	return nil
})
```
