# msgqueue

Bounded in-memory message queue combining **producer–consumer** flow with a **worker pool**:

- Producers take a token from `bufferVacancies`, then append under `queueMu`; `queueCond.Broadcast` wakes workers.
- Workers dequeue from `pendingMessages`, return a vacancy token, and invoke `handler` with `shutdownCtx`.
- `Close` cancels `shutdownCtx` passed to handlers, sets `isClosed`, broadcasts, and waits on `workerWaitGroup`.

## Usage

```go
var sum atomic.Int64
q := msgqueue.New(8, 1024, func(ctx context.Context, n int) error {
	sum.Add(int64(n))
	return nil
})
defer func() { _ = q.Close(context.Background()) }()

_ = q.Publish(context.Background(), 1)
```
