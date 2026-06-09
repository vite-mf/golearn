# msgqueue

Bounded in-memory message queue combining **producer–consumer** flow with a **worker pool**:

- Producers acquire a slot from a token channel, then append under `sync.Mutex` and `sync.Cond` broadcast wakes workers.
- Workers dequeue messages, return the slot token, and invoke the shared `Handler`.
- `Close` cancels `stopCtx` passed to handlers, marks the queue closed, broadcasts, and waits on `sync.WaitGroup`.

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
