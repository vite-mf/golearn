# Technical Design Document: Bounded In-Memory Message Queue

**Pattern:** Multiple-Producer Multiple-Consumer (MPMC) Worker Pool

## 1. Core Philosophy & Design Goals

The `msgqueue` package implements an in-memory asynchronous job manager using the **Producer-Consumer pattern**. Instead of spawning unbounded Goroutines for every task (which risks memory exhaustion), it orchestrates a **fixed-size pool of persistent workers** interacting with a **bounded queue**.

The architecture solves four major concurrency challenges in multi-threaded Go: **Backpressure**, **Thread Safety**, **CPU Efficiency**, and **Graceful Shutdown**.

---

## 2. The Four Architectural Pillars

### Ⅰ. Backpressure Control (The Token Semaphore)

* **Mechanism:** `bufferVacancies chan struct{}` (Buffered Channel)
* **Philosophy:** To prevent Out-Of-Memory (OOM) crashes under heavy load, the queue size must be capped. Go's buffered channels are leveraged as a *counting semaphore*.
* **Behavior:** A producer must acquire a token (`<-bufferVacancies`) *before* enqueueing. A worker returns a token (`bufferVacancies <- struct{}{}`) *after* dequeueing. If the queue is full, producers block cleanly at the gateway.

### Ⅱ. Thread-Safe Storage (Mutex-Backed Slice)

* **Mechanism:** `pendingMessages []T` + `sync.Mutex`
* **Philosophy:** While Go channels can act as queues, closing a channel with active producers causes a `panic` (send on closed channel).
* **Decision:** To eliminate this race condition entirely, tasks are stored in a standard Go **slice** protected by a **Mutex lock**. The channel is only used for capacity tracking, never for direct data transfer.

### Ⅲ. CPU Optimization (Conditional Park/Wake)

* **Mechanism:** `sync.Cond`
* **Philosophy:** When the queue is empty, workers should not spin endlessly in a loop wasting CPU cycles (busy-waiting).
* **Behavior:** Workers are put to sleep via `q.queueCond.Wait()`. When a new message arrives or the queue closes, `q.queueCond.Broadcast()` triggers a hardware-level wake-up signal to the sleeping workers.

### Ⅳ. Graceful Shutdown & Lifecycle Management

* **Mechanism:** `context.Context` + `sync.WaitGroup`
* **Philosophy:** Shutting down must be non-destructive. Active jobs must finish, pending jobs must be drained, and no goroutines should be left leaking in memory.
* **Behavior:** `Close()` broadcasts a shutdown signal, safely flips an `isClosed` flag to reject new inputs, and utilizes a `sync.WaitGroup` to track worker termination, bound by a caller-supplied timeout context.

---

## 3. Component Walkthrough (Talking Points for Mentor Alignment)

### A. Initialization (`New`)

1. Generates a root `shutdownCtx` to govern long-running task cancellations.
2. Fills the `bufferVacancies` channel to maximum capacity with empty structs.
3. Spawns $N$ persistent worker goroutines nguyen-ban (background threads) and registers them into the `workerWaitGroup`.

### B. The Producer Journey (`Publish`)

```
[Incoming Task] -> [Check Token Availability & Context Timeout] -> [Lock Mutex] -> [Append to Slice] -> [Broadcast Wakeup] -> [Unlock]

```

* **Context Awareness:** The producer uses a `select` block to race between slot availability (`bufferVacancies`), global shutdown, and its own execution timeout (`waitCtx`).
* **Double-Check Lock:** If a thread wins a buffer slot but the queue closes a millisecond later, the lock detects it, safely returns the token to the channel (`Non-blocking send`), and rejects the job with `ErrClosed`.

### C. The Consumer Journey (`worker` & `dequeue`)

* **The Worker Loop:** Every worker runs an infinite `for` loop executing `dequeue()`. If `dequeue` returns `ok == false`, the worker breaks the loop, triggers `defer workerWaitGroup.Done()`, and terminates.
* **The Dequeue Logic:** 1. Acquires the Mutex.
2. If the slice is empty and the queue is open, it releases the lock and falls asleep via `Wait()`.
3. Upon waking, it performs a FIFO extraction (`copy` to shift slice elements).
4. Returns a slot token back to `bufferVacancies` and broadcasts the state change.

### D. The Janitor (`Close`)

1. Triggers `shutdownCancel()`, cascading a stop signal to all actively running handlers.
2. Flips `isClosed = true` under a Mutex lock and wakes up all idle workers so they can read the flag and exit.
3. Spawns a supervisor goroutine to watch `workerWaitGroup.Wait()`.
4. Uses a `select` block to allow a clean exit once all workers leave, or forces a shutdown if the operator's grace-period context expires.

---

## 4. Conceptual Cheat Sheet (Node.js vs. Go Mentality)

| Architectural Goal | Node.js Approach | Go `msgqueue` Approach |
| --- | --- | --- |
| **Data Safety** | Single-threaded Event Loop (Naturally safe) | `sync.Mutex` (Explicit memory locking) |
| **Throttling Load** | Stream Backpressure / Manual Array checks | Bounded Channel Semaphore (`bufferVacancies`) |
| **Thread Sleeping** | Event-driven callbacks / Async-Await | `sync.Cond` (Low-level thread coordination) |
| **Task Tracking** | `Promise.all()` | `sync.WaitGroup` |
| **Forced Abort** | `AbortController` | `context.Context` |