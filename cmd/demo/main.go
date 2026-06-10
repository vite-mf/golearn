// Command demo runs an end-to-end pipeline: producers publish to pkg/msgqueue;
// each message is handled by submitting work to pkg/asyncjob, then both
// components shut down cleanly.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"golearn/pkg/asyncjob"
	"golearn/pkg/msgqueue"
)

func main() {
	root := context.Background()

	const (
		nMsgs        = 12
		queueWorkers = 2
		queueBuf     = 4
		jobWorkers   = 3
		jobQueue     = 8
	)

	jobs := asyncjob.NewManager(jobWorkers, jobQueue)
	var asyncDone atomic.Int32
	var jobWG sync.WaitGroup

	// Queue handler: offload each message as an async job (do not use the
	// queue's shutdown context as Submit's wait context, or Submit may fail
	// while Close cancels that context).
	q := msgqueue.New(queueWorkers, queueBuf, func(_ context.Context, id int) error {
		jobWG.Add(1)
		err := jobs.Submit(context.Background(), func(execCtx context.Context) error {
			defer jobWG.Done()
			select {
			case <-time.After(15 * time.Millisecond):
			case <-execCtx.Done():
				return execCtx.Err()
			}
			asyncDone.Add(1)
			fmt.Printf("  async job finished for message id=%d\n", id)
			return nil
		})
		if err != nil {
			jobWG.Done()
		}
		return err
	})

	fmt.Println("1) Publishing messages to msgqueue…")
	for i := 1; i <= nMsgs; i++ {
		if err := q.Publish(root, i); err != nil {
			log.Fatalf("publish: %v", err)
		}
		fmt.Printf("  published id=%d\n", i)
	}

	fmt.Println("2) Closing msgqueue (drain handlers; each handler returned after its Submit)…")
	if err := q.Close(root); err != nil {
		log.Fatalf("queue close: %v", err)
	}

	// Handlers have returned, but asyncjob workers may still be running queued
	// jobs. Wait before Shutdown so jobExecutionCtx is not cancelled mid-flight.
	fmt.Println("2b) Waiting for all async jobs to finish…")
	jobWG.Wait()

	fmt.Println("3) Shutting down asyncjob (clean exit)…")
	if err := jobs.Shutdown(root); err != nil {
		log.Fatalf("manager shutdown: %v", err)
	}

	fmt.Printf("Done: %d messages through queue, %d async jobs completed.\n", nMsgs, asyncDone.Load())
	if int(asyncDone.Load()) != nMsgs {
		log.Fatalf("expected %d async jobs, got %d", nMsgs, asyncDone.Load())
	}
}
