# golearn

Small Go learning experiments.

- [`pkg/msgqueue/`](pkg/msgqueue/) — bounded in-memory queue with a worker pool (`sync.Cond`, backpressure via token channel). See [pkg/msgqueue/README.md](pkg/msgqueue/README.md).
- [`pkg/asyncjob/`](pkg/asyncjob/) — in-memory job manager with worker pool and bounded queue. See [pkg/asyncjob/README.md](pkg/asyncjob/README.md).
- Run the full pipeline (queue → async jobs): `go run ./cmd/demo`
