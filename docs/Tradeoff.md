# Tradeoffs

Analyzing the tradeoffs we have handled across this project is exactly what converts this codebase from a neat hobby script into a masterclass. In distributed systems, there is no such thing as a "perfect" architecture—every feature we wrote was a deliberate choice to favor one guarantee while sacrificing something else.

Here is the breakdown of the major tradeoffs we managed as we evolved your distributed task queue from a simple transport loop into a robust control plan

---

## 1. Delivery Guarantees: At-Least-Once vs. At-Most-Once

* **The Shift:** We moved from a simple `BRPOP` (which deletes the task immediately upon extraction) to `BLMove` (which atomically shifts the task to an `:inflight` isolation list).
* **The Tradeoff:** We traded **system simplicity and storage overhead** to gain **fault tolerance**.
* *At-Most-Once (`BRPOP`):* High performance and requires zero cleanup tracking. However, if a worker crashes mid-task, that message vanishes forever.
* *At-Least-Once (`BLMove`):* Guarantees that no message is ever lost. If a worker dies, the task remains safe in the `:inflight` list. The catch? We had to introduce a background Janitor to sweep abandoned tasks, adding network traffic and extra storage key

---

## 2. Processing Strategy: Pushing Logic to the Broker vs. Client Roundtrips

* **The Shift:** We refactored `PolledDelayedTasks` from a sequential Go `ZRangeArgs` loop paired with client-side fallback `ZRem` operations into a server-side atomic **Redis Lua Script**.
* **The Tradeoff:** We traded **Broker CPU utilization** to gain **strict transactional atomicity and zero network overhead**.
* *Go Client Loop:* Spreads the computational work across your stateless Go worker nodes, keeping Redis free to just serve bytes. However, it requires multiple network roundtrips, opening up the "Lost Task" Catch-22 vulnerability window if a network partition occurs mid-flight.
* *Redis Lua Script:* Forces Redis to handle the entire look-and-move iteration inside its own single-threaded engine. It blocks all other execution lines for a microsecond but guarantees that a task is completely promoted or completely rolled back—meaning absolute safety against data loss or duplicate polling races.

---

## 3. Distributed Coordination: Centralized Time vs. Local Machine Clocks

* **The Shift:** We stopped using Go’s native `time.Now().Unix()` inside the worker nodes and began querying the centralized Redis cluster using `s.rdb.Time(ctx)`.
* **The Tradeoff:** We traded **execution latency (adding an extra network ping per job)** to eliminate the risk of **Distributed Clock Skew**.
* *Local Clock (`time.Now()`):* Instantaneous execution (sub-nanosecond CPU register read). But if Worker A's system clock drifts even 3 seconds ahead of the Janitor’s host clock, your visibility timeout thresholds crumble.
* *Central Clock (`rdb.Time()`):* Adds a minor network hop before recording a timestamp. However, it ensures every worker in your fleet evaluates expiration boundaries against the exact same source of truth.

---

## 4. System Isolation: Microservices Architecture vs. Shared Libraries

* **The Shift:** We split the monolith into decoupled binaries: `cmd/api-server/main.go` and `cmd/worker-pool/main.go`.
* **The Tradeoff:** We traded **deployment ease and code simplicity** to achieve **horizontal scalability and decoupled fault isolation**.
* *Monolith:* Simple to spin up (`go run main.go`), runs in a single process, and doesn't require configuring multi-service orchestration. However, if your workers face an OOM (Out of Memory) crash due to processing a massive video file, your API control plane goes down with it.
* *Decoupled Control/Data Planes:* The API server handles incoming user traffic effortlessly on port 8080 and only needs minimal memory. You can scale your worker pools independently from 1 node to 100 nodes across various cloud instances depending entirely on the queue depth.

---

## 5. Architectural Memory: Database Consistency vs. Memory Speed

* **The Shift:** We dropped the simulated in-memory Go maps inside our concurrency loop and introduced an atomic Postgres unique row-constraint via `TryStartJob()`.
* **The Tradeoff:** We traded **raw microsecond memory throughput** to guarantee **strong data durability and distributed idempotency**.
* *In-Memory Maps:* High throughput, but state is localized to a single machine. If multiple workers are running on separate servers, they cannot communicate execution counters, leading to duplicate processing.
* *PostgreSQL Repository Logging:* Imposes disk I/O and transactional locking latency onto your hot execution path. However, it creates a persistent, un-wipeable audit trail of your DAG states, which serves as a shield against double-processing hazards when Redis needs to redeliver a task during network hiccups.

---
