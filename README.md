# PyBgWorker

A lightweight, production-ready background task library for Python.

PyBgWorker provides a durable SQLite-backed task queue, scheduling (cron and
countdown/ETA), rate limiting, retries, and structured observability without
external infrastructure.

It is designed to be simple, reliable, and easy to deploy.

---

## Features

- Persistent SQLite task queue
- Multi-worker safe execution
- Task scheduling: cron + countdown/ETA
- Retry + failure handling with `retry_for` exception filtering
- Task cancellation (queued, running, and retrying tasks)
- Crash isolation via subprocess
- Automatic stale task reaping for dead workers
- Task priority execution
- Task status tracking
- Result storage and retrieval
- Task progress reporting (`set_progress` / `AsyncResult.progress`)
- Bulk / batch enqueue (`delay_many`)
- Success/failure callbacks (`on_success`, `on_failure`)
- Idempotency keys for safe duplicate enqueue
- Worker statistics and monitoring
- JSON structured logging
- Task duration tracking
- Rate limiting (per-task and global; limits start rate, not concurrency)
- Heartbeat monitoring
- Configurable single-worker concurrency
- Machine-readable JSON output for `inspect` and `stats`
- CLI tools: inspect, retry, failed, dead, purge, cancel, stats
- Production-safe worker loop
- Pluggable storage backend (BaseQueue / BaseBackend interfaces)

---

## Installation

```bash
pip install pybgworker
```

---

## Basic Usage

### Define a task

```python
from pybgworker.task import task

@task(name="add")
def add(a, b):
    return a + b
```

### Enqueue a task

```python
res = add.delay(1, 2)
print(res.status)   # "queued" / "running" / "success" …
print(res.result)   # 3  (once success)
print(res.error)    # None (or traceback string on failure)
```

---

## Run worker

```bash
python -m pybgworker.cli run --app example
```

---

## Worker Concurrency

Run multiple tasks in parallel within a single worker process:

```bash
PYBGWORKER_CONCURRENCY=4 python -m pybgworker.cli run --app example
```

Or with a CLI flag:

```bash
python -m pybgworker.cli run --app example --concurrency 4
```

Defaults to `1` for backward-compatible behavior.

---

## Cron Scheduler

Run recurring tasks:

```python
from pybgworker.scheduler import cron
from pybgworker.task import task

@task(name="heartbeat_task")
@cron("*/1 * * * *")
def heartbeat():
    print("alive")
```

Cron runs automatically inside the worker.

---

## Retry with Exception Filtering

Restrict automatic retries to specific exception types:

```python
@task(
    name="api_call",
    retries=5,
    retry_delay=2,
    retry_backoff=True,
    retry_backoff_factor=2,
    retry_max_delay=60,
    retry_jitter=0.2,
    retry_for=(TimeoutError, ConnectionError),   # only retry transient errors
)
def api_call():
    ...
```

- `retry_for`: tuple of exception types to retry on (inheritance respected).
  Default `(Exception,)` retries on any non-`BaseException` error.
  A `ValueError` (logic error) would go straight to **dead** without retrying.
- `retry_backoff`: enable exponential backoff
- `retry_backoff_factor`: multiplier per attempt (default `2`)
- `retry_max_delay`: cap delay in seconds
- `retry_jitter`: randomize delay (ratio `<=1` or seconds if `>1`)

---

## Rate Limiting

### Global rate limit

Set in `config.py` (default **5 tasks/second**):

```python
RATE_LIMIT = 5  # tasks per second
```

### Per-task override

```python
@task(name="tasks.heavy_api", rate_limit=2)
def heavy_api():
    ...
```

`rate_limit=2` means this task is started at most **2 times per second**,
regardless of the global setting. The task-level value takes precedence.

> **Important:** `rate_limit` controls *start rate* — how quickly new task
> executions are launched. It does **not** limit how many run concurrently.
> For concurrency control use `PYBGWORKER_CONCURRENCY`.

---

## Task Cancellation

Cancel any task that is `queued`, `retrying`, or `running`:

```bash
python -m pybgworker.cli cancel <task_id>
```

Or programmatically:

```python
from pybgworker.sqlite_queue import SQLiteQueue
SQLiteQueue().cancel(task_id)
```

> Running tasks are marked `cancelled` in the database immediately.
> The subprocess is only terminated on the next worker poll cycle.

---

## Task Progress Reporting

Report progress from inside a long-running task:

```python
from pybgworker import task, set_progress

@task(name="tasks.process_file")
def process_file(path):
    chunks = list(read_chunks(path))
    for i, chunk in enumerate(chunks):
        process(chunk)
        set_progress(int((i + 1) / len(chunks) * 100), f"chunk {i+1}/{len(chunks)}")
```

Poll progress from the caller:

```python
res = process_file.delay("/data/big.csv")
while not res.ready():
    p = res.progress
    if p:
        print(f"{p['percent']}% — {p['message']}")
    time.sleep(0.5)
```

---

## Bulk / Batch Enqueue

Enqueue thousands of tasks in a single database transaction:

```python
# Instead of:
for user in users:
    send_email.delay(user.email)

# Use (much faster — one transaction):
results = send_email.delay_many([
    ((user.email,), {}) for user in users
])
```

`delay_many` returns a list of `AsyncResult` objects in input order.

---

## Success / Failure Callbacks

```python
def alert_on_failure(task_id, error):
    send_slack_message(f"Task {task_id} failed: {error[:200]}")

@task(
    name="tasks.critical_job",
    on_success=lambda task_id: print(f"done: {task_id}"),
    on_failure=alert_on_failure,
)
def critical_job():
    ...
```

- `on_success(task_id)` — called after a successful completion.
- `on_failure(task_id, error)` — called when a task reaches `dead` or `failed`.

Callback exceptions are caught and logged; they never crash the worker.

---

## Idempotency Keys

Prevent duplicate task rows when a producer retries an enqueue call:

```python
res = send_email.delay(
    "alice@example.com",
    idempotency_key="welcome-email-user-42",
)
# Calling again with the same key returns the original AsyncResult:
res2 = send_email.delay(
    "alice@example.com",
    idempotency_key="welcome-email-user-42",
)
assert res.task_id == res2.task_id  # True — same row
```

---

## AsyncResult API

```python
res = my_task.delay(...)

res.status        # "queued" | "running" | "success" | "failed" | "dead" | "cancelled"
res.result        # return value (deserialized) when status == "success", else None
res.error         # traceback/error string when failed/dead, else None
res.progress      # {"percent": int, "message": str|None} or None

res.ready()       # True if in any terminal state
res.successful()  # True only if "success"
res.failed()      # True only if "failed"
res.dead()        # True only if "dead"
res.cancelled()   # True only if "cancelled"

res.get()         # blocks until done; returns result or raises TaskFailedError (includes exception_class)
res.get(timeout=30)  # raises TimeoutError after 30 s

res.forget()      # deletes the task row from the database entirely
```

---

## JSON Logging

All worker events are structured JSON:

```json
{"event":"task_start","task_id":"..."}
{"event":"task_success","duration":0.12}
```

---

## Machine-Readable CLI Output

```bash
python -m pybgworker.cli inspect --json
python -m pybgworker.cli stats   --json
```

Emits a JSON object instead of formatted text — useful for monitoring
pipelines or custom dashboards.

---

## CLI Commands

```bash
python -m pybgworker.cli inspect          # queue status + worker health
python -m pybgworker.cli inspect --json   # same, as JSON
python -m pybgworker.cli stats            # worker stats + queue depth
python -m pybgworker.cli stats   --json   # same, as JSON
python -m pybgworker.cli retry   <id>     # re-queue a failed/dead task
python -m pybgworker.cli cancel  <id>     # cancel a queued/retrying/running task
python -m pybgworker.cli purge            # delete all queued tasks
python -m pybgworker.cli failed           # list failed + dead tasks
python -m pybgworker.cli dead             # list dead tasks only
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PYBGWORKER_DB` | `pybgworker.db` | SQLite file path. Change to run multiple projects/queues on one machine. |
| `PYBGWORKER_WORKER_NAME` | `worker-1` | Unique name for this worker instance. Shown in `inspect`/`stats` output and logs. |
| `PYBGWORKER_CONCURRENCY` | `1` | Number of tasks to run in parallel per worker process. |
| `PYBGWORKER_POLL_INTERVAL` | `1.0` | Seconds the worker sleeps between queue polls when idle. Lower = more responsive, higher CPU/DB load. |
| `PYBGWORKER_WORKER_TIMEOUT` | `15` | Seconds before a lock held by a missing worker is considered stale and reclaimed. |
| `PYBGWORKER_RETENTION_DAYS` | `0` | Days to keep finished tasks. `0` disables automatic cleanup. |
| `PYBGWORKER_CLEANUP_INTERVAL_HOURS` | `24` | Hours between cleanup runs (when retention is enabled). |

Example — running two independent queues on one machine:

```bash
PYBGWORKER_DB=queue_a.db PYBGWORKER_WORKER_NAME=worker-a python -m pybgworker.cli run --app tasks_a
PYBGWORKER_DB=queue_b.db PYBGWORKER_WORKER_NAME=worker-b python -m pybgworker.cli run --app tasks_b
```

---

## Database Cleanup

Enable automatic retention cleanup for completed tasks:

```bash
python -m pybgworker.cli run --app example --retention-days 30
```

Environment variable alternative:

```bash
PYBGWORKER_RETENTION_DAYS=30 python -m pybgworker.cli run --app example
```

Optional cleanup interval (hours, default 24):

```bash
python -m pybgworker.cli run --app example --cleanup-interval-hours 12
```

When enabled, PyBgWorker prunes finished tasks older than the retention window and runs a `VACUUM` after deletions.

---

## Direct SQLite Access (Advanced)

Because all state lives in plain SQLite you can query the database directly
for custom monitoring dashboards or audit scripts without going through the
CLI:

```sql
-- Live worker health
SELECT name, last_seen FROM workers;

-- Queue depth by status
SELECT status, COUNT(*) FROM tasks GROUP BY status;

-- Recent failures
SELECT id, name, last_error FROM tasks WHERE status IN ('failed','dead') ORDER BY updated_at DESC LIMIT 20;
```

---

## Extending PyBgWorker

`queue.py` and `backends.py` define abstract base classes (`BaseQueue` and
`BaseBackend`) that `SQLiteQueue` and `SQLiteBackend` implement.  These are
the intended extension points for custom storage backends:

```python
from pybgworker.queue import BaseQueue
from pybgworker.backends import BaseBackend

class MyRedisQueue(BaseQueue):
    def enqueue(self, task): ...
    def fetch_next(self, worker_name): ...
    def ack(self, task_id): ...
    def fail(self, task_id, error): ...
    def reschedule(self, task_id, run_at): ...
```

---

## Failed vs Dead

- `failed`: a task failed but may still be retried (or was manually marked failed).
- `dead`: a task exhausted all retries and was moved to a terminal state for inspection.

Use `pybgworker failed` to see both failed + dead, or `pybgworker dead` for dead-only.

---

## Design Goals

- zero external dependencies
- SQLite durability
- safe multiprocessing
- operator-friendly CLI
- production observability
- infrastructure protection

---

## Roadmap

Planned but not yet included:

- Named queues + routing (dedicate workers to fast vs slow task types)
- Pluggable Redis backend
- Cluster coordination / leader election for scheduler
- Cron catch-up / misfire policy
- Workflow chaining (chain / group API)
- Asyncio coroutine task support
- Metrics endpoint and health checks
- Simple read-only web dashboard
- Multi-tenancy / namespacing

---

## Feedback and Issues

For feedback, enhancement requests, or error reports, please use this form:

[Submit feedback / report an issue](https://forms.gle/bUFRximzAGN6bCBQA)

Copy/paste link:
```text
https://forms.gle/bUFRximzAGN6bCBQA
```

---

## License

MIT License
