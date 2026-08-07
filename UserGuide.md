# PyBgWorker — Unified User & Production Guide

A lightweight background task worker for Python applications using SQLite as the queue backend. PyBgWorker enables background job execution, scheduling, retries, monitoring, and production-safe deployment without requiring Redis or RabbitMQ.

---

## 1. Introduction

PyBgWorker allows applications to:

- Run tasks in the background
- Schedule jobs (cron + countdown/ETA)
- Retry failed tasks automatically (with optional exception filtering)
- Cancel queued, retrying, or running tasks
- Limit execution rate (global and per-task)
- Execute priority jobs first
- Monitor job status and progress
- Fetch task results
- Inspect worker state
- Manage queues via CLI
- Recover safely from crashes
- Run multiple workers safely

It is suitable for email sending, image and file processing, API synchronisation, scheduled operations, batch processing, and background web backend jobs — all with zero external dependencies.

---

## 2. Core Features

- ✅ Background job execution
- ✅ Task scheduling (countdown & ETA)
- ✅ Automatic retry with exception-type filtering (`retry_for`)
- ✅ Task cancellation (queued, retrying, and running tasks)
- ✅ Rate limiting — start rate, not concurrency (global + per-task override)
- ✅ Task priority execution
- ✅ Task status tracking
- ✅ Result storage & retrieval
- ✅ Task progress reporting (`set_progress` / `AsyncResult.progress`)
- ✅ Bulk / batch enqueue (`delay_many`)
- ✅ Success/failure callbacks (`on_success`, `on_failure`)
- ✅ Idempotency keys for duplicate-safe enqueue
- ✅ Worker statistics & monitoring
- ✅ Machine-readable JSON output (`inspect --json`, `stats --json`)
- ✅ Queue inspection tools
- ✅ Failed job inspection
- ✅ Queue purging utilities
- ✅ Crash recovery & task requeue
- ✅ Multi-worker safe execution
- ✅ Pluggable storage backend (BaseQueue / BaseBackend)

---

## 3. Installation & Upgrade

### Install from PyPI

```bash
pip install pybgworker
```

### Upgrade to Latest Version

```bash
pip install --upgrade pybgworker
```

### Verify Installation

```bash
python -m pybgworker.cli --help
```

---

## 4. Core Concepts

### Task

A Python function decorated with `@task(name=...)` that is executed in the background by a worker.

### Worker

A long-running process that continuously fetches tasks from the queue and executes them in isolated subprocesses.

### Queue

The SQLite database that stores all task rows and worker heartbeat records.

### AsyncResult

The handle returned by `.delay()`.  Use it to check status, retrieve results, monitor progress, and clean up finished tasks.

---

## 5. Quick Start

### Step 1 — Define tasks (`tasks.py`)

```python
from pybgworker import task
import time

@task(name="tasks.add")
def add(x, y):
    time.sleep(2)
    return x + y
```

### Step 2 — Run worker

```bash
python -m pybgworker.cli run --app tasks
# or with concurrency:
python -m pybgworker.cli run --app tasks --concurrency 4
```

### Step 3 — Enqueue a task

```python
from tasks import add

res = add.delay(5, 7)
print("Task ID:", res.task_id)
```

### Step 4 — Check result

```python
print(res.ready())   # False while running, True once terminal
print(res.status)    # "queued" | "running" | "success" | …
print(res.result)    # 12  (once success)
```

---

## 6. Defining Tasks

```python
from pybgworker import task

@task(name="tasks.hello")
def hello(name):
    return f"Hello {name}"

# Enqueue
hello.delay("Prabhat")
```

### Full `@task` parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | str | **required** | Unique task name. Must be stable across deployments. |
| `retries` | int | `3` | Max retry attempts after first failure. |
| `retry_delay` | float | `0` | Base delay in seconds between retries. |
| `retry_backoff` | bool | `False` | Enable exponential backoff. |
| `retry_backoff_factor` | float | `2` | Backoff multiplier per attempt. |
| `retry_max_delay` | float | `None` | Cap on retry delay in seconds. |
| `retry_jitter` | float | `0.0` | Randomise delay. `<=1` = fraction of delay; `>1` = absolute seconds. |
| `retry_for` | tuple | `(Exception,)` | Only retry for these exception types (inheritance respected). |
| `timeout` | float | `None` | Per-task timeout in seconds. Falls back to global 150 s. |
| `rate_limit` | float | `None` | Max starts per second for this task. Overrides global. |
| `on_success` | callable | `None` | Called after success: `fn(task_id)`. |
| `on_failure` | callable | `None` | Called after permanent failure: `fn(task_id, error)`. |

---

## 7. Task Scheduling

### Delay Execution (Countdown)

```python
add.delay(10, 2, countdown=10)   # runs 10 seconds from now
```

### Run at Specific Time (ETA)

```python
from datetime import datetime, timedelta, timezone

eta_time = datetime.now(timezone.utc) + timedelta(minutes=1)
add.delay(5, 6, eta=eta_time)
```

ETA should use a timezone-aware datetime.  The worker must be running at execution time.

### Cron (Recurring)

```python
from pybgworker.scheduler import cron
from pybgworker import task

@task(name="tasks.report")
@cron("0 9 * * *")   # every day at 09:00 UTC
def daily_report():
    generate_report()
```

Cron tasks are automatically enqueued by the scheduler thread that runs inside the worker.

---

## 8. Task Retries

```python
@task(name="tasks.api_call", retries=3, retry_delay=5)
def api_call():
    raise Exception("Temporary failure")
```

Retries occur automatically with a delay between attempts.

### Backoff, Jitter, and Exception Filtering

```python
@task(
    name="tasks.api_call",
    retries=5,
    retry_delay=2,
    retry_backoff=True,
    retry_backoff_factor=2,
    retry_max_delay=60,
    retry_jitter=0.2,
    retry_for=(TimeoutError, ConnectionError),  # skip retries for logic errors
)
def api_call():
    raise TimeoutError("network timeout")
```

#### How `retry_for` works

`retry_for` is a tuple of exception types.  When a task fails, the worker checks whether the raised exception is an instance of any of those types (using full Python inheritance, across the subprocess boundary).  If the exception does **not** match, the task goes straight to **dead** — no retry.

```python
# Only retry transient network errors:
retry_for=(TimeoutError, ConnectionError)

# Retry on anything (default):
retry_for=(Exception,)

# Only retry a custom exception:
retry_for=(MyTransientError,)
```

#### Retry option reference

| Option | Effect |
|---|---|
| `retry_backoff=True` | Multiplies `retry_delay` by `retry_backoff_factor` each attempt |
| `retry_backoff_factor=2` | Doubles delay each attempt (default) |
| `retry_max_delay=60` | Caps delay at 60 s regardless of backoff |
| `retry_jitter=0.2` | Adds ±20% random noise to the computed delay |

---

## 9. Task Timeout

```python
@task(name="tasks.heavy", timeout=30)
def heavy():
    ...
```

If the task exceeds `timeout` seconds, the worker terminates it and either retries (if attempts remain) or marks it **dead**.  The global default is **150 seconds** when `timeout` is not set.

---

## 10. Rate Limiting

### Global limit (default 5/s)

Configured by the `RATE_LIMIT` constant in `config.py`.

### Per-task override

```python
@task(name="tasks.api", rate_limit=2)
def api_call():
    ...
```

`rate_limit=2` means at most **2 starts per second** for this specific task, overriding the global 5/s limit.

> **Important:** `rate_limit` controls the *start rate* — how quickly new task executions are launched per second.  It does **not** limit how many tasks run concurrently.  For concurrency control use `PYBGWORKER_CONCURRENCY` (or `--concurrency`).

---

## 11. Task Priority

```python
add.delay(10, 2, priority=1)   # high priority
add.delay(10, 2, priority=9)   # low priority
```

Lower value = higher priority.  Default is `5`.  The worker always picks the highest-priority eligible task first.

---

## 12. Task Status & Results (AsyncResult API)

```python
res = add.delay(3, 4)
```

### Properties (not methods — no parentheses)

```python
res.status    # "queued" | "running" | "success" | "failed" | "dead" | "cancelled"
res.result    # deserialized return value when success, else None
res.error     # traceback string when failed/dead, else None
res.progress  # {"percent": int, "message": str|None} or None
```

> ⚠️ `res.status`, `res.result`, `res.error`, and `res.progress` are **properties**, not methods.
> Write `res.status`, **not** `res.status()`.

### Boolean helpers

```python
res.ready()       # True if in any terminal state (success/failed/dead/cancelled)
res.successful()  # True only if "success"
res.failed()      # True only if "failed"
res.dead()        # True only if "dead"  (all retries exhausted)
res.cancelled()   # True only if "cancelled"
```

### Blocking get

```python
# Block until done, return the result:
result = res.get()

# Block with timeout:
result = res.get(timeout=30)   # raises TimeoutError after 30 s
```

`get()` raises `TaskFailedError` for failed/dead tasks and `TaskCancelledError` for cancelled tasks.  Both carry `task_id` and `state` attributes.

### Cleanup

```python
res.forget()   # deletes the task row from the DB entirely
```

Useful for one-off or sensitive results that should not wait for the scheduled retention cleanup.

### Full example

```python
res = add.delay(3, 4)

if res.successful():
    print("Answer:", res.result)
elif res.dead():
    print("Gave up after retries:", res.error)
elif res.failed():
    print("Failed:", res.error)
elif res.cancelled():
    print("Was cancelled")

# Or just block:
try:
    answer = res.get(timeout=10)
    print("Answer:", answer)
except TimeoutError:
    print("Still running after 10 s")
except TaskFailedError as e:
    print(f"Failed [{e.state}]:", e)
```

---

## 13. Task Progress Reporting

Report progress from inside a long-running task:

```python
from pybgworker import task, set_progress

@task(name="tasks.process_file")
def process_file(path):
    chunks = list(read_chunks(path))
    total = len(chunks)
    for i, chunk in enumerate(chunks):
        process(chunk)
        set_progress(int((i + 1) / total * 100), f"chunk {i+1}/{total}")
    return "done"
```

Poll from the caller:

```python
import time

res = process_file.delay("/data/big.csv")
while not res.ready():
    p = res.progress
    if p:
        print(f"{p['percent']}% complete — {p['message']}")
    time.sleep(0.5)

print(res.result)
```

`set_progress()` is a no-op when called outside a worker (e.g. in tests), so no test code needs to change.

---

## 14. Bulk / Batch Enqueue

Enqueue many tasks in a single database transaction — much faster than a loop:

```python
# Slow — one connection + INSERT per call:
for user in users:
    send_email.delay(user.email)

# Fast — one transaction for all INSERTs:
results = send_email.delay_many([
    ((user.email,), {}) for user in users
])
# results is a list[AsyncResult] in input order
```

`delay_many` accepts an iterable of `(args_tuple, kwargs_dict)` pairs and an optional `priority` keyword argument that applies to all tasks in the batch.

---

## 15. Success / Failure Callbacks

```python
def on_done(task_id):
    print(f"Task {task_id} succeeded")

def on_error(task_id, error):
    send_slack_alert(f"Task {task_id} failed permanently:\n{error[:200]}")

@task(
    name="tasks.critical_job",
    on_success=on_done,
    on_failure=on_error,
)
def critical_job():
    ...
```

- `on_success(task_id)` — invoked by the worker after a successful terminal state.
- `on_failure(task_id, error)` — invoked when the task reaches `dead` or `failed`.

Callback exceptions are caught and logged; they never crash the worker.

---

## 16. Idempotency Keys

Prevent duplicate task rows when a producer retries an enqueue call:

```python
res = send_email.delay(
    "alice@example.com",
    idempotency_key="welcome-email-user-42",
)

# If called again with the same key, the existing AsyncResult is returned:
res2 = send_email.delay(
    "alice@example.com",
    idempotency_key="welcome-email-user-42",
)

assert res.task_id == res2.task_id   # True — no duplicate row created
```

The idempotency key is stored in the database with a unique index.  The deduplication check happens atomically inside `enqueue()`.

---

## 17. Queue & Job Management Commands

### Task States

| State | Meaning |
|---|---|
| `queued` | Waiting to be picked up by a worker |
| `running` | Currently executing in a subprocess |
| `retrying` | Failed and scheduled for retry |
| `success` | Completed successfully |
| `failed` | Permanently failed (no retries left or retry not eligible) |
| `dead` | Exhausted all retries — inspect `last_error` |
| `cancelled` | Cancelled before or during execution |

### Failed vs Dead

- `failed`: task failed but retry was not applicable (no retries configured, or exception type not in `retry_for`).
- `dead`: task exhausted all configured retry attempts.

Use `pybgworker failed` to see both, or `pybgworker dead` for dead-only.

### Cancel Task

```bash
python -m pybgworker.cli cancel <task_id>
```

Cancels tasks in `queued`, `retrying`, or `running` state.

> Running tasks are marked `cancelled` in the database immediately.
> The subprocess is terminated on the next worker poll cycle (up to ~100 ms delay).

### Retry Failed Task

```bash
python -m pybgworker.cli retry <task_id>
```

Re-queues a task that is in `failed` or `dead` state, resetting its `attempt` counter.

### View Failed/Dead Tasks

```bash
python -m pybgworker.cli failed   # failed + dead
python -m pybgworker.cli dead     # dead only
```

### Inspect Queue

```bash
python -m pybgworker.cli inspect
python -m pybgworker.cli inspect --json   # machine-readable JSON
```

Shows task counts by status and worker health.

### Worker Stats

```bash
python -m pybgworker.cli stats
python -m pybgworker.cli stats --json   # machine-readable JSON
```

Shows worker liveness and queue depth.

### Purge Queue

```bash
python -m pybgworker.cli purge
```

Deletes all `queued` and `retrying` tasks.

---

## 18. Environment Variables

All `PYBGWORKER_*` variables are read at import time from the process environment.

| Variable | Default | Description |
|---|---|---|
| `PYBGWORKER_DB` | `pybgworker.db` | SQLite file path. Set this to isolate projects or queues on the same machine. |
| `PYBGWORKER_WORKER_NAME` | `worker-1` | Unique name for this worker. Shown in `inspect`/`stats` and logs. Set per-instance when running multiple workers. |
| `PYBGWORKER_CONCURRENCY` | `1` | Tasks to run in parallel per worker process. |
| `PYBGWORKER_POLL_INTERVAL` | `1.0` | Seconds to sleep between queue polls when idle. Trade responsiveness vs CPU/DB load. |
| `PYBGWORKER_WORKER_TIMEOUT` | `15` | Seconds after which a lock held by a non-heartbeating worker is reclaimed. |
| `PYBGWORKER_RETENTION_DAYS` | `0` | Days to keep finished tasks. `0` disables cleanup. |
| `PYBGWORKER_CLEANUP_INTERVAL_HOURS` | `24` | Hours between cleanup runs (when retention is enabled). |

### Running two independent queues on one machine

```bash
PYBGWORKER_DB=queue_a.db  PYBGWORKER_WORKER_NAME=worker-a \
    python -m pybgworker.cli run --app tasks_a

PYBGWORKER_DB=queue_b.db  PYBGWORKER_WORKER_NAME=worker-b \
    python -m pybgworker.cli run --app tasks_b
```

---

## 19. Worker Lifecycle

### Start Worker

```bash
python -m pybgworker.cli run --app tasks
```

### Graceful Shutdown (Ctrl+C once)

1. Worker stops fetching new tasks
2. Running task(s) finish normally
3. Worker exits safely

### Force Shutdown (Ctrl+C twice)

1. All running tasks are marked `cancelled`
2. Subprocesses are terminated
3. Worker exits immediately

---

## 20. Worker Internals

1. Poll queue with an atomic `UPDATE … RETURNING` that locks the task
2. Start a subprocess (`_run_task_with_id`) which sets `PYBGWORKER_CURRENT_TASK_ID`
3. Execute the task function; report progress via `set_progress()` if desired
4. Collect result/error from the subprocess result queue
5. Store result / apply `retry_for` filter / reschedule or mark dead
6. Fire `on_success` or `on_failure` callbacks

Timeout protection is applied on every loop iteration.

---

## 21. Worker Crash Recovery

If a worker crashes without updating the heartbeat:

1. Task locks expire (after `PYBGWORKER_WORKER_TIMEOUT` seconds)
2. Tasks are automatically reclaimed by another worker
3. No manual recovery required

---

## 22. Running Multiple Workers

```bash
# Terminal 1
PYBGWORKER_WORKER_NAME=worker-1 python -m pybgworker.cli run --app tasks

# Terminal 2
PYBGWORKER_WORKER_NAME=worker-2 python -m pybgworker.cli run --app tasks
```

SQLite WAL locking distributes tasks safely.  Always set a unique `PYBGWORKER_WORKER_NAME` per instance so they appear distinctly in `inspect`/`stats`.

---

## 23. Database Cleanup

```bash
python -m pybgworker.cli run --app tasks --retention-days 30
```

Or:

```bash
PYBGWORKER_RETENTION_DAYS=30 python -m pybgworker.cli run --app tasks
```

Optional cleanup interval:

```bash
python -m pybgworker.cli run --app tasks --cleanup-interval-hours 12
```

---

## 24. Direct SQLite Access (Advanced)

Because everything is stored in a plain SQLite database, you can query it
directly for custom monitoring dashboards, audit logs, or admin tooling without
going through the CLI:

```sql
-- Worker health
SELECT name, last_seen FROM workers;

-- Queue depth by status
SELECT status, COUNT(*) FROM tasks GROUP BY status;

-- Recent permanent failures
SELECT id, name, last_error, finished_at
FROM tasks
WHERE status IN ('failed', 'dead')
ORDER BY updated_at DESC
LIMIT 20;

-- Tasks currently running
SELECT id, name, locked_by, locked_at FROM tasks WHERE status = 'running';
```

The `workers` table has two columns: `name` (primary key) and `last_seen` (ISO timestamp updated every 5 s by the heartbeat thread).  Workers not seen within `PYBGWORKER_WORKER_TIMEOUT` seconds are considered dead.

---

## 25. Extending PyBgWorker

`queue.py` and `backends.py` define abstract base classes that are the
**official extension points** for custom storage backends:

```python
# queue.py
class BaseQueue(ABC):
    def enqueue(self, task: dict): ...
    def fetch_next(self, worker_name: str): ...
    def ack(self, task_id: str): ...
    def fail(self, task_id: str, error: str): ...
    def reschedule(self, task_id: str, run_at): ...

# backends.py
class BaseBackend(ABC):
    def get_task(self, task_id): ...
    def store_result(self, task_id, result): ...
    def forget(self, task_id): ...
```

Implement both classes and pass them to `SQLiteQueue`/`SQLiteBackend`-accepting
constructors to swap out the storage layer.  The roadmap includes a first-party
`RedisQueue` / `RedisBackend` pair.

---

## 26. Production Deployment Tips

- Run workers via `systemd`, `Supervisor`, or Docker with `restart=always`
- Set a unique `PYBGWORKER_WORKER_NAME` per container/process
- Set `PYBGWORKER_DB` to a shared path accessible by all workers
- Enable retention cleanup (`PYBGWORKER_RETENTION_DAYS`) to prevent unbounded DB growth
- Monitor worker health via `pybgworker stats --json` fed to your alerting pipeline
- Use `retry_for` to distinguish transient from logic errors

---

## 27. FastAPI Integration Example

```python
from fastapi import FastAPI
from tasks import add

app = FastAPI()

@app.get("/add")
def add_numbers(a: int, b: int):
    res = add.delay(a, b)
    return {"task_id": res.task_id}

@app.get("/result/{task_id}")
def get_result(task_id: str):
    from pybgworker.result import AsyncResult
    res = AsyncResult(task_id)
    return {
        "status": res.status,
        "result": res.result,
        "error": res.error,
        "progress": res.progress,
    }
```

---

## 28. Best Practices

- Use unique, stable task names (dotted module-style strings)
- Keep tasks small and independent
- Set `retry_for` to limit retries to recoverable errors
- Use `on_failure` callbacks for critical alerting
- Use `idempotency_key` when producers may retry enqueue calls
- Apply `rate_limit` for tasks that call external APIs
- Use `delay_many` for bulk workloads (campaigns, batch updates)
- Set `PYBGWORKER_WORKER_NAME` distinctly per worker instance

---

## 29. Conclusion

PyBgWorker gives Python apps background execution, job scheduling, retry handling, task tracking, progress reporting, and zero external dependencies — all in a single, auditable SQLite file.
