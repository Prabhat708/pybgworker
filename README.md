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
- Retry + failure handling
- Task cancellation support
- Crash isolation via subprocess
- Task priority execution
- Task status tracking
- Result storage and retrieval
- Worker statistics and monitoring
- JSON structured logging
- Task duration tracking
- Rate limiting (overload protection)
- Heartbeat monitoring
- Configurable single-worker concurrency
- CLI tools: inspect, retry, failed, purge, cancel, stats
- Production-safe worker loop

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
add.delay(1, 2)
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

## JSON Logging

All worker events are structured JSON:

```json
{"event":"task_start","task_id":"..."}
{"event":"task_success","duration":0.12}
```

This enables:

- monitoring
- analytics
- alerting
- observability pipelines

---

## Rate Limiting

Protect infrastructure from overload:

```python
RATE_LIMIT = 5  # tasks per second
```

Ensures predictable execution under heavy load.

---

## Retry Backoff + Jitter

Enable exponential backoff and jitter to spread retry load:

```python
from pybgworker.task import task

@task(
    name="api_call",
    retries=5,
    retry_delay=2,
    retry_backoff=True,
    retry_backoff_factor=2,
    retry_max_delay=60,
    retry_jitter=0.2,
)
def api_call():
    ...
```

- `retry_backoff`: enable exponential backoff
- `retry_backoff_factor`: multiplier per attempt (default `2`)
- `retry_max_delay`: cap delay in seconds
- `retry_jitter`: randomize delay (ratio `<=1` or seconds if `>1`)


---

## CLI Commands

Inspect queue:

```bash
python -m pybgworker.cli inspect
```

Retry failed task:

```bash
python -m pybgworker.cli retry <task_id>
```

Cancel task:

```bash
python -m pybgworker.cli cancel <task_id>
```

Purge queued tasks:

```bash
python -m pybgworker.cli purge
```

---

## Observability

PyBgWorker logs:

- worker start
- cron events
- task start
- success
- retry
- failure
- timeout
- crash
- heartbeat errors

All machine-readable.

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

- Retry backoff + jitter policies
- Dead-letter queue for exhausted retries
- Task/result TTL and automatic DB cleanup
- Multiple named queues + routing
- Pluggable backends (Redis first)
- Cluster coordination / leader election for scheduler
- Metrics endpoint and health checks
- Dashboard API + web UI
- Workflow pipelines / DAGs

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
