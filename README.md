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

- Single-worker concurrency (process pool)
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

## License

MIT License
