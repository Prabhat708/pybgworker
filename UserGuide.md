# PyBgWorker — Unified User & Production Guide

A lightweight background task worker for Python applications using SQLite as the queue backend. PyBgWorker enables background job execution, scheduling, retries, monitoring, and production-safe deployment without requiring Redis or RabbitMQ.

## 1. Introduction

PyBgWorker allows applications to:

Run tasks in the background

Schedule jobs

Retry failed tasks automatically

Cancel jobs

Limit execution rate

Execute priority jobs first

Monitor job status

Fetch task results

Inspect worker state

Manage queues via CLI

Recover safely from crashes

Run multiple workers safely

It is suitable for:

Email sending

Image and file processing

API synchronization

Scheduled operations

Batch processing

Background web backend jobs

Dashboard task tracking

All with zero external dependencies.

## 2. Core Features

PyBgWorker provides the following built-in capabilities:

✅ Background job execution✅ Task scheduling (countdown & ETA)✅ Automatic retry for failed tasks✅ Task cancellation support✅ Rate limiting for tasks✅ Task priority execution✅ Task status tracking✅ Result storage & retrieval✅ Worker statistics & monitoring✅ Queue inspection tools✅ Failed job inspection✅ Queue purging utilities✅ Crash recovery & task requeue✅ Multi-worker safe execution

## 3. Installation & Upgrade

### Install from PyPI

pip install pybgworker

### Upgrade to Latest Version

pip install --upgrade pybgworker

This upgrades PyBgWorker to the newest available version from PyPI.

### Verify Installation

pybgworker --help

If CLI command is unavailable:

python -m pybgworker.cli --help

## 4. Core Concepts

### Task

A Python function executed in the background.

### Worker

A process that continuously fetches and runs tasks.

### Queue

SQLite database storing jobs and results.

### AsyncResult

Object returned when scheduling a task, used to check status and retrieve results.

## 5. Quick Start Example

### Step 1 — Create tasks file

# tasks.pyfrom pybgworker import taskimport time@task(name="tasks.add")def add(x, y):    time.sleep(2)    return x + y

### Step 2 — Run worker

pybgworker run --app tasks

Alternative:

python -m pybgworker.cli run --app tasks

To run multiple tasks in parallel within one worker:

python -m pybgworker.cli run --app tasks --concurrency 4

### Step 3 — Enqueue task

from tasks import addresult = add.delay(5, 7)print("Task ID:", result.id)

### Step 4 — Fetch result

print(result.ready())print(result.result())

## 6. Defining Tasks

### Basic Task

@task(name="tasks.hello")def hello(name):    return f"Hello {name}"

Usage:

hello.delay("Prabhat")

## 7. Task Scheduling

### Delay Execution (Countdown)

add.delay(10, 2, countdown=10)

Task executes after 10 seconds.

### Run at Specific Time (ETA)

ETA allows tasks to run at an exact future time.

Example:

from datetime import datetime, timedelta, timezoneeta_time = datetime.now(timezone.utc) + timedelta(minutes=1)add.delay(5, 6, eta=eta_time)

Explanation:

Current UTC time is taken

One minute is added

Task is scheduled to execute exactly at that timestamp

Use cases:

Scheduled notifications

Billing execution

Delayed processing

Reminder systems

Important notes:

ETA should use timezone-aware datetime

Worker must be running at execution time

## 8. Task Retries

@task(name="tasks.api_call", retries=3, retry_delay=5)def api_call():    raise Exception("Temporary failure")

Retries occur automatically with delay between attempts.

## 9. Task Timeout

@task(name="tasks.heavy", timeout=30)def heavy():    ...

Behavior:

Task stops after timeout

Marked as failed

Worker continues execution

If timeout not provided used globle timeout =150 sec.

## 10. Rate Limiting

@task(name="tasks.api", rate_limit=2)def api_call():    ...

Limits executions per second.

Worker-level limits may also apply.

If not provided default ratelimit is 5 per sec.

## 11. Task Priority

add.delay(10, 2, priority=1)

Lower value = higher priority.

## 12. Task Status & Results

res = add.delay(3, 4)print(res.status())print(res.result())

Possible states:

queued

running

success

failed

cancelled

## 13. Queue & Job Management Commands

### Cancel Task

pybgworker cancel TASK_ID

Example:

pybgworker cancel c0771b98-xxxx

Cancels queued tasks immediately. Running tasks stop on forced shutdown.

### Retry Failed Task

pybgworker retry TASK_ID

Example:

pybgworker retry c0771b98-xxxx

Retries a previously failed job.

### View Failed Tasks

pybgworker failed

Displays all failed jobs in the queue.

### Inspect Queue

pybgworker inspect

Shows queued and running jobs.

### Purge Queue

pybgworker purge

Clears all queued jobs.

### Worker Stats

pybgworker stats

Shows worker load and job counts.

## 14. Worker Lifecycle

### Start Worker

pybgworker run --app tasks

### Graceful Shutdown

Press Ctrl+C once:

Worker stops fetching tasks

Running task finishes

Worker exits safely

### Force Shutdown

Press Ctrl+C twice:

Running task cancelled

Immediate shutdown

## 15. Worker Internals

Worker operation:

Poll queue

Lock task

Execute in subprocess

Store result

Retry if configured

Release lock

Timeout protection applied automatically.

## 16. Worker Crash Recovery

If worker crashes:

Task locks expire

Tasks are requeued

Another worker resumes processing

No manual recovery required.

## 17. Running Multiple Workers

Multiple workers can safely run simultaneously:

pybgworker run --app tasks

SQLite locking distributes tasks safely.

## 18. Production Deployment Tips

Recommended production practices:

Run workers via system services or Docker

Restart workers automatically

Run multiple worker instances

Monitor queue health

Common tools:

systemd

Supervisor

Docker

Kubernetes

## 19. FastAPI Integration Example

from fastapi import FastAPIfrom tasks import addapp = FastAPI()@app.get("/add")def add_numbers(a: int, b: int):    res = add.delay(a, b)    return {"task_id": res.id}

Clients later fetch results using the task ID.

## 20. Best Practices

Use unique task names

Keep tasks small and independent

Avoid long blocking operations

Retry only safe operations

Log failures clearly

Apply rate limits for APIs

## 21. Performance Notes

Current model:

One subprocess per task

Configurable per-worker concurrency via PYBGWORKER_CONCURRENCY (default 1)

Safe and predictable execution

## 22. Minimal Project Structure

project/│├── tasks.py├── main.py└── db.sqlite

## 23. Conclusion

PyBgWorker provides background execution, scheduling, retries, monitoring, and simple deployment without heavy infrastructure.

## Practical Use Cases Supported by PyBgWorker

Below are real-world use cases that are already possible using PyBgWorker today, without adding new features.

### 1. Background Email Sending

Instead of sending email during a web request:

send_email.delay(user_email)

User gets fast response while email is sent in background.

### 2. File Processing in Background

Example: image or CSV processing.

process_file.delay(file_path)

Large files are handled without blocking the application.

### 3. API Calls with Automatic Retry

Useful when external APIs fail temporarily.

fetch_remote_data.delay()

Retries handle temporary network or service failures.

### 4. Scheduled Job Execution

Run jobs later using countdown or ETA.

add.delay(10, 2, countdown=300)

Example uses:

·         delayed notifications

·         scheduled processing

·         delayed billing actions

### 5. Priority Job Execution

Important jobs can run first.

generate_invoice.delay(priority=1)

Useful when urgent tasks must execute earlier.

### 6. Bulk Task Processing

Process many items asynchronously.

for user in users:    send_email.delay(user.email)

Ideal for campaigns, reports, or batch updates.

### 7. Background API Processing

Fast API responses while heavy work runs later.

@app.post(“/upload”)def upload():    process_upload.delay(file_path)    return {“status”: “processing”}

### 8. Retry Failed Operations

Recover automatically from temporary errors.

payment_sync.delay()

Worker retries until success or retry limit reached.

### 9. Job Monitoring & Tracking

Applications can monitor progress.

res = add.delay(1, 2)print(res.status())

Used in dashboards or admin panels.

### 10. Queue Maintenance & Cleanup

Admins can manage queue state.

pybgworker purgepybgworker failedpybgworker stats

Useful for operations and maintenance.

## Conclusion

PyBgWorker gives Python apps:

·         Background execution

·         Job scheduling

·         Retry handling

·         Task tracking

·         Zero external dependencies

with simple setup.
