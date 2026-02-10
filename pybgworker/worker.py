import time
import traceback
import threading
import signal
import os
import random
from multiprocessing import Process, Queue as MPQueue

from .logger import log
from .sqlite_queue import SQLiteQueue
from .task import TASK_REGISTRY
from .config import (
    WORKER_NAME,
    POLL_INTERVAL,
    RATE_LIMIT,
    RETENTION_DAYS,
    CLEANUP_INTERVAL_HOURS,
    get_worker_concurrency,
)
from .utils import loads, get_conn, now
from .backends import SQLiteBackend
from .scheduler import run_scheduler
from .ratelimit import RateLimiter


queue = SQLiteQueue()
backend = SQLiteBackend()
limiter = RateLimiter(RATE_LIMIT)

TASK_TIMEOUT = 150  # default timeout


shutdown_requested = False
last_shutdown_signal = 0
active_tasks = {}
active_tasks_lock = threading.Lock()


def handle_shutdown(signum, frame):
    global shutdown_requested, last_shutdown_signal

    now_ts = time.time()

    # Ignore duplicate signals (Windows issue)
    if now_ts - last_shutdown_signal < 1:
        return

    last_shutdown_signal = now_ts

    # Second Ctrl+C → force exit
    if shutdown_requested:
        log("worker_force_exit", worker=WORKER_NAME)

        with active_tasks_lock:
            task_ids = list(active_tasks.keys())
            processes = [info["process"] for info in active_tasks.values()]

        for task_id in task_ids:
            queue.cancel(task_id)
            log("task_cancelled", task_id=task_id)

        for process in processes:
            if process.is_alive():
                process.terminate()

        os._exit(1)

    shutdown_requested = True
    log("worker_shutdown_requested", worker=WORKER_NAME)


def heartbeat():
    while True:
        try:
            with get_conn() as conn:
                conn.execute("""
                    INSERT INTO workers(name, last_seen)
                    VALUES (?, ?)
                    ON CONFLICT(name)
                    DO UPDATE SET last_seen=excluded.last_seen
                """, (WORKER_NAME, now().isoformat()))
                conn.commit()
        except Exception as e:
            log("heartbeat_error", error=str(e))

        time.sleep(5)


def maintenance_loop():
    interval_hours = max(1, CLEANUP_INTERVAL_HOURS)
    interval_seconds = interval_hours * 3600

    while True:
        try:
            result = queue.cleanup(
                retention_days=RETENTION_DAYS,
                vacuum=True,
            )
            log(
                "db_cleanup",
                retention_days=RETENTION_DAYS,
                deleted=result["deleted"],
                deleted_finished=result.get("deleted_finished", 0),
                vacuumed=result["vacuumed"],
                locked=result["locked"],
            )
        except Exception as e:
            log("db_cleanup_error", error=str(e))

        time.sleep(interval_seconds)


def run_task(func, args, kwargs, result_queue):
    # Child process ignores Ctrl+C
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    try:
        result = func(*args, **kwargs)
        result_queue.put(("success", result))
    except Exception:
        result_queue.put(("error", traceback.format_exc()))


def compute_retry_delay(task, meta):
    base_delay = meta.get("retry_delay") or 0
    if base_delay < 0:
        base_delay = 0

    delay = base_delay
    if meta.get("retry_backoff"):
        factor = meta.get("retry_backoff_factor") or 2
        if factor < 1:
            factor = 1
        delay = base_delay * (factor ** task["attempt"])

    max_delay = meta.get("retry_max_delay")
    if max_delay is not None:
        delay = min(delay, max_delay)

    jitter = meta.get("retry_jitter") or 0.0
    if jitter < 0:
        jitter = 0.0

    if jitter:
        if jitter <= 1:
            jitter_amount = delay * jitter
        else:
            jitter_amount = jitter
        delay = max(0.0, delay + random.uniform(-jitter_amount, jitter_amount))

    return delay


def start_task(task):
    meta = TASK_REGISTRY.get(task["name"])
    if not meta:
        queue.fail(task["id"], "Task not registered")
        log("task_invalid", task_id=task["id"])
        return None

    limiter.acquire(meta.get("rate_limit"))

    func = meta["func"]
    args = loads(task["args"])
    kwargs = loads(task["kwargs"])

    task_id = task["id"]
    log("task_start", task_id=task_id, worker=WORKER_NAME)

    result_queue = MPQueue()
    process = Process(target=run_task, args=(func, args, kwargs, result_queue))

    process.start()

    timeout = meta.get("timeout") or TASK_TIMEOUT

    return {
        "task": task,
        "process": process,
        "result_queue": result_queue,
        "start_time": now(),
        "start_monotonic": time.monotonic(),
        "timeout": timeout,
        "retry_meta": {
            "retry_delay": meta.get("retry_delay"),
            "retry_backoff": meta.get("retry_backoff"),
            "retry_backoff_factor": meta.get("retry_backoff_factor"),
            "retry_max_delay": meta.get("retry_max_delay"),
            "retry_jitter": meta.get("retry_jitter"),
        },
    }


def handle_timeout(task_id, info):
    info["process"].terminate()

    backend_info = backend.get_task(task_id)
    if backend_info["status"] == "cancelled":
        log("task_cancelled", task_id=task_id)
        return

    task = info["task"]
    if task["attempt"] < task["max_retries"]:
        delay = compute_retry_delay(task, info["retry_meta"])
        queue.reschedule(task_id, delay)
        log("task_timeout", task_id=task_id)
        log("task_retry_scheduled", task_id=task_id, delay=delay)
    else:
        queue.dead(task_id, "Task timeout")
        log("task_timeout", task_id=task_id)
        log("task_dead", task_id=task_id)


def handle_completed(task_id, info):
    result_queue = info["result_queue"]

    if result_queue.empty():
        queue.fail(task_id, "Task crashed")
        log("task_crash", task_id=task_id)
        return

    status, payload = result_queue.get()
    duration = (now() - info["start_time"]).total_seconds()

    if status == "success":
        backend.store_result(task_id, payload)
        queue.ack(task_id)
        log(
            "task_success",
            task_id=task_id,
            duration=duration,
            worker=WORKER_NAME,
        )
        return

    task = info["task"]
    if task["attempt"] < task["max_retries"]:
        delay = compute_retry_delay(task, info["retry_meta"])
        queue.reschedule(task_id, delay)
    else:
        queue.dead(task_id, payload)


def run_worker():
    global shutdown_requested

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    concurrency = max(1, get_worker_concurrency())
    log("worker_start", worker=WORKER_NAME, concurrency=concurrency)

    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=run_scheduler, daemon=True).start()
    if RETENTION_DAYS > 0:
        threading.Thread(target=maintenance_loop, daemon=True).start()

    while True:
        if not shutdown_requested:
            while not shutdown_requested:
                with active_tasks_lock:
                    slots_available = len(active_tasks) < concurrency

                if not slots_available:
                    break

                task = queue.fetch_next(WORKER_NAME)
                if not task:
                    break

                info = start_task(task)
                if not info:
                    continue

                with active_tasks_lock:
                    active_tasks[task["id"]] = info

        with active_tasks_lock:
            active_snapshot = list(active_tasks.items())

        if not active_snapshot:
            if shutdown_requested:
                break
            time.sleep(POLL_INTERVAL)
            continue

        finished_any = False
        for task_id, info in active_snapshot:
            process = info["process"]

            if process.is_alive():
                if time.monotonic() - info["start_monotonic"] > info["timeout"]:
                    handle_timeout(task_id, info)
                    finished_any = True
                    with active_tasks_lock:
                        active_tasks.pop(task_id, None)
                continue

            handle_completed(task_id, info)
            finished_any = True
            with active_tasks_lock:
                active_tasks.pop(task_id, None)

        if not finished_any:
            time.sleep(0.1)

    log("worker_stopped", worker=WORKER_NAME)
