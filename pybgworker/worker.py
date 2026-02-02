import time
import traceback
import threading
import signal
import os
from multiprocessing import Process, Queue as MPQueue

from .logger import log
from .sqlite_queue import SQLiteQueue
from .task import TASK_REGISTRY
from .config import WORKER_NAME, POLL_INTERVAL, RATE_LIMIT
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
current_task_id = None
current_process = None


def handle_shutdown(signum, frame):
    global shutdown_requested, last_shutdown_signal
    global current_task_id, current_process

    now_ts = time.time()

    # Ignore duplicate signals (Windows issue)
    if now_ts - last_shutdown_signal < 1:
        return

    last_shutdown_signal = now_ts

    # Second Ctrl+C → force exit
    if shutdown_requested:
        log("worker_force_exit", worker=WORKER_NAME)

        if current_task_id:
            queue.cancel(current_task_id)
            log("task_cancelled", task_id=current_task_id)

        if current_process and current_process.is_alive():
            current_process.terminate()

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


def run_task(func, args, kwargs, result_queue):
    # Child process ignores Ctrl+C
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    try:
        result = func(*args, **kwargs)
        result_queue.put(("success", result))
    except Exception:
        result_queue.put(("error", traceback.format_exc()))


def run_worker():
    global shutdown_requested, current_task_id, current_process

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    log("worker_start", worker=WORKER_NAME)

    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=run_scheduler, daemon=True).start()

    while not shutdown_requested:
        task = queue.fetch_next(WORKER_NAME)

        if not task:
            if shutdown_requested:
                break
            time.sleep(POLL_INTERVAL)
            continue

        meta = TASK_REGISTRY.get(task["name"])
        if not meta:
            queue.fail(task["id"], "Task not registered")
            log("task_invalid", task_id=task["id"])
            continue

        # -------- Rate limit per task --------
        limiter.acquire(meta.get("rate_limit"))

        func = meta["func"]
        retry_delay = meta["retry_delay"]

        args = loads(task["args"])
        kwargs = loads(task["kwargs"])

        start_time = now()
        current_task_id = task["id"]

        log("task_start", task_id=current_task_id, worker=WORKER_NAME)

        result_queue = MPQueue()
        process = Process(target=run_task, args=(func, args, kwargs, result_queue))
        current_process = process

        process.start()

        # -------- Timeout per task --------
        timeout = meta.get("timeout") or TASK_TIMEOUT

        start_join = time.time()

        while process.is_alive():
            if time.time() - start_join > timeout:
                break
            time.sleep(0.2)

        if process.is_alive():
            process.terminate()

            info = backend.get_task(current_task_id)
            if info["status"] == "cancelled":
                log("task_cancelled", task_id=current_task_id)
                current_task_id = None
                current_process = None
                continue

            queue.fail(current_task_id, "Task timeout")
            log("task_timeout", task_id=current_task_id)
            log("task_failed", task_id=current_task_id)
            current_task_id = None
            current_process = None
            continue

        if result_queue.empty():
            queue.fail(current_task_id, "Task crashed")
            log("task_crash", task_id=current_task_id)
            current_task_id = None
            current_process = None
            continue

        status, payload = result_queue.get()
        duration = (now() - start_time).total_seconds()

        if status == "success":
            backend.store_result(current_task_id, payload)
            queue.ack(current_task_id)
            log(
                "task_success",
                task_id=current_task_id,
                duration=duration,
                worker=WORKER_NAME,
            )
        else:
            if task["attempt"] < task["max_retries"]:
                queue.reschedule(current_task_id, retry_delay)
            else:
                queue.fail(current_task_id, payload)

        current_task_id = None
        current_process = None

    log("worker_stopped", worker=WORKER_NAME)
