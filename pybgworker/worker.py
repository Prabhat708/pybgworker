import time
import traceback
import threading
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

TASK_TIMEOUT = 150  # seconds


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
    try:
        result = func(*args, **kwargs)
        result_queue.put(("success", result))
    except Exception:
        result_queue.put(("error", traceback.format_exc()))


def run_worker():
    log("worker_start", worker=WORKER_NAME)

    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=run_scheduler, daemon=True).start()

    while True:
        task = queue.fetch_next(WORKER_NAME)

        if not task:
            time.sleep(POLL_INTERVAL)
            continue

        # ⭐ rate limiting happens here
        limiter.acquire()

        meta = TASK_REGISTRY.get(task["name"])
        if not meta:
            queue.fail(task["id"], "Task not registered")
            log("task_invalid", task_id=task["id"])
            continue

        func = meta["func"]
        retry_delay = meta["retry_delay"]

        args = loads(task["args"])
        kwargs = loads(task["kwargs"])

        start_time = now()
        log("task_start", task_id=task["id"], worker=WORKER_NAME)

        result_queue = MPQueue()
        process = Process(target=run_task, args=(func, args, kwargs, result_queue))

        process.start()
        process.join(TASK_TIMEOUT)

        if process.is_alive():
            process.terminate()

            info = backend.get_task(task["id"])
            if info["status"] == "cancelled":
                log("task_cancelled", task_id=task["id"])
                continue

            queue.fail(task["id"], "Task timeout")
            log("task_timeout", task_id=task["id"])
            continue

        if result_queue.empty():
            queue.fail(task["id"], "Task crashed without result")
            log("task_crash", task_id=task["id"])
            continue

        status, payload = result_queue.get()
        duration = (now() - start_time).total_seconds()

        if status == "success":
            backend.store_result(task["id"], payload)
            queue.ack(task["id"])
            log("task_success",
                task_id=task["id"],
                duration=duration,
                worker=WORKER_NAME)

        else:
            if task["attempt"] < task["max_retries"]:
                queue.reschedule(task["id"], retry_delay)
                log("task_retry",
                    task_id=task["id"],
                    attempt=task["attempt"] + 1,
                    max=task["max_retries"])
            else:
                queue.fail(task["id"], payload)
                log("task_failed", task_id=task["id"])
