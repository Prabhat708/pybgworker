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
    get_worker_concurrency,
)
from . import config
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
            try:
                queue.cancel(task_id)
                log("task_cancelled", task_id=task_id)
            except ValueError:
                # Task already reached a terminal state (success/failed/dead)
                # between the active_tasks snapshot and this cancel attempt.
                # Safe to ignore — the task is already done.
                log("task_cancel_skipped", task_id=task_id,
                    reason="already in terminal state")

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
    interval_hours = max(1, config.CLEANUP_INTERVAL_HOURS)
    interval_seconds = interval_hours * 3600

    while True:
        try:
            result = queue.cleanup(
                retention_days=config.RETENTION_DAYS,
                vacuum=True,
            )
            log(
                "db_cleanup",
                retention_days=config.RETENTION_DAYS,
                deleted=result["deleted"],
                deleted_finished=result.get("deleted_finished", 0),
                vacuumed=result["vacuumed"],
                locked=result["locked"],
            )
        except Exception as e:
            log("db_cleanup_error", error=str(e))

        time.sleep(interval_seconds)


def reap_stale_tasks_loop():
    while True:
        try:
            reaped = queue.reap_stale_tasks()
            if reaped > 0:
                log("stale_tasks_reaped", count=reaped)
        except Exception as e:
            log("reap_stale_error", error=str(e))
        # Sleep for half WORKER_TIMEOUT so we detect crashes promptly,
        # but at least 5s and at most 60s to avoid hammering the database.
        interval = max(5, min(60, config.WORKER_TIMEOUT // 2))
        time.sleep(interval)


def _run_task_with_id(task_id, func, args, kwargs, result_queue):
    """Thin wrapper executed in each child process.

    Sets ``PYBGWORKER_CURRENT_TASK_ID`` so that :func:`~pybgworker.progress.set_progress`
    can find the current task id without needing context injection.
    Ignores SIGINT so Ctrl+C in the parent does not race with state updates.
    """
    os.environ["PYBGWORKER_CURRENT_TASK_ID"] = task_id
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    run_task(func, args, kwargs, result_queue)


def run_task(func, args, kwargs, result_queue):
    try:
        result = func(*args, **kwargs)
        result_queue.put(("success", result))
    except Exception as exc:
        # Send the full MRO so the parent can do inheritance-aware matching
        # e.g. ValueError's MRO is ["ValueError", "Exception", "BaseException", "object"]
        mro_names = [c.__name__ for c in type(exc).__mro__]
        result_queue.put(("error", traceback.format_exc(), type(exc).__name__, mro_names))


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

    limiter.acquire(meta.get("rate_limit"), name=task["name"])

    func = meta["func"]
    args = loads(task["args"])
    kwargs = loads(task["kwargs"])

    task_id = task["id"]
    log("task_start", task_id=task_id, worker=WORKER_NAME)

    result_queue = MPQueue()
    process = Process(
        target=_run_task_with_id,
        args=(task_id, func, args, kwargs, result_queue),
    )

    process.start()

    user_timeout = meta.get("timeout")
    timeout = user_timeout if user_timeout is not None else TASK_TIMEOUT

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
            "retry_for": meta.get("retry_for", (Exception,)),
        },
    }


def handle_timeout(task_id, info):
    info["process"].terminate()

    backend_info = backend.get_task(task_id)
    if not backend_info:
        log("task_not_found", task_id=task_id)
        return
    if backend_info["status"] == "cancelled":
        log("task_cancelled", task_id=task_id)
        return

    task = info["task"]
    if task["attempt"] < task["max_retries"]:
        delay = compute_retry_delay(task, info["retry_meta"])
        try:
            queue.reschedule(task_id, delay)
        except ValueError:
            # Task was cancelled externally between the status check and here.
            log("task_cancel_skipped", task_id=task_id,
                reason="cancelled before timeout reschedule")
            return
        log("task_timeout", task_id=task_id)
        log("task_retry_scheduled", task_id=task_id, delay=delay)
    else:
        try:
            queue.dead(task_id, "Task timeout")
        except ValueError:
            log("task_cancel_skipped", task_id=task_id,
                reason="cancelled before timeout dead-letter")
            return
        log("task_timeout", task_id=task_id)
        log("task_dead", task_id=task_id)
        meta = TASK_REGISTRY.get(task["name"], {})
        _fire_callback(meta.get("on_failure"), task_id, "Task timeout")


def _exc_matches_retry_for(exc_class_name, exc_mro, retry_for):
    """Return True if the raised exception matches any type in retry_for,
    respecting inheritance.

    e.g. retry_for=(Exception,) will match ValueError, RuntimeError, etc.
    because they are all subclasses of Exception.

    exc_mro is the list of class names in the exception's MRO,
    e.g. ["ValueError", "Exception", "BaseException", "object"].
    This lets us check inheritance across the process boundary without
    importing or reconstructing the exception type in the parent.
    """
    mro_set = set(exc_mro) if exc_mro else {exc_class_name}
    for exc_type in retry_for:
        if exc_type.__name__ in mro_set:
            return True
    return False


def _fire_callback(callback, *args):
    """Invoke a user-supplied callback, logging but not re-raising exceptions."""
    if callback is None:
        return
    try:
        callback(*args)
    except Exception:
        log("callback_error", error=traceback.format_exc())


def handle_completed(task_id, info):
    # Re-check DB status: an external cancel (e.g. `pybgworker cancel <id>`)
    # may have set status='cancelled' while the subprocess was still executing.
    # If so, skip all queue transitions — calling ack/fail/reschedule on a
    # cancelled task raises ValueError from validate_transition() and would
    # propagate up to crash the entire run_worker() thread.
    current = backend.get_task(task_id)
    if current and current["status"] == "cancelled":
        log("task_cancelled", task_id=task_id)
        return

    result_queue = info["result_queue"]
    meta = TASK_REGISTRY.get(info["task"]["name"], {})

    if result_queue.empty():
        task = info["task"]
        if task["attempt"] < task["max_retries"]:
            delay = compute_retry_delay(task, info["retry_meta"])
            try:
                queue.reschedule(task_id, delay)
            except ValueError:
                log("task_cancel_skipped", task_id=task_id,
                    reason="cancelled before crash reschedule")
                return
            log("task_crash", task_id=task_id)
            log("task_retry_scheduled", task_id=task_id, delay=delay)
        else:
            try:
                queue.fail(task_id, "Task crashed")
            except ValueError:
                log("task_cancel_skipped", task_id=task_id,
                    reason="cancelled before crash fail")
                return
            log("task_crash", task_id=task_id)
            _fire_callback(meta.get("on_failure"), task_id, "Task crashed")
        return

    item = result_queue.get()
    status = item[0]
    payload = item[1]
    exc_class_name = item[2] if len(item) > 2 else None
    exc_mro = item[3] if len(item) > 3 else None
    duration = (now() - info["start_time"]).total_seconds()

    if status == "success":
        backend.store_result(task_id, payload)
        try:
            queue.ack(task_id)
        except ValueError:
            log("task_cancel_skipped", task_id=task_id,
                reason="cancelled before ack")
            return
        log(
            "task_success",
            task_id=task_id,
            duration=duration,
            worker=WORKER_NAME,
        )
        _fire_callback(meta.get("on_success"), task_id)
        return

    task = info["task"]
    retry_for = info["retry_meta"].get("retry_for", (Exception,))

    # Check inheritance-aware match across the process boundary
    exc_eligible = exc_class_name is None or _exc_matches_retry_for(
        exc_class_name, exc_mro, retry_for
    )

    if exc_eligible and task["attempt"] < task["max_retries"]:
        delay = compute_retry_delay(task, info["retry_meta"])
        try:
            queue.reschedule(task_id, delay)
        except ValueError:
            log("task_cancel_skipped", task_id=task_id,
                reason="cancelled before error reschedule")
            return
        log("task_retry_scheduled", task_id=task_id, delay=delay)
    else:
        if not exc_eligible:
            log("task_retry_skipped", task_id=task_id, exc_type=exc_class_name)
        try:
            queue.dead(task_id, payload)
        except ValueError:
            log("task_cancel_skipped", task_id=task_id,
                reason="cancelled before dead-letter")
            return
        log("task_dead", task_id=task_id)
        _fire_callback(meta.get("on_failure"), task_id, payload)


def run_worker():
    global shutdown_requested

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    concurrency = max(1, get_worker_concurrency())
    log("worker_start", worker=WORKER_NAME, concurrency=concurrency)

    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=run_scheduler, daemon=True).start()
    
    # We also run stale lock reaping in a separate thread so it happens quickly!
    threading.Thread(target=reap_stale_tasks_loop, daemon=True).start()

    if config.RETENTION_DAYS > 0:
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
