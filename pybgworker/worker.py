import time
import traceback
import threading
from multiprocessing import Process, Queue as MPQueue

from .sqlite_queue import SQLiteQueue
from .task import TASK_REGISTRY
from .config import WORKER_NAME, POLL_INTERVAL
from .utils import loads, get_conn, now
from .backends import SQLiteBackend


queue = SQLiteQueue()
backend = SQLiteBackend()

TASK_TIMEOUT = 150  # seconds (make configurable later)


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
            print("⚠ Heartbeat error:", e)

        time.sleep(5)


def run_task(func, args, kwargs, result_queue):
    """
    Runs inside subprocess.
    Sends back ("success", result) OR ("error", traceback)
    """
    try:
        result = func(*args, **kwargs)
        result_queue.put(("success", result))
    except Exception:
        result_queue.put(("error", traceback.format_exc()))


def run_worker():
    print(f"🚀 Worker {WORKER_NAME} started")

    # start heartbeat thread
    threading.Thread(target=heartbeat, daemon=True).start()

    while True:
        task = queue.fetch_next(WORKER_NAME)

        if not task:
            time.sleep(POLL_INTERVAL)
            continue

        meta = TASK_REGISTRY.get(task["name"])
        if not meta:
            queue.fail(task["id"], "Task not registered")
            continue

        func = meta["func"]
        retry_for = meta["retry_for"]
        retry_delay = meta["retry_delay"]

        args = loads(task["args"])
        kwargs = loads(task["kwargs"])

        result_queue = MPQueue()
        process = Process(
            target=run_task,
            args=(func, args, kwargs, result_queue)
        )

        process.start()
        process.join(TASK_TIMEOUT)

        # ---- TIMEOUT CASE ----
        if process.is_alive():
            process.terminate()

            # check if cancelled
            info = backend.get_task(task["id"])
            if info["status"] == "cancelled":
                print(f"🛑 Cancelled {task['id']}")
                continue

            queue.fail(task["id"], "Task timeout")


        # ---- PROCESS RETURNED ----
        if result_queue.empty():
            queue.fail(task["id"], "Task crashed without result")
            print(f"💥 Crash {task['id']}")
            continue

        status, payload = result_queue.get()

        if status == "success":
            backend.store_result(task["id"], payload)
            queue.ack(task["id"])
            print(f"✅ Success {task['id']}")

        else:
            # retry logic preserved
            if task["attempt"] < task["max_retries"]:
                queue.reschedule(task["id"], retry_delay)
                print(f"🔁 Retry {task['attempt']+1}/{task['max_retries']}")
            else:
                queue.fail(task["id"], payload)
                print(f"❌ Failed permanently {task['id']}")
