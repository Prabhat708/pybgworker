import time
import traceback
from .sqlite_queue import SQLiteQueue
from .task import TASK_REGISTRY
from .config import WORKER_NAME, POLL_INTERVAL
from .utils import loads

queue = SQLiteQueue()

def run_worker():
    print(f"🚀 Worker {WORKER_NAME} started")

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

        try:
            result = func(*loads(task["args"]), **loads(task["kwargs"]))
            queue.ack(task["id"], result=str(result))
            print(f"✅ Success {task['id']}")

        except retry_for:
            if task["attempt"] < task["max_retries"]:
                queue.reschedule(task["id"], retry_delay)
                print(f"🔁 Retry {task['attempt']+1}/{task['max_retries']}")
            else:
                queue.fail(task["id"], traceback.format_exc())
                print(f"❌ Failed permanently")

        except Exception:
            queue.fail(task["id"], traceback.format_exc())
