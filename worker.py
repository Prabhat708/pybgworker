import time
import traceback
from .sqlite_queue import SQLiteQueue
from .task import TASK_REGISTRY
from .config import WORKER_NAME, POLL_INTERVAL

queue = SQLiteQueue()

def run_worker():
    while True:
        task = queue.fetch_next(WORKER_NAME)
        if not task:
            time.sleep(POLL_INTERVAL)
            continue

        try:
            func = TASK_REGISTRY[task["name"]]
            args = task["args"]
            kwargs = task["kwargs"]
            func(*args, **kwargs)
            queue.ack(task["id"])
        except Exception as e:
            queue.fail(task["id"], traceback.format_exc())
