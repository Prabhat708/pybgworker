import time
from croniter import croniter
from datetime import datetime, timezone
from .task import queue
from .utils import generate_id, now, dumps
from .state import TaskState

CRON_REGISTRY = []


def cron(expr):
    def decorator(func):
        CRON_REGISTRY.append((expr, func))
        return func
    return decorator


def run_scheduler():
    print("🕒 Cron scheduler started")

    next_run = {}

    while True:
        current = datetime.now(timezone.utc)

        for expr, func in CRON_REGISTRY:
            if expr not in next_run:
                next_run[expr] = croniter(expr, current).get_next(datetime)

            if current >= next_run[expr]:
                task = {
                    "id": generate_id(),
                    "name": func.__name__,
                    "args": dumps(()),
                    "kwargs": dumps({}),
                    "status": TaskState.QUEUED.value,
                    "attempt": 0,
                    "max_retries": 0,
                    "run_at": now().isoformat(),
                    "priority": 5,
                    "locked_by": None,
                    "locked_at": None,
                    "last_error": None,
                    "result": None,
                    "created_at": now().isoformat(),
                    "updated_at": now().isoformat(),
                    "finished_at": None,
                }

                queue.enqueue(task)
                print(f"⏰ Cron fired: {func.__name__}")

                next_run[expr] = croniter(expr, current).get_next(datetime)

        time.sleep(1)
