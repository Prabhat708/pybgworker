import time
from croniter import croniter
from datetime import datetime, timezone
from .task import queue, TASK_REGISTRY
from .utils import generate_id, now, dumps
from .state import TaskState
from .logger import log

CRON_REGISTRY = []


def cron(expr):
    def decorator(func):
        CRON_REGISTRY.append((expr, func))
        return func
    return decorator


def run_scheduler():
    log("scheduler_start")

    next_run = {}

    while True:
        current = datetime.now(timezone.utc)

        for expr, func in CRON_REGISTRY:
            if expr not in next_run:
                next_run[expr] = croniter(expr, current).get_next(datetime)

            if current >= next_run[expr]:
                # Use the registered task name (from @task(name=...)), not the
                # bare Python function name, so the registry lookup always hits.
                registered_name = getattr(func, "_task_name", func.__name__)
                meta = TASK_REGISTRY.get(registered_name, {})
                task = {
                    "id": generate_id(),
                    "name": registered_name,
                    "args": dumps(()),
                    "kwargs": dumps({}),
                    "status": TaskState.QUEUED.value,
                    "attempt": 0,
                    "max_retries": meta.get("max_retries", 0),
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
                log("cron_fired", task_name=registered_name)

                next_run[expr] = croniter(expr, current).get_next(datetime)

        time.sleep(1)
