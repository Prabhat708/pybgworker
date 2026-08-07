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

    # Key: (cron_expression, registered_task_name) → next scheduled datetime.
    # Using a composite key prevents two tasks that share the same cron
    # expression from overwriting each other's scheduling state.
    next_run = {}

    while True:
        current = datetime.now(timezone.utc)

        for expr, func in CRON_REGISTRY:
            # Use the registered task name (from @task(name=...)), not the
            # bare Python function name, so the registry lookup always hits.
            registered_name = getattr(func, "_task_name", func.__name__)

            # Composite key: expression + task identity → no collision when
            # multiple tasks share the same schedule.
            task_key = (expr, registered_name)

            if task_key not in next_run:
                next_run[task_key] = croniter(expr, current).get_next(datetime)

            if current >= next_run[task_key]:
                # Pull all metadata from the registry so the cron execution
                # path stays consistent with the delay() execution path.
                meta = TASK_REGISTRY.get(registered_name, {})

                task = {
                    "id": generate_id(),
                    "name": registered_name,
                    "args": dumps(()),
                    "kwargs": dumps({}),
                    "status": TaskState.QUEUED.value,
                    "attempt": 0,
                    # Retry configuration — mirrors what delay() stores.
                    "max_retries": meta.get("max_retries", 0),
                    "retry_delay": meta.get("retry_delay", 0),
                    "retry_backoff": meta.get("retry_backoff", False),
                    "retry_backoff_factor": meta.get("retry_backoff_factor", 2),
                    "retry_max_delay": meta.get("retry_max_delay", None),
                    "retry_jitter": meta.get("retry_jitter", 0.0),
                    # Execution options — mirrors what delay() stores.
                    "timeout": meta.get("timeout", None),
                    "run_at": now().isoformat(),
                    "priority": meta.get("priority", 5),
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

                next_run[task_key] = croniter(expr, current).get_next(datetime)

        time.sleep(1)
