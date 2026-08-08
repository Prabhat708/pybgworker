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
            try:
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
                        # max_retries is a DB column — stores the retry cap so the
                        # worker can compare attempt vs max_retries without hitting
                        # TASK_REGISTRY again. All other retry metadata (retry_delay,
                        # retry_backoff, etc.) is NOT a DB column — the worker reads
                        # those from TASK_REGISTRY at execution time.
                        "max_retries": meta.get("max_retries", 0),
                        "run_at": now().isoformat(),
                        "priority": meta.get("priority", 5),
                        "locked_by": None,
                        "locked_at": None,
                        "last_error": None,
                        "result": None,
                        "created_at": now().isoformat(),
                        "updated_at": now().isoformat(),
                        "finished_at": None,
                        "progress": None,
                        "idempotency_key": None,
                    }

                    queue.enqueue(task)
                    log("cron_fired", task_name=registered_name)

                    next_run[task_key] = croniter(expr, current).get_next(datetime)
            except Exception as e:
                log("cron_error", error=str(e), expr=expr)

        time.sleep(1)
