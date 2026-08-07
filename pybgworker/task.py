from functools import wraps
from datetime import timedelta
from .sqlite_queue import SQLiteQueue
from .utils import generate_id, dumps, now
from .state import TaskState
from .backends import SQLiteBackend

TASK_REGISTRY = {}
queue = SQLiteQueue()
backend = SQLiteBackend()


def task(
    name=None,
    retries=3,
    retry_delay=0,
    retry_backoff=False,
    retry_backoff_factor=2,
    retry_max_delay=None,
    retry_jitter=0.0,
    retry_for=(Exception,),
    timeout=None,
    rate_limit=None,
):
    if name is None:
        raise ValueError("Task name is required to avoid __main__ issues")

    def decorator(func):
        task_name = name or f"{func.__module__}.{func.__name__}"

        # Store task metadata
        TASK_REGISTRY[task_name] = {
            "func": func,
            "max_retries": retries,
            "retry_delay": retry_delay,
            "retry_backoff": retry_backoff,
            "retry_backoff_factor": retry_backoff_factor,
            "retry_max_delay": retry_max_delay,
            "retry_jitter": retry_jitter,
            "retry_for": retry_for,
            "timeout": timeout,
            "rate_limit": rate_limit,
        }

        @wraps(func)
        def delay(*args, countdown=None, eta=None, priority=5, **kwargs):
            run_at = now()

            if countdown:
                run_at += timedelta(seconds=countdown)

            if eta:
                run_at = eta

            task = {
                "id": generate_id(),
                "name": task_name,
                "args": dumps(args),
                "kwargs": dumps(kwargs),
                "status": TaskState.QUEUED.value,
                "attempt": 0,
                "max_retries": retries,
                "run_at": run_at.isoformat(),
                "priority": priority,
                "locked_by": None,
                "locked_at": None,
                "last_error": None,
                "result": None,
                "created_at": now().isoformat(),
                "updated_at": now().isoformat(),
                "finished_at": None,
            }

            queue.enqueue(task)

            from .result import AsyncResult
            return AsyncResult(task["id"], backend=backend)

        func.delay = delay
        func._task_name = task_name  # expose registered name for scheduler lookup
        return func

    return decorator
