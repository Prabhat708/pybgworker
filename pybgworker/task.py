from functools import wraps
import json
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
    on_success=None,
    on_failure=None,
):
    """Decorator that registers a function as a background task.

    Args:
        name (str): **Required.** Globally unique task name, e.g. ``"tasks.send_email"``.
        retries (int): Max retry attempts after first failure.  Default ``3``.
        retry_delay (float): Base delay in seconds between retries.  Default ``0``.
        retry_backoff (bool): Enable exponential backoff.
        retry_backoff_factor (float): Backoff multiplier.  Default ``2``.
        retry_max_delay (float | None): Cap on retry delay in seconds.
        retry_jitter (float): Random noise on delay.  ``<=1`` = fraction of
            delay; ``>1`` = absolute seconds.
        retry_for (tuple[type[Exception], ...]): Only retry for these exception
            types (inheritance respected).  Default ``(Exception,)`` retries on
            any non-BaseException error.
        timeout (float | None): Per-task timeout in seconds.  Falls back to
            global ``TASK_TIMEOUT`` (150 s) when ``None``.
        rate_limit (float | None): Max starts per second for this task.
            Overrides global ``RATE_LIMIT`` (5/s).  ``None`` = use global.
            **This limits start rate, not concurrent executions.**
        on_success (callable | None): Called by the worker after the task
            succeeds.  Receives one argument: the task id string.
        on_failure (callable | None): Called by the worker after the task
            reaches a permanent failure state (dead/failed).  Receives two
            arguments: task id string and the error string.
    """
    if name is None:
        raise ValueError("Task name is required to avoid __main__ issues")

    def decorator(func):
        task_name = name or f"{func.__module__}.{func.__name__}"

        # Store task metadata in the registry.
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
            "on_success": on_success,
            "on_failure": on_failure,
        }

        def _build_task_dict(args, kwargs, countdown=None, eta=None,
                             priority=5, idempotency_key=None):
            """Build the raw task dict for DB insertion."""
            run_at = now()
            if countdown is not None:
                run_at += timedelta(seconds=countdown)
            if eta is not None:
                run_at = eta

            return {
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
                "progress": None,
                "idempotency_key": idempotency_key,
            }

        @wraps(func)
        def delay(*args, countdown=None, eta=None, priority=5,
                  idempotency_key=None, **kwargs):
            """Enqueue this task and return an :class:`AsyncResult`.

            Args:
                *args: Positional arguments forwarded to the task function.
                countdown (float | None): Delay in seconds before execution.
                eta (datetime | None): Exact UTC datetime to run at.
                priority (int): Lower = higher priority.  Default ``5``.
                idempotency_key (str | None): Optional deduplication key.
                    If a task with this key already exists (any state) the
                    existing :class:`AsyncResult` is returned without inserting.
                **kwargs: Keyword arguments forwarded to the task function.

            Raises:
                TypeError: If *args* or *kwargs* contain non-JSON-serializable
                    values.  Task arguments must be primitives (str, int, float,
                    bool, list, dict, None).
            """
            # Validate serializability early so the error points here, not deep
            # inside the enqueue pipeline.
            try:
                json.dumps(args)
                json.dumps(kwargs)
            except TypeError as exc:
                raise TypeError(
                    f"Task arguments must be JSON-serializable. "
                    f"Got non-serializable value in '{task_name}': {exc}"
                ) from exc

            task_dict = _build_task_dict(
                args, kwargs,
                countdown=countdown, eta=eta,
                priority=priority, idempotency_key=idempotency_key,
            )
            actual_id = queue.enqueue(task_dict)
            from .result import AsyncResult
            return AsyncResult(actual_id, backend=backend)

        def delay_many(arg_pairs, priority=5):
            """Enqueue multiple calls in a single database transaction.

            Substantially faster than calling ``delay()`` in a loop for large
            batches because all INSERTs share one connection and one commit.

            Args:
                arg_pairs: Iterable of ``(args_tuple, kwargs_dict)`` pairs.
                    Use ``((), {})`` for tasks that take no arguments.
                priority (int): Priority applied to every task in the batch.

            Returns:
                list[AsyncResult]: One result handle per enqueued task.

            Example::

                send_email.delay_many([
                    (("alice@example.com",), {}),
                    (("bob@example.com",),   {}),
                ])
            """
            task_dicts = [
                _build_task_dict(args, kwargs, priority=priority)
                for args, kwargs in arg_pairs
            ]
            queue.enqueue_many(task_dicts)
            from .result import AsyncResult
            return [AsyncResult(t["id"], backend=backend) for t in task_dicts]

        func.delay = delay
        func.delay_many = delay_many
        func._task_name = task_name  # expose registered name for scheduler lookup
        return func

    return decorator
