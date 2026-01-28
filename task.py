from functools import wraps
from .sqlite_queue import SQLiteQueue
from .utils import generate_id, dumps, now
from .state import TaskState

TASK_REGISTRY = {}
queue = SQLiteQueue()

def task(retries: int = 0, retry_delay: int = 0):
    def decorator(func):
        task_name = f"{func.__module__}.{func.__name__}"
        TASK_REGISTRY[task_name] = func

        @wraps(func)
        def delay(*args, **kwargs):
            task_id = generate_id()
            task = {
                "id": task_id,
                "name": task_name,
                "args": dumps(args),
                "kwargs": dumps(kwargs),
                "status": TaskState.QUEUED.value,
                "retries_left": retries,
                "run_at": now().isoformat(),
                "locked_by": None,
                "locked_at": None,
                "last_error": None,
                "created_at": now().isoformat(),
                "updated_at": now().isoformat(),
            }
            queue.enqueue(task)
            return task_id

        func.delay = delay
        return func
    return decorator
