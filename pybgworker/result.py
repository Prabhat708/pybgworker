from .sqlite_queue import SQLiteQueue
from .state import TaskState

class AsyncResult:
    def __init__(self, task_id):
        self.task_id = task_id
        self.queue = SQLiteQueue()

    def _fetch(self):
        row = self.queue.conn.execute(
            "SELECT * FROM tasks WHERE id=?",
            (self.task_id,)
        ).fetchone()
        return dict(row) if row else None

    @property
    def status(self):
        task = self._fetch()
        return task["status"] if task else None

    @property
    def result(self):
        task = self._fetch()
        return task["result"] if task else None

    @property
    def error(self):
        task = self._fetch()
        return task["last_error"] if task else None

    def ready(self):
        return self.status in (
            TaskState.SUCCESS.value,
            TaskState.FAILED.value,
        )

    def successful(self):
        return self.status == TaskState.SUCCESS.value

    def failed(self):
        return self.status == TaskState.FAILED.value
