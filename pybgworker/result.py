import time
import json
from .state import TaskState
from .backends import BaseBackend, SQLiteBackend


class AsyncResult:
    def __init__(self, task_id, backend: BaseBackend = None):
        self.task_id = task_id
        self.backend = backend or SQLiteBackend()

    def _fetch(self):
        return self.backend.get_task(self.task_id)

    @property
    def task_info(self):
        return self._fetch()

    @property
    def status(self):
        task = self._fetch()
        return task["status"] if task else None

    @property
    def result(self):
        task = self._fetch()
        if task and task["result"]:
            return json.loads(task["result"])
        return None

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

    def get(self, timeout=None):
        start_time = time.time()
        while True:
            if self.ready():
                if self.successful():
                    return self.result
                else:
                    raise Exception(self.error)
            if timeout and time.time() - start_time > timeout:
                raise TimeoutError("Timeout waiting for task to complete")
            time.sleep(0.1)

    def forget(self):
        self.backend.forget(self.task_id)

    def __repr__(self):
        return f"<AsyncResult(task_id='{self.task_id}', status='{self.status}')>"
