import time
import json
from .state import TaskState
from .backends import BaseBackend, SQLiteBackend


class TaskFailedError(Exception):
    """Raised by AsyncResult.get() when a task ended in FAILED or DEAD state.

    Attributes:
        exception_class: The name of the original exception type that caused the
            failure, as stored in the task's ``last_error`` field (may be None
            if not recorded).
        task_id: The task ID that failed.
        state: The terminal ``TaskState`` value (``"failed"`` or ``"dead"``).
    """

    def __init__(self, message, *, exception_class=None, task_id=None, state=None):
        super().__init__(message)
        self.exception_class = exception_class
        self.task_id = task_id
        self.state = state


class TaskCancelledError(TaskFailedError):
    """Raised by AsyncResult.get() when a task was cancelled before completion."""


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
        """Return True if the task has reached any terminal state.

        Terminal states are: SUCCESS, FAILED, DEAD, and CANCELLED.
        Previously only SUCCESS and FAILED were treated as terminal, which
        caused get() to poll indefinitely for cancelled or dead tasks.
        """
        return self.status in (
            TaskState.SUCCESS.value,
            TaskState.FAILED.value,
            TaskState.DEAD.value,
            TaskState.CANCELLED.value,
        )

    def successful(self):
        return self.status == TaskState.SUCCESS.value

    def failed(self):
        """Return True only if the task ended with a FAILED status.

        Use dead() to test for the DEAD state (all retries exhausted) or
        cancelled() for the CANCELLED state.
        """
        return self.status == TaskState.FAILED.value

    def dead(self):
        """Return True if the task exhausted all retries and is permanently dead."""
        return self.status == TaskState.DEAD.value

    def cancelled(self):
        """Return True if the task was cancelled before or during execution."""
        return self.status == TaskState.CANCELLED.value

    def get(self, timeout=None):
        """Block until the task reaches a terminal state and return the result.

        Raises:
            TaskCancelledError: if the task was cancelled.
            TaskFailedError: if the task failed or is dead. The ``state``
                attribute indicates which terminal state was reached.
            TimeoutError: if ``timeout`` seconds elapse before the task
                completes.
        """
        start_time = time.time()
        while True:
            if self.ready():
                if self.successful():
                    return self.result

                current_status = self.status

                if current_status == TaskState.CANCELLED.value:
                    raise TaskCancelledError(
                        f"Task {self.task_id} was cancelled",
                        task_id=self.task_id,
                        state=current_status,
                    )

                # FAILED or DEAD
                raise TaskFailedError(
                    self.error or f"Task ended with status '{current_status}'",
                    task_id=self.task_id,
                    state=current_status,
                )

            if timeout and time.time() - start_time > timeout:
                raise TimeoutError("Timeout waiting for task to complete")
            time.sleep(0.1)

    def forget(self):
        self.backend.forget(self.task_id)

    def __repr__(self):
        return f"<AsyncResult(task_id='{self.task_id}', status='{self.status}')>"
