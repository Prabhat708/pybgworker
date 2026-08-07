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
    """Handle for inspecting a task's status, result, error, and progress.

    Returned by every ``.delay()`` and ``.delay_many()`` call.

    Attributes:
        task_id (str): The unique task identifier.

    Properties:
        status:     Current task state string (``"queued"``, ``"running"``,
                    ``"success"``, ``"failed"``, ``"dead"``, ``"cancelled"``).
        result:     Deserialized return value when ``status == "success"``,
                    else ``None``.
        error:      Raw error/traceback string for failed or dead tasks,
                    else ``None``.
        progress:   Dict ``{"percent": int, "message": str|None}`` written by
                    ``set_progress()``, or ``None`` if not yet reported.

    Methods:
        ready()       → bool  — True for any terminal state.
        successful()  → bool  — True only for ``"success"``.
        failed()      → bool  — True only for ``"failed"``.
        dead()        → bool  — True only for ``"dead"``.
        cancelled()   → bool  — True only for ``"cancelled"``.
        get(timeout)  — Block until terminal; return result or raise.
        forget()      — Delete the task row from the database.
    """

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
        """Current task state string."""
        task = self._fetch()
        return task["status"] if task else None

    @property
    def result(self):
        """Deserialized return value when ``status == "success"``, else ``None``."""
        task = self._fetch()
        if task and task["result"]:
            return json.loads(task["result"])
        return None

    @property
    def error(self):
        """Raw error/traceback string for failed or dead tasks, else ``None``."""
        task = self._fetch()
        return task["last_error"] if task else None

    @property
    def progress(self):
        """Most recent progress snapshot written by ``set_progress()``.

        Returns a dict ``{"percent": int, "message": str | None}`` while
        the task is running, or ``None`` if no progress has been reported yet.

        Example::

            res = process_file.delay(path)
            while not res.ready():
                p = res.progress
                if p:
                    print(f"{p['percent']}% — {p['message']}")
                time.sleep(0.5)
        """
        task = self._fetch()
        if task and task.get("progress"):
            return json.loads(task["progress"])
        return None

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
        """Return True only if the task completed with status ``"success"``."""
        return self.status == TaskState.SUCCESS.value

    def failed(self):
        """Return True only if the task ended with status ``"failed"``.

        Use :meth:`dead` to test for the DEAD state (all retries exhausted)
        or :meth:`cancelled` for the CANCELLED state.
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

        Polls every 100 ms.  Useful for simple scripts that need to wait for a
        result without setting up callbacks or polling manually.

        Args:
            timeout (float | None): Maximum seconds to wait.  ``None`` blocks
                indefinitely.

        Returns:
            The task's return value (same as ``self.result``).

        Raises:
            TaskCancelledError: if the task was cancelled.
            TaskFailedError: if the task failed or is dead.  The ``state``
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
        """Delete this task's row entirely from the database.

        Useful for cleaning up one-off or sensitive results without waiting
        for the scheduled retention cleanup.  After calling ``forget()``,
        all further calls to properties on this ``AsyncResult`` will return
        ``None`` (the row no longer exists).
        """
        self.backend.forget(self.task_id)

    def __repr__(self):
        return f"<AsyncResult(task_id='{self.task_id}', status='{self.status}')>"
