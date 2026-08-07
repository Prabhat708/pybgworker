"""
progress.py — in-task progress reporting helper.

Call ``set_progress()`` from inside a running task body to update a
progress percentage (and optional message) that callers can read back
via ``AsyncResult.progress``.

Example::

    from pybgworker.progress import set_progress

    @task(name="tasks.process_file")
    def process_file(path):
        chunks = list(read_chunks(path))
        total = len(chunks)
        for i, chunk in enumerate(chunks):
            process(chunk)
            set_progress(int((i + 1) / total * 100), f"chunk {i+1}/{total}")

The worker sets the ``PYBGWORKER_CURRENT_TASK_ID`` environment variable
in the child process before calling the task function.  If this variable
is absent (e.g. the task is called directly in tests), ``set_progress``
is a no-op so existing tests keep working without modification.
"""

import os


def set_progress(percent: int, message: str = None) -> None:
    """Update the progress of the currently-running task.

    Args:
        percent: Completion percentage, 0–100 (clamped automatically).
        message: Optional human-readable status message, e.g. ``"chunk 4/10"``.

    Returns:
        None.  Silently does nothing if called outside a worker subprocess
        (i.e. when ``PYBGWORKER_CURRENT_TASK_ID`` is not set in the environment).
    """
    task_id = os.environ.get("PYBGWORKER_CURRENT_TASK_ID")
    if not task_id:
        return

    # Instantiate lazily using the current config.DB_PATH so that any
    # runtime override of PYBGWORKER_DB (e.g. in tests) is respected.
    # A module-level singleton would bake in the DB path at import time,
    # causing set_progress() to silently write to the wrong database when
    # the path is changed after the module is first imported.
    from .config import DB_PATH
    from .sqlite_queue import SQLiteQueue
    _queue = SQLiteQueue(DB_PATH)

    percent = max(0, min(100, int(percent)))
    _queue.set_progress(task_id, percent, message)
