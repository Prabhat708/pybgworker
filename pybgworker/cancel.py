from .utils import get_conn, now
from .logger import log


def cancel(task_id):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT status FROM tasks WHERE id=?",
            (task_id,)
        ).fetchone()

        if not row:
            log("task_not_found", task_id=task_id)
            return

        if row[0] not in ("queued", "running", "retrying"):
            log("task_not_cancellable", task_id=task_id, status=row[0])
            return

    # Delegate to the queue's cancel() which calls validate_transition()
    # and keeps all cancel logic in a single place. This ensures cancel.py
    # stays in sync with any future state-machine changes in state.py.
    # Wrap in try/except for TOCTOU safety: the task could transition to a
    # terminal state (success/failed/dead) between the status check above
    # and the actual cancel — in which case validate_transition() raises
    # ValueError. We log it as a warning instead of crashing the CLI.
    from .sqlite_queue import SQLiteQueue
    try:
        SQLiteQueue().cancel(task_id)
    except ValueError:
        log("task_cancel_skipped", task_id=task_id,
            reason="task transitioned to terminal state before cancel completed")
        return

    log("task_cancelled", task_id=task_id)
