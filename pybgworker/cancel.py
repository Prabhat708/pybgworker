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

        conn.execute("""
            UPDATE tasks
            SET status='cancelled',
                finished_at=?,
                updated_at=?,
                locked_by=NULL,
                locked_at=NULL
            WHERE id=?
        """, (now().isoformat(), now().isoformat(), task_id))

        conn.commit()

    log("task_cancelled", task_id=task_id)
