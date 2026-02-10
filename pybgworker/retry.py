from .utils import get_conn, now
from .logger import log


def retry(task_id):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT status FROM tasks WHERE id=?",
            (task_id,)
        ).fetchone()

        if not row:
            log("task_not_found", task_id=task_id)
            return

        if row[0] not in ("failed", "dead"):
            log("task_not_failed", task_id=task_id)
            return

        conn.execute("""
            UPDATE tasks
            SET status='queued',
                attempt=0,
                last_error=NULL,
                updated_at=?
            WHERE id=?
        """, (now().isoformat(), task_id))

        conn.commit()

    log("task_requeued", task_id=task_id)
