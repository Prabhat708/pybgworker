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

        if row[0] != "running":
            log("task_not_running", task_id=task_id)
            return

        conn.execute("""
            UPDATE tasks
            SET status='cancelled',
                finished_at=?,
                updated_at=?
            WHERE id=?
        """, (now().isoformat(), now().isoformat(), task_id))

        conn.commit()

    log("task_cancelled", task_id=task_id)
