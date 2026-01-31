from .utils import get_conn, now


def cancel(task_id):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT status FROM tasks WHERE id=?",
            (task_id,)
        ).fetchone()

        if not row:
            print("❌ Task not found")
            return

        if row[0] != "running":
            print("⚠ Task is not running")
            return

        conn.execute("""
            UPDATE tasks
            SET status='cancelled',
                finished_at=?,
                updated_at=?
            WHERE id=?
        """, (now().isoformat(), now().isoformat(), task_id))

        conn.commit()

    print("🛑 Task cancelled")
