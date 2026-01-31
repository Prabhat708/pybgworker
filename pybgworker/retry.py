from .utils import get_conn, now


def retry(task_id):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT status FROM tasks WHERE id=?",
            (task_id,)
        ).fetchone()

        if not row:
            print("❌ Task not found")
            return

        if row[0] != "failed":
            print("⚠ Task is not failed")
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

    print("🔁 Task requeued")
