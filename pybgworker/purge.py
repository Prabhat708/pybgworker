from .utils import get_conn


def purge():
    with get_conn() as conn:
        cursor = conn.execute("""
            DELETE FROM tasks
            WHERE status IN ('queued', 'retrying')
        """)

        deleted = cursor.rowcount
        conn.commit()

    print(f"🧹 Purged {deleted} queued tasks")
