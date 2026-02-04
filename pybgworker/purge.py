from .utils import get_conn
from .logger import log


def purge():
    with get_conn() as conn:
        cursor = conn.execute("""
            DELETE FROM tasks
            WHERE status IN ('queued', 'retrying')
        """)

        deleted = cursor.rowcount
        conn.commit()

    log("purged_tasks", count=deleted)
