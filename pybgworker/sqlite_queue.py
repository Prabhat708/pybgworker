import sqlite3
from datetime import timedelta
from .queue import BaseQueue
from .config import DB_PATH, WORKER_TIMEOUT
from .utils import now, get_conn


class SQLiteQueue(BaseQueue):

    def __init__(self, db_path=DB_PATH):
        self._init_db()

    # ---------------- DB init ----------------

    def _init_db(self):
        with get_conn() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                name TEXT,
                args TEXT,
                kwargs TEXT,
                status TEXT,
                attempt INTEGER,
                max_retries INTEGER,
                run_at TEXT,
                priority INTEGER DEFAULT 5,
                locked_by TEXT,
                locked_at TEXT,
                last_error TEXT,
                result TEXT,
                created_at TEXT,
                updated_at TEXT,
                finished_at TEXT
            )
            """)

            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_priority_runat
            ON tasks(status, priority, run_at)
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                name TEXT PRIMARY KEY,
                last_seen TEXT
            )
            """)

            conn.commit()

    # ---------------- enqueue ----------------

    def enqueue(self, task):
        with get_conn() as conn:
            conn.execute("""
                INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, tuple(task.values()))
            conn.commit()

    # ---------------- atomic fetch ----------------

    def fetch_next(self, worker):
        stale_time = (now() - timedelta(seconds=WORKER_TIMEOUT)).isoformat()

        with get_conn() as conn:
            conn.row_factory = sqlite3.Row

            row = conn.execute("""
                UPDATE tasks
                SET status='running',
                    locked_by=?,
                    locked_at=?,
                    updated_at=?
                WHERE id = (
                    SELECT t.id FROM tasks t
                    LEFT JOIN workers w ON t.locked_by = w.name
                    WHERE
                        (
                            t.status IN ('queued','retrying')
                            OR
                            (t.status='running' AND w.last_seen < ?)
                        )
                    AND t.run_at <= ?
                    ORDER BY t.priority ASC, t.run_at ASC
                    LIMIT 1
                )
                RETURNING *
            """, (
                worker,
                now().isoformat(),
                now().isoformat(),
                stale_time,
                now().isoformat()
            )).fetchone()

            conn.commit()
            return dict(row) if row else None

    # ---------------- ack ----------------

    def ack(self, task_id):
        with get_conn() as conn:
            conn.execute("""
                UPDATE tasks
                SET status='success',
                    finished_at=?,
                    updated_at=?,
                    locked_by=NULL,
                    locked_at=NULL
                WHERE id=?
            """, (now().isoformat(), now().isoformat(), task_id))
            conn.commit()

    # ---------------- fail ----------------

    def fail(self, task_id, error):
        with get_conn() as conn:
            conn.execute("""
                UPDATE tasks
                SET status='failed',
                    last_error=?,
                    finished_at=?,
                    updated_at=?,
                    locked_by=NULL,
                    locked_at=NULL
                WHERE id=?
            """, (error, now().isoformat(), now().isoformat(), task_id))
            conn.commit()

    # ---------------- dead ----------------

    def dead(self, task_id, error):
        with get_conn() as conn:
            conn.execute("""
                UPDATE tasks
                SET status='dead',
                    last_error=?,
                    finished_at=?,
                    updated_at=?,
                    locked_by=NULL,
                    locked_at=NULL
                WHERE id=?
            """, (error, now().isoformat(), now().isoformat(), task_id))
            conn.commit()

    # ---------------- retry ----------------

    def reschedule(self, task_id, delay):
        run_at = now() + timedelta(seconds=delay)
        with get_conn() as conn:
            conn.execute("""
                UPDATE tasks
                SET status='retrying',
                    attempt=attempt+1,
                    run_at=?,
                    updated_at=?,
                    locked_by=NULL,
                    locked_at=NULL
                WHERE id=?
            """, (run_at.isoformat(), now().isoformat(), task_id))
            conn.commit()

    # ---------------- cancel ----------------

    def cancel(self, task_id):
        with get_conn() as conn:
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

    # ---------------- maintenance ----------------

    def cleanup(
        self,
        retention_days,
        vacuum=True,
    ):
        if retention_days <= 0:
            return {
                "deleted": 0,
                "deleted_finished": 0,
                "vacuumed": False,
                "locked": False,
            }

        deleted_finished = 0
        locked = False

        retention_cutoff = None

        if retention_days > 0:
            retention_cutoff = (now() - timedelta(days=retention_days)).isoformat()

        with get_conn() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError:
                locked = True
            else:
                if retention_cutoff:
                    cur = conn.execute("""
                        DELETE FROM tasks
                        WHERE finished_at IS NOT NULL
                        AND finished_at < ?
                    """, (retention_cutoff,))
                    deleted_finished = cur.rowcount

                conn.commit()

        deleted = deleted_finished

        vacuumed = False
        if vacuum and deleted > 0 and not locked:
            with get_conn() as conn:
                try:
                    conn.execute("VACUUM")
                    conn.commit()
                    vacuumed = True
                except sqlite3.OperationalError:
                    vacuumed = False

        return {
            "deleted": deleted,
            "deleted_finished": deleted_finished,
            "vacuumed": vacuumed,
            "locked": locked,
        }
