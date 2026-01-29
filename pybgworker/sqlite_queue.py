import sqlite3
from datetime import timedelta
from .queue import BaseQueue
from .state import TaskState, validate_transition
from .config import DB_PATH, LOCK_TIMEOUT
from .utils import now

class SQLiteQueue(BaseQueue):

    def __init__(self, db_path=DB_PATH):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            name TEXT,
            args TEXT,
            kwargs TEXT,
            status TEXT,
            attempt INTEGER,
            max_retries INTEGER,
            run_at TEXT,
            locked_by TEXT,
            locked_at TEXT,
            last_error TEXT,
            result TEXT,
            created_at TEXT,
            updated_at TEXT,
            finished_at TEXT
        )
        """)
        self.conn.commit()

    def enqueue(self, task):
        self.conn.execute(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            tuple(task.values())
        )
        self.conn.commit()

    def fetch_next(self, worker):
        stale = (now() - timedelta(seconds=LOCK_TIMEOUT)).isoformat()

        row = self.conn.execute("""
            SELECT * FROM tasks
            WHERE (
                status IN ('queued','retrying')
                OR (status='running' AND locked_at < ?)
            )
            AND run_at <= ?
            ORDER BY run_at
            LIMIT 1
        """, (stale, now().isoformat())).fetchone()

        if not row:
            return None

        validate_transition(row["status"], TaskState.RUNNING.value)

        updated = self.conn.execute("""
            UPDATE tasks
            SET status='running', locked_by=?, locked_at=?, updated_at=?
            WHERE id=? AND status!='success'
        """, (worker, now().isoformat(), now().isoformat(), row["id"]))

        self.conn.commit()
        return dict(row) if updated.rowcount else None

    def ack(self, task_id, result=None):
        self.conn.execute("""
            UPDATE tasks
            SET status='success',
                result=?,
                finished_at=?,
                updated_at=?
            WHERE id=?
        """, (result, now().isoformat(), now().isoformat(), task_id))
        self.conn.commit()

    def fail(self, task_id, error):
        self.conn.execute("""
            UPDATE tasks
            SET status='failed',
                last_error=?,
                finished_at=?,
                updated_at=?
            WHERE id=?
        """, (error, now().isoformat(), now().isoformat(), task_id))
        self.conn.commit()

    def reschedule(self, task_id, delay):
        run_at = now() + timedelta(seconds=delay)
        self.conn.execute("""
            UPDATE tasks
            SET status='retrying',
                attempt=attempt+1,
                run_at=?,
                updated_at=?
            WHERE id=?
        """, (run_at.isoformat(), now().isoformat(), task_id))
        self.conn.commit()
