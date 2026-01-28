import sqlite3
from .queue import BaseQueue
from .state import TaskState
from .config import DB_PATH
from .utils import now

class SQLiteQueue(BaseQueue):

    def __init__(self, db_path: str = DB_PATH):
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
            retries_left INTEGER,
            run_at TEXT,
            locked_by TEXT,
            locked_at TEXT,
            last_error TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """)
        self.conn.commit()

    def enqueue(self, task: dict):
        self.conn.execute(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            tuple(task.values())
        )
        self.conn.commit()

    def fetch_next(self, worker_name: str):
        # Placeholder: locking logic will go here
        pass

    def ack(self, task_id: str):
        self.conn.execute(
            "UPDATE tasks SET status=?, updated_at=? WHERE id=?",
            (TaskState.SUCCESS.value, now(), task_id)
        )
        self.conn.commit()

    def fail(self, task_id: str, error: str):
        self.conn.execute(
            "UPDATE tasks SET status=?, last_error=?, updated_at=? WHERE id=?",
            (TaskState.FAILED.value, error, now(), task_id)
        )
        self.conn.commit()

    def reschedule(self, task_id: str, run_at):
        self.conn.execute(
            "UPDATE tasks SET status=?, run_at=?, updated_at=? WHERE id=?",
            (TaskState.RETRYING.value, run_at, now(), task_id)
        )
        self.conn.commit()
