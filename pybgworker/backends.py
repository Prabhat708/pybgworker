from abc import ABC, abstractmethod
import sqlite3
import json
from .config import DB_PATH

class BaseBackend(ABC):
    @abstractmethod
    def get_task(self, task_id):
        pass

    @abstractmethod
    def store_result(self, task_id, result):
        pass

    @abstractmethod
    def forget(self, task_id):
        pass


class SQLiteBackend(BaseBackend):
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path

    def get_task(self, task_id):
        from .utils import get_conn
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM tasks WHERE id=?",
                (task_id,)
            ).fetchone()
            return dict(row) if row else None

    def store_result(self, task_id, result):
        from .utils import get_conn
        with get_conn() as conn:
            conn.execute(
                "UPDATE tasks SET result=? WHERE id=?",
                (json.dumps(result), task_id)
            )
            conn.commit()

    def forget(self, task_id):
        from .utils import get_conn
        with get_conn() as conn:
            conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
            conn.commit()
