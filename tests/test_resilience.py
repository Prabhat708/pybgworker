import sqlite3
import pytest
from pybgworker.sqlite_queue import SQLiteQueue
from pybgworker.task import task

TEST_DB = "test_resilience.db"

@task(name="tests.resilience.task")
def res_task():
    return "ok"

def test_enqueue_resilience(monkeypatch):
    queue = SQLiteQueue(TEST_DB)
    queue._init_db()

    # Mock the execute method to raise OperationalError
    original_connect = sqlite3.connect

    class LockedConnection:
        def __init__(self, *args, **kwargs):
            self.conn = original_connect(*args, **kwargs)
        
        def execute(self, *args, **kwargs):
            if "INSERT" in args[0]:
                raise sqlite3.OperationalError("database is locked")
            return self.conn.execute(*args, **kwargs)
            
        def commit(self):
            self.conn.commit()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.conn.close()

        def close(self):
            self.conn.close()

    monkeypatch.setattr(sqlite3, "connect", LockedConnection)

    with pytest.raises(sqlite3.OperationalError):
        queue.enqueue({
            "id": "1",
            "name": "res_task",
            "args": "[]",
            "kwargs": "{}",
            "status": "queued",
            "attempt": 0,
            "max_retries": 3,
            "run_at": "2026-01-01T00:00:00",
            "priority": 5,
            "locked_by": None,
            "locked_at": None,
            "last_error": None,
            "result": None,
            "created_at": "2026-01-01T00:00:00",
            "updated_at": "2026-01-01T00:00:00",
            "finished_at": None,
            "progress": None,
            "idempotency_key": None
        })
