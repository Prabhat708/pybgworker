import time
import os
import sqlite3
import pytest
import gc

from pybgworker import task, AsyncResult
from pybgworker.sqlite_queue import SQLiteQueue
from pybgworker.backends import SQLiteBackend
from pybgworker.worker import run_worker
from pybgworker.config import DB_PATH

# --- TEST SETUP ---

TEST_DB = "test_pybgworker.db"

def _safe_remove(path, retries=5, delay=0.05):
    for _ in range(retries):
        try:
            os.remove(path)
            return
        except PermissionError:
            gc.collect()
            time.sleep(delay)
    if os.path.exists(path):
        os.remove(path)

@pytest.fixture(autouse=True)
def setup_test_db(monkeypatch):
    """
    Use a fresh test database for each test
    """
    monkeypatch.setenv("PYBGWORKER_DB", TEST_DB)
    
    from pybgworker import config, utils
    config.DB_PATH = TEST_DB
    utils.DB_PATH = TEST_DB
    
    from pybgworker.task import queue, backend
    queue.db_path = TEST_DB
    backend.db_path = TEST_DB

    # Remove old test DB if exists
    if os.path.exists(TEST_DB):
        _safe_remove(TEST_DB)

    # Re-initialize tables for the global queue
    queue._init_db()

    yield

    if os.path.exists(TEST_DB):
        _safe_remove(TEST_DB)


# --- TEST TASK ---

attempt = {"count": 0}

@task(name="tests.flaky_task", retries=3, retry_delay=1)
def flaky_task():
    """
    Fails 3 times, succeeds on 4th attempt
    """
    attempt["count"] += 1

    if attempt["count"] <= 3:
        raise Exception("Temporary failure")

    return "SUCCESS"


# --- TEST CASE ---

def test_task_retries_and_succeeds():
    """
    Verify:
    - task retries 3 times
    - task succeeds on final attempt
    """

    # Submit task
    result = flaky_task.delay()

    # Run worker loop manually (limited iterations)
    queue = SQLiteQueue(TEST_DB)
    backend = SQLiteBackend(TEST_DB)

    for _ in range(6):
        task_row = queue.fetch_next("test-worker")

        if task_row:
            try:
                flaky_task()
                backend.store_result(task_row["id"], "SUCCESS")
                queue.ack(task_row["id"])
            except Exception:
                if task_row["attempt"] < task_row["max_retries"]:
                    queue.reschedule(task_row["id"], 0)
                else:
                    queue.fail(task_row["id"], "FAILED")

        time.sleep(0.2)

    # Check final task state
    final = AsyncResult(result.task_id, backend=backend)

    assert final.ready() is True
    assert final.successful() is True
    assert final.result == "SUCCESS"
