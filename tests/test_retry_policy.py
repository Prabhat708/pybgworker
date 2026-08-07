import os
import gc
import time
from datetime import timedelta

import pytest

import pybgworker.config as config
import pybgworker.utils as utils
import pybgworker.worker as worker
from pybgworker.sqlite_queue import SQLiteQueue


TEST_DB = "test_pybgworker_policy.db"

def _safe_remove(path, retries=5, delay=0.05):
    for _ in range(retries):
        try:
            os.remove(path)
            return
        except PermissionError:
            # give SQLite time to release file handles on Windows
            gc.collect()
            time.sleep(delay)
    os.remove(path)


@pytest.fixture(autouse=True)
def setup_db(monkeypatch):
    monkeypatch.setenv("PYBGWORKER_DB", TEST_DB)
    config.DB_PATH = TEST_DB
    utils.DB_PATH = TEST_DB

    if os.path.exists(TEST_DB):
        _safe_remove(TEST_DB)

    yield

    if os.path.exists(TEST_DB):
        _safe_remove(TEST_DB)


def test_compute_retry_delay_backoff_and_jitter(monkeypatch):
    task = {"attempt": 2}
    meta = {
        "retry_delay": 1,
        "retry_backoff": True,
        "retry_backoff_factor": 2,
        "retry_max_delay": None,
        "retry_jitter": 0.5,
    }

    monkeypatch.setattr(worker.random, "uniform", lambda a, b: a)

    delay = worker.compute_retry_delay(task, meta)

    # base=1, factor=2, attempt=2 => 4; jitter=0.5 => jitter_amount=2; uniform returns -2
    assert delay == 2


def test_compute_retry_delay_negative_base_and_cap(monkeypatch):
    task = {"attempt": 3}
    meta = {
        "retry_delay": -5,
        "retry_backoff": True,
        "retry_backoff_factor": 2,
        "retry_max_delay": 10,
        "retry_jitter": 3,
    }

    monkeypatch.setattr(worker.random, "uniform", lambda a, b: b)



def test_compute_retry_delay_backoff_and_jitter(monkeypatch):
    task = {"attempt": 2}
    meta = {
        "retry_delay": 1,
        "retry_backoff": True,
        "retry_backoff_factor": 2,
        "retry_max_delay": None,
        "retry_jitter": 0.5,
    }

    monkeypatch.setattr(worker.random, "uniform", lambda a, b: a)

    delay = worker.compute_retry_delay(task, meta)

    # base=1, factor=2, attempt=2 => 4; jitter=0.5 => jitter_amount=2; uniform returns -2
    assert delay == 2


def test_compute_retry_delay_negative_base_and_cap(monkeypatch):
    task = {"attempt": 3}
    meta = {
        "retry_delay": -5,
        "retry_backoff": True,
        "retry_backoff_factor": 2,
        "retry_max_delay": 10,
        "retry_jitter": 3,
    }

    monkeypatch.setattr(worker.random, "uniform", lambda a, b: b)

    delay = worker.compute_retry_delay(task, meta)

    # negative base becomes 0, backoff keeps 0, jitter adds up to +3
    assert delay == 3


def _insert_task(conn, task_id, status, created_at, finished_at=None, result=None):
    conn.execute(
        "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            task_id,
            "test.task",
            "[]",
            "{}",
            status,
            0,
            0,
            created_at,
            5,
            None,
            None,
            None,
            result,
            created_at,
            created_at,
            finished_at,
            None,   # progress
            None,   # idempotency_key
        ),
    )


def test_cleanup_retention_deletes_finished_tasks():
    queue = SQLiteQueue()

    old_finished = (utils.now() - timedelta(days=2)).isoformat()
    new_finished = (utils.now() - timedelta(hours=2)).isoformat()

    with utils.get_conn() as conn:
        _insert_task(
            conn,
            "old",
            "success",
            old_finished,
            finished_at=old_finished,
            result='{"ok": true}',
        )
        _insert_task(
            conn,
            "new",
            "success",
            new_finished,
            finished_at=new_finished,
            result='{"ok": true}',
        )
        conn.commit()

    result = queue.cleanup(
        retention_days=1,
        vacuum=False,
    )

    with utils.get_conn() as conn:
        remaining = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]

    assert result["deleted_finished"] == 1
    assert remaining == 1
