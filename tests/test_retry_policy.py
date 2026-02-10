import os
from datetime import timedelta

import pytest

import pybgworker.config as config
import pybgworker.utils as utils
import pybgworker.worker as worker
from pybgworker.sqlite_queue import SQLiteQueue


TEST_DB = "test_pybgworker_policy.db"


@pytest.fixture(autouse=True)
def setup_db(monkeypatch):
    monkeypatch.setenv("PYBGWORKER_DB", TEST_DB)
    config.DB_PATH = TEST_DB
    utils.DB_PATH = TEST_DB

    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

    yield

    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


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
        "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
        ),
    )


def test_cleanup_task_ttl_deletes_queued_tasks():
    queue = SQLiteQueue()

    old_time = (utils.now() - timedelta(seconds=120)).isoformat()
    new_time = (utils.now() - timedelta(seconds=10)).isoformat()

    with utils.get_conn() as conn:
        _insert_task(conn, "old", "queued", old_time)
        _insert_task(conn, "new", "queued", new_time)
        conn.commit()

    result = queue.cleanup(
        retention_days=0,
        task_ttl_seconds=60,
        result_ttl_seconds=0,
        vacuum=False,
    )

    with utils.get_conn() as conn:
        remaining = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]

    assert result["deleted_expired"] == 1
    assert remaining == 1


def test_cleanup_result_ttl_clears_results_only():
    queue = SQLiteQueue()

    old_finished = (utils.now() - timedelta(seconds=120)).isoformat()

    with utils.get_conn() as conn:
        _insert_task(
            conn,
            "finished",
            "success",
            old_finished,
            finished_at=old_finished,
            result='{"ok": true}',
        )
        conn.commit()

    result = queue.cleanup(
        retention_days=0,
        task_ttl_seconds=0,
        result_ttl_seconds=60,
        vacuum=False,
    )

    with utils.get_conn() as conn:
        row = conn.execute(
            "SELECT result FROM tasks WHERE id='finished'"
        ).fetchone()

    assert result["cleared_results"] == 1
    assert row[0] is None
