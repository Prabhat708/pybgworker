"""
Tests for @task decorator and delay() / delay_many() behaviour.
Covers: registration, JSON validation, countdown, eta, priority,
        idempotency_key, delay_many batch insert.
"""
import json
import time
import pytest
from datetime import datetime, timezone, timedelta

from pybgworker import task, AsyncResult
from pybgworker.sqlite_queue import SQLiteQueue
from pybgworker.utils import now, get_conn

TEST_DB = "test_suite.db"


# ---------- task registration ----------

@task(name="task_test.add")
def add(a, b):
    return a + b


@task(name="task_test.echo")
def echo(msg):
    return msg


def test_task_registration():
    from pybgworker.task import TASK_REGISTRY
    assert "task_test.add" in TASK_REGISTRY
    assert "task_test.echo" in TASK_REGISTRY
    assert TASK_REGISTRY["task_test.add"]["func"] is add


def test_task_has_delay_and_delay_many():
    assert callable(add.delay)
    assert callable(add.delay_many)


def test_task_name_exposed():
    assert add._task_name == "task_test.add"


# ---------- delay() ----------

def test_delay_inserts_row():
    q = SQLiteQueue(TEST_DB)
    res = add.delay(1, 2)
    assert res.task_id is not None
    assert res.status == "queued"


def test_delay_serializes_args():
    res = add.delay(10, 20)
    with get_conn(TEST_DB) as conn:
        row = conn.execute("SELECT args, kwargs FROM tasks WHERE id=?", (res.task_id,)).fetchone()
    assert json.loads(row[0]) == [10, 20]
    assert json.loads(row[1]) == {}


def test_delay_with_kwargs():
    @task(name="task_test.kw")
    def kw_task(a, b=0):
        return a + b

    res = kw_task.delay(5, b=3)
    with get_conn(TEST_DB) as conn:
        row = conn.execute("SELECT args, kwargs FROM tasks WHERE id=?", (res.task_id,)).fetchone()
    assert json.loads(row[0]) == [5]
    assert json.loads(row[1]) == {"b": 3}


def test_delay_json_validation_raises_clear_error():
    """Non-serializable arg must raise TypeError with helpful message."""
    with pytest.raises(TypeError, match="JSON-serializable"):
        add.delay(datetime.now(timezone.utc), 2)


def test_delay_non_serializable_return_type_in_error_message():
    """The error message must name the task."""
    with pytest.raises(TypeError, match="task_test.add"):
        add.delay(object(), 2)


def test_delay_countdown():
    future = now() + timedelta(seconds=30)
    res = add.delay(1, 2, countdown=30)
    with get_conn(TEST_DB) as conn:
        row = conn.execute("SELECT run_at FROM tasks WHERE id=?", (res.task_id,)).fetchone()
    run_at = datetime.fromisoformat(row[0])
    assert run_at >= future - timedelta(seconds=1)


def test_delay_countdown_zero_runs_immediately():
    """countdown=0 must NOT be treated as 'no countdown' (falsy-zero fix)."""
    before = now()
    res = add.delay(1, 2, countdown=0)
    with get_conn(TEST_DB) as conn:
        row = conn.execute("SELECT run_at FROM tasks WHERE id=?", (res.task_id,)).fetchone()
    run_at = datetime.fromisoformat(row[0])
    # run_at should be very close to before (within 1 second)
    assert run_at <= before + timedelta(seconds=1)


def test_delay_eta():
    eta = now() + timedelta(minutes=5)
    res = add.delay(1, 2, eta=eta)
    with get_conn(TEST_DB) as conn:
        row = conn.execute("SELECT run_at FROM tasks WHERE id=?", (res.task_id,)).fetchone()
    run_at = datetime.fromisoformat(row[0])
    assert abs((run_at - eta).total_seconds()) < 1


def test_delay_priority_stored():
    res_high = add.delay(1, 2, priority=1)
    res_low = add.delay(1, 2, priority=9)
    with get_conn(TEST_DB) as conn:
        p_high = conn.execute("SELECT priority FROM tasks WHERE id=?", (res_high.task_id,)).fetchone()[0]
        p_low  = conn.execute("SELECT priority FROM tasks WHERE id=?", (res_low.task_id,)).fetchone()[0]
    assert p_high == 1
    assert p_low == 9


def test_delay_returns_asyncresult():
    res = add.delay(1, 2)
    assert isinstance(res, AsyncResult)
    assert res.task_id is not None


def test_delay_idempotency_key_deduplicates():
    res1 = add.delay(1, 2, idempotency_key="unique-key-1")
    res2 = add.delay(1, 2, idempotency_key="unique-key-1")
    assert res1.task_id == res2.task_id
    with get_conn(TEST_DB) as conn:
        count = conn.execute("SELECT COUNT(*) FROM tasks WHERE idempotency_key='unique-key-1'").fetchone()[0]
    assert count == 1


def test_delay_different_idempotency_keys_create_separate_tasks():
    res1 = add.delay(1, 2, idempotency_key="key-a")
    res2 = add.delay(1, 2, idempotency_key="key-b")
    assert res1.task_id != res2.task_id


# ---------- delay_many() ----------

def test_delay_many_inserts_all():
    results = add.delay_many([
        ((1, 2), {}),
        ((3, 4), {}),
        ((5, 6), {}),
    ])
    assert len(results) == 3
    assert all(isinstance(r, AsyncResult) for r in results)
    with get_conn(TEST_DB) as conn:
        count = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
    assert count == 3


def test_delay_many_returns_results_in_order():
    pairs = [((i, i), {}) for i in range(5)]
    results = add.delay_many(pairs)
    ids = [r.task_id for r in results]
    assert len(set(ids)) == 5  # all unique


def test_delay_many_priority_applied():
    results = add.delay_many([((1, 2), {}), ((3, 4), {})], priority=2)
    for r in results:
        with get_conn(TEST_DB) as conn:
            p = conn.execute("SELECT priority FROM tasks WHERE id=?", (r.task_id,)).fetchone()[0]
        assert p == 2


def test_delay_many_empty_list_returns_empty():
    results = add.delay_many([])
    assert results == []
