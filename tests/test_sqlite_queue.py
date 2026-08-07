"""
Tests for SQLiteQueue: all queue operations.
Covers: enqueue, enqueue_many, fetch_next, ack, fail, dead, reschedule,
        cancel, set_progress, cleanup.
"""
import json
import sqlite3
import time
import pytest
from datetime import timedelta

from pybgworker.sqlite_queue import SQLiteQueue
from pybgworker.utils import now, get_conn

TEST_DB = "test_suite.db"


def q():
    return SQLiteQueue(TEST_DB)


def _base_task(task_id="t1", name="test.task", status="queued",
               attempt=0, max_retries=3, countdown_secs=0):
    run_at = now() + timedelta(seconds=countdown_secs)
    return {
        "id": task_id, "name": name, "args": "[]", "kwargs": "{}",
        "status": status, "attempt": attempt, "max_retries": max_retries,
        "run_at": run_at.isoformat(), "priority": 5,
        "locked_by": None, "locked_at": None, "last_error": None,
        "result": None, "created_at": now().isoformat(),
        "updated_at": now().isoformat(), "finished_at": None,
        "progress": None, "idempotency_key": None,
    }


# ---------- enqueue ----------

def test_enqueue_inserts_row():
    queue = q()
    t = _base_task("e1")
    returned_id = queue.enqueue(t)
    assert returned_id == "e1"
    with get_conn(TEST_DB) as conn:
        row = conn.execute("SELECT id, status FROM tasks WHERE id='e1'").fetchone()
    assert row[0] == "e1"
    assert row[1] == "queued"


def test_enqueue_idempotency_returns_existing_id():
    queue = q()
    t1 = _base_task("e2")
    t1["idempotency_key"] = "idem-1"
    queue.enqueue(t1)

    t2 = _base_task("e3")
    t2["idempotency_key"] = "idem-1"
    returned = queue.enqueue(t2)

    assert returned == "e2"  # existing id
    with get_conn(TEST_DB) as conn:
        count = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
    assert count == 1


def test_enqueue_without_idempotency_key_always_inserts():
    queue = q()
    queue.enqueue(_base_task("e4"))
    queue.enqueue(_base_task("e5"))
    with get_conn(TEST_DB) as conn:
        count = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
    assert count == 2


# ---------- enqueue_many ----------

def test_enqueue_many_inserts_all():
    queue = q()
    tasks = [_base_task(f"em{i}") for i in range(5)]
    inserted = queue.enqueue_many(tasks)
    assert inserted == 5
    with get_conn(TEST_DB) as conn:
        count = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
    assert count == 5


def test_enqueue_many_empty_returns_zero():
    assert q().enqueue_many([]) == 0


def test_enqueue_many_skips_duplicates_with_idempotency():
    queue = q()
    t1 = _base_task("em6")
    t1["idempotency_key"] = "batch-idem"
    queue.enqueue(t1)

    t2 = _base_task("em7")
    t2["idempotency_key"] = "batch-idem"
    inserted = queue.enqueue_many([t2])
    assert inserted == 0  # duplicate skipped


# ---------- fetch_next ----------

def test_fetch_next_returns_queued_task():
    queue = q()
    queue.enqueue(_base_task("f1"))
    row = queue.fetch_next("worker-1")
    assert row is not None
    assert row["id"] == "f1"
    assert row["status"] == "running"
    assert row["locked_by"] == "worker-1"


def test_fetch_next_respects_run_at():
    queue = q()
    future_task = _base_task("f2", countdown_secs=3600)  # runs in 1 hour
    queue.enqueue(future_task)
    row = queue.fetch_next("worker-1")
    assert row is None  # not yet eligible


def test_fetch_next_respects_priority_order():
    queue = q()
    low = _base_task("f3")
    low["priority"] = 9
    high = _base_task("f4")
    high["priority"] = 1
    queue.enqueue(low)
    queue.enqueue(high)

    first = queue.fetch_next("worker-1")
    assert first["id"] == "f4"  # highest priority (lowest int)


def test_fetch_next_returns_none_when_empty():
    row = q().fetch_next("worker-1")
    assert row is None


def test_fetch_next_picks_up_retrying_task():
    queue = q()
    t = _base_task("f5", status="retrying")
    queue.enqueue(t)
    row = queue.fetch_next("worker-1")
    assert row is not None
    assert row["id"] == "f5"


# ---------- ack ----------

def test_ack_sets_success():
    queue = q()
    queue.enqueue(_base_task("a1"))
    queue.fetch_next("worker-1")
    queue.ack("a1")
    with get_conn(TEST_DB) as conn:
        status = conn.execute("SELECT status FROM tasks WHERE id='a1'").fetchone()[0]
    assert status == "success"


def test_ack_clears_locked_by():
    queue = q()
    queue.enqueue(_base_task("a2"))
    queue.fetch_next("worker-1")
    queue.ack("a2")
    with get_conn(TEST_DB) as conn:
        row = conn.execute("SELECT locked_by, locked_at FROM tasks WHERE id='a2'").fetchone()
    assert row[0] is None
    assert row[1] is None


def test_ack_sets_finished_at():
    queue = q()
    queue.enqueue(_base_task("a3"))
    queue.fetch_next("worker-1")
    queue.ack("a3")
    with get_conn(TEST_DB) as conn:
        finished = conn.execute("SELECT finished_at FROM tasks WHERE id='a3'").fetchone()[0]
    assert finished is not None


def test_ack_invalid_transition_raises():
    queue = q()
    queue.enqueue(_base_task("a4"))
    # Don't fetch — still queued. ack from queued should fail.
    with pytest.raises(ValueError, match="Invalid transition"):
        queue.ack("a4")


# ---------- fail ----------

def test_fail_sets_failed_status():
    queue = q()
    queue.enqueue(_base_task("fa1"))
    queue.fetch_next("worker-1")
    queue.fail("fa1", "something went wrong")
    with get_conn(TEST_DB) as conn:
        row = conn.execute("SELECT status, last_error FROM tasks WHERE id='fa1'").fetchone()
    assert row[0] == "failed"
    assert row[1] == "something went wrong"


def test_fail_invalid_transition_raises():
    queue = q()
    queue.enqueue(_base_task("fa2"))
    with pytest.raises(ValueError, match="Invalid transition"):
        queue.fail("fa2", "error")


# ---------- dead ----------

def test_dead_sets_dead_status():
    queue = q()
    queue.enqueue(_base_task("d1"))
    queue.fetch_next("worker-1")
    queue.dead("d1", "permanent failure")
    with get_conn(TEST_DB) as conn:
        row = conn.execute("SELECT status, last_error FROM tasks WHERE id='d1'").fetchone()
    assert row[0] == "dead"
    assert row[1] == "permanent failure"


# ---------- reschedule ----------

def test_reschedule_sets_retrying_and_increments_attempt():
    queue = q()
    queue.enqueue(_base_task("r1"))
    queue.fetch_next("worker-1")
    queue.reschedule("r1", delay=5)
    with get_conn(TEST_DB) as conn:
        row = conn.execute("SELECT status, attempt FROM tasks WHERE id='r1'").fetchone()
    assert row[0] == "retrying"
    assert row[1] == 1


def test_reschedule_sets_future_run_at():
    queue = q()
    queue.enqueue(_base_task("r2"))
    queue.fetch_next("worker-1")
    before = now()
    queue.reschedule("r2", delay=10)
    with get_conn(TEST_DB) as conn:
        run_at = conn.execute("SELECT run_at FROM tasks WHERE id='r2'").fetchone()[0]
    from datetime import datetime
    dt = datetime.fromisoformat(run_at)
    assert dt >= before


def test_reschedule_clears_lock():
    queue = q()
    queue.enqueue(_base_task("r3"))
    queue.fetch_next("worker-1")
    queue.reschedule("r3", delay=0)
    with get_conn(TEST_DB) as conn:
        row = conn.execute("SELECT locked_by, locked_at FROM tasks WHERE id='r3'").fetchone()
    assert row[0] is None
    assert row[1] is None


# ---------- cancel ----------

def test_cancel_queued_task():
    queue = q()
    queue.enqueue(_base_task("c1"))
    queue.cancel("c1")
    with get_conn(TEST_DB) as conn:
        status = conn.execute("SELECT status FROM tasks WHERE id='c1'").fetchone()[0]
    assert status == "cancelled"


def test_cancel_retrying_task():
    queue = q()
    t = _base_task("c2", status="retrying")
    queue.enqueue(t)
    queue.cancel("c2")
    with get_conn(TEST_DB) as conn:
        status = conn.execute("SELECT status FROM tasks WHERE id='c2'").fetchone()[0]
    assert status == "cancelled"


def test_cancel_running_task():
    queue = q()
    queue.enqueue(_base_task("c3"))
    queue.fetch_next("worker-1")
    queue.cancel("c3")
    with get_conn(TEST_DB) as conn:
        status = conn.execute("SELECT status FROM tasks WHERE id='c3'").fetchone()[0]
    assert status == "cancelled"


def test_cancel_already_success_raises():
    """Cancelling a terminal task raises ValueError from validate_transition."""
    queue = q()
    queue.enqueue(_base_task("c4"))
    queue.fetch_next("worker-1")
    queue.ack("c4")
    with pytest.raises(ValueError, match="Invalid transition"):
        queue.cancel("c4")


# ---------- set_progress ----------

def test_set_progress_stores_json():
    queue = q()
    queue.enqueue(_base_task("p1"))
    queue.set_progress("p1", 50, "half done")
    with get_conn(TEST_DB) as conn:
        progress = conn.execute("SELECT progress FROM tasks WHERE id='p1'").fetchone()[0]
    data = json.loads(progress)
    assert data["percent"] == 50
    assert data["message"] == "half done"


def test_set_progress_clamps_to_0_100():
    queue = q()
    queue.enqueue(_base_task("p2"))
    queue.set_progress("p2", 200)
    with get_conn(TEST_DB) as conn:
        p = json.loads(conn.execute("SELECT progress FROM tasks WHERE id='p2'").fetchone()[0])
    assert p["percent"] == 100

    queue.set_progress("p2", -10)
    with get_conn(TEST_DB) as conn:
        p = json.loads(conn.execute("SELECT progress FROM tasks WHERE id='p2'").fetchone()[0])
    assert p["percent"] == 0


def test_set_progress_no_message():
    queue = q()
    queue.enqueue(_base_task("p3"))
    queue.set_progress("p3", 75)
    with get_conn(TEST_DB) as conn:
        p = json.loads(conn.execute("SELECT progress FROM tasks WHERE id='p3'").fetchone()[0])
    assert p["message"] is None


# ---------- cleanup ----------

def test_cleanup_deletes_old_finished_tasks():
    queue = q()
    old_ts = (now() - timedelta(days=10)).isoformat()
    recent_ts = (now() - timedelta(hours=1)).isoformat()

    with get_conn(TEST_DB) as conn:
        for tid, ts in [("cl1", old_ts), ("cl2", recent_ts)]:
            conn.execute(
                "INSERT INTO tasks(id,name,args,kwargs,status,attempt,max_retries,run_at,"
                "priority,locked_by,locked_at,last_error,result,created_at,updated_at,"
                "finished_at,progress,idempotency_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (tid, "t", "[]", "{}", "success", 0, 0, now().isoformat(),
                 5, None, None, None, None, ts, ts, ts, None, None)
            )
        conn.commit()

    result = queue.cleanup(retention_days=5, vacuum=False)
    assert result["deleted_finished"] == 1

    with get_conn(TEST_DB) as conn:
        remaining = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
    assert remaining == 1


def test_cleanup_retention_zero_skips():
    queue = q()
    result = queue.cleanup(retention_days=0, vacuum=False)
    assert result["deleted"] == 0
    assert result["vacuumed"] is False


def test_cleanup_returns_correct_counts():
    queue = q()
    result = queue.cleanup(retention_days=30, vacuum=False)
    assert "deleted" in result
    assert "deleted_finished" in result
    assert "vacuumed" in result
    assert "locked" in result
