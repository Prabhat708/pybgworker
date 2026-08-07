"""
Tests for AsyncResult: all properties, helper methods, get(), forget().
"""
import json
import time
import pytest
from datetime import timezone

from pybgworker import AsyncResult
from pybgworker.result import TaskFailedError, TaskCancelledError
from pybgworker.sqlite_queue import SQLiteQueue
from pybgworker.backends import SQLiteBackend
from pybgworker.utils import now, get_conn

TEST_DB = "test_suite.db"


def _insert(task_id, status, result=None, error=None, attempt=0, max_retries=3,
            progress=None):
    with get_conn(TEST_DB) as conn:
        conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        conn.execute(
            "INSERT INTO tasks(id,name,args,kwargs,status,attempt,max_retries,run_at,"
            "priority,locked_by,locked_at,last_error,result,created_at,updated_at,"
            "finished_at,progress,idempotency_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (task_id, "test.task", "[]", "{}", status, attempt, max_retries,
             now().isoformat(), 5, None, None, error,
             json.dumps(result) if result is not None else None,
             now().isoformat(), now().isoformat(),
             now().isoformat() if status in ("success", "failed", "dead", "cancelled") else None,
             json.dumps(progress) if progress else None, None)
        )
        conn.commit()


# ---------- status property ----------

def test_status_queued():
    _insert("ar-1", "queued")
    r = AsyncResult("ar-1", backend=SQLiteBackend(TEST_DB))
    assert r.status == "queued"


def test_status_success():
    _insert("ar-2", "success", result=42)
    r = AsyncResult("ar-2", backend=SQLiteBackend(TEST_DB))
    assert r.status == "success"


def test_status_missing_task_returns_none():
    r = AsyncResult("does-not-exist", backend=SQLiteBackend(TEST_DB))
    assert r.status is None


# ---------- result property ----------

def test_result_deserialized():
    _insert("ar-3", "success", result={"answer": 42})
    r = AsyncResult("ar-3", backend=SQLiteBackend(TEST_DB))
    assert r.result == {"answer": 42}


def test_result_none_when_not_success():
    _insert("ar-4", "failed", error="oops")
    r = AsyncResult("ar-4", backend=SQLiteBackend(TEST_DB))
    assert r.result is None


# ---------- error property ----------

def test_error_returns_traceback():
    _insert("ar-5", "failed", error="Traceback...")
    r = AsyncResult("ar-5", backend=SQLiteBackend(TEST_DB))
    assert r.error == "Traceback..."


def test_error_none_when_success():
    _insert("ar-6", "success", result=1)
    r = AsyncResult("ar-6", backend=SQLiteBackend(TEST_DB))
    assert r.error is None


# ---------- progress property ----------

def test_progress_returns_dict():
    _insert("ar-7", "running", progress={"percent": 50, "message": "half done"})
    r = AsyncResult("ar-7", backend=SQLiteBackend(TEST_DB))
    assert r.progress == {"percent": 50, "message": "half done"}


def test_progress_none_when_not_set():
    _insert("ar-8", "running")
    r = AsyncResult("ar-8", backend=SQLiteBackend(TEST_DB))
    assert r.progress is None


# ---------- task_info property ----------

def test_task_info_returns_full_row():
    _insert("ar-9", "queued")
    r = AsyncResult("ar-9", backend=SQLiteBackend(TEST_DB))
    info = r.task_info
    assert isinstance(info, dict)
    assert info["id"] == "ar-9"
    assert info["status"] == "queued"
    assert "created_at" in info
    assert "attempt" in info


def test_task_info_none_when_missing():
    r = AsyncResult("gone", backend=SQLiteBackend(TEST_DB))
    assert r.task_info is None


# ---------- ready() ----------

def test_ready_false_for_queued():
    _insert("ar-10", "queued")
    r = AsyncResult("ar-10", backend=SQLiteBackend(TEST_DB))
    assert r.ready() is False


def test_ready_false_for_running():
    _insert("ar-11", "running")
    r = AsyncResult("ar-11", backend=SQLiteBackend(TEST_DB))
    assert r.ready() is False


def test_ready_false_for_retrying():
    _insert("ar-12", "retrying")
    r = AsyncResult("ar-12", backend=SQLiteBackend(TEST_DB))
    assert r.ready() is False


def test_ready_true_for_success():
    _insert("ar-13", "success", result=1)
    r = AsyncResult("ar-13", backend=SQLiteBackend(TEST_DB))
    assert r.ready() is True


def test_ready_true_for_failed():
    _insert("ar-14", "failed", error="err")
    r = AsyncResult("ar-14", backend=SQLiteBackend(TEST_DB))
    assert r.ready() is True


def test_ready_true_for_dead():
    _insert("ar-15", "dead", error="dead")
    r = AsyncResult("ar-15", backend=SQLiteBackend(TEST_DB))
    assert r.ready() is True


def test_ready_true_for_cancelled():
    _insert("ar-16", "cancelled")
    r = AsyncResult("ar-16", backend=SQLiteBackend(TEST_DB))
    assert r.ready() is True


# ---------- successful / failed / dead / cancelled ----------

def test_successful_true_only_for_success():
    _insert("ar-17", "success", result=1)
    r = AsyncResult("ar-17", backend=SQLiteBackend(TEST_DB))
    assert r.successful() is True
    assert r.failed() is False
    assert r.dead() is False
    assert r.cancelled() is False


def test_failed_true_only_for_failed():
    _insert("ar-18", "failed", error="err")
    r = AsyncResult("ar-18", backend=SQLiteBackend(TEST_DB))
    assert r.failed() is True
    assert r.successful() is False
    assert r.dead() is False
    assert r.cancelled() is False


def test_dead_true_only_for_dead():
    _insert("ar-19", "dead", error="dead")
    r = AsyncResult("ar-19", backend=SQLiteBackend(TEST_DB))
    assert r.dead() is True
    assert r.successful() is False


def test_cancelled_true_only_for_cancelled():
    _insert("ar-20", "cancelled")
    r = AsyncResult("ar-20", backend=SQLiteBackend(TEST_DB))
    assert r.cancelled() is True
    assert r.successful() is False


# ---------- get() ----------

def test_get_returns_result_for_success():
    _insert("ar-21", "success", result="hello")
    r = AsyncResult("ar-21", backend=SQLiteBackend(TEST_DB))
    assert r.get(timeout=1) == "hello"


def test_get_raises_task_failed_error_for_failed():
    _insert("ar-22", "failed", error="something broke")
    r = AsyncResult("ar-22", backend=SQLiteBackend(TEST_DB))
    with pytest.raises(TaskFailedError) as exc_info:
        r.get(timeout=1)
    assert "something broke" in str(exc_info.value)
    assert exc_info.value.state == "failed"
    assert exc_info.value.task_id == "ar-22"


def test_get_raises_task_failed_error_for_dead():
    _insert("ar-23", "dead", error="no retries left")
    r = AsyncResult("ar-23", backend=SQLiteBackend(TEST_DB))
    with pytest.raises(TaskFailedError) as exc_info:
        r.get(timeout=1)
    assert exc_info.value.state == "dead"


def test_get_raises_task_cancelled_error_for_cancelled():
    _insert("ar-24", "cancelled")
    r = AsyncResult("ar-24", backend=SQLiteBackend(TEST_DB))
    with pytest.raises(TaskCancelledError) as exc_info:
        r.get(timeout=1)
    assert exc_info.value.task_id == "ar-24"


def test_get_raises_timeout_error():
    """Task stays queued and never runs — get() must time out."""
    _insert("ar-25", "queued")
    r = AsyncResult("ar-25", backend=SQLiteBackend(TEST_DB))
    with pytest.raises(TimeoutError):
        r.get(timeout=0.3)


def test_get_timeout_zero_raises_immediately():
    """get(timeout=0) must raise TimeoutError immediately, not hang."""
    _insert("ar-26", "queued")
    r = AsyncResult("ar-26", backend=SQLiteBackend(TEST_DB))
    start = time.time()
    with pytest.raises(TimeoutError):
        r.get(timeout=0)
    assert time.time() - start < 0.5  # must return almost instantly


def test_get_timeout_none_is_blocking():
    """get(timeout=None) is blocking; verify it doesn't time out before task completes."""
    _insert("ar-27", "success", result="done")
    r = AsyncResult("ar-27", backend=SQLiteBackend(TEST_DB))
    # Already terminal — returns immediately
    assert r.get(timeout=None) == "done"


# ---------- forget() ----------

def test_forget_deletes_row():
    _insert("ar-28", "success", result=99)
    r = AsyncResult("ar-28", backend=SQLiteBackend(TEST_DB))
    r.forget()
    with get_conn(TEST_DB) as conn:
        row = conn.execute("SELECT * FROM tasks WHERE id='ar-28'").fetchone()
    assert row is None


def test_forget_makes_status_none():
    _insert("ar-29", "success", result=1)
    r = AsyncResult("ar-29", backend=SQLiteBackend(TEST_DB))
    r.forget()
    assert r.status is None


# ---------- __repr__ ----------

def test_repr_shows_task_id_and_status():
    _insert("ar-30", "queued")
    r = AsyncResult("ar-30", backend=SQLiteBackend(TEST_DB))
    text = repr(r)
    assert "ar-30" in text
    assert "queued" in text
