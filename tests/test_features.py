"""
Tests for cancel.py, retry.py, state.py (validate_transition),
purge.py, failed.py, dead.py, inspect.py, stats.py, progress.py,
ratelimit.py, worker utility functions, backends.py.
"""
import json
import time
import pytest
from datetime import timedelta, timezone

from pybgworker.utils import now, get_conn
from pybgworker.sqlite_queue import SQLiteQueue
from pybgworker.backends import SQLiteBackend

TEST_DB = "test_suite.db"


def q():
    return SQLiteQueue(TEST_DB)


def _insert(task_id, status, attempt=0, max_retries=3, error=None,
            result=None, run_at=None):
    ts = now().isoformat()
    with get_conn(TEST_DB) as conn:
        conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        conn.execute(
            "INSERT INTO tasks(id,name,args,kwargs,status,attempt,max_retries,run_at,"
            "priority,locked_by,locked_at,last_error,result,created_at,updated_at,"
            "finished_at,progress,idempotency_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (task_id, "test.task", "[]", "{}", status, attempt, max_retries,
             run_at or ts, 5, None, None, error,
             json.dumps(result) if result is not None else None,
             ts, ts,
             ts if status in ("success", "failed", "dead", "cancelled") else None,
             None, None)
        )
        conn.commit()


# ============================================================
# state.py — validate_transition
# ============================================================

class TestValidateTransition:
    def test_queued_to_running_allowed(self):
        from pybgworker.state import validate_transition
        validate_transition("queued", "running")  # no exception

    def test_running_to_success_allowed(self):
        from pybgworker.state import validate_transition
        validate_transition("running", "success")

    def test_running_to_retrying_allowed(self):
        from pybgworker.state import validate_transition
        validate_transition("running", "retrying")

    def test_running_to_failed_allowed(self):
        from pybgworker.state import validate_transition
        validate_transition("running", "failed")

    def test_running_to_dead_allowed(self):
        from pybgworker.state import validate_transition
        validate_transition("running", "dead")

    def test_retrying_to_running_allowed(self):
        from pybgworker.state import validate_transition
        validate_transition("retrying", "running")

    def test_success_to_cancelled_raises(self):
        from pybgworker.state import validate_transition
        with pytest.raises(ValueError, match="Invalid transition"):
            validate_transition("success", "cancelled")

    def test_failed_to_running_raises(self):
        from pybgworker.state import validate_transition
        with pytest.raises(ValueError, match="Invalid transition"):
            validate_transition("failed", "running")

    def test_dead_to_success_raises(self):
        from pybgworker.state import validate_transition
        with pytest.raises(ValueError, match="Invalid transition"):
            validate_transition("dead", "success")

    def test_error_message_is_ascii(self):
        """Error message must not contain non-ASCII (crashes Windows terminals)."""
        from pybgworker.state import validate_transition
        try:
            validate_transition("success", "cancelled")
        except ValueError as e:
            msg = str(e)
            msg.encode("ascii")  # will raise if non-ASCII (e.g. → arrow)


# ============================================================
# cancel.py
# ============================================================

class TestCancel:
    def test_cancels_queued_task(self):
        from pybgworker.cancel import cancel
        _insert("cn1", "queued")
        cancel("cn1")
        with get_conn(TEST_DB) as conn:
            s = conn.execute("SELECT status FROM tasks WHERE id='cn1'").fetchone()[0]
        assert s == "cancelled"

    def test_cancels_running_task(self):
        from pybgworker.cancel import cancel
        _insert("cn2", "running")
        cancel("cn2")
        with get_conn(TEST_DB) as conn:
            s = conn.execute("SELECT status FROM tasks WHERE id='cn2'").fetchone()[0]
        assert s == "cancelled"

    def test_cancels_retrying_task(self):
        from pybgworker.cancel import cancel
        _insert("cn3", "retrying")
        cancel("cn3")
        with get_conn(TEST_DB) as conn:
            s = conn.execute("SELECT status FROM tasks WHERE id='cn3'").fetchone()[0]
        assert s == "cancelled"

    def test_noop_for_success_task(self, capsys):
        from pybgworker.cancel import cancel
        _insert("cn4", "success")
        cancel("cn4")  # must not crash
        with get_conn(TEST_DB) as conn:
            s = conn.execute("SELECT status FROM tasks WHERE id='cn4'").fetchone()[0]
        assert s == "success"  # unchanged

    def test_noop_for_missing_task(self):
        from pybgworker.cancel import cancel
        cancel("does-not-exist")  # must not crash

    def test_toctou_race_does_not_crash(self):
        """cancel() on a terminal task must not crash (TOCTOU safety)."""
        from pybgworker.cancel import cancel
        # Insert as success — simulates task completing between status check and cancel
        _insert("cn5", "success")
        cancel("cn5")  # no exception expected


# ============================================================
# retry.py
# ============================================================

class TestRetry:
    def test_requeues_failed_task(self):
        from pybgworker.retry import retry
        _insert("rt1", "failed", attempt=2, error="oops")
        retry("rt1")
        with get_conn(TEST_DB) as conn:
            row = conn.execute("SELECT status, attempt, last_error FROM tasks WHERE id='rt1'").fetchone()
        assert row[0] == "queued"
        assert row[1] == 0
        assert row[2] is None

    def test_requeues_dead_task(self):
        from pybgworker.retry import retry
        _insert("rt2", "dead", attempt=3, error="dead")
        retry("rt2")
        with get_conn(TEST_DB) as conn:
            s = conn.execute("SELECT status FROM tasks WHERE id='rt2'").fetchone()[0]
        assert s == "queued"

    def test_retry_resets_run_at_to_now(self):
        """run_at must be reset so the task is immediately eligible."""
        from pybgworker.retry import retry
        from datetime import datetime
        future = (now() + timedelta(hours=2)).isoformat()
        _insert("rt3", "failed", run_at=future)
        retry("rt3")
        with get_conn(TEST_DB) as conn:
            run_at = conn.execute("SELECT run_at FROM tasks WHERE id='rt3'").fetchone()[0]
        dt = datetime.fromisoformat(run_at)
        assert dt <= now() + timedelta(seconds=2)

    def test_retry_noop_for_queued_task(self):
        from pybgworker.retry import retry
        _insert("rt4", "queued")
        retry("rt4")
        with get_conn(TEST_DB) as conn:
            s = conn.execute("SELECT status FROM tasks WHERE id='rt4'").fetchone()[0]
        assert s == "queued"  # unchanged

    def test_retry_noop_for_missing_task(self):
        from pybgworker.retry import retry
        retry("does-not-exist")  # must not crash


# ============================================================
# purge.py
# ============================================================

class TestPurge:
    def test_purge_deletes_queued_and_retrying(self):
        from pybgworker.purge import purge
        _insert("pu1", "queued")
        _insert("pu2", "retrying")
        _insert("pu3", "success")  # should be kept
        purge()
        with get_conn(TEST_DB) as conn:
            rows = conn.execute("SELECT id FROM tasks").fetchall()
        ids = [r[0] for r in rows]
        assert "pu1" not in ids
        assert "pu2" not in ids
        assert "pu3" in ids

    def test_purge_returns_no_error_when_empty(self):
        from pybgworker.purge import purge
        purge()  # must not crash on empty table


# ============================================================
# failed.py / dead.py
# ============================================================

class TestFailedAndDead:
    def test_list_failed_includes_both_failed_and_dead(self, capsys):
        from pybgworker.failed import list_failed
        _insert("lf1", "failed", error="fail-msg")
        _insert("lf2", "dead", error="dead-msg")
        _insert("lf3", "success")
        list_failed()
        out = capsys.readouterr().out
        assert "lf1" in out
        assert "lf2" in out
        assert "lf3" not in out

    def test_list_failed_empty(self, capsys):
        from pybgworker.failed import list_failed
        list_failed()
        out = capsys.readouterr().out
        assert "No failed" in out

    def test_list_dead_includes_only_dead(self, capsys):
        from pybgworker.dead import list_dead
        _insert("ld1", "failed", error="fail")
        _insert("ld2", "dead", error="dead-msg")
        list_dead()
        out = capsys.readouterr().out
        assert "ld2" in out
        assert "ld1" not in out

    def test_list_dead_empty(self, capsys):
        from pybgworker.dead import list_dead
        list_dead()
        out = capsys.readouterr().out
        assert "No dead" in out


# ============================================================
# inspect.py / stats.py
# ============================================================

class TestInspect:
    def test_inspect_does_not_crash(self, capsys):
        from pybgworker.inspect import inspect
        _insert("insp1", "queued")
        inspect()
        out = capsys.readouterr().out
        assert "queued" in out

    def test_inspect_json_output(self, capsys):
        from pybgworker.inspect import inspect
        _insert("insp2", "success")
        inspect(as_json=True)
        out = capsys.readouterr().out
        data = json.loads(out)
        assert "tasks" in data
        assert "total" in data
        assert "workers" in data

    def test_inspect_handles_null_last_seen_worker(self, capsys):
        """inspect must not crash when a worker has NULL last_seen."""
        from pybgworker.inspect import inspect
        with get_conn(TEST_DB) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO workers(name, last_seen) VALUES (?, ?)",
                ("ghost", None)
            )
            conn.commit()
        inspect()  # must not raise
        out = capsys.readouterr().out
        assert "ghost" in out
        assert "unknown" in out

    def test_inspect_json_null_last_seen(self, capsys):
        from pybgworker.inspect import inspect
        with get_conn(TEST_DB) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO workers(name, last_seen) VALUES (?, ?)",
                ("ghost2", None)
            )
            conn.commit()
        inspect(as_json=True)
        data = json.loads(capsys.readouterr().out)
        ghosts = [w for w in data["workers"] if w["name"] == "ghost2"]
        assert ghosts[0]["status"] == "unknown"
        assert ghosts[0]["seconds_ago"] is None


class TestStats:
    def test_stats_does_not_crash(self, capsys):
        from pybgworker.stats import stats
        stats()
        capsys.readouterr()

    def test_stats_json_output(self, capsys):
        from pybgworker.stats import stats
        _insert("st1", "queued")
        stats(as_json=True)
        data = json.loads(capsys.readouterr().out)
        assert "workers" in data
        assert "queue_depth" in data
        assert data["queue_depth"] == 1

    def test_stats_handles_null_last_seen(self, capsys):
        from pybgworker.stats import stats
        with get_conn(TEST_DB) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO workers(name, last_seen) VALUES (?, ?)",
                ("ghost3", None)
            )
            conn.commit()
        stats()  # must not crash
        capsys.readouterr()

    def test_stats_json_null_last_seen(self, capsys):
        from pybgworker.stats import stats
        with get_conn(TEST_DB) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO workers(name, last_seen) VALUES (?, ?)",
                ("ghost4", None)
            )
            conn.commit()
        stats(as_json=True)
        data = json.loads(capsys.readouterr().out)
        ghosts = [w for w in data["workers"] if w["name"] == "ghost4"]
        assert ghosts[0]["status"] == "unknown"


# ============================================================
# progress.py — set_progress
# ============================================================

class TestProgress:
    def test_set_progress_noop_outside_worker(self):
        """set_progress() must be a no-op when PYBGWORKER_CURRENT_TASK_ID is not set."""
        import os
        os.environ.pop("PYBGWORKER_CURRENT_TASK_ID", None)
        from pybgworker.progress import set_progress
        set_progress(50, "test")  # must not crash or write anything

    def test_set_progress_writes_when_env_var_set(self):
        import os
        _insert("pr1", "queued")
        os.environ["PYBGWORKER_CURRENT_TASK_ID"] = "pr1"
        try:
            from pybgworker.progress import set_progress
            set_progress(75, "three quarters")
            with get_conn(TEST_DB) as conn:
                raw = conn.execute("SELECT progress FROM tasks WHERE id='pr1'").fetchone()[0]
            data = json.loads(raw)
            assert data["percent"] == 75
            assert data["message"] == "three quarters"
        finally:
            os.environ.pop("PYBGWORKER_CURRENT_TASK_ID", None)

    def test_set_progress_clamps(self):
        import os
        _insert("pr2", "queued")
        os.environ["PYBGWORKER_CURRENT_TASK_ID"] = "pr2"
        try:
            from pybgworker.progress import set_progress
            set_progress(999)
            with get_conn(TEST_DB) as conn:
                raw = conn.execute("SELECT progress FROM tasks WHERE id='pr2'").fetchone()[0]
            data = json.loads(raw)
            assert data["percent"] == 100
        finally:
            os.environ.pop("PYBGWORKER_CURRENT_TASK_ID", None)


# ============================================================
# ratelimit.py — RateLimiter
# ============================================================

class TestRateLimiter:
    def test_acquire_does_not_raise_within_limit(self):
        from pybgworker.ratelimit import RateLimiter
        limiter = RateLimiter(10.0)
        # First call at 10/s should not block or raise
        limiter.acquire(rate=10.0, name="task.a")  # no exception = pass

    def test_acquire_sleeps_when_rate_exceeded(self):
        from pybgworker.ratelimit import RateLimiter
        limiter = RateLimiter(5.0)  # 5 per second
        # Fire 6 rapid calls — the 6th must wait
        start = time.time()
        for _ in range(6):
            limiter.acquire(rate=5.0, name="task.b")
        elapsed = time.time() - start
        # At 5/s, 6 calls must span at least 1 second
        assert elapsed >= 0.9

    def test_per_task_buckets_are_independent(self):
        from pybgworker.ratelimit import RateLimiter
        limiter = RateLimiter(1.0)
        # Exhaust bucket for task.x
        limiter.acquire(rate=1.0, name="task.x")
        # task.y should have its own fresh bucket
        start = time.time()
        limiter.acquire(rate=1.0, name="task.y")
        assert time.time() - start < 0.5  # didn't wait for task.x's bucket

    def test_no_rate_returns_immediately(self):
        from pybgworker.ratelimit import RateLimiter
        limiter = RateLimiter(0.0)
        # rate=None and default 0 means no limit — should return immediately
        start = time.time()
        limiter.acquire(rate=None, name="task.z")
        assert time.time() - start < 0.1


# ============================================================
# backends.py — SQLiteBackend
# ============================================================

class TestSQLiteBackend:
    def test_get_task_returns_dict(self):
        _insert("bk1", "queued")
        backend = SQLiteBackend(TEST_DB)
        row = backend.get_task("bk1")
        assert isinstance(row, dict)
        assert row["id"] == "bk1"
        assert row["status"] == "queued"

    def test_get_task_returns_none_for_missing(self):
        backend = SQLiteBackend(TEST_DB)
        assert backend.get_task("missing") is None

    def test_store_result_writes_json(self):
        _insert("bk2", "running")
        backend = SQLiteBackend(TEST_DB)
        backend.store_result("bk2", {"value": 42})
        with get_conn(TEST_DB) as conn:
            raw = conn.execute("SELECT result FROM tasks WHERE id='bk2'").fetchone()[0]
        assert json.loads(raw) == {"value": 42}

    def test_forget_removes_row(self):
        _insert("bk3", "success")
        backend = SQLiteBackend(TEST_DB)
        backend.forget("bk3")
        with get_conn(TEST_DB) as conn:
            row = conn.execute("SELECT * FROM tasks WHERE id='bk3'").fetchone()
        assert row is None


# ============================================================
# worker.py utilities — compute_retry_delay, _exc_matches_retry_for
# ============================================================

class TestWorkerUtils:
    def test_compute_retry_delay_base(self):
        from pybgworker.worker import compute_retry_delay
        task = {"attempt": 0, "max_retries": 3}
        meta = {"retry_delay": 5, "retry_backoff": False, "retry_jitter": 0}
        assert compute_retry_delay(task, meta) == 5

    def test_compute_retry_delay_backoff(self):
        from pybgworker.worker import compute_retry_delay
        task = {"attempt": 2, "max_retries": 5}
        meta = {"retry_delay": 2, "retry_backoff": True,
                "retry_backoff_factor": 2, "retry_jitter": 0,
                "retry_max_delay": None}
        delay = compute_retry_delay(task, meta)
        # attempt=2: 2 * 2^2 = 8
        assert delay == 8

    def test_compute_retry_delay_max_cap(self):
        from pybgworker.worker import compute_retry_delay
        task = {"attempt": 10, "max_retries": 20}
        meta = {"retry_delay": 2, "retry_backoff": True,
                "retry_backoff_factor": 2, "retry_jitter": 0,
                "retry_max_delay": 60}
        delay = compute_retry_delay(task, meta)
        assert delay <= 60

    def test_compute_retry_delay_jitter_adds_noise(self):
        from pybgworker.worker import compute_retry_delay
        task = {"attempt": 0, "max_retries": 3}
        meta = {"retry_delay": 10, "retry_backoff": False, "retry_jitter": 0.5,
                "retry_max_delay": None}
        delays = [compute_retry_delay(task, meta) for _ in range(20)]
        # jitter should produce varying values
        assert len(set(delays)) > 1

    def test_exc_matches_retry_for_direct_match(self):
        from pybgworker.worker import _exc_matches_retry_for
        result = _exc_matches_retry_for(
            "TimeoutError",
            ["TimeoutError", "Exception", "BaseException", "object"],
            (TimeoutError,)
        )
        assert result is True

    def test_exc_matches_retry_for_inheritance(self):
        from pybgworker.worker import _exc_matches_retry_for
        # ValueError is a subclass of Exception
        result = _exc_matches_retry_for(
            "ValueError",
            ["ValueError", "Exception", "BaseException", "object"],
            (Exception,)
        )
        assert result is True

    def test_exc_matches_retry_for_no_match(self):
        from pybgworker.worker import _exc_matches_retry_for
        result = _exc_matches_retry_for(
            "RuntimeError",
            ["RuntimeError", "Exception", "BaseException", "object"],
            (TimeoutError, ConnectionError)
        )
        assert result is False

    def test_exc_matches_retry_for_none_mro(self):
        from pybgworker.worker import _exc_matches_retry_for
        # When exc_mro is None, falls back to exc_class_name
        result = _exc_matches_retry_for("TimeoutError", None, (TimeoutError,))
        assert result is True
