"""
Shared test DB setup — imported by all test modules via conftest.py.
Tests that manage their own DB isolation (test_concurrency, test_retry,
test_retry_policy, test_cli) are excluded via the NO_ISOLATE_DB marker.
"""
import os
import gc
import time
import pytest

TEST_DB = "test_suite.db"

# Tests that handle their own DB setup — skip the shared fixture for these
_SELF_MANAGED = {
    "test_concurrency",
    "test_retry",
    "test_retry_policy",
    "test_cli",
}

def _clear_db():
    from pybgworker.utils import get_conn
    with get_conn(TEST_DB) as conn:
        conn.execute("DELETE FROM tasks")
        conn.execute("DELETE FROM workers")
        conn.commit()


@pytest.fixture(autouse=True)
def isolate_db(request, monkeypatch):
    """Point every module at the test DB and wipe it before/after each test.
    Skipped for test modules that manage their own DB isolation.
    """
    module_name = request.module.__name__.rsplit(".", 1)[-1]
    if module_name in _SELF_MANAGED:
        yield
        return

    monkeypatch.setenv("PYBGWORKER_DB", TEST_DB)

    import pybgworker.config as config
    import pybgworker.utils as utils
    config.DB_PATH = TEST_DB
    utils.DB_PATH = TEST_DB

    from pybgworker.task import queue as tq, backend as tb
    from pybgworker.backends import SQLiteBackend
    from pybgworker.sqlite_queue import SQLiteQueue
    tq.db_path = TEST_DB
    tb.db_path = TEST_DB

    SQLiteQueue(TEST_DB)._init_db()  # ensure fresh schema
    _clear_db()

    yield

    _clear_db()
