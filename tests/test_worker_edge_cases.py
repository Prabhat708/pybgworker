import pytest
import time
import os
import signal
import threading
from pybgworker.sqlite_queue import SQLiteQueue
from pybgworker.task import task
from pybgworker import worker
from pybgworker.config import DB_PATH

@task(name="tests.timeout_task", timeout=0.1)
def timeout_task():
    time.sleep(1.0)
    return "done"

@task(name="tests.crash_task")
def crash_task():
    import sys
    sys.exit(1)

def test_worker_handle_timeout_and_crash(monkeypatch):
    from pybgworker import config
    queue = SQLiteQueue(config.DB_PATH)
    queue._init_db()

    # enqueue timeout task
    res_timeout = timeout_task.delay()

    # run worker loop manually
    from pybgworker.worker import start_task, handle_timeout, handle_completed, active_tasks, active_tasks_lock
    import pybgworker.worker as w

    monkeypatch.setattr(w, "POLL_INTERVAL", 0.01)
    
    t_row = queue.fetch_next("worker-test")
    assert t_row is not None, "Failed to fetch task from queue"
    info = start_task(t_row)
    
    # wait for timeout
    time.sleep(0.2)
    handle_timeout(res_timeout.task_id, info)
    
    # should be rescheduled since max_retries > attempt
    from pybgworker.worker import backend
    backend.db_path = config.DB_PATH
    task_db = backend.get_task(res_timeout.task_id)
    assert task_db["status"] == "retrying"
    
    # now let's fake it exceeding max retries
    time.sleep(0.01)
    t_row = queue.fetch_next("worker-test")
    assert t_row is not None, "Failed to fetch retried task from queue"
    t_row["attempt"] = 100
    t_row["max_retries"] = 3
    info["task"] = t_row
    handle_timeout(res_timeout.task_id, info)
    
    task_db = backend.get_task(res_timeout.task_id)
    assert task_db["status"] == "dead"

    # crash task
    res_crash = crash_task.delay()
    t_row = queue.fetch_next("worker-test")
    assert t_row is not None, "Failed to fetch crash task"
    info = start_task(t_row)
    info["process"].join() # wait for it to crash
    handle_completed(res_crash.task_id, info)
    
    task_db = backend.get_task(res_crash.task_id)
    assert task_db["status"] == "retrying" # rescheduled
    
    time.sleep(0.01)
    t_row = queue.fetch_next("worker-test")
    assert t_row is not None, "Failed to fetch retried crash task"
    t_row["attempt"] = 100
    t_row["max_retries"] = 3
    info["task"] = t_row
    handle_completed(res_crash.task_id, info)
    task_db = backend.get_task(res_crash.task_id)
    assert task_db["status"] == "failed"

def test_worker_shutdown(monkeypatch):
    import pybgworker.worker as w
    import threading

    w.shutdown_requested = False
    w.last_shutdown_signal = 0
    w.active_tasks.clear()

    # first signal
    w.handle_shutdown(signal.SIGINT, None)
    assert w.shutdown_requested is True

    # duplicate signal ignored
    w.handle_shutdown(signal.SIGINT, None)

    # test exit path
    time.sleep(1.1) # wait for duplicate signal threshold
    with pytest.raises(SystemExit):
        def mock_exit(code): raise SystemExit(code)
        monkeypatch.setattr(os, "_exit", mock_exit)
        w.handle_shutdown(signal.SIGINT, None)

def test_maintenance_loop(monkeypatch):
    import pybgworker.worker as w
    
    called = []
    def mock_cleanup(*args, **kwargs):
        called.append(True)
        return {"deleted": 1, "vacuumed": False, "locked": False}
        
    class MockQueue:
        def cleanup(self, *args, **kwargs):
            return mock_cleanup(*args, **kwargs)

    monkeypatch.setattr(w, "queue", MockQueue())
    
    # mock sleep to raise so we can exit the infinite loop
    def mock_sleep(s):
        raise KeyboardInterrupt()
    monkeypatch.setattr(time, "sleep", mock_sleep)

    try:
        w.maintenance_loop()
    except KeyboardInterrupt:
        pass
        
    assert called
