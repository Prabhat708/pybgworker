import pytest
import time
from datetime import datetime, timezone
from pybgworker.scheduler import cron, run_scheduler, CRON_REGISTRY
import pybgworker.scheduler as s

@cron("* * * * *")
def my_cron_job():
    pass

def test_run_scheduler_loop(monkeypatch):
    called = []
    
    class MockQueue:
        def enqueue(self, task):
            called.append(task)
            
    monkeypatch.setattr(s, "queue", MockQueue())
    
    # Mock datetime to return a time, then advance it past the cron schedule
    class MockDatetime(datetime):
        _current = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        @classmethod
        def now(cls, tz=None):
            ret = cls._current
            cls._current = datetime(2026, 1, 1, 12, 2, 0, tzinfo=timezone.utc)
            return ret
            
    monkeypatch.setattr(s, "datetime", MockDatetime)
    
    # Mock sleep to break the infinite loop after one iteration
    sleep_calls = []
    def mock_sleep(secs):
        sleep_calls.append(secs)
        if len(sleep_calls) == 2:
            raise KeyboardInterrupt()
        
    monkeypatch.setattr(time, "sleep", mock_sleep)
    
    try:
        run_scheduler()
    except KeyboardInterrupt:
        pass
        
    assert len(called) > 0
    assert called[0]["name"] == "tests.test_scheduler.my_cron_job" or called[0]["name"] == "my_cron_job"
