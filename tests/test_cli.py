import pytest
import os
import sys
from pybgworker.cli import main

def test_cli_commands(monkeypatch):
    import pybgworker.cli as cli
    import pybgworker.worker as worker
    import pybgworker.inspect as inspect_mod
    import pybgworker.retry as retry_mod
    import pybgworker.purge as purge_mod
    import pybgworker.cancel as cancel_mod
    import pybgworker.stats as stats_mod
    import pybgworker.failed as failed_mod
    import pybgworker.dead as dead_mod

    # Mock the underlying functions
    called = {}
    def mock_run_worker(): called["run"] = True
    def mock_inspect(as_json=False): called["inspect"] = as_json
    def mock_retry(task_id): called["retry"] = task_id
    def mock_purge(): called["purge"] = True
    def mock_cancel(task_id): called["cancel"] = task_id
    def mock_stats(as_json=False): called["stats"] = as_json
    def mock_failed(): called["failed"] = True
    def mock_dead(): called["dead"] = True

    monkeypatch.setattr(worker, "run_worker", mock_run_worker)
    monkeypatch.setattr(inspect_mod, "inspect", mock_inspect)
    monkeypatch.setattr(retry_mod, "retry", mock_retry)
    monkeypatch.setattr(purge_mod, "purge", mock_purge)
    monkeypatch.setattr(cancel_mod, "cancel", mock_cancel)
    monkeypatch.setattr(stats_mod, "stats", mock_stats)
    monkeypatch.setattr(failed_mod, "list_failed", mock_failed)
    monkeypatch.setattr(dead_mod, "list_dead", mock_dead)

    def run_cmd(*args, expect_exit=False):
        monkeypatch.setattr(sys, "argv", ["pybgworker"] + list(args))
        if expect_exit:
            with pytest.raises(SystemExit):
                main()
        else:
            main()

    # test run
    run_cmd("run", "--app", "tests.dummy_app")
    assert called.get("run") is True

    # test run missing app
    run_cmd("run", expect_exit=True)

    # test inspect
    run_cmd("inspect", "--json")
    assert called.get("inspect") is True

    # test retry
    run_cmd("retry", "task-123")
    assert called.get("retry") == "task-123"

    run_cmd("retry", expect_exit=True)

    # test purge
    run_cmd("purge")
    assert called.get("purge") is True

    # test cancel
    run_cmd("cancel", "task-123")
    assert called.get("cancel") == "task-123"

    run_cmd("cancel", expect_exit=True)

    # test stats
    run_cmd("stats", "--json")
    assert called.get("stats") is True

    # test failed
    run_cmd("failed")
    assert called.get("failed") is True

    # test dead
    run_cmd("dead")
    assert called.get("dead") is True

    # test concurrency and retention args
    run_cmd("run", "--app", "tests.dummy_app", "--concurrency", "5", "--retention-days", "7", "--cleanup-interval-hours", "24")
    assert os.environ.get("PYBGWORKER_CONCURRENCY") == "5"
    assert os.environ.get("PYBGWORKER_RETENTION_DAYS") == "7"
    assert os.environ.get("PYBGWORKER_CLEANUP_INTERVAL_HOURS") == "24"
