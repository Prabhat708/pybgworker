import os


def _get_int_setting(env_key, default):
    if env_key in os.environ:
        return int(os.environ[env_key])
    return default


WORKER_TIMEOUT = 15
RATE_LIMIT = 5  # tasks per second
DB_PATH = os.getenv("PYBGWORKER_DB", "pybgworker.db")
WORKER_NAME = os.getenv("PYBGWORKER_WORKER_NAME", "worker-1")
POLL_INTERVAL = float(os.getenv("PYBGWORKER_POLL_INTERVAL", 1.0))
LOCK_TIMEOUT = int(os.getenv("PYBGWORKER_LOCK_TIMEOUT", 60))
RETENTION_DAYS = _get_int_setting("PYBGWORKER_RETENTION_DAYS", 0)
CLEANUP_INTERVAL_HOURS = _get_int_setting("PYBGWORKER_CLEANUP_INTERVAL_HOURS", 24)


def get_worker_concurrency():
    return _get_int_setting("PYBGWORKER_CONCURRENCY", 1)
