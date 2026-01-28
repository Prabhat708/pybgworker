import os

DB_PATH = os.getenv("PYWORKER_DB", "pyworker.db")
WORKER_NAME = os.getenv("PYWORKER_WORKER_NAME", "worker-1")
POLL_INTERVAL = float(os.getenv("PYWORKER_POLL_INTERVAL", 1.0))
LOCK_TIMEOUT = int(os.getenv("PYWORKER_LOCK_TIMEOUT", 60))
