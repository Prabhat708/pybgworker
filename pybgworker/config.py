import os
WORKER_TIMEOUT = 15 
DB_PATH = os.getenv("PYBGWORKER_DB", "pybgworker.db")
WORKER_NAME = os.getenv("PYBGWORKER_WORKER_NAME", "worker-1")
POLL_INTERVAL = float(os.getenv("PYBGWORKER_POLL_INTERVAL", 1.0))
LOCK_TIMEOUT = int(os.getenv("PYBGWORKER_LOCK_TIMEOUT", 60))
