import os
import sys
import time
import subprocess
from pybgworker.sqlite_queue import SQLiteQueue
from pybgworker.utils import get_conn

os.environ["PYBGWORKER_DB"] = "stress_test.db"

def main():
    print("Initializing stress test DB...")
    db_files = ["stress_test.db", "stress_test.db-shm", "stress_test.db-wal"]
    for f in db_files:
        if os.path.exists(f):
            try:
                os.remove(f)
            except:
                pass
                
    queue = SQLiteQueue("stress_test.db")
    queue._init_db()

    # Create thousands of tasks
    tasks = []
    for i in range(1000):
        tasks.append({
            "id": f"stress-{i}",
            "name": "tests.resilience.task",
            "args": "[]",
            "kwargs": "{}",
            "status": "queued",
            "attempt": 0,
            "max_retries": 3,
            "run_at": "2026-01-01T00:00:00",
            "priority": 5,
            "locked_by": None,
            "locked_at": None,
            "last_error": None,
            "result": None,
            "created_at": "2026-01-01T00:00:00",
            "updated_at": "2026-01-01T00:00:00",
            "finished_at": None,
            "progress": None,
            "idempotency_key": None
        })
    
    queue.enqueue_many(tasks)
    
    print("Starting 10 worker processes, concurrency=5...")
    procs = []
    for _ in range(10):
        p = subprocess.Popen(
            [sys.executable, "-m", "pybgworker.cli", "run", "--app", "tests.test_resilience"],
            env=dict(os.environ, PYBGWORKER_DB="stress_test.db", PYBGWORKER_CONCURRENCY="5", PYBGWORKER_POLL_INTERVAL="0.1")
        )
        procs.append(p)
        
    print("Waiting 10 seconds to allow processing...")
    time.sleep(10)
    
    for p in procs:
        p.terminate()
        p.wait()

    with get_conn("stress_test.db") as conn:
        success = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='success'").fetchone()[0]
        queued = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='queued'").fetchone()[0]
        running = conn.execute("SELECT COUNT(*) FROM tasks WHERE status='running'").fetchone()[0]
        
    print(f"Results: success={success}, queued={queued}, running={running}")
    
    assert success > 0, "No tasks were processed"

if __name__ == "__main__":
    main()
