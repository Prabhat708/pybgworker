import time
import threading


class RateLimiter:
    """
    Start-rate limiter: controls how many tasks can be *started* per second.

    This is NOT a concurrency limiter. Setting rate_limit=5 means "start at most
    5 tasks per second", not "run at most 5 tasks at the same time". For long-
    running tasks, many more than `rate_limit` executions may be in-flight
    simultaneously once they have all been started.

    To cap the number of tasks executing concurrently, use the worker-level
    `concurrency` setting (WORKER_CONCURRENCY env var / --concurrency flag),
    which controls total in-flight slots across all task types.
    """

    def __init__(self, rate_per_sec):
        # default/global start-rate (tasks started per second)
        self.default_rate = rate_per_sec
        self.lock = threading.Lock()
        self.timestamps = []

    def acquire(self, rate=None):
        """
        Block until a start-rate token is available.

        Args:
            rate: per-task-name start-rate override (tasks/sec). Falls back
                  to the global default when not set. This limits how quickly
                  new tasks are *started*, not how many run concurrently.
        """
        limit = rate or self.default_rate

        # No limit configured
        if not limit or limit <= 0:
            return

        with self.lock:
            now = time.time()

            # Remove timestamps older than 1 second
            self.timestamps = [
                t for t in self.timestamps
                if now - t < 1
            ]

            # Wait if limit reached
            if len(self.timestamps) >= limit:
                sleep_time = 1 - (now - self.timestamps[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)

                now = time.time()
                self.timestamps = [
                    t for t in self.timestamps
                    if now - t < 1
                ]

            self.timestamps.append(time.time())
