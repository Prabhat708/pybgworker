import time
import threading
from collections import defaultdict


class RateLimiter:
    """
    Start-rate limiter: controls how many tasks can be *started* per second,
    independently per task name.

    This is NOT a concurrency limiter. Setting rate_limit=5 means "start at most
    5 tasks per second", not "run at most 5 tasks at the same time". For long-
    running tasks, many more than `rate_limit` executions may be in-flight
    simultaneously once they have all been started.

    To cap the number of tasks executing concurrently, use the worker-level
    `concurrency` setting (WORKER_CONCURRENCY env var / --concurrency flag),
    which controls total in-flight slots across all task types.

    Each task name gets its own independent sliding-window bucket so that a
    high-throughput task cannot throttle a low-throughput one and vice-versa.
    Callers that do not pass a ``name`` share a single ``"__global__"`` bucket,
    which preserves backwards compatibility.
    """

    def __init__(self, rate_per_sec):
        # default/global start-rate (tasks started per second)
        self.default_rate = rate_per_sec
        self.lock = threading.Lock()
        # Per-name sliding-window timestamp lists.
        # Keyed by task name; unknown names are auto-initialised to [].
        self.timestamps = defaultdict(list)

    def acquire(self, rate=None, name="__global__"):
        """
        Block until a start-rate token is available for the given task name.

        Args:
            rate: per-task-name start-rate override (tasks/sec). Falls back
                  to the global default when not set. This limits how quickly
                  new tasks are *started*, not how many run concurrently.
            name: task name used to select the per-name bucket. Defaults to
                  ``"__global__"`` so callers that omit it share one bucket.
        """
        limit = rate or self.default_rate

        # No limit configured
        if not limit or limit <= 0:
            return

        with self.lock:
            now = time.time()
            bucket = self.timestamps[name]

            # Remove timestamps older than 1 second
            bucket[:] = [t for t in bucket if now - t < 1]

            # Wait if limit reached for this task name
            if len(bucket) >= limit:
                sleep_time = 1 - (now - bucket[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)

                now = time.time()
                bucket[:] = [t for t in bucket if now - t < 1]

            bucket.append(time.time())
