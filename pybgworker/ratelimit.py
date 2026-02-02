import time
import threading


class RateLimiter:
    def __init__(self, rate_per_sec):
        # default/global rate
        self.default_rate = rate_per_sec
        self.lock = threading.Lock()
        self.timestamps = []

    def acquire(self, rate=None):
        """
        rate: optional per-task rate limit
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
