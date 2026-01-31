import time
import threading


class RateLimiter:
    def __init__(self, rate_per_sec):
        self.rate = rate_per_sec
        self.lock = threading.Lock()
        self.timestamps = []

    def acquire(self):
        with self.lock:
            now = time.time()

            # remove old timestamps
            self.timestamps = [
                t for t in self.timestamps
                if now - t < 1
            ]

            if len(self.timestamps) >= self.rate:
                sleep_time = 1 - (now - self.timestamps[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)

            self.timestamps.append(time.time())
