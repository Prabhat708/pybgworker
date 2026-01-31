import json
import sys
from datetime import datetime, timezone


def log(event, **fields):
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields
    }

    sys.stdout.write(json.dumps(entry) + "\n")
    sys.stdout.flush()
