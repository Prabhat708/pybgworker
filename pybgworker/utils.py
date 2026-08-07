import uuid
import json
import sqlite3
from datetime import datetime, timezone
from .config import DB_PATH


def generate_id():
    return str(uuid.uuid4())


def now():
    return datetime.now(timezone.utc)


def dumps(obj):
    return json.dumps(obj)


def loads(data):
    return json.loads(data)


def get_conn(db_path=None):
    """Return a new SQLite connection.

    Args:
        db_path: Path to the database file. Defaults to ``config.DB_PATH``
                 (the process-global ``PYBGWORKER_DB`` setting) when omitted
                 or ``None``.  Pass an explicit path to use an isolated
                 database — e.g. for tests or multi-tenant setups.
    """
    conn = sqlite3.connect(db_path or DB_PATH, timeout=30)

    # production SQLite settings
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")  # wait 30 seconds if locked

    return conn
