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


from contextlib import contextmanager

@contextmanager
def get_conn(db_path=None):
    """Return a new SQLite connection context manager.

    Args:
        db_path: Path to the database file. Defaults to ``config.DB_PATH``
                 (the process-global ``PYBGWORKER_DB`` setting) when omitted
                 or ``None``.  Pass an explicit path to use an isolated
                 database — e.g. for tests or multi-tenant setups.
    """
    if db_path is None:
        db_path = DB_PATH
    conn = sqlite3.connect(
        db_path,
        timeout=30,
        isolation_level=None,  # We handle transactions manually where needed
        check_same_thread=False
    )

    # production SQLite settings
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")  # wait 30 seconds if locked

    try:
        yield conn
    finally:
        conn.close()
