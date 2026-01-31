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


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)

    # production SQLite settings
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")  # wait 30 seconds if locked

    return conn
