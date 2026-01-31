import uuid
import json
from datetime import datetime, timezone
import sqlite3
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
    return sqlite3.connect(DB_PATH, timeout=30)