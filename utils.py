import uuid
import json
from datetime import datetime, timedelta

def generate_id() -> str:
    return str(uuid.uuid4())

def now():
    return datetime.utcnow()

def dumps(obj) -> str:
    return json.dumps(obj)

def loads(data: str):
    return json.loads(data)
