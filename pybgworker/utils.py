import uuid
import json
from datetime import datetime, timezone

def generate_id():
    return str(uuid.uuid4())

def now():
    return datetime.now(timezone.utc)

def dumps(obj):
    return json.dumps(obj)

def loads(data):
    return json.loads(data)
