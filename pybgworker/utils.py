import uuid
import json
from datetime import datetime

def generate_id():
    return str(uuid.uuid4())

def now():
    return datetime.utcnow()

def dumps(obj):
    return json.dumps(obj)

def loads(data):
    return json.loads(data)
