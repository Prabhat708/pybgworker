from enum import Enum

class TaskState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    FAILED = "failed"
    SUCCESS = "success"

ALLOWED_TRANSITIONS = {
    TaskState.QUEUED: {TaskState.RUNNING},
    TaskState.RUNNING: {TaskState.SUCCESS, TaskState.RETRYING, TaskState.FAILED},
    TaskState.RETRYING: {TaskState.RUNNING},
}

def validate_transition(old, new):
    if new not in ALLOWED_TRANSITIONS.get(TaskState(old), set()):
        raise ValueError(f"Invalid transition {old} → {new}")
